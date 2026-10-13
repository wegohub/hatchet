package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/examples/codec/payload"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// reviewNativeExecution 只记录官方实时输出，结束后的 Send 不应调用它。
type reviewNativeExecution struct {
	// Execution 禁止测试意外调用其他执行能力。
	ports.Execution
	// writes 由同一个测试调用方串行读取与修改。
	writes int
}

// Stream 模拟已经就绪的原生出口，避免用无连接错误掩盖迟到发布。
func (e *reviewNativeExecution) Stream(context.Context, []byte) error { e.writes++; return nil }

// reviewDecodeObserver 在解码完成与入队之间插入关闭，固定复现取消竞态。
type reviewDecodeObserver struct {
	// close 只在成功解码指标上触发，不干扰帧的准备编码。
	close func()
}

// ObserveProtocol 模拟解码刚结束时另一协程完成流关闭。
func (o *reviewDecodeObserver) ObserveProtocol(metric ports.ProtocolMetric) {
	if metric.Name == "wego_frame_decode_total" && metric.Outcome == "success" {
		o.close()
	}
}

// reviewDecodeCounter 借用真实 gzip，只统计业务解码是否早于执行 deadline 检查。
type reviewDecodeCounter struct {
	// Payload 提供实际编码与格式标识。
	middleware.Payload
	// calls 由同一测试 goroutine 在调用前后检查。
	calls int
}

// DecodeLimit 记录真实组件调用；过期输入必须在到达这里前失败。
func (c *reviewDecodeCounter) DecodeLimit(ctx context.Context, data []byte, limit int) ([]byte, error) {
	c.calls++
	return c.Payload.DecodeLimit(ctx, data, limit)
}

// TestReviewV6RPCDeadlineBeforeCodec 覆盖 unary 与三种流的执行入口，历史读取不重用执行 deadline。
func TestReviewV6RPCDeadlineBeforeCodec(t *testing.T) {
	s, _, cancel := fixtureStream(t, "UploadHellos")
	defer cancel()
	codec := &reviewDecodeCounter{Payload: &payload.Gzip{}}
	s.engine.Config.Middleware = []middleware.Option{middleware.WithPayload(codec)}
	if err := s.engine.Config.Validate(); err != nil {
		t.Fatal(err)
	}
	methods, err := binding.Methods(&pb.Greeter_ServiceDesc, &streamFixtureService{})
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range methods {
		// definition 复用正式入口，明确检查过期输入不会到达业务或组件。
		var definition ports.Definition
		if method.Stream == nil {
			definition, err = UnaryDefinition(context.Background(), s.engine, method, spec.Task{})
		} else {
			definition, err = StreamDefinition(s.engine, method, spec.Task{}, nil)
		}
		if err != nil {
			t.Fatal(err)
		}
		input := wire.NewEnvelope(context.Background(), method.FullName, "", json.RawMessage(`{}`), nil)
		input.IdempotencyKey = "expired-operation"
		input.InputDigest = "owned-input"
		input.Deadline = time.Now().Add(-time.Hour).UnixNano()
		input, err = wire.Seal(context.Background(), input, s.engine.Config, 1024, 2048, 4096)
		if err != nil {
			t.Fatal(err)
		}
		function := definition.Function.(func(context.Context, any) (any, error))
		if _, err := function(context.Background(), input); !errors.Is(err, context.DeadlineExceeded) || codec.calls != 0 {
			t.Fatalf("%s: error=%v external decode calls=%d", method.FullName, err, codec.calls)
		}
	}
}

// TestReviewV6ServerTerminalBarrier 终态形成后，业务遗留 goroutine 不得追加 chunk 或响应头。
func TestReviewV6ServerTerminalBarrier(t *testing.T) {
	s, _, cancel := fixtureStream(t, "WatchHellos")
	defer cancel()
	methods, err := binding.Methods(&pb.Greeter_ServiceDesc, &streamFixtureService{})
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range methods {
		if method.FullName != s.method {
			continue
		}
		execution := &reviewNativeExecution{}
		ctx := callctx.Bind(wire.WithResponse(context.Background(), false), &callctx.State{Execution: execution})
		server := &taskServerStream{ctx: ctx, engine: s.engine, method: method, options: s.options, native: true}
		if _, err := server.finalOutput(nil); err != nil {
			t.Fatal(err)
		}
		if err := server.SendMsg(&pb.Reply{Message: "late"}); err == nil || execution.writes != 0 {
			t.Fatalf("late output: error=%v writes=%d", err, execution.writes)
		}
		if err := server.SetHeader(metadata.Pairs("late", "header")); err == nil {
			t.Fatal("late headers accepted")
		}
		if err := server.SendHeader(metadata.Pairs("late", "header")); err == nil {
			t.Fatal("late SendHeader accepted")
		}
		server.SetTrailer(metadata.Pairs("late", "trailer"))
		if len(server.trailers) != 0 {
			t.Fatal("terminal trailers changed")
		}
	}
}

// TestReviewV6ClosedClientRejectsLateFrame 取消后的订阅回调不能重新填充已释放队列或推进日志状态。
func TestReviewV6ClosedClientRejectsLateFrame(t *testing.T) {
	s, fixture, cancel := fixtureStream(t, "WatchHellos")
	defer cancel()
	s.mu.Lock()
	s.state.TaskID = fixture.taskID
	s.state.RunID = fixture.runID
	s.state.Method = s.method
	s.state.InputDigest = "digest"
	s.mu.Unlock()
	frame := &wire.LogFrame{Version: wire.LogVersion, Kind: "CLAIM", TaskRunId: fixture.taskID, RunId: fixture.runID, Method: s.method, InputDigest: "digest", Writer: "writer", WorkerKey: "worker", FrameId: "frame"}
	data, err := s.codec.Encode(context.Background(), s.method, frame)
	if err != nil {
		t.Fatal(err)
	}
	s.codec, err = wire.NewFrameCodec(s.engine.Config, s.options.MaxFrameBytes, s.options.MaxEncodedMessageBytes, &reviewDecodeObserver{close: func() {
		s.finish(status.Error(codes.Canceled, "closed"))
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.accept(ports.DurableEntry{Cursor: "1", Payload: data}); status.Code(err) != codes.Canceled {
		t.Fatalf("late callback must observe cancellation: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.Claimed || len(s.queue) != 0 {
		t.Fatal("closed stream state changed")
	}
}

// TestReviewV6UnaryOutputBinding 拦截器返回字段兼容的其他 protobuf 类型时也必须拒绝。
func TestReviewV6UnaryOutputBinding(t *testing.T) {
	s, _, cancel := fixtureStream(t, "UploadHellos")
	defer cancel()
	s.engine.Config.Middleware = []middleware.Option{middleware.WithUnaryServer(func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		// Request 与 Reply 都有 message 字段；JSON 字段匹配不能替代注册类型。
		return &pb.Request{Message: "wrong response type"}, nil
	})}
	methods, err := binding.Methods(&pb.Greeter_ServiceDesc, &streamFixtureService{})
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range methods {
		if method.FullName != pb.Greeter_SayHello_FullMethodName {
			continue
		}
		definition, err := UnaryDefinition(context.Background(), s.engine, method, spec.Task{})
		if err != nil {
			t.Fatal(err)
		}
		function := definition.Function.(func(context.Context, any) (any, error))
		if _, err := function(context.Background(), json.RawMessage(`{"message":"hello"}`)); status.Code(err) != codes.Internal {
			t.Fatalf("wrong response type: %v", err)
		}
	}
}
