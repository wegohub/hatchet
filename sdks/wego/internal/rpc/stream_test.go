package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// streamFixture 记录真实适配器的冻结输入和持久日志；不模拟 grpc handler 本身
type streamFixture struct {
	// Backend 的未使用方法不能影响本测试的实际调用路径
	ports.Backend
	// mu 保护触发计数、持久帧和独立运行身份
	mu sync.Mutex
	// definition 使用生产代码生成的 StandaloneTask 定义
	definition ports.Definition
	// input 保存实际提交的 JSON，便于检查数组、routing 和独立快照
	input json.RawMessage
	// runID 和 taskID 有意不同，防止任务 topic 使用错误身份
	runID, taskID string
	// entries 按发布顺序保存完整编码帧
	entries []ports.DurableEntry
	// producers 记录重复物理发布的明确 producer/sequence
	producers map[string]bool
	// changed 唤醒所有订阅者重新检查持久前缀
	changed chan struct{}
	// result 保存 Function 的实际输出
	result ports.Result
	// err 是 Function 的实际业务错误
	err error
	// done 是结果上报屏障
	done chan struct{}
	// calls 记录实际触发次数，重复 CloseSend 不能增加它
	calls int
}

// Run 先经过正式 CLAIM 门禁，再运行生产的标准 gRPC 适配 handler
func (f *streamFixture) Run(ctx context.Context, _ string, input any, _ model.RunOptions) (ports.Run, error) {
	f.mu.Lock()
	f.calls++
	f.input, _ = json.Marshal(input)
	raw := append(json.RawMessage(nil), f.input...)
	f.mu.Unlock()
	go func() {
		// execution 的真实任务身份来自 fixture，两种身份始终不相同
		execution := &streamFixtureExecution{info: model.TaskInfo{RunID: f.runID, TaskRunID: f.taskID, WorkerKey: "fixture-worker"}}
		ctx := callctx.Bind(context.WithoutCancel(ctx), &callctx.State{Execution: execution})
		if f.definition.BeforeStart != nil {
			admission, err := f.definition.BeforeStart(ctx, ports.StartInfo{Task: execution.Info(), WorkerKey: execution.info.WorkerKey, Input: raw})
			if err != nil {
				f.err = err
				close(f.done)
				return
			}
			if !admission.Owned {
				f.err = status.Error(codes.Aborted, "fixture lost CLAIM")
				close(f.done)
				return
			}
			if admission.Context != nil {
				ctx = admission.Context
			}
		}
		fn := f.definition.Function.(func(context.Context, any) (any, error))
		out, err := fn(ctx, input)
		f.result = ports.Result{RunID: f.runID, Outputs: map[string]any{"task": out}}
		f.err = err
		close(f.done)
	}()
	return ports.Run{ID: f.runID, Wait: func(ctx context.Context) (ports.Result, error) {
		select {
		case <-ctx.Done():
			return ports.Result{}, ctx.Err()
		case <-f.done:
			return f.result, f.err
		}
	}}, nil
}

// LookupRun 返回与 RunID 不同的 task 身份，使用可观察的生产数据路径
func (f *streamFixture) LookupRun(context.Context, string) (string, ports.Run, error) {
	return f.taskID, ports.Run{}, nil
}

// PublishDurable 保存首次序号字节并唤醒消费，重复发布不覆盖规范内容
func (f *streamFixture) PublishDurable(ctx context.Context, message ports.DurableMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := message.Producer + ":" + strconv.FormatInt(message.Sequence, 10)
	if f.producers[key] {
		return nil
	}
	f.producers[key] = true
	f.entries = append(f.entries, ports.DurableEntry{Payload: append([]byte(nil), message.Payload...), Cursor: strconv.Itoa(len(f.entries) + 1)})
	close(f.changed)
	f.changed = make(chan struct{})
	return nil
}

// SubscribeDurable 按独立扫描位置读取持久日志，回调不持有 fixture 锁
func (f *streamFixture) SubscribeDurable(ctx context.Context, request ports.DurableSubscription, consume func(ports.DurableEntry) error) error {
	index := 0
	if request.Cursor != nil {
		index, _ = strconv.Atoi(*request.Cursor)
	}
	for {
		f.mu.Lock()
		if index < len(f.entries) {
			entry := f.entries[index]
			index++
			f.mu.Unlock()
			if err := consume(entry); err != nil {
				return err
			}
			continue
		}
		changed := f.changed
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Close 没有单独拥有的连接；订阅资源必须由客户端 context 回收
func (f *streamFixture) Close() error { return nil }

// streamFixtureExecution 只提供普通任务身份，其他 durable 能力不应被本场景使用
type streamFixtureExecution struct {
	// Execution 未使用的能力不伪造成功实现
	ports.Execution
	// info 供 handler 和可靠门禁读取独立任务身份
	info model.TaskInfo
}

// Info 返回 fixture 的普通任务身份
func (e *streamFixtureExecution) Info() model.TaskInfo { return e.info }

// streamFixtureService 使用生成接口实现正常输出和业务错误
type streamFixtureService struct {
	// UnimplementedGreeterServer 满足生成接口的未导出注册约束
	pb.UnimplementedGreeterServer
}

// UploadHellos 按 Recv EOF 收集请求；返回的文本可证明 Send 保存的是快照
func (s *streamFixtureService) UploadHellos(stream grpc.ClientStreamingServer[pb.Request, pb.Reply]) error {
	out := &pb.Reply{}
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(out)
		}
		if err != nil {
			return err
		}
		out.Count++
		out.Message += in.Message
	}
}

// WatchHellos 发布按序输出，Fail 请求在已经输出之后返回业务状态
func (s *streamFixtureService) WatchHellos(in *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	for i := int32(0); i < in.Count; i++ {
		if err := stream.Send(&pb.Reply{Count: i, Message: in.Message}); err != nil {
			return err
		}
	}
	if in.Fail {
		return status.Error(codes.Aborted, "fixture business failure")
	}
	return nil
}

// ChatHellos 从整批输入逐条 Recv 并 Send，EOF 只关闭输入
func (s *streamFixtureService) ChatHellos(stream grpc.BidiStreamingServer[pb.Request, pb.Reply]) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.Reply{Count: in.Count, Message: in.Message}); err != nil {
			return err
		}
	}
}

// fixtureStream 构造有 deadline 的生产客户端和 Worker 定义，不启动网络或任务服务
func fixtureStream(t *testing.T, name string, options ...grpc.CallOption) (*taskClientStream, *streamFixture, context.CancelFunc) {
	t.Helper()
	config := spec.Defaults()
	config.Telemetry.Trace.DisableWorkerExporter = true
	config.StreamMethods = map[string]spec.StreamOptions{"/wego.example.v1.Greeter/" + name: {PrefetchMessages: 1}}
	fixture := &streamFixture{runID: uuid.NewString(), taskID: uuid.NewString(), changed: make(chan struct{}), done: make(chan struct{}), producers: map[string]bool{}}
	e, err := engine.NewWithBackend(config, fixture)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	methods, err := binding.Methods(&pb.Greeter_ServiceDesc, &streamFixtureService{})
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range methods {
		if method.FullName != "/wego.example.v1.Greeter/"+name {
			continue
		}
		fixture.definition, err = StreamDefinition(e, method, spec.Task{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		raw, err := newTaskClient(ctx, e, method.Stream, method.FullName, func(error) {}, options...)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		return raw.(*taskClientStream), fixture, cancel
	}
	t.Fatal("missing generated stream method")
	return nil, nil, nil
}

// TestBufferedStreamSnapshotAndSingleSubmit 验证独立 Send 快照、批次数组、重复 CloseSend 和最终响应自动清理
func TestBufferedStreamSnapshotAndSingleSubmit(t *testing.T) {
	s, f, cancel := fixtureStream(t, "UploadHellos")
	defer cancel()
	request := &pb.Request{Message: "first"}
	if err := s.SendMsg(request); err != nil {
		t.Fatal(err)
	}
	request.Message = "second"
	if err := s.SendMsg(request); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	calls := f.calls
	f.mu.Unlock()
	if calls != 0 {
		t.Fatal("Send submitted before input EOF")
	}
	if err := s.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseSend(); err != nil {
		t.Fatal(err)
	}
	out := &pb.Reply{}
	if err := s.RecvMsg(out); err != nil {
		t.Fatal(err)
	}
	if out.Count != 2 || out.Message != "firstsecond" {
		t.Fatalf("snapshot changed: %+v", out)
	}
	if err := s.RecvMsg(out); err != io.EOF {
		t.Fatalf("repeated receive: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != 1 {
		t.Fatal("CloseSend triggered more than one task")
	}
	// input 的数组必须直接是 request 数组，不能变成 protobuf 二进制或内部控制任务
	if !json.Valid(f.input) {
		t.Fatal("invalid task JSON")
	}
}

// TestReliableOutputAuthorityAndPrefetch 验证真实 task topic、有界预取、多条输出和完整 EOF
func TestReliableOutputAuthorityAndPrefetch(t *testing.T) {
	for _, name := range []string{"WatchHellos", "ChatHellos"} {
		t.Run(name, func(t *testing.T) {
			s, _, cancel := fixtureStream(t, name)
			defer cancel()
			if name == "WatchHellos" {
				if err := s.SendMsg(&pb.Request{Count: 3}); err != nil {
					t.Fatal(err)
				}
			} else {
				for i := int32(0); i < 3; i++ {
					if err := s.SendMsg(&pb.Request{Count: i}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := s.CloseSend(); err != nil {
				t.Fatal(err)
			}
			for i := int32(0); i < 3; i++ {
				out := &pb.Reply{}
				if err := s.RecvMsg(out); err != nil || out.Count != i {
					t.Fatalf("output %d: %+v %v", i, out, err)
				}
			}
			if err := s.RecvMsg(&pb.Reply{}); err != io.EOF {
				t.Fatal(err)
			}
		})
	}
}

// TestInputCancellationCreatesNoRun 验证封包之前取消不会遗留任务，错误消息也不推进缓冲区
func TestInputCancellationCreatesNoRun(t *testing.T) {
	s, f, cancel := fixtureStream(t, "UploadHellos")
	if err := s.SendMsg((*pb.Request)(nil)); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	cancel()
	if err := s.CloseSend(); status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls != 0 {
		t.Fatal("cancelled buffer was submitted")
	}
}

// TestSendAfterCloseAndReceiveBeforeClose 明确 Worker 批次输入的半关闭约束
func TestSendAfterCloseAndReceiveBeforeClose(t *testing.T) {
	s, f, cancel := fixtureStream(t, "UploadHellos")
	defer cancel()
	// 先接收并不会提交空数组；另一个 goroutine 显式关闭输入后才交付响应
	received := make(chan error, 1)
	go func() { received <- s.RecvMsg(&pb.Reply{}) }()
	f.mu.Lock()
	if f.calls != 0 {
		t.Fatal("receive submitted partial input")
	}
	f.mu.Unlock()
	if err := s.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if err := s.SendMsg(&pb.Request{}); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	select {
	case err := <-received:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("receive did not finish after input EOF")
	}
}
