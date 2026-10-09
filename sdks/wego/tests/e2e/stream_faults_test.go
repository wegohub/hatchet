//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	coreclient "github.com/hatchet-dev/hatchet/sdks/wego/internal/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// faultService 可阻塞的流 handler，供乱序、断流与 owner 退出故障注入
type faultService struct {
	// pb.UnimplementedGreeterServer 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	pb.UnimplementedGreeterServer
	// started 入口是否已经启动；与停止并发时在锁内读写
	started atomic.Int32
	// gate 故障测试的阻塞门，关闭后允许 handler 继续
	gate <-chan struct{}
}

// UploadHellos 接收多条 Request，收到输入 EOF 后返回最终 Reply，例如两条输入得到 Count=2
func (s *faultService) UploadHellos(stream grpc.ClientStreamingServer[pb.Request, pb.Reply]) error {
	s.started.Add(1)
	if s.gate != nil {
		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		case <-s.gate:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	// count 此步骤已经观察到的执行或消息次数，后续与预期重试数、输入数或峰值比较
	count := int32(0)
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		// in, err 接收 stream.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		in, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(scenarios.Reply(stream.Context(), &pb.Request{Count: count}))
		}
		if err != nil {
			return err
		}
		// 检查 in.Count != count；不满足协议或配置约束时返回 DataLoss（handler input order mismatch）
		if in.Count != count {
			return status.Error(codes.DataLoss, "handler input order mismatch")
		}

		count++
	}
}

// WatchHellos 处理单条 Request 并连续发送 Reply，例如 Count=3 时发送三条输出
func (s *faultService) WatchHellos(in *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	s.started.Add(1)
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
	for i := int32(0); i < in.Count; i++ {
		// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
		if err := stream.Send(scenarios.Reply(stream.Context(), &pb.Request{Count: i})); err != nil {
			return err
		}
	}
	return nil
}

// ChatHellos 边接收 Request 边发送 Reply，输入 EOF 只结束输入方向
func (s *faultService) ChatHellos(stream grpc.BidiStreamingServer[pb.Request, pb.Reply]) error {
	s.started.Add(1)
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		// in, err 接收 stream.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = stream.Send(scenarios.Reply(stream.Context(), in)); err != nil {
			return err
		}
	}
}

// faultBackend 只改变测试客户端接收路径；真实 Worker、任务调度和输出存储保持正常
type faultBackend struct {
	// Backend 是当前 Engine 拥有的实际后端，不建立额外控制 Worker
	ports.Backend
	// mode 选择明确的传输故障
	mode string
	// codec 与测试 Server 使用相同的整帧格式
	codec *wire.FrameCodec
	// authority 在实际权威结果返回后关闭，延迟订阅不依赖固定 sleep
	authority chan struct{}
	// complete 保证观察结果屏障只关闭一次
	complete sync.Once
	// reconnect 保证只注入一次短暂断流
	reconnect atomic.Bool
	// calls 计数实际任务触发，不包含持久流发布
	calls atomic.Int32
	// mu 保护运行证据列表
	mu sync.Mutex
	// runIDs 保存已确认的真实运行身份
	runIDs []string
}

// Run 记录实际提交并通过正式 Wait 建立最终结果屏障
func (b *faultBackend) Run(ctx context.Context, name string, input any, options model.RunOptions) (ports.Run, error) {
	b.calls.Add(1)
	ref, err := b.Backend.Run(ctx, name, input, options)
	if err != nil {
		return ref, err
	}
	b.mu.Lock()
	b.runIDs = append(b.runIDs, ref.ID)
	b.mu.Unlock()
	wait := ref.Wait
	ref.Wait = func(ctx context.Context) (ports.Result, error) {
		result, err := wait(ctx)
		b.complete.Do(func() { close(b.authority) })
		return result, err
	}
	return ref, nil
}

// LookupRun 转发真实任务身份，不能用可读任务名构建 topic
func (b *faultBackend) LookupRun(ctx context.Context, id string) (string, ports.Run, error) {
	return b.Backend.(ports.RunLookup).LookupRun(ctx, id)
}

// RunInput 保留可靠幂等冲突恢复的输入核对能力
func (b *faultBackend) RunInput(ctx context.Context, id string) (json.RawMessage, error) {
	return b.Backend.(ports.RunInputReader).RunInput(ctx, id)
}

// PublishDurable 不改变测试发布侧，producer 去重仍由真实服务端执行
func (b *faultBackend) PublishDurable(ctx context.Context, message ports.DurableMessage) error {
	return b.Backend.(ports.DurableStreams).PublishDurable(ctx, message)
}

// SubscribeDurable 在实际存储帧之上注入故障，重新订阅仍使用实际持久游标
func (b *faultBackend) SubscribeDurable(ctx context.Context, request ports.DurableSubscription, consume func(ports.DurableEntry) error) error {
	if b.mode == "none" {
		return b.Backend.(ports.DurableStreams).SubscribeDurable(ctx, request, consume)
	}
	if b.mode == "delayed_subscription" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.authority:
		}
	}
	return b.Backend.(ports.DurableStreams).SubscribeDurable(ctx, request, func(entry ports.DurableEntry) error {
		frame, err := b.codec.Decode(ctx, "/wego.example.v1.Greeter/WatchHellos", entry.Payload)
		if err != nil {
			return err
		}
		if b.mode == "missing_headers" && frame.Kind == "HEADERS" || b.mode == "out_of_order" && frame.Kind == "DATA" && frame.OutputSeq == 1 {
			return nil
		}
		if b.mode == "wrong_identity" && frame.Kind == "DATA" {
			frame.TaskRunId = "different-task"
			entry.Payload, err = b.codec.Encode(ctx, frame.Method, frame)
			if err != nil {
				return err
			}
		}
		if b.mode == "missing_output" && frame.Kind == "DATA" && frame.OutputSeq == 2 {
			return nil
		}
		if b.mode == "missing_end" && frame.Kind == "ATTEMPT_END" {
			return nil
		}
		if b.mode == "corrupt_frame" && frame.Kind == "DATA" {
			entry.Payload = []byte{0xff}
		}
		if err := consume(entry); err != nil {
			return err
		}
		if b.mode == "duplicate_cursor" && frame.Kind == "DATA" {
			return consume(entry)
		}
		if b.mode == "reconnect" && frame.Kind == "DATA" && b.reconnect.CompareAndSwap(false, true) {
			return status.Error(codes.Unavailable, "injected temporary subscription loss")
		}
		return nil
	})
}

// streamNames 返回全部实际注册的 RPC，每个方法对应一个独立 workflow
func streamNames() []string {
	names := []string{}
	for _, method := range pb.Greeter_ServiceDesc.Methods {
		names = append(names, "/wego.example.v1.Greeter/"+method.MethodName)
	}
	for _, method := range pb.Greeter_ServiceDesc.Streams {
		names = append(names, "/wego.example.v1.Greeter/"+method.StreamName)
	}
	return names
}

// faultConn 使用实例连接创建测试传输代理，API 边界仍全部由 wego 自有端口组成
func faultConn(t *testing.T, namespace, mode string) (*coreclient.Conn, *faultBackend) {
	t.Helper()
	config := spec.Defaults()
	for _, option := range scenarios.Runtime(namespace) {
		option(&config)
	}
	runtime.WithStreamOptions(pb.Greeter_WatchHellos_FullMethodName, runtime.StreamOptions{CompletionTimeout: 300 * time.Millisecond})(&config)
	e, err := engine.New(config)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := wire.NewFrameCodec(config.Middleware, 4<<20, 4<<20)
	if err != nil {
		_ = e.Close()
		t.Fatal(err)
	}
	proxy := &faultBackend{Backend: e.Backend, mode: mode, codec: codec, authority: make(chan struct{})}
	e.Backend = proxy
	conn := coreclient.FromEngine(e, false)
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	return conn, proxy
}

// TestStreamFaults 使用实际引擎验证延迟订阅、同游标重复、重连和明确的输出缺失失败
func TestStreamFaults(t *testing.T) {
	preflight(t)
	results := map[string]string{}
	records := []map[string]any{}
	for _, mode := range []string{"delayed_subscription", "duplicate_cursor", "reconnect", "missing_output", "missing_end", "corrupt_frame", "out_of_order", "wrong_identity", "zero_output", "missing_headers"} {
		executed := false
		passed := t.Run(mode, func(t *testing.T) {
			executed = true
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			harness, err := scenarios.StartRegistered(ctx, "stream-fault", &scenarios.Report{}, func(registrar grpc.ServiceRegistrar) { pb.RegisterGreeterServer(registrar, &faultService{}) }, streamNames(), nil)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			defer func() {
				if !closed {
					if err := harness.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			conn, proxy := faultConn(t, harness.Namespace, mode)
			count := int32(3)
			if mode == "zero_output" {
				count = 0
			}
			watch, err := pb.NewGreeterClient(conn).WatchHellos(ctx, &pb.Request{Count: count})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "missing_headers" {
				if _, err := watch.Header(); status.Code(err) != codes.DataLoss {
					t.Fatalf("missing headers were not bounded: %v", err)
				}
			}
			received := 0
			for {
				reply, next := watch.Recv()
				if next != nil {
					err = next
					break
				}
				if reply.Count != int32(received) {
					t.Fatalf("output order: %+v", reply)
				}
				received++
			}
			expected := codes.OK
			if mode == "missing_output" || mode == "missing_end" || mode == "corrupt_frame" || mode == "out_of_order" || mode == "wrong_identity" || mode == "missing_headers" {
				expected = codes.DataLoss
			}
			if mode == "missing_end" {
				expected = codes.Unavailable
			}
			if expected == codes.OK {
				if err != io.EOF || received != int(count) {
					t.Fatalf("complete output: messages=%d error=%v", received, err)
				}
			} else if status.Code(err) != expected {
				t.Fatalf("gap or corrupt output was not rejected: %v", err)
			}
			if proxy.calls.Load() != 1 {
				t.Fatalf("fault restarted task: %d", proxy.calls.Load())
			}
			proxy.mu.Lock()
			ids := append([]string(nil), proxy.runIDs...)
			proxy.mu.Unlock()
			info, err := conn.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := harness.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			records = append(records, map[string]any{"mode": mode, "run_ids": ids, "messages": received, "terminal_code": expected.String(), "submitted_tasks": 1, "server_version": info.Version, "namespace": harness.Namespace, "cleanup": "worker stopped; definitions deleted; run and topic history retained"})
		})
		if !executed {
			results[mode] = "NOT_RUN"
		} else if passed {
			results[mode] = "PASSED"
		} else {
			results[mode] = "FAILED"
		}
	}
	if !t.Failed() {
		writeStreamEvidence(t, "stream-faults", map[string]any{"results": results, "records": records, "cleanup": "all scenario workers and connections closed; definitions deleted; run and topic history retained"})
	}
}
