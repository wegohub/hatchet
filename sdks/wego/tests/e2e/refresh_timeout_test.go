//go:build e2e

package e2e

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// TestRefreshTimeoutUsesFixedIncrement 验证真实引擎允许超过初始预算，且每次续期使用指定增量。
func TestRefreshTimeoutUsesFixedIncrement(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	service := &scenarios.Service{
		// Say 在任务入口执行约 2 秒；初始预算仅 1 秒，必须依靠多次续期完成。
		Say: func(ctx context.Context, req *pb.Request) (*pb.Reply, error) {
			return task.WithRefreshTimeout(ctx, 200*time.Millisecond, func(ctx context.Context) (*pb.Reply, error) {
				timer := time.NewTimer(2200 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
					return scenarios.Reply(ctx, req), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			})
		},
	}
	h, err := scenarios.Start(ctx, "fixed-refresh", &scenarios.Report{}, service, nil,
		worker.WithTask(pb.UnaryGreeter_SayHello_FullMethodName,
			task.WithExecutionTimeout(time.Second),
			task.WithRetries(0),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	}()

	result, err := h.RPC.SayHello(ctx, &pb.Request{Message: "refresh"})
	if err != nil || result == nil || result.Message != "refresh" || result.RunId == "" {
		t.Fatalf("refreshed run: result=%v, err=%v", result, err)
	}
}

// refreshStreams 将三种标准流 handler 交给同一续期入口执行。
type refreshStreams struct {
	// 未覆盖的方法沿用生成桩的未实现响应。
	pb.UnimplementedGreeterServer
}

// UploadHellos 验证 client stream 的输入 EOF 与最终单条响应。
func (refreshStreams) UploadHellos(stream grpc.ClientStreamingServer[pb.Request, pb.Reply]) error {
	return task.WithRefreshStreamTimeout(stream.Context(), 200*time.Millisecond, func(ctx context.Context) error {
		// count 记录已交付的请求数；两条输入应产生 Count=2。
		var count int32
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			_, err := stream.Recv()
			if err == io.EOF {
				return stream.SendAndClose(&pb.Reply{Count: count})
			}
			if err != nil {
				return err
			}
			count++
		}
	})
}

// WatchHellos 验证输出流可以在多次续期后超过初始执行预算。
func (refreshStreams) WatchHellos(in *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	return task.WithRefreshStreamTimeout(stream.Context(), 200*time.Millisecond, func(ctx context.Context) error {
		timer := time.NewTimer(1400 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
		for i := int32(0); i < in.Count; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := stream.Send(&pb.Reply{Count: i}); err != nil {
				return err
			}
		}
		return nil
	})
}

// ChatHellos 验证双向流在输入 EOF 后仍完整交付已经产生的输出。
func (refreshStreams) ChatHellos(stream grpc.BidiStreamingServer[pb.Request, pb.Reply]) error {
	return task.WithRefreshStreamTimeout(stream.Context(), 200*time.Millisecond, func(ctx context.Context) error {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			in, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err := stream.Send(&pb.Reply{Count: in.Count}); err != nil {
				return err
			}
		}
	})
}

// TestRefreshStreamTimeoutGRPCShapes 验证三种流的真实调度、续期和结果交付。
func TestRefreshStreamTimeoutGRPCShapes(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	h, err := scenarios.StartRegistered(ctx, "stream-refresh", &scenarios.Report{}, func(registrar grpc.ServiceRegistrar) {
		pb.RegisterGreeterServer(registrar, refreshStreams{})
	}, streamNames(), nil, worker.WithTask(pb.Greeter_WatchHellos_FullMethodName,
		task.WithExecutionTimeout(time.Second), task.WithRetries(0)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	}()

	rpc := pb.NewGreeterClient(h.Conn)
	upload, err := rpc.UploadHellos(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := upload.Send(&pb.Request{}); err != nil {
			t.Fatal(err)
		}
	}
	if reply, err := upload.CloseAndRecv(); err != nil || reply.Count != 2 {
		t.Fatalf("client stream: reply=%v, err=%v", reply, err)
	}

	watch, err := rpc.WatchHellos(ctx, &pb.Request{Count: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 2; i++ {
		reply, err := watch.Recv()
		if err != nil || reply.Count != i {
			t.Fatalf("server stream: reply=%v, err=%v", reply, err)
		}
	}
	if _, err := watch.Recv(); err != io.EOF {
		t.Fatalf("server stream EOF: %v", err)
	}

	chat, err := rpc.ChatHellos(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 2; i++ {
		if err := chat.Send(&pb.Request{Count: i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := chat.CloseSend(); err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 2; i++ {
		reply, err := chat.Recv()
		if err != nil || reply.Count != i {
			t.Fatalf("bidi stream: reply=%v, err=%v", reply, err)
		}
	}
	if _, err := chat.Recv(); err != io.EOF {
		t.Fatalf("bidi stream EOF: %v", err)
	}
}
