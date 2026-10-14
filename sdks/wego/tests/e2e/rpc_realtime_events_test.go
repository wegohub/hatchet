//go:build e2e

package e2e

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/backend"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware/payload"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// realtimeEventService 通过实际收到 DATA 的确认完成验收，不假设官方实时订阅具备就绪 ACK。
type realtimeEventService struct {
	// eventService 复用身份记录与标准 protobuf 注册。
	eventService
	// received 由真实订阅者关闭；重复发送只为建立消费屏障，不宣称完整历史回放。
	received chan struct{}
}

// sendUntilReceived 在订阅确实消费业务数据后结束，计时器只是重发间隔，不是成功判据。
func (s *realtimeEventService) sendUntilReceived(ctx context.Context, method string, send func(*pb.Reply) error) error {
	s.record(ctx, method)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := send(&pb.Reply{Message: "event", Count: 7}); err != nil {
			return err
		}
		select {
		case <-s.received:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// WatchHellos 使用正式实时输出，最终结果由引擎另行确认。
func (s *realtimeEventService) WatchHellos(_ *pb.Request, st grpc.ServerStreamingServer[pb.Reply]) error {
	return s.sendUntilReceived(st.Context(), pb.Greeter_WatchHellos_FullMethodName, st.Send)
}

// ChatHellos 先通过标准 Recv 消费输入数组，再验证 codec 后的实时响应。
func (s *realtimeEventService) ChatHellos(st grpc.BidiStreamingServer[pb.Request, pb.Reply]) error {
	for {
		if _, err := st.Recv(); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	return s.sendUntilReceived(st.Context(), pb.Greeter_ChatHellos_FullMethodName, st.Send)
}

// TestRPCRealtimeEvents 验证事件触发的 server 和 bidi 流仍走 PutStream 的完整 wego codec 帧。
func TestRPCRealtimeEvents(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, method := range []string{pb.Greeter_WatchHellos_FullMethodName, pb.Greeter_ChatHellos_FullMethodName} {
		service := &realtimeEventService{eventService: eventService{executions: make(chan eventExecution, 1)}, received: make(chan struct{})}
		h, err := scenarios.StartRegistered(ctx, "realtime_events", &scenarios.Report{}, func(r grpc.ServiceRegistrar) { pb.RegisterGreeterServer(r, service) }, streamNames(), []runtime.Option{runtime.WithStreamMode(method, runtime.Realtime), runtime.WithMiddleware(middleware.WithPayload(&payload.Gzip{}))})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := h.Close(); err != nil {
				t.Error(err)
			}
		})
		config := spec.Defaults()
		for _, option := range h.Runtime {
			option(&config)
		}
		observer, err := backend.New(config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := observer.Close(); err != nil {
				t.Error(err)
			}
		})
		// value 依据注册形态使用单消息或请求数组。
		var value any = &pb.Request{Count: 1}
		if method == pb.Greeter_ChatHellos_FullMethodName {
			value = []*pb.Request{{Count: 1}}
		}
		if err := h.Conn.Events().Push(ctx, method, value); err != nil {
			t.Fatal(err)
		}
		// execution 来自真正运行中的 handler，而不是发布结果。
		var execution eventExecution
		select {
		case execution = <-service.executions:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		codec, err := wire.NewFrameCodec(config, 1<<20, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		received := false
		err = observer.Stream(ctx, execution.runID, func(encoded string) error {
			data, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				return err
			}
			frame, err := codec.Decode(ctx, method, data)
			if err != nil {
				return err
			}
			if frame.Kind == "DATA" {
				// reply 来自实时订阅收到且通过 codec 验证的 DATA。
				var reply pb.Reply
				if err := protojson.Unmarshal(frame.Payload, &reply); err != nil {
					return err
				}
				if reply.Message != "event" || reply.Count != 7 {
					t.Fatal("realtime event data", &reply)
				}
				if !received {
					received = true
					close(service.received)
				}
			}
			if frame.Kind == "ATTEMPT_END" {
				return io.EOF
			}
			return nil
		})
		if !errors.Is(err, io.EOF) || !received {
			t.Fatal("missing realtime event output", err)
		}
		_, ref, err := observer.LookupRun(ctx, execution.runID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ref.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
}
