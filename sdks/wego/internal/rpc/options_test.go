package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// TestCallOptionsLimitsAndSubmissionScope 校验重复大小最后生效、空消息零上限及恢复禁止重新调度。
func TestCallOptionsLimitsAndSubmissionScope(t *testing.T) {
	parsed, err := ParseCallOptions([]grpc.CallOption{grpc.MaxCallSendMsgSize(10), grpc.MaxCallSendMsgSize(0), grpc.MaxCallRecvMsgSize(2), grpc.MaxCallRecvMsgSize(5), NewRunCallOption(spec.WithRouting(map[string]any{"group": "one"}))}, false)
	if err != nil || parsed.MaxSend != 0 || parsed.MaxRecv != 5 || parsed.Run.Routing["group"] != "one" {
		t.Fatal(parsed, err)
	}
	for _, option := range []grpc.CallOption{grpc.MaxCallSendMsgSize(-1), grpc.MaxCallRecvMsgSize(-1)} {
		if _, err := ParseCallOptions([]grpc.CallOption{option}, false); status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
	}
	if _, err := ParseCallOptions([]grpc.CallOption{grpc.WaitForReady(true)}, false); status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}
	if _, err := ParseCallOptions([]grpc.CallOption{NewRunCallOption(spec.WithRouting(map[string]any{"group": "one"}))}, true); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if _, err := ParseCallOptions([]grpc.CallOption{grpc.MaxCallSendMsgSize(0)}, true); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
}

// TestStreamInterceptorFailureReleasesTransport 外层创建真实流再失败，不遗留订阅或实例登记。
func TestStreamInterceptorFailureReleasesTransport(t *testing.T) {
	seed, _, cancel := fixtureStream(t, "UploadHellos")
	defer cancel()
	seed.finish(nil)
	expected := errors.New("fixture rejects created transport")
	seed.engine.Config.Middleware = append(seed.engine.Config.Middleware, middleware.WithStreamClient(func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, next grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		_, err := next(ctx, desc, conn, method, opts...)
		if err != nil {
			return nil, err
		}
		return nil, expected
	}))
	completed := make(chan error, 1)
	_, err := NewStream(context.Background(), seed.engine, &grpc.StreamDesc{ClientStreams: true}, pb.Greeter_UploadHellos_FullMethodName, grpc.OnFinish(func(err error) { completed <- err }))
	if !errors.Is(err, expected) {
		t.Fatal(err)
	}
	select {
	case finishErr := <-completed:
		if !errors.Is(finishErr, expected) {
			t.Fatal(finishErr)
		}
	case <-time.After(time.Second):
		t.Fatal("OnFinish not called")
	}
	budget, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := seed.engine.Shutdown(budget); err != nil {
		t.Fatal("transport registration leaked", err)
	}
}

// TestFinishCallbackCanReenterStream 完成通知不持有状态锁或 sync.Once，回调可安全查询终态。
func TestFinishCallbackCanReenterStream(t *testing.T) {
	s, _, cancel := fixtureStream(t, "UploadHellos")
	defer cancel()
	s.complete = func(error) { s.finish(nil) }
	completed := make(chan struct{})
	go func() { s.finish(nil); close(completed) }()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("completion callback reentrant deadlock")
	}
}
