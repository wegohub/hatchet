package rpc

import (
	"io"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
)

// TestStreamLimitsNeverSubmitPrefix 第二条请求超限后整批失败，不能只执行第一条
func TestStreamLimitsNeverSubmitPrefix(t *testing.T) {
	s, fixture, cancel := fixtureStream(t, "UploadHellos", grpc.MaxCallSendMsgSize(8))
	defer cancel()
	if err := s.SendMsg(&pb.Request{Message: "ok"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SendMsg(&pb.Request{Message: "too large for this call"}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	if err := s.CloseSend(); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.calls != 0 || len(s.inputs) != 0 || len(s.plains) != 0 {
		t.Fatal("oversized input submitted partial prefix or retained payload")
	}
}

// TestEmptyInputArrayUsesBudget 空输入也占 [] 的两字节，不能绕过整批预算提交
func TestEmptyInputArrayUsesBudget(t *testing.T) {
	s, fixture, cancel := fixtureStream(t, "UploadHellos")
	defer cancel()
	s.options.MaxInputBytes = 1
	if err := s.CloseSend(); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.calls != 0 {
		t.Fatal("empty array bypassed input budget")
	}
}

// TestServerSendFailureIsSticky 发布前超限后，小消息也不能绕过首次错误并伪报完整成功
func TestServerSendFailureIsSticky(t *testing.T) {
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
		server := &taskServerStream{ctx: s.ctx, engine: s.engine, method: method, options: s.options}
		server.options.MaxMessageBytes = 1
		first := server.SendMsg(&pb.Reply{Message: "oversized"})
		if status.Code(first) != codes.ResourceExhausted || server.sendError != first {
			t.Fatal("send failure not retained", first)
		}
		if err := server.SendMsg(&pb.Reply{}); err != first {
			t.Fatal("second send bypassed earlier failure", err)
		}
		return
	}
	t.Fatal("generated method missing")
}

// TestStreamCallLimitsAreDirectional 发送 5 字节不限制 7 字节响应；接收上限独立拒绝大响应
func TestStreamCallLimitsAreDirectional(t *testing.T) {
	for _, limit := range []int{100, 3} {
		s, _, cancel := fixtureStream(t, "UploadHellos", grpc.MaxCallSendMsgSize(5), grpc.MaxCallRecvMsgSize(limit))
		if err := s.SendMsg(&pb.Request{Message: "abc"}); err != nil {
			t.Fatal(err)
		}
		if err := s.CloseSend(); err != nil {
			t.Fatal(err)
		}
		err := s.RecvMsg(&pb.Reply{})
		if limit == 100 && err != nil || limit == 3 && status.Code(err) != codes.ResourceExhausted {
			t.Fatal("cross-direction message limit", limit, err)
		}
		cancel()
	}
}

// TestConcurrentCloseSendCreatesOneRun 所有 EOF 调用共享一次提交，随后仍能正常读出业务结果
func TestConcurrentCloseSendCreatesOneRun(t *testing.T) {
	s, fixture, cancel := fixtureStream(t, "UploadHellos")
	defer cancel()
	if err := s.SendMsg(&pb.Request{Message: "single"}); err != nil {
		t.Fatal(err)
	}
	// group 确认所有并发 EOF 返回，再读取唯一提交结果
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := s.CloseSend(); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	if err := s.RecvMsg(&pb.Reply{}); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.calls != 1 {
		t.Fatal("concurrent EOF duplicated run", fixture.calls)
	}
}

// TestCheckpointDoesNotAdvanceWithPrefetch 预取三条输出时 checkpoint 仍从零开始，只有成功 Recv 后推进
func TestCheckpointDoesNotAdvanceWithPrefetch(t *testing.T) {
	s, _, cancel := fixtureStream(t, "WatchHellos")
	defer cancel()
	if err := s.SendMsg(&pb.Request{Count: 3}); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Header(); err != nil {
		t.Fatal(err)
	}
	// 通过队列条件屏障确认实际发生预取，不用固定 sleep 推测订阅进度
	for {
		s.mu.Lock()
		ready, changed := len(s.queue) > 0, s.changed
		s.mu.Unlock()
		if ready {
			break
		}
		select {
		case <-changed:
		case <-s.ctx.Done():
			t.Fatal(s.ctx.Err())
		}
	}
	cp, err := s.checkpoint()
	if err != nil || cp.OutputSeq != 0 || cp.Cursor != "" || !cp.HeadersSeen {
		t.Fatal("prefetch advanced checkpoint", cp, err)
	}
	for i := uint64(1); i <= 3; i++ {
		if err := s.RecvMsg(&pb.Reply{}); err != nil {
			t.Fatal(err)
		}
		cp, err = s.checkpoint()
		if err != nil || cp.OutputSeq != i || cp.Cursor == "" {
			t.Fatal("delivery checkpoint mismatch", cp, err)
		}
	}
	if err := s.RecvMsg(&pb.Reply{}); err != io.EOF {
		t.Fatal(err)
	}
}
