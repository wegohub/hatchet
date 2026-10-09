package backend

import (
	"context"
	"errors"
	"testing"
	"time"

	dispatcherpb "github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts"
	"google.golang.org/grpc"
)

// blockedResultDispatcher 模拟结果订阅发送因连接流控而阻塞
type blockedResultDispatcher struct {
	// DispatcherClient 委托本用例不使用的协议方法
	dispatcherpb.DispatcherClient
	// entered 确认取消发生在真实 Send 阶段
	entered chan struct{}
	// exited 确认发送已退出，不能遗留等待 goroutine
	exited chan struct{}
}

// SubscribeToWorkflowRuns 为每次等待建立独立的请求预算
func (d *blockedResultDispatcher) SubscribeToWorkflowRuns(ctx context.Context, _ ...grpc.CallOption) (dispatcherpb.Dispatcher_SubscribeToWorkflowRunsClient, error) {
	return &blockedResultStream{ctx: ctx, entered: d.entered, exited: d.exited}, nil
}

// blockedResultStream 发送等待预算取消；不同等待不共享取消信号
type blockedResultStream struct {
	// ClientStream 委托本测试不使用的 Header/Trailer 等方法
	grpc.ClientStream
	// ctx 当前订阅的请求预算
	ctx context.Context
	// entered 通知发送已阻塞
	entered chan struct{}
	// exited 确认发送退出
	exited chan struct{}
}

// Context 返回独立订阅预算，后端不能将它替换成 Background
func (s *blockedResultStream) Context() context.Context { return s.ctx }

// CloseSend 无附加资源，预算由创建方取消
func (s *blockedResultStream) CloseSend() error { return nil }

// Send 持续阻塞到请求取消，模拟流控耗尽时等待被取消的情况
func (s *blockedResultStream) Send(*dispatcherpb.SubscribeToWorkflowRunsRequest) error {
	close(s.entered)
	defer close(s.exited)
	<-s.ctx.Done()
	return s.ctx.Err()
}

// Recv 不交付结果，用于覆盖发送尚未完成的取消分支
func (s *blockedResultStream) Recv() (*dispatcherpb.WorkflowRunEvent, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

// TestResultCancellationWhileSubscriptionSendBlocked 确认发送阶段取消贯通，并回收发送者
func TestResultCancellationWhileSubscriptionSendBlocked(t *testing.T) {
	// d 用两个屏障区分发送已进入与发送已退出，断言不依赖调度速度
	d := &blockedResultDispatcher{entered: make(chan struct{}), exited: make(chan struct{})}
	// b 只需要结果协议，不创建真实引擎或共享监听器
	b := &Backend{rpcDispatcher: d}
	// ctx/cancel 限制当前结果等待，不影响其他运行
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// done 返回结果等待错误，缓冲允许取消后发送者退出
	done := make(chan error, 1)
	go func() {
		// err 是发送取消后的等待结果，应保留 context.Canceled
		_, err := b.runRef("run-one").Wait(ctx)
		done <- err
	}()
	select {
	case <-d.entered:
	case <-time.After(time.Second):
		t.Fatal("subscription did not enter Send")
	}
	cancel()
	select {
	// err 在发送退出确认后返回；nil 或其他状态不能算取消成功
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
		select {
		case <-d.exited:
		default:
			t.Fatal("Result returned before Send exited")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not interrupt subscription Send")
	}
}
