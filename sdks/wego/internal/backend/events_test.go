package backend

import (
	"context"
	"io"
	"testing"

	dispatcherpb "github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// realtimeDispatcherFixture 在正式协议入口返回未带业务结束帧的 EOF
type realtimeDispatcherFixture struct {
	// DispatcherClient 的其余能力与此断流测试无关
	dispatcherpb.DispatcherClient
	// receiver 保存 backend 拥有的实际订阅 context
	receiver *realtimeReceiverFixture
}

// SubscribeToWorkflowEvents 使用正式生成签名，不提供虚构的订阅就绪 ACK
func (f *realtimeDispatcherFixture) SubscribeToWorkflowEvents(ctx context.Context, _ *dispatcherpb.SubscribeToWorkflowEventsRequest, _ ...grpc.CallOption) (dispatcherpb.Dispatcher_SubscribeToWorkflowEventsClient, error) {
	f.receiver.ctx = ctx
	return f.receiver, nil
}

// realtimeReceiverFixture 只需要证明 EOF 与成功业务结束不同
type realtimeReceiverFixture struct {
	// ClientStream 的其余通用方法不参与本次 Recv
	grpc.ClientStream
	// ctx 由 backend 派生，订阅返回后必须释放
	ctx context.Context
}

// Recv 立即返回 EOF，模拟对端正常关闭传输而业务尚未确认完成
func (*realtimeReceiverFixture) Recv() (*dispatcherpb.WorkflowEvent, error) { return nil, io.EOF }

// TestRealtimeEOFIsNotSuccess 禁止把底层订阅 EOF 当成 gRPC 业务 EOF
func TestRealtimeEOFIsNotSuccess(t *testing.T) {
	receiver := &realtimeReceiverFixture{}
	b := &Backend{rpcDispatcher: &realtimeDispatcherFixture{receiver: receiver}}
	err := b.Stream(context.Background(), "run", func(string) error { t.Fatal("unexpected message"); return nil })
	if status.Code(err) != codes.Unavailable || receiver.ctx.Err() != context.Canceled {
		t.Fatal(err, receiver.ctx.Err())
	}
}
