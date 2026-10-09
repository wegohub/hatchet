package backend

import (
	"context"
	"errors"
	"io"

	dispatcherpb "github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Stream 直接订阅正式实时事件协议，保留断流错误；EOF 不等于业务完成
// 正式版没有订阅就绪 ACK，调用方必须通过帧序号和最终结果发现丢帧
func (b *Backend) Stream(ctx context.Context, id string, fn func(string) error) error {
	if fn == nil {
		return status.Error(codes.InvalidArgument, "wego: realtime consumer cannot be nil")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := b.rpcDispatcher.SubscribeToWorkflowEvents(b.auth(ctx), &dispatcherpb.SubscribeToWorkflowEventsRequest{WorkflowRunId: &id})
	if err != nil {
		return Normalize(err)
	}
	for {
		event, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, io.EOF) {
				return status.Error(codes.Unavailable, "wego: realtime subscription ended without verified completion")
			}
			return Normalize(err)
		}
		if event.EventType != dispatcherpb.ResourceEventType_RESOURCE_EVENT_TYPE_STREAM {
			continue
		}
		if err := fn(event.EventPayload); err != nil {
			return err
		}
	}
}

// Publish 向真实 TaskRunID 的 PutStream 实时出口发布，不提供持久游标回放
func (b *Backend) Publish(ctx context.Context, id string, data []byte) error {
	return Normalize(b.raw.Event().PutStreamEvent(ctx, id, data))
}
