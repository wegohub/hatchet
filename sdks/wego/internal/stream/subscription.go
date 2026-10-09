package stream

import (
	"context"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

// SubscribeResumable 只重连传输故障，回调拒绝的帧不能被重试或跳过
// 游标仅在消费成功后推进，例如 DATA 解码失败仍从该 DATA 之前恢复
func SubscribeResumable(ctx context.Context, transport ports.DurableStreams, request ports.DurableSubscription, consume func(ports.DurableEntry) error) error {
	delay := 100 * time.Millisecond
	for ctx.Err() == nil {
		// callbackErr 保留业务停止屏障，不能将完成 sentinel 的 Unknown 误判为断网
		var callbackErr error
		err := transport.SubscribeDurable(ctx, request, func(entry ports.DurableEntry) error {
			if request.Cursor != nil && entry.Cursor != "" && entry.Cursor == *request.Cursor {
				return nil
			}
			if callbackErr = consume(entry); callbackErr != nil {
				return callbackErr
			}
			cursor := entry.Cursor
			request.Cursor = &cursor
			delay = 100 * time.Millisecond
			return nil
		})
		if callbackErr != nil {
			return callbackErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		code := status.Code(err)
		if err != nil && code != codes.Unavailable && code != codes.DeadlineExceeded && code != codes.Unknown {
			return err
		}
		// 空 hangup 也只表示连接结束；固定上限和抖动避免多个恢复扫描同步重连
		timer := time.NewTimer(delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, 2*time.Second)
	}
	return ctx.Err()
}
