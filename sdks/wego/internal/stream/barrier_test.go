package stream

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// barrierFixture 用确定性历史与阻塞模拟屏障预算，不模拟引擎执行终态
type barrierFixture struct {
	// mu 保护已经发布的字节快照
	mu sync.Mutex
	// saved 是唯一一次发布后的独立字节，用于核对重试不重新编码
	saved []byte
	// mode 选择超时或历史超限，不包含外部租户名称
	mode string
	// historical 为重复扫描使用的不可变有效历史帧
	historical []byte
}

// PublishDurable 保存已编码的 JOIN；fixture 不对字节作任何变换
func (f *barrierFixture) PublishDurable(ctx context.Context, message ports.DurableMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saved = append([]byte(nil), message.Payload...)
	return nil
}

// SubscribeDurable 按确定性模式等待预算或达到扫描上限，成功模式返回保存的真实屏障
func (f *barrierFixture) SubscribeDurable(ctx context.Context, _ ports.DurableSubscription, consume func(ports.DurableEntry) error) error {
	if f.mode == "timeout" {
		<-ctx.Done()
		return ctx.Err()
	}
	if f.mode == "history_limit" {
		for i := 0; i <= 100000; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := consume(ports.DurableEntry{Cursor: strconv.Itoa(i), Payload: f.historical}); err != nil {
				return err
			}
		}
	}
	f.mu.Lock()
	data := append([]byte(nil), f.saved...)
	f.mu.Unlock()
	return consume(ports.DurableEntry{Cursor: "barrier", Payload: data})
}

// TestJoinBudgetsAndReadback 验证空 Publish 响应不能代替游标，超时和扫描超限均明确失败
func TestJoinBudgetsAndReadback(t *testing.T) {
	codec, err := wire.NewFrameCodec(nil, 4096, 8192)
	if err != nil {
		t.Fatal(err)
	}
	history, err := codec.Encode(context.Background(), EventMethod, &wire.LogFrame{Version: wire.LogVersion, Kind: "JOIN", FrameId: "historical"})
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"success", "timeout", "history_limit"} {
		t.Run(scenario, func(t *testing.T) {
			budget := time.Second
			if scenario == "timeout" {
				budget = 20 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			fixture := &barrierFixture{mode: scenario, historical: history}
			// defaults 为每个子场景提供独立的通知预算。
			defaults := spec.Defaults()
			options := defaults.WorkerEventOptionsFor()
			if scenario == "history_limit" {
				options.MaxReplayFrames = 3
			}
			cursor, err := Join(ctx, fixture, codec, "fixture", "broadcast", "worker", options)
			switch scenario {
			case "success":
				if err != nil || cursor != "barrier" {
					t.Fatal(cursor, err)
				}
			case "timeout":
				if err != context.DeadlineExceeded || cursor != "" {
					t.Fatal(cursor, err)
				}
			case "history_limit":
				if status.Code(err) != codes.ResourceExhausted || cursor != "" {
					t.Fatal(cursor, err)
				}
			}
		})
	}
}
