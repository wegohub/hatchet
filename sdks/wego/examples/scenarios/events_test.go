package scenarios

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestEventRunVisibility 等待异步复制时仅重试 404，真实服务错误与调用预算仍严格生效。
func TestEventRunVisibility(t *testing.T) {
	t.Run("eventually_visible", func(t *testing.T) {
		// ctx 为测试设置明确预算，第二次只读查询还原记录后必须立即结束。
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// calls 模拟首次尚未复制，后续可见；不依赖真实网络时序。
		calls := 0
		// err 通过实际等待辅助函数检查重试分类，而非固定 sleep 后假定复制完成。
		err := awaitVisibleEventRun(ctx, func(context.Context) error {
			calls++
			if calls == 1 {
				return errors.New("received status 404")
			}
			return nil
		})
		if err != nil || calls != 2 {
			t.Fatalf("visibility: calls=%d err=%v", calls, err)
		}
	})
	t.Run("service_failure", func(t *testing.T) {
		// failure 代表服务实际故障，不能当作异步可见性重试直到超时。
		failure := errors.New("received status 500")
		// calls 验证服务错误仅发起一次查询。
		calls := 0
		// err 必须保留原错误实例，便于调用方判断原因。
		err := awaitVisibleEventRun(context.Background(), func(context.Context) error {
			calls++
			return failure
		})
		if err != failure || calls != 1 {
			t.Fatalf("service failure: calls=%d err=%v", calls, err)
		}
	})
	t.Run("cancelled_before_lookup", func(t *testing.T) {
		// ctx 在登记等待前已经取消，不能因为立即查询计时器同时就绪而再发请求。
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		// calls 记录不应发生的查询。
		calls := 0
		// err 已取消调用必须直接返回 Canceled。
		err := awaitVisibleEventRun(ctx, func(context.Context) error {
			calls++
			return nil
		})
		if !errors.Is(err, context.Canceled) || calls != 0 {
			t.Fatalf("cancel: calls=%d err=%v", calls, err)
		}
	})
	t.Run("visibility_budget_expires", func(t *testing.T) {
		// ctx 控制持续不可见的场景，必须保留 DeadlineExceeded。
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		// err 第一次 404 后等待预算结束，不能报告业务通过。
		err := awaitVisibleEventRun(ctx, func(context.Context) error { return errors.New("received status 404") })
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	})
}
