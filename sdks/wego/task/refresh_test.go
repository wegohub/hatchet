package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

// refreshExecution 只替换续期能力；其他执行方法不属于这些辅助函数的调用路径。
type refreshExecution struct {
	// Execution 占位满足接口；测试只调用下方覆写的续期方法。
	ports.Execution
	// refresh 记录增量或模拟故障，不建立真实后端连接。
	refresh func(context.Context, time.Duration) error
}

// Refresh 将实际增量交给测试替身，以检验没有隐式乘以二。
func (e refreshExecution) Refresh(ctx context.Context, d time.Duration) error {
	return e.refresh(ctx, d)
}

// refreshTaskContext 在标准 context 上加入任务能力，模拟 Worker handler 的入口。
func refreshTaskContext(ctx context.Context, fn func(context.Context, time.Duration) error) context.Context {
	return callctx.Bind(ctx, &callctx.State{Execution: refreshExecution{refresh: fn}})
}

// TestWithRefreshTimeoutIncrement 验证默认增量和显式覆盖都保持固定，不随 tick 次数增长。
func TestWithRefreshTimeoutIncrement(t *testing.T) {
	for _, tc := range []struct {
		// name 区分默认与重复选项的最后一次覆盖。
		name string
		// increment 是每次调用后端所需的增量。
		increment time.Duration
		// opts 保存此子场景的可选配置。
		opts []RefreshOption
	}{
		{name: "default_matches_interval", increment: 5 * time.Millisecond},
		{name: "explicit_increment", increment: 7 * time.Millisecond, opts: []RefreshOption{
			WithRefreshIncrement(6 * time.Millisecond), WithRefreshIncrement(7 * time.Millisecond),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			calls := make(chan time.Duration, 4)
			ctx = refreshTaskContext(ctx, func(_ context.Context, d time.Duration) error {
				calls <- d
				return nil
			})

			// 等到三次真实 tick，验证每次都只增加固定增量，而非随运行时长累加。
			result, err := WithRefreshTimeout(ctx, 5*time.Millisecond, func(ctx context.Context) (int, error) {
				for range 3 {
					select {
					case got := <-calls:
						if got != tc.increment {
							return 0, errors.New("unexpected refresh increment")
						}
					case <-ctx.Done():
						return 0, ctx.Err()
					}
				}
				return 42, nil
			}, tc.opts...)
			if err != nil || result != 42 {
				t.Fatalf("result=%d, err=%v", result, err)
			}
		})
	}
}

// TestWithRefreshTimeoutOptions 拒绝不安全配置，并验证取消后不会启动业务回调。
func TestWithRefreshTimeoutOptions(t *testing.T) {
	for _, tc := range []struct {
		// name 对应一个无效参数组合。
		name string
		// every 是续期间隔；零值和负值必须被拒绝。
		every time.Duration
		// opts 覆盖续期增量或本地总时长上限。
		opts []RefreshOption
	}{
		{name: "zero_interval"},
		{name: "negative_interval", every: -time.Second},
		{name: "zero_increment", every: time.Second, opts: []RefreshOption{WithRefreshIncrement(0)}},
		{name: "increment_below_interval", every: time.Second, opts: []RefreshOption{WithRefreshIncrement(time.Millisecond)}},
		{name: "negative_max_duration", every: time.Second, opts: []RefreshOption{WithRefreshMaxDuration(-time.Second)}},
		{name: "nil_option", every: time.Second, opts: []RefreshOption{nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			_, err := WithRefreshTimeout(context.Background(), tc.every, func(context.Context) (int, error) {
				called = true
				return 1, nil
			}, tc.opts...)
			if err == nil || called {
				t.Fatalf("err=%v, callback called=%t", err, called)
			}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	_, err := WithRefreshTimeout(ctx, time.Second, func(context.Context) (int, error) {
		called = true
		return 1, nil
	})
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("pre-canceled context: err=%v, callback called=%t", err, called)
	}
	if _, err := WithRefreshTimeout[int](context.Background(), time.Second, nil); err == nil {
		t.Fatal("nil value callback accepted")
	}
	if err := WithRefreshStreamTimeout(context.Background(), time.Second, nil); err == nil {
		t.Fatal("nil stream callback accepted")
	}
}

// TestWithRefreshTimeoutNetworkAndMaxDuration 验证网络入口不需任务身份，仍遵守本地上限。
func TestWithRefreshTimeoutNetworkAndMaxDuration(t *testing.T) {
	result, err := WithRefreshTimeout(context.Background(), time.Hour, func(context.Context) (int, error) {
		return 7, nil
	})
	if err != nil || result != 7 {
		t.Fatalf("network result=%d, err=%v", result, err)
	}

	// 网络入口没有任务续期能力，但本次回调的总时长仍由派生 context 控制。
	err = WithRefreshStreamTimeout(context.Background(), time.Hour, func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}, WithRefreshMaxDuration(15*time.Millisecond))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("max duration err=%v", err)
	}
}

// TestWithRefreshStreamTimeoutRefreshFailure 验证续期错误可取消流回调并返回原始故障。
func TestWithRefreshStreamTimeoutRefreshFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	refreshErr := errors.New("dispatcher unavailable")
	ctx = refreshTaskContext(ctx, func(context.Context, time.Duration) error { return refreshErr })
	err := WithRefreshStreamTimeout(ctx, 5*time.Millisecond, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, refreshErr) {
		t.Fatalf("refresh failure not returned: %v", err)
	}
}

// TestWithRefreshTimeoutCompletionDuringRefresh 验证正常收尾取消进行中的续期不报任务失败。
func TestWithRefreshTimeoutCompletionDuringRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := make(chan struct{})
	ctx = refreshTaskContext(ctx, func(ctx context.Context, _ time.Duration) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	// 回调正常结束会取消进行中的续期 RPC；该取消不是任务失败。
	_, err := WithRefreshTimeout(ctx, 5*time.Millisecond, func(ctx context.Context) (int, error) {
		select {
		case <-started:
			return 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})
	if err != nil {
		t.Fatalf("normal completion reported refresh cancellation: %v", err)
	}
}
