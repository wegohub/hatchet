package task

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

// RefreshOption 调整一次自动续期；同一选项重复设置时以后一个为准。
type RefreshOption interface{ applyRefresh(*refreshConfig) }

// refreshConfig 是单次调用的不可共享配置；默认续期间隔与增量相等。
type refreshConfig struct {
	// extendBy 是每次传给 Hatchet 的增量，不从间隔推算额外倍数。
	extendBy time.Duration
	// maxDuration 从本次辅助函数入口计时，零值不增加本地截止时间。
	maxDuration time.Duration
}

// refreshOptionFunc 将公开选项应用到本次调用，不修改其他 handler 的配置。
type refreshOptionFunc func(*refreshConfig)

// applyRefresh 实现仅由 task 包构造的续期选项。
func (f refreshOptionFunc) applyRefresh(c *refreshConfig) { f(c) }

// WithRefreshIncrement 指定每次向现有服务端截止时间增加的时长。
// 例如 every=5 秒、increment=6 秒时，每次续期增加 6 秒。
func WithRefreshIncrement(d time.Duration) RefreshOption {
	return refreshOptionFunc(func(c *refreshConfig) { c.extendBy = d })
}

// WithRefreshMaxDuration 限制本次回调的协作式运行时长；不设置则不限制。
// 截止后取消回调 context，不会强制停止忽略 context 的代码或阻塞的流 I/O。
func WithRefreshMaxDuration(d time.Duration) RefreshOption {
	return refreshOptionFunc(func(c *refreshConfig) { c.maxDuration = d })
}

// WithRefreshTimeout 运行返回业务值的 handler，并在任务入口定期延长执行截止时间。
// 网络 gRPC 入口没有任务身份，照常执行 fn，但不向 Hatchet 发起续期。
func WithRefreshTimeout[T any](ctx context.Context, every time.Duration, fn func(context.Context) (T, error), opts ...RefreshOption) (T, error) {
	// result 只由同步回调写入，续期协程不会访问业务返回值。
	var result T
	if fn == nil {
		return result, fmt.Errorf("wego: refresh callback is nil")
	}
	err := withRefreshTimeout(ctx, every, func(callCtx context.Context) error {
		// callErr 是回调错误，由共用收尾流程与续期错误合并。
		var callErr error
		result, callErr = fn(callCtx)
		return callErr
	}, opts...)
	return result, err
}

// WithRefreshStreamTimeout 运行只返回 error 的流 handler；适用于 client、server 和 bidi stream。
// 回调须检查派生 context；仅取消它不能强制中断正在阻塞的 stream.Send 或 stream.Recv。
func WithRefreshStreamTimeout(ctx context.Context, every time.Duration, fn func(context.Context) error, opts ...RefreshOption) error {
	if fn == nil {
		return fmt.Errorf("wego: refresh callback is nil")
	}
	return withRefreshTimeout(ctx, every, fn, opts...)
}

// withRefreshTimeout 统一配置校验、任务续期和网络入口的回调生命周期。
func withRefreshTimeout(ctx context.Context, every time.Duration, fn func(context.Context) error, opts ...RefreshOption) error {
	if every <= 0 {
		return fmt.Errorf("wego: refresh interval must be positive")
	}
	config := refreshConfig{extendBy: every}
	for _, opt := range opts {
		if opt == nil {
			return fmt.Errorf("wego: refresh option is nil")
		}
		opt.applyRefresh(&config)
	}
	if config.extendBy <= 0 || config.extendBy < every {
		return fmt.Errorf("wego: refresh increment must be at least the interval")
	}
	if config.maxDuration < 0 {
		return fmt.Errorf("wego: refresh max duration must be nonnegative")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// 本地上限从进入辅助函数开始计时；父 context 更早取消时仍以父预算为准。
	var callCtx context.Context
	// cancel 同时结束回调预算与后台续期；正常收尾不得误报续期失败。
	var cancel context.CancelFunc
	if config.maxDuration > 0 {
		callCtx, cancel = context.WithTimeout(ctx, config.maxDuration)
	} else {
		callCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// 网络入口没有执行能力，仍应用同一回调与可选总时长上限。
	e, err := execution(ctx)
	if err != nil {
		return refreshResult(fn(callCtx), callCtx.Err(), nil)
	}

	done := make(chan error, 1)
	ticker := time.NewTicker(every)
	go refreshLoop(callCtx, cancel, ticker, e, config.extendBy, done)
	// panic 时也必须停止定时器并等待后台续期退出。
	defer func() {
		if done != nil {
			cancel()
			<-done
		}
	}()

	callbackErr := fn(callCtx)
	contextErr := callCtx.Err()
	cancel()
	refreshErr := <-done
	done = nil
	return refreshResult(callbackErr, contextErr, refreshErr)
}

// refreshLoop 串行续期；一次失败取消回调，避免继续执行业务但不再续期。
func refreshLoop(ctx context.Context, cancel context.CancelFunc, ticker *time.Ticker, e ports.Execution, increment time.Duration, done chan<- error) {
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				done <- nil
				return
			}
			if err := e.Refresh(ctx, increment); err != nil {
				// 回调完成或父 context 到期后的取消属于正常收尾。
				if ctx.Err() != nil {
					done <- nil
				} else {
					done <- fmt.Errorf("wego: refresh timeout: %w", err)
					cancel()
				}
				return
			}
		}
	}
}

// refreshResult 保留业务错误与真实续期错误；本地截止时间不能被成功回调掩盖。
func refreshResult(callbackErr, contextErr, refreshErr error) error {
	if refreshErr != nil {
		return errors.Join(callbackErr, refreshErr)
	}
	if contextErr != nil && !errors.Is(callbackErr, contextErr) {
		return errors.Join(callbackErr, contextErr)
	}
	return callbackErr
}
