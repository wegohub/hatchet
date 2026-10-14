package task

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// eventWaitExecution 只替换事件等待能力，记录真实入口传递的条件和目标。
type eventWaitExecution struct {
	// Execution 补齐无关执行方法，测试不得调用这些占位方法。
	ports.Execution
	// wait 断言本次等待的独立参数；Into 同样使用它，不登记第二次等待。
	wait func(context.Context, string, string, model.EventWait) (map[string]any, error)
}

// WaitEvent 模拟一次持久等待，向测试交付已解析的配置。
func (e *eventWaitExecution) WaitEvent(ctx context.Context, key, expression string, opts model.EventWait) (map[string]any, error) {
	return e.wait(ctx, key, expression, opts)
}

// WaitEventInto 在同一次等待后写入目标，便于断言两种公开入口共用配置语义。
func (e *eventWaitExecution) WaitEventInto(ctx context.Context, key, expression string, target any, opts model.EventWait) error {
	result, err := e.wait(ctx, key, expression, opts)
	if err == nil {
		*target.(*map[string]any) = result
	}
	return err
}

// eventWaitContext 为本次测试注入等待替身，不建立后端连接。
func eventWaitContext(wait func(context.Context, string, string, model.EventWait) (map[string]any, error)) context.Context {
	return callctx.Bind(context.Background(), &callctx.State{Execution: &eventWaitExecution{wait: wait}})
}

// invokeEventWait 经两个公开入口调用替身，Into 的目标只由一次等待填充。
func invokeEventWait(ctx context.Context, into bool, opts ...EventWaitOption) (map[string]any, error) {
	if into {
		// result 接收 Into 写入的业务对象，空配置不影响其解码路径。
		var result map[string]any
		err := WaitForEventInto(ctx, "update", "true", &result, opts...)
		return result, err
	}
	return WaitForEvent(ctx, "update", "true", opts...)
}

// TestEventWaitOptionsPresence 验证不限制 scope、显式空 scope、零值时间和最后一次覆盖。
func TestEventWaitOptionsPresence(t *testing.T) {
	since := time.Unix(100, 0)
	for _, tc := range []struct {
		// name 描述当前存在性或覆盖规则。
		name string
		// opts 为业务使用的函数选项。
		opts []EventWaitOption
		// scoped 判断是否应发送 scope，空值不能代替此标记。
		scoped bool
		// scope 为最终的显式范围。
		scope string
		// since 为期望的历史起点；nil 表示不回溯。
		since *time.Time
	}{
		{name: "unscoped"},
		{name: "empty_scope", opts: []EventWaitOption{WithEventScope("")}, scoped: true},
		{name: "last_option_wins", opts: []EventWaitOption{
			WithEventScope("first"), WithConsiderEventsSince(time.Unix(0, 0)),
			WithEventScope("last"), WithConsiderEventsSince(since),
		}, scoped: true, scope: "last", since: &since},
		{name: "explicit_zero_time", opts: []EventWaitOption{
			WithEventScope(""), WithConsiderEventsSince(time.Time{}),
		}, scoped: true, since: &time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, into := range []bool{false, true} {
				calls := 0
				ctx := eventWaitContext(func(_ context.Context, key, expression string, opts model.EventWait) (map[string]any, error) {
					calls++
					if key != "update" || expression != "true" || (opts.Scope != nil) != tc.scoped {
						t.Fatal("wait identity or scope presence changed", key, expression, opts)
					}
					if opts.Scope != nil && *opts.Scope != tc.scope {
						t.Fatal("scope changed", *opts.Scope)
					}
					if (opts.ConsiderEventsSince != nil) != (tc.since != nil) ||
						(tc.since != nil && !opts.ConsiderEventsSince.Equal(*tc.since)) {
						t.Fatal("lookback presence or timestamp changed", opts.ConsiderEventsSince)
					}
					return map[string]any{"message": "matched"}, nil
				})
				result, err := invokeEventWait(ctx, into, tc.opts...)
				if err != nil || result["message"] != "matched" || calls != 1 {
					t.Fatalf("into=%v: result=%v calls=%d error=%v", into, result, calls, err)
				}
			}
		})
	}
}

// TestEventWaitOptionsValidation 非法选项不能登记持久等待或占用一个重放序号。
func TestEventWaitOptionsValidation(t *testing.T) {
	for _, opts := range [][]EventWaitOption{
		{nil},
		{WithConsiderEventsSince(time.Unix(0, 0))},
		{WithEventScope("scope"), WithConsiderEventsSince(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC))},
		{WithEventScope("scope"), WithConsiderEventsSince(time.Date(0, 12, 31, 23, 59, 59, 0, time.UTC))},
	} {
		for _, into := range []bool{false, true} {
			ctx := eventWaitContext(func(context.Context, string, string, model.EventWait) (map[string]any, error) {
				t.Fatal("invalid options registered a durable wait")
				return nil, nil
			})
			if _, err := invokeEventWait(ctx, into, opts...); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("into=%v: expected InvalidArgument, got %v", into, err)
			}
		}
	}
}

// TestEventWaitOptionsIsolation 并发复用同一组选项，后端修改快照不能污染另一次等待。
func TestEventWaitOptionsIsolation(t *testing.T) {
	since := time.Unix(100, 0)
	opts := []EventWaitOption{WithEventScope("scope"), WithConsiderEventsSince(since)}
	// calls 统计并发成功登记的次数，不让测试替身引入数据竞争。
	var calls atomic.Int32
	ctx := eventWaitContext(func(_ context.Context, _, _ string, config model.EventWait) (map[string]any, error) {
		if config.Scope == nil || *config.Scope != "scope" || config.ConsiderEventsSince == nil || !config.ConsiderEventsSince.Equal(since) {
			t.Error("another wait changed the option snapshot")
			return nil, errors.New("option snapshot changed")
		}
		*config.Scope = "changed"
		*config.ConsiderEventsSince = time.Time{}
		calls.Add(1)
		return map[string]any{"message": "matched"}, nil
	})
	// group 等待所有下标独立的调用结束，使断言包含全部配置复用路径。
	var group sync.WaitGroup
	for i := range 32 {
		group.Go(func() {
			if _, err := invokeEventWait(ctx, i%2 == 0, opts...); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	if calls.Load() != 32 {
		t.Fatal("missing completed waits", calls.Load())
	}
}

// TestEventWaitContextAndFailure 保留普通上下文拒绝与后端错误，不将失败转成空事件。
func TestEventWaitContextAndFailure(t *testing.T) {
	failure := errors.New("wait interrupted")
	ctx := eventWaitContext(func(context.Context, string, string, model.EventWait) (map[string]any, error) {
		return nil, failure
	})
	for _, into := range []bool{false, true} {
		if _, err := invokeEventWait(context.Background(), into); !errors.Is(err, model.ErrTaskContext) {
			t.Fatal("ordinary context gained task capabilities", err)
		}
		if _, err := invokeEventWait(ctx, into); !errors.Is(err, failure) {
			t.Fatal("wait failure changed", err)
		}
	}
}
