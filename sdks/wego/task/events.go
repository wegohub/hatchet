package task

import (
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// EventWaitOption 配置一次事件等待；省略选项不设置对应条件，重复设置以最后一次为准。
type EventWaitOption interface{ applyEventWait(*model.EventWait) }

// eventWaitOptionFunc 为每次等待构造独立参数，不共享可修改的 scope 或时间指针。
type eventWaitOptionFunc func(*model.EventWait)

// applyEventWait 将选项应用到本次等待的配置快照。
func (f eventWaitOptionFunc) applyEventWait(c *model.EventWait) { f(c) }

// WithEventScope 限制匹配同一范围的事件；例如 "user:123" 不匹配 "user:456"。
// 省略该选项不限制范围；显式传入 "" 则匹配空字符串范围，不能当作省略。
func WithEventScope(scope string) EventWaitOption {
	return eventWaitOptionFunc(func(c *model.EventWait) {
		value := scope
		c.Scope = &value
	})
}

// WithConsiderEventsSince 回溯指定时间以来的事件，必须同时设置 WithEventScope。
// 例如使用 task.Now(ctx) 返回的时间减去一分钟，重放时保持相同的历史查询起点。
func WithConsiderEventsSince(since time.Time) EventWaitOption {
	return eventWaitOptionFunc(func(c *model.EventWait) {
		value := since
		c.ConsiderEventsSince = &value
	})
}

// eventWaitOptions 在持久等待登记前完成校验；非法配置不能消耗等待序号。
func eventWaitOptions(opts []EventWaitOption) (model.EventWait, error) {
	config := model.EventWait{}
	for _, opt := range opts {
		if opt == nil {
			return config, status.Error(codes.InvalidArgument, "wego: event wait option is nil")
		}
		opt.applyEventWait(&config)
	}
	if config.ConsiderEventsSince != nil {
		if config.Scope == nil {
			return config, status.Error(codes.InvalidArgument, "wego: event lookback requires an event scope")
		}
		// 官方 Timestamp 支持 UTC 公元 1–9999 年；零值时间仍是显式起点，不代表未配置。
		if year := config.ConsiderEventsSince.UTC().Year(); year < 1 || year > 9999 {
			return config, status.Error(codes.InvalidArgument, "wego: event lookback time must be in UTC years 1..9999")
		}
	}
	return config, nil
}
