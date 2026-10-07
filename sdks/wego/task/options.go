package task

import (
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/option"
)

// Option 配置函数或自有配置视图；按传入顺序应用，相同字段后者覆盖前者。
type Option = spec.TaskOption

// EvictionPolicy durable 挂起执行的 TTL、容量驱逐许可及优先级配置。
type EvictionPolicy = model.EvictionPolicy

// Concurrency 按 CEL 分组的并发约束；如 input.routing.group 将 group-a 与 group-b 分开限制。
type Concurrency = model.Concurrency

// RateLimit 静态或动态限流定义；如一分钟总额度 10、单次成本 2 允许五次额度消耗。
type RateLimit = model.RateLimit

// DefaultFilter 默认事件过滤器，包括条件、scope 和业务附加 payload。
type DefaultFilter = model.DefaultFilter

const (
	// StickySoft 优先复用关联 Worker；不可用时允许调度到其他 Worker。
	StickySoft = model.StickySoft
	// StickyHard 必须复用关联 Worker，不能回退到其他 Worker。
	StickyHard = model.StickyHard
)

// WithRetries 配置 Retries：失败后的重试次数；显式设 0 表示不重试，不等同于未配置。
func WithRetries(v int) Option {
	return func(c *spec.Task) {
		c.Retries = option.Some(v)
	}
}

// WithExecutionTimeout 配置 ExecutionTimeout：每次业务执行的最长时间，例如 10 秒。
func WithExecutionTimeout(v time.Duration) Option {
	return func(c *spec.Task) {
		c.ExecutionTimeout = option.Some(v)
	}
}

// WithScheduleTimeout 配置 ScheduleTimeout：任务排队等待调度的最长时间。
func WithScheduleTimeout(v time.Duration) Option {
	return func(c *spec.Task) {
		c.ScheduleTimeout = option.Some(v)
	}
}

// WithRetryBackoff 配置重试等待倍率与最大值，限制重复失败时的提交频率。
func WithRetryBackoff(factor float32, max time.Duration) Option {
	return func(c *spec.Task) {
		c.BackoffFactor = option.Some(factor)
		c.BackoffMax = option.Some(max)
	}
}

// WithSlotCost 配置 SlotCost：单次执行消耗的容量；如容量 4、成本 2 最多并发两次。
// 例如普通槽数为 4、SlotCost=2 时，同时执行两项就已用满容量。
func WithSlotCost(v int) Option {
	return func(c *spec.Task) {
		c.SlotCost = option.Some(v)
	}
}

// WithSticky 配置 Sticky：任务或调用的粘性调度配置；使关联执行优先或强制复用 Worker。
func WithSticky(v model.StickyStrategy) Option {
	return func(c *spec.Task) {
		c.Sticky = option.Some(v)
	}
}

// WithEviction 配置 Eviction：durable 挂起执行的驱逐策略。
func WithEviction(v EvictionPolicy) Option {
	return func(c *spec.Task) {
		c.Eviction = option.Some(v)
	}
}

// WithConcurrency 配置 Concurrency：并发限制集合，可同时约束多个分组。
func WithConcurrency(v ...Concurrency) Option {
	return func(c *spec.Task) {
		c.Concurrency = v
	}
}

// WithRateLimits 配置 RateLimits：任务消耗的限流额度定义集合。
func WithRateLimits(v ...RateLimit) Option {
	return func(c *spec.Task) {
		c.RateLimits = v
	}
}

// WithCron 设置定义级 Cron 表达式，如 * * * * * 表示每分钟触发。
func WithCron(v ...string) Option {
	return func(c *spec.Task) {
		c.Cron = v
	}
}

// WithCronInput 设置定义级 Cron 输入，RPC 任务使用携带方法名和 protobuf 的 model.RPCInput。
func WithCronInput(v any) Option {
	return func(c *spec.Task) {
		c.CronInput = v
		c.CronInputSet = true
	}
}

// WithEvents 设置事件触发键，匹配事件由引擎调度执行。
func WithEvents(v ...string) Option {
	return func(c *spec.Task) {
		c.Events = v
	}
}

// WithDefaultFilters 设置事件过滤条件与 filter payload，供 handler 读取匹配数据。
func WithDefaultFilters(v ...DefaultFilter) Option {
	return func(c *spec.Task) {
		c.Filters = v
	}
}

// WithIdempotency 设置通过输入计算幂等键的表达式，相同有效键提交时返回包含 ExistingRunID 的冲突错误。
func WithIdempotency(expression string, status bool) Option {
	return func(c *spec.Task) {
		c.IdempotencyExpression = expression
		c.IdempotencyStatus = status
	}
}

// WithIdempotencyTTL 设置幂等记录有效期，到期后相同键可以再次创建运行。
func WithIdempotencyTTL(expression string, status bool, ttl time.Duration) Option {
	return func(c *spec.Task) {
		c.IdempotencyExpression = expression
		c.IdempotencyStatus = status
		c.IdempotencyTTL = ttl
	}
}
