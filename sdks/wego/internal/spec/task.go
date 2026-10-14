package spec

import (
	"fmt"
	"strings"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/option"
)

// Task 任务级策略快照；Option.Set 保留显式零值，切片和表达式分别按合并规则处理
type Task struct {
	// Retries 失败后的重试次数；显式设 0 表示不重试，不等同于未配置
	Retries option.Option[int]
	// ExecutionTimeout 每次业务执行的最长时间，例如 10 秒
	ExecutionTimeout option.Option[time.Duration]
	// ScheduleTimeout 任务排队等待调度的最长时间
	ScheduleTimeout option.Option[time.Duration]
	// BackoffFactor 重试退避倍率；如 2 表示后一次等待按倍率增长
	BackoffFactor option.Option[float32]
	// BackoffMax 重试退避等待的最大时长
	BackoffMax option.Option[time.Duration]
	// SlotCost 单次执行消耗的容量；如容量 4、成本 2 最多并发两次
	SlotCost option.Option[int]
	// Sticky 任务或调用的粘性调度配置；使关联执行优先或强制复用 Worker
	Sticky option.Option[model.StickyStrategy]
	// Eviction durable 挂起执行的驱逐策略
	Eviction option.Option[model.EvictionPolicy]
	// Concurrency 并发限制集合，可同时约束多个分组
	Concurrency []model.Concurrency
	// RateLimits 任务消耗的限流额度定义集合
	RateLimits []model.RateLimit
	// Cron Cron 触发表达式，例如 * * * * * 表示每分钟
	Cron []string
	// CronInput Cron 触发时提交的输入；固定输入可用 model.Input 显式携带 routing
	CronInput any
	// CronInputSet 区分未设置和显式 nil 清除，独立于 Cron 表达式
	CronInputSet bool
	// Events 触发此任务的事件键集合
	Events []string
	// Filters 默认事件过滤器及传给业务的 filter payload
	Filters []model.DefaultFilter
	// Idempotency 完整的提交去重策略；未设置时继承，显式配置整体覆盖默认值。
	Idempotency option.Option[model.Idempotency]
	// Durable 是否使用可重放执行上下文；普通网络请求没有 durable 身份
	Durable bool
	// Batch 批聚合配置；nil 表示逐条执行
	Batch *model.BatchConfig
}

// Merge 只覆盖明确设置的选项；切片用 nil 表示继承，空切片表示清空
func Merge(base, override Task) Task {
	// 只转换显式设置的 override.Retries；例如 Set=true、Value=0 仍必须覆盖默认值
	if override.Retries.Set {
		base.Retries = override.Retries
	}
	// 只转换显式设置的 override.ExecutionTimeout；例如 Set=true、Value=0 仍必须覆盖默认值
	if override.ExecutionTimeout.Set {
		base.ExecutionTimeout = override.ExecutionTimeout
	}
	// 只转换显式设置的 override.ScheduleTimeout；例如 Set=true、Value=0 仍必须覆盖默认值
	if override.ScheduleTimeout.Set {
		base.ScheduleTimeout = override.ScheduleTimeout
	}
	// 只转换显式设置的 override.BackoffFactor；例如 Set=true、Value=0 仍必须覆盖默认值
	if override.BackoffFactor.Set {
		base.BackoffFactor = override.BackoffFactor
	}
	// 只转换显式设置的 override.BackoffMax；例如 Set=true、Value=0 仍必须覆盖默认值
	if override.BackoffMax.Set {
		base.BackoffMax = override.BackoffMax
	}
	// 只转换显式设置的 override.SlotCost；例如 Set=true、Value=0 仍必须覆盖默认值
	if override.SlotCost.Set {
		base.SlotCost = override.SlotCost
	}
	// 只转换显式设置的 override.Sticky；例如 Set=true、Value=0 仍必须覆盖默认值
	if override.Sticky.Set {
		base.Sticky = override.Sticky
	}
	// 只转换显式设置的 override.Eviction；例如 Set=true、Value=0 仍必须覆盖默认值
	if override.Eviction.Set {
		base.Eviction = override.Eviction
	}
	// 使用显式提供的 Concurrency 覆盖默认列表；非 nil 空列表表示主动清空
	if override.Concurrency != nil {
		base.Concurrency = override.Concurrency
	}
	// 使用显式提供的 RateLimits 覆盖默认列表；非 nil 空列表表示主动清空
	if override.RateLimits != nil {
		base.RateLimits = override.RateLimits
	}
	// 使用显式提供的 Cron 覆盖默认列表；非 nil 空列表表示主动清空
	if override.Cron != nil {
		base.Cron = override.Cron
	}
	// 单独覆盖输入不会改 Cron；单独改 Cron 也不会清除已有输入
	if override.CronInputSet || override.CronInput != nil {
		base.CronInput, base.CronInputSet = override.CronInput, true
	}
	// 使用显式提供的 Events 覆盖默认列表；非 nil 空列表表示主动清空
	if override.Events != nil {
		base.Events = override.Events
	}
	// 使用显式提供的 Filters 覆盖默认列表；非 nil 空列表表示主动清空
	if override.Filters != nil {
		base.Filters = override.Filters
	}
	if override.Idempotency.Set {
		// METHOD、表达式与 TTL 必须来自同一份配置，不拼接不同层的策略。
		base.Idempotency = override.Idempotency
	}
	base.Durable = override.Durable
	// 使用显式提供的 Batch 覆盖默认列表；非 nil 空列表表示主动清空
	if override.Batch != nil {
		base.Batch = override.Batch
	}
	return base
}

// Validate 检查配置组合的有效性，在资源启动前拒绝不合法的容量和预算
func (t *Task) Validate() error {
	if t.Idempotency.Set {
		// 空策略是配置错误，不能被当作未设置而意外继承其他任务的幂等键。
		config := t.Idempotency.Value
		if strings.TrimSpace(config.Expression) == "" {
			return fmt.Errorf("wego: idempotency expression must not be empty")
		}
		switch config.Method {
		case "", model.IdempotencyMethodTTL, model.IdempotencyMethodStatus:
		default:
			return fmt.Errorf("wego: invalid idempotency method %q", config.Method)
		}
		// 官方以毫秒存储；500 微秒会被截为 0，必须在注册前拒绝。
		if config.TTL < time.Millisecond {
			return fmt.Errorf("wego: idempotency TTL must be at least 1ms")
		}
	}
	// 静态上限 0 表示未指定；动态表达式返回 0 的暂停语义不在此处校验。
	for _, concurrency := range t.Concurrency {
		if concurrency.MaxRuns < 0 {
			return fmt.Errorf("wego: concurrency max runs must be nonnegative")
		}
	}
	// 批配置的零值保留官方默认；先校验再转为毫秒，避免负数或溢出变成有效配置。
	if batch := t.Batch; batch != nil {
		if batch.MaxSize < 1 || batch.MaxSize > 100000 {
			return fmt.Errorf("wego: batch max size must be in range 1..100000")
		}
		if batch.MaxInterval != 0 && (batch.MaxInterval < time.Millisecond || batch.MaxInterval > 24*time.Hour) {
			return fmt.Errorf("wego: batch max interval must be zero or in range 1ms..24h")
		}
		if batch.GroupMaxRuns < 0 || batch.GroupMaxRuns > 10000 {
			return fmt.Errorf("wego: batch group max runs must be in range 0..10000")
		}
	}
	// 显式配置存在时将其传给当前策略；例如设置 Value=0、Set=true 仍必须生效
	if t.Retries.Set && t.Retries.Value < 0 {
		return fmt.Errorf("wego: retries must be nonnegative")
	}
	// 显式配置存在时将其传给当前策略；例如设置 Value=0、Set=true 仍必须生效
	if t.SlotCost.Set && t.SlotCost.Value < 1 {
		return fmt.Errorf("wego: slot cost must be positive")
	}
	// 显式配置存在时将其传给当前策略；例如设置 Value=0、Set=true 仍必须生效
	if t.ExecutionTimeout.Set && t.ExecutionTimeout.Value <= 0 {
		return fmt.Errorf("wego: execution timeout must be positive")
	}
	// 显式配置存在时将其传给当前策略；例如设置 Value=0、Set=true 仍必须生效
	if t.ScheduleTimeout.Set && t.ScheduleTimeout.Value <= 0 {
		return fmt.Errorf("wego: schedule timeout must be positive")
	}
	// 显式配置存在时将其传给当前策略；例如设置 Value=0、Set=true 仍必须生效
	if t.Eviction.Set && !t.Durable {
		return fmt.Errorf("wego: eviction requires durable execution")
	}

	return nil
}

// TaskOption 构造期配置函数或自有别名，按传入顺序合并到对应配置
type TaskOption func(*Task)
