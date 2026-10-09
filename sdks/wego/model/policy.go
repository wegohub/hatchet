package model

import (
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/option"
)

// ConcurrencyLimitStrategy 并发超限时的调度策略枚举
type ConcurrencyLimitStrategy string

const (
	// CancelInProgress 超过并发容量时取消正在执行的较早运行，为新运行释放容量
	CancelInProgress ConcurrencyLimitStrategy = "CANCEL_IN_PROGRESS"
	// CancelNewest 超过并发容量时取消较新的运行
	CancelNewest ConcurrencyLimitStrategy = "CANCEL_NEWEST"
	// GroupRoundRobin 在并发分组间轮转调度，避免一个分组长时间占用全部机会
	GroupRoundRobin ConcurrencyLimitStrategy = "GROUP_ROUND_ROBIN"
	// DropNewest 容量不足时丢弃最新提交的运行
	DropNewest ConcurrencyLimitStrategy = "DROP_NEWEST"
	// QueueNewest 容量不足时保留新运行排队，待已有运行释放容量后执行
	QueueNewest ConcurrencyLimitStrategy = "QUEUE_NEWEST"
	// CancelQueuedExceptNewest 同组排队运行只保留最新的一次，其余排队运行取消
	CancelQueuedExceptNewest ConcurrencyLimitStrategy = "CANCEL_QUEUED_EXCEPT_NEWEST"
	// CancelQueuedExceptOldest 同组排队运行只保留最早的一次，其余排队运行取消
	CancelQueuedExceptOldest ConcurrencyLimitStrategy = "CANCEL_QUEUED_EXCEPT_OLDEST"
)

// Concurrency 按 CEL 分组的并发约束；如 input.routing.group 将 group-a 与 group-b 分开限制
type Concurrency struct {
	// Expression CEL 或触发表达式；语法由对应管理或执行功能解释
	Expression string
	// MaxRuns 静态并发上限；如 2 表示相同分组最多两次同时执行
	MaxRuns *int32
	// LimitStrategy 超过并发上限时的取消、丢弃或排队策略
	LimitStrategy *ConcurrencyLimitStrategy
	// Name 资源或任务名称，注册与调用必须使用相同值
	Name string
	// IsTenantScoped 是否在授权范围内共享并发限制，而非仅限当前任务
	IsTenantScoped bool
	// MaxRunsExpression 动态并发上限表达式，与固定 MaxRuns 区分
	MaxRunsExpression *string
}

// RateLimitDuration 限流计数周期枚举，使用后端支持的周期字符串
type RateLimitDuration string

const (
	// Second 以秒为限流统计周期
	Second RateLimitDuration = "second"
	// Minute 以分钟为限流统计周期
	Minute RateLimitDuration = "minute"
	// Hour 以小时为限流统计周期
	Hour RateLimitDuration = "hour"
	// Day 以天为限流统计周期
	Day RateLimitDuration = "day"
	// Week 以周为限流统计周期
	Week RateLimitDuration = "week"
	// Month 以月为限流统计周期
	Month RateLimitDuration = "month"
	// Year 以年为限流统计周期
	Year RateLimitDuration = "year"
)

// RateLimit 静态或动态限流定义；如一分钟总额度 10、单次成本 2 允许五次额度消耗
type RateLimit struct {
	// Key 任务运行、幂等或限流使用的键
	Key string
	// KeyExpr 动态限流键的 CEL 表达式，例如 input.routing.group
	KeyExpr *string
	// Units 每次执行消耗的静态额度；如 2 表示扣除两单位
	Units *int
	// UnitsExpr 按输入计算本次额度消耗的表达式
	UnitsExpr *string
	// LimitValueExpr 动态计算限流上限的表达式
	LimitValueExpr *string
	// Duration 限流统计周期，例如 Minute 对应一分钟
	Duration *RateLimitDuration
}

// StickyStrategy 任务粘性策略枚举，区分优先复用与必须复用 Worker
type StickyStrategy int32

const (
	// StickySoft 优先复用关联 Worker；不可用时允许调度到其他 Worker
	StickySoft StickyStrategy = iota
	// StickyHard 必须复用关联 Worker，不能回退到其他 Worker
	StickyHard
)

// WorkerLabelComparator Worker 标签比较方式枚举
type WorkerLabelComparator int32

const (
	// LabelEqual 标签值必须等于期望值，例如 region == "local"
	LabelEqual WorkerLabelComparator = iota
	// LabelNotEqual 标签值必须不等于期望值
	LabelNotEqual
	// LabelGreater 标签值必须大于期望值，例如 capacity > 2
	LabelGreater
	// LabelGreaterEqual 标签值必须大于或等于期望值
	LabelGreaterEqual
	// LabelLess 标签值必须小于期望值
	LabelLess
	// LabelLessEqual 标签值必须小于或等于期望值
	LabelLessEqual
)

// DesiredWorkerLabel 单个标签的亲和性规则；Required=true 不允许回退到不匹配的 Worker
type DesiredWorkerLabel struct {
	// Value 配置或标签比较使用的值；有效性由所属类型及 Set 决定
	Value any
	// Required 亲和性是否必须满足；true 时不能回退到不匹配的 Worker
	Required bool
	// Weight 软亲和性匹配权重，用于排序候选 Worker
	Weight int32
	// Comparator 标签比较方式；nil 使用后端默认比较规则
	Comparator *WorkerLabelComparator
}

// EvictionPolicy durable 挂起执行的 TTL、容量驱逐许可及优先级配置
type EvictionPolicy struct {
	// TTL durable 等待超过此时长后可驱逐；Option 区分未配置与显式值
	TTL option.Option[time.Duration]
	// AllowCapacityEviction 是否允许因容量不足驱逐挂起执行
	AllowCapacityEviction option.Option[bool]
	// Priority 调度或驱逐优先级；指针或 Option 用于区分未设置与显式零值
	Priority option.Option[int]
}

// BatchConfig 批聚合配置；大小、时间窗口和分组共同决定哪些输入进入同一批
type BatchConfig struct {
	// MaxSize 每批最大条数；例如 3 条输入组成一个批次
	MaxSize int32
	// MaxInterval 批次首次入队后的最大聚合等待时间
	MaxInterval *time.Duration
	// GroupKey 批聚合分组表达式；不同 group 的输入不会混入同一批
	GroupKey *string
	// GroupMaxRuns 同一批分组允许的同时运行批次数
	GroupMaxRuns *int32
	// BroadcastOutput 是否将一个批次结果广播给每条输入；false 使用逐条结果映射
	BroadcastOutput bool
}
