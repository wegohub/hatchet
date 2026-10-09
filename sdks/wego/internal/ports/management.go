package ports

import "github.com/hatchet-dev/hatchet/sdks/wego/model"

// FeatureRequest 是封闭的管理命令集合，边界不接受字符串操作名或位置参数
// 每个具体请求固定参数数量与类型，例如 CronsCreate 必须同时提供 Name 和 Trigger
// 标记方法使用值接收者，使请求值和指针均实现此接口；标记不读写任何请求状态。
type FeatureRequest interface {
	// featureRequest 将实现范围限制在此包声明的请求类型
	featureRequest()
}

// CELDebug 定义 CEL.Debug 的管理参数；身份、策略与业务载荷不混用
type CELDebug struct {
	// Expression 待调试的 CEL 表达式
	Expression string
	// Input 已编码的事件输入或 CEL 输入
	Input map[string]any
	// Metadata 可选 CEL 元数据，nil 表示未提供
	Metadata *map[string]any
	// FilterPayload 可选过滤载荷，nil 表示未提供
	FilterPayload *map[string]any
}

// featureRequest 声明 CELDebug 属于封闭命令集合
func (CELDebug) featureRequest() {}

// CronsCreate 定义 Crons.Create 的管理参数；身份、策略与业务载荷不混用
type CronsCreate struct {
	// Name 待注册或查询的任务名称
	Name string
	// Trigger 触发时间或表达式以及已编码的任务输入
	Trigger model.CreateCronTrigger
}

// featureRequest 声明 CronsCreate 属于封闭命令集合
func (CronsCreate) featureRequest() {}

// CronsDelete 定义 Crons.Delete 的管理参数；身份、策略与业务载荷不混用
type CronsDelete struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 CronsDelete 属于封闭命令集合
func (CronsDelete) featureRequest() {}

// CronsGet 定义 Crons.Get 的管理参数；身份、策略与业务载荷不混用
type CronsGet struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 CronsGet 属于封闭命令集合
func (CronsGet) featureRequest() {}

// CronsList 定义 Crons.List 的管理参数；身份、策略与业务载荷不混用
type CronsList struct {
	// Query wego 自有查询字段，仅在 backend 转换为后端 DTO
	Query model.Query
}

// featureRequest 声明 CronsList 属于封闭命令集合
func (CronsList) featureRequest() {}

// EventsPush 定义 Events.Push 的管理参数；身份、策略与业务载荷不混用
type EventsPush struct {
	// Key 事件键，例如 invoice.created
	Key string
	// Input 已编码的事件输入或 CEL 输入
	Input any
	// Scope 可选事件作用域，nil 表示不隔离
	Scope *string
}

// featureRequest 声明 EventsPush 属于封闭命令集合
func (EventsPush) featureRequest() {}

// FiltersCreate 定义 Filters.Create 的管理参数；身份、策略与业务载荷不混用
type FiltersCreate struct {
	// Request wego 自有资源请求，业务 JSON 扩展字段保持原样
	Request model.Resource
}

// featureRequest 声明 FiltersCreate 属于封闭命令集合
func (FiltersCreate) featureRequest() {}

// FiltersDelete 定义 Filters.Delete 的管理参数；身份、策略与业务载荷不混用
type FiltersDelete struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 FiltersDelete 属于封闭命令集合
func (FiltersDelete) featureRequest() {}

// FiltersGet 定义 Filters.Get 的管理参数；身份、策略与业务载荷不混用
type FiltersGet struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 FiltersGet 属于封闭命令集合
func (FiltersGet) featureRequest() {}

// FiltersList 定义 Filters.List 的管理参数；身份、策略与业务载荷不混用
type FiltersList struct {
	// Query wego 自有查询字段，仅在 backend 转换为后端 DTO
	Query model.Query
}

// featureRequest 声明 FiltersList 属于封闭命令集合
func (FiltersList) featureRequest() {}

// FiltersUpdate 定义 Filters.Update 的管理参数；身份、策略与业务载荷不混用
type FiltersUpdate struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
	// Request wego 自有资源请求，业务 JSON 扩展字段保持原样
	Request model.Resource
}

// featureRequest 声明 FiltersUpdate 属于封闭命令集合
func (FiltersUpdate) featureRequest() {}

// LogsList 定义 Logs.List 的管理参数；身份、策略与业务载荷不混用
type LogsList struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
	// Query wego 自有查询字段，仅在 backend 转换为后端 DTO
	Query model.Query
}

// featureRequest 声明 LogsList 属于封闭命令集合
func (LogsList) featureRequest() {}

// MetricsGetQueueMetrics 定义 Metrics.GetQueueMetrics 的管理参数；身份、策略与业务载荷不混用
type MetricsGetQueueMetrics struct {
	// Query wego 自有查询字段，仅在 backend 转换为后端 DTO
	Query model.Query
}

// featureRequest 声明 MetricsGetQueueMetrics 属于封闭命令集合
func (MetricsGetQueueMetrics) featureRequest() {}

// MetricsGetTaskQueueMetrics 定义 Metrics.GetTaskQueueMetrics 的管理参数；身份、策略与业务载荷不混用
type MetricsGetTaskQueueMetrics struct {
}

// featureRequest 声明 MetricsGetTaskQueueMetrics 属于封闭命令集合
func (MetricsGetTaskQueueMetrics) featureRequest() {}

// MetricsGetWorkflowMetrics 定义 Metrics.GetWorkflowMetrics 的管理参数；身份、策略与业务载荷不混用
type MetricsGetWorkflowMetrics struct {
	// Name 待注册或查询的任务名称
	Name string
	// Query wego 自有查询字段，仅在 backend 转换为后端 DTO
	Query model.Query
}

// featureRequest 声明 MetricsGetWorkflowMetrics 属于封闭命令集合
func (MetricsGetWorkflowMetrics) featureRequest() {}

// RateLimitsList 定义 RateLimits.List 的管理参数；身份、策略与业务载荷不混用
type RateLimitsList struct {
	// Query wego 自有查询字段，仅在 backend 转换为后端 DTO
	Query model.Query
}

// featureRequest 声明 RateLimitsList 属于封闭命令集合
func (RateLimitsList) featureRequest() {}

// RateLimitsUpsert 定义 RateLimits.Upsert 的管理参数；身份、策略与业务载荷不混用
type RateLimitsUpsert struct {
	// Options 限流键、额度与时间单位
	Options model.CreateRatelimitOpts
}

// featureRequest 声明 RateLimitsUpsert 属于封闭命令集合
func (RateLimitsUpsert) featureRequest() {}

// RunsCancel 定义 Runs.Cancel 的管理参数；身份、策略与业务载荷不混用
type RunsCancel struct {
	// Request wego 自有资源请求，业务 JSON 扩展字段保持原样
	Request model.Resource
}

// featureRequest 声明 RunsCancel 属于封闭命令集合
func (RunsCancel) featureRequest() {}

// RunsGet 定义 Runs.Get 的管理参数；身份、策略与业务载荷不混用
type RunsGet struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 RunsGet 属于封闭命令集合
func (RunsGet) featureRequest() {}

// RunsGetDetails 定义 Runs.GetDetails 的管理参数；身份、策略与业务载荷不混用
type RunsGetDetails struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 RunsGetDetails 属于封闭命令集合
func (RunsGetDetails) featureRequest() {}

// RunsGetStatus 定义 Runs.GetStatus 的管理参数；身份、策略与业务载荷不混用
type RunsGetStatus struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 RunsGetStatus 属于封闭命令集合
func (RunsGetStatus) featureRequest() {}

// RunsList 定义 Runs.List 的管理参数；身份、策略与业务载荷不混用
type RunsList struct {
	// Query wego 自有查询字段，仅在 backend 转换为后端 DTO
	Query model.Query
}

// featureRequest 声明 RunsList 属于封闭命令集合
func (RunsList) featureRequest() {}

// RunsReplay 定义 Runs.Replay 的管理参数；身份、策略与业务载荷不混用
type RunsReplay struct {
	// Request wego 自有资源请求，业务 JSON 扩展字段保持原样
	Request model.Resource
}

// featureRequest 声明 RunsReplay 属于封闭命令集合
func (RunsReplay) featureRequest() {}

// RunsRestore 定义 Runs.Restore 的管理参数；身份、策略与业务载荷不混用
type RunsRestore struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 RunsRestore 属于封闭命令集合
func (RunsRestore) featureRequest() {}

// RuntimeInfo 定义 Runtime.Info 的管理参数；身份、策略与业务载荷不混用
type RuntimeInfo struct {
}

// featureRequest 声明 RuntimeInfo 属于封闭命令集合
func (RuntimeInfo) featureRequest() {}

// SchedulesCreate 定义 Schedules.Create 的管理参数；身份、策略与业务载荷不混用
type SchedulesCreate struct {
	// Name 待注册或查询的任务名称
	Name string
	// Trigger 触发时间或表达式以及已编码的任务输入
	Trigger model.CreateScheduledRunTrigger
}

// featureRequest 声明 SchedulesCreate 属于封闭命令集合
func (SchedulesCreate) featureRequest() {}

// SchedulesDelete 定义 Schedules.Delete 的管理参数；身份、策略与业务载荷不混用
type SchedulesDelete struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 SchedulesDelete 属于封闭命令集合
func (SchedulesDelete) featureRequest() {}

// SchedulesGet 定义 Schedules.Get 的管理参数；身份、策略与业务载荷不混用
type SchedulesGet struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 SchedulesGet 属于封闭命令集合
func (SchedulesGet) featureRequest() {}

// SchedulesList 定义 Schedules.List 的管理参数；身份、策略与业务载荷不混用
type SchedulesList struct {
	// Query wego 自有查询字段，仅在 backend 转换为后端 DTO
	Query model.Query
}

// featureRequest 声明 SchedulesList 属于封闭命令集合
func (SchedulesList) featureRequest() {}

// TenantGet 定义 Tenant.Get 的管理参数；身份、策略与业务载荷不混用
type TenantGet struct {
}

// featureRequest 声明 TenantGet 属于封闭命令集合
func (TenantGet) featureRequest() {}

// WebhooksCreate 定义 Webhooks.Create 的管理参数；身份、策略与业务载荷不混用
type WebhooksCreate struct {
	// Request wego 自有资源请求，业务 JSON 扩展字段保持原样
	Request model.CreateWebhookOpts
}

// featureRequest 声明 WebhooksCreate 属于封闭命令集合
func (WebhooksCreate) featureRequest() {}

// WebhooksDelete 定义 Webhooks.Delete 的管理参数；身份、策略与业务载荷不混用
type WebhooksDelete struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 WebhooksDelete 属于封闭命令集合
func (WebhooksDelete) featureRequest() {}

// WebhooksGet 定义 Webhooks.Get 的管理参数；身份、策略与业务载荷不混用
type WebhooksGet struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 WebhooksGet 属于封闭命令集合
func (WebhooksGet) featureRequest() {}

// WebhooksList 定义 Webhooks.List 的管理参数；身份、策略与业务载荷不混用
type WebhooksList struct {
	// Query wego 自有查询字段，仅在 backend 转换为后端 DTO
	Query model.Query
}

// featureRequest 声明 WebhooksList 属于封闭命令集合
func (WebhooksList) featureRequest() {}

// WebhooksUpdate 定义 Webhooks.Update 的管理参数；身份、策略与业务载荷不混用
type WebhooksUpdate struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
	// Request wego 自有资源请求，业务 JSON 扩展字段保持原样
	Request model.Resource
}

// featureRequest 声明 WebhooksUpdate 属于封闭命令集合
func (WebhooksUpdate) featureRequest() {}

// WorkersGet 定义 Workers.Get 的管理参数；身份、策略与业务载荷不混用
type WorkersGet struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 WorkersGet 属于封闭命令集合
func (WorkersGet) featureRequest() {}

// WorkersList 定义 Workers.List 的管理参数；身份、策略与业务载荷不混用
type WorkersList struct {
}

// featureRequest 声明 WorkersList 属于封闭命令集合
func (WorkersList) featureRequest() {}

// WorkersPause 定义 Workers.Pause 的管理参数；身份、策略与业务载荷不混用
type WorkersPause struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 WorkersPause 属于封闭命令集合
func (WorkersPause) featureRequest() {}

// WorkersUnpause 定义 Workers.Unpause 的管理参数；身份、策略与业务载荷不混用
type WorkersUnpause struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 WorkersUnpause 属于封闭命令集合
func (WorkersUnpause) featureRequest() {}

// WorkflowsDelete 定义 Workflows.Delete 的管理参数；身份、策略与业务载荷不混用
type WorkflowsDelete struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 WorkflowsDelete 属于封闭命令集合
func (WorkflowsDelete) featureRequest() {}

// WorkflowsGet 定义 Workflows.Get 的管理参数；身份、策略与业务载荷不混用
type WorkflowsGet struct {
	// ID 目标资源身份或工作流名称，由 backend 应用命名空间规则
	ID string
}

// featureRequest 声明 WorkflowsGet 属于封闭命令集合
func (WorkflowsGet) featureRequest() {}

// WorkflowsList 定义 Workflows.List 的管理参数；身份、策略与业务载荷不混用
type WorkflowsList struct {
	// Query wego 自有查询字段，仅在 backend 转换为后端 DTO
	Query model.Query
}

// featureRequest 声明 WorkflowsList 属于封闭命令集合
func (WorkflowsList) featureRequest() {}
