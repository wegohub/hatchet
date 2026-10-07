package model

import (
	"time"
)

// Resource 保存管理接口的动态结果，后端 DTO 在返回前复制为普通 JSON 值。
type Resource map[string]any

// ID 返回资源的注册身份；未找到合法身份时返回空字符串。
func (r Resource) ID() string {
	// metadata 为嵌套资源身份对象，只在实际 map 类型匹配时读取 id。
	if metadata, ok := r["metadata"].(map[string]any); ok {
		// id 读取 REST 资源的嵌套身份，缺少或类型错误时继续检查明确的扁平字段。
		if id, ok := metadata["id"].(string); ok {
			return id
		}
	}
	// id 读取扁平资源的身份字段，类型不是 string 时不能把动态值当作资源 ID。
	if id, ok := r["id"].(string); ok {
		return id
	}

	return ""
}

// Query 管理查询参数，例如 {"limit":10}；具体支持项由对应 API 决定。
type Query map[string]any

// InstanceInfo 实例连接信息与能力快照，验收前用于确认服务版本和必要能力。
type InstanceInfo struct {
	// Version SDK、服务或协议版本；各自用于报告或兼容性校验。
	Version string
	// TenantID 当前授权范围的身份，由实例配置解析。
	TenantID string
	// APIURL 实例 HTTP API 地址。
	APIURL string
	// GRPCAddress 实例 gRPC 地址。
	GRPCAddress string
	// TLS TLS 配置或启用状态；Runtime 中 nil 配合 TLSSet=true 表示明确选择明文。
	TLS bool
	// DurableEviction 实例是否支持 durable 驱逐功能。
	DurableEviction bool
}

// RunStatus 运行状态枚举，供查询与轮询终态使用。
type RunStatus string

const (
	// Queued 任务已提交但尚未开始执行。
	Queued RunStatus = "QUEUED"
	// Running 任务正在执行。
	Running RunStatus = "RUNNING"
	// Completed 任务已成功完成。
	Completed RunStatus = "COMPLETED"
	// Failed 任务已失败。
	Failed RunStatus = "FAILED"
	// Cancelled 任务已被取消。
	Cancelled RunStatus = "CANCELLED"
)

// WebhookSource Webhook 来源枚举，决定输入解析和认证规则。
type WebhookSource string

const (
	// Generic 通用 HTTP Webhook 来源。
	Generic WebhookSource = "GENERIC"
	// GitHub GitHub Webhook 来源，使用相应请求解析与验签规则。
	GitHub WebhookSource = "GITHUB"
	// Stripe Stripe Webhook 来源。
	Stripe WebhookSource = "STRIPE"
	// Slack Slack Webhook 来源。
	Slack WebhookSource = "SLACK"
	// Svix Svix Webhook 来源。
	Svix WebhookSource = "SVIX"
)

// WebhookAuth 封闭的 wego Webhook 认证契约，不接受任意后端类型。
type WebhookAuth interface{ webhookAuth() }

// BasicAuth Webhook Basic 认证配置。
type BasicAuth struct {
	// Username, Password Basic 认证的用户名与密码，仅用于受控请求认证。
	Username, Password string
}

// webhookAuth 实现封闭认证接口标记，防止任意后端对象作为公开认证配置。
func (BasicAuth) webhookAuth() {}

// APIKeyAuth 指定请求头的 API key 认证配置。
type APIKeyAuth struct {
	// HeaderName, APIKey API key 所在的请求头名称及匹配值。
	HeaderName, APIKey string
}

// webhookAuth 实现封闭认证接口标记，防止任意后端对象作为公开认证配置。
func (APIKeyAuth) webhookAuth() {}

// HMACAuth Webhook HMAC 验签配置，包含密钥、算法和签名编码。
type HMACAuth struct {
	// SigningSecret, SignatureHeaderName, Algorithm, Encoding HMAC 密钥、签名头、算法及编码方式，联合决定验签格式。
	SigningSecret, SignatureHeaderName, Algorithm, Encoding string
}

// webhookAuth 实现封闭认证接口标记，防止任意后端对象作为公开认证配置。
func (HMACAuth) webhookAuth() {}

// SvixAuth Webhook Svix 验签配置。
type SvixAuth struct {
	// SigningSecret Webhook 验签密钥，不写入日志或报告。
	SigningSecret string
}

// webhookAuth 实现封闭认证接口标记，防止任意后端对象作为公开认证配置。
func (SvixAuth) webhookAuth() {}

// CreateWebhookOpts 创建 Webhook 的来源、事件表达式和认证配置。
type CreateWebhookOpts struct {
	// Name 资源或任务名称，注册与调用必须使用相同值。
	Name string
	// SourceName Webhook 来源类型，决定解析和认证方式。
	SourceName WebhookSource
	// EventKeyExpression 从 HTTP 请求中生成事件键的表达式。
	EventKeyExpression string
	// ScopeExpression 从 HTTP 请求中计算事件隔离范围的表达式。
	ScopeExpression *string
	// StaticPayload 附加的静态事件数据；nil 表示不附加。
	StaticPayload *map[string]any
	// ReturnEventAsResponsePayload 是否把生成的事件作为 HTTP 响应返回。
	ReturnEventAsResponsePayload *bool
	// Auth Webhook 认证策略，仅接受 wego 定义的认证类型。
	Auth WebhookAuth
}

// CreateCronTrigger 定时重复触发配置，例如 * * * * * 配合 RPCInput 每分钟调用一次方法。
type CreateCronTrigger struct {
	// Name 资源或任务名称，注册与调用必须使用相同值。
	Name string `json:"name"`
	// Expression CEL 或触发表达式；语法由对应管理或执行功能解释。
	Expression string `json:"expression"`
	// Input 提交给任务的业务输入；RPC 入口使用 protobuf envelope。
	Input any `json:"input,omitempty"`
	// AdditionalMetadata 随运行保存的附加 metadata，用于检索和追踪关联。
	AdditionalMetadata map[string]any `json:"additionalMetadata,omitempty"`
	// Priority 调度或驱逐优先级；指针或 Option 用于区分未设置与显式零值。
	Priority *int32 `json:"priority,omitempty"`
}

// CreateScheduledRunTrigger 一次性调度配置，TriggerAt 为绝对触发时间。
type CreateScheduledRunTrigger struct {
	// TriggerAt 一次性调度的触发时间，例如当前时间加 5 秒。
	TriggerAt time.Time `json:"triggerAt"`
	// Input 提交给任务的业务输入；RPC 入口使用 protobuf envelope。
	Input any `json:"input,omitempty"`
	// AdditionalMetadata 随运行保存的附加 metadata，用于检索和追踪关联。
	AdditionalMetadata map[string]any `json:"additionalMetadata,omitempty"`
	// Priority 调度或驱逐优先级；指针或 Option 用于区分未设置与显式零值。
	Priority *int32 `json:"priority,omitempty"`
}

// CreateRatelimitOpts 限流资源定义，例如一分钟最多 10 单位额度。
type CreateRatelimitOpts struct {
	// Key 任务运行、幂等或限流使用的键。
	Key string
	// Limit 指定周期内允许消耗的总额度；例如 10 单位/分钟。
	Limit int
	// Duration 限流统计周期，例如 Minute 对应一分钟。
	Duration RateLimitDuration
}
