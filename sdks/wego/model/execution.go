package model

import (
	"time"

	"google.golang.org/protobuf/proto"
)

// RPCInput 为 Cron、Schedule 和 Event 指定 RPC 方法与 protobuf 消息
type RPCInput struct {
	// Method 完整 RPC 方法名，例如 /wego.example.v1.UnaryGreeter/SayHello；调用目标已经为完整方法时可省略
	Method string
	// Message 业务 protobuf 消息，例如 Request{Message: "hello"}
	Message proto.Message
	// Routing 是本次触发的调度字段；nil 采用方法默认值，顶层键覆盖默认值
	Routing map[string]any
}

// DefaultFilter 默认事件过滤器，包括条件、scope 和业务附加 payload
type DefaultFilter struct {
	// Expression CEL 或触发表达式；语法由对应管理或执行功能解释
	Expression string `json:"expression"`
	// Scope 事件或过滤器的隔离范围；例如 scope-a 与 scope-b 互不匹配
	Scope string `json:"scope"`
	// Payload 业务载荷；wire 中为 protobuf 字节，JSON 编码时自动转为 base64
	Payload map[string]any `json:"payload,omitempty"`
}

// TaskInfo 是执行信息的快照，字段和嵌套值均为 wego 自有类型
type TaskInfo struct {
	// Durable 是否使用可重放执行上下文；普通网络请求没有 durable 身份
	Durable bool
	// RunID 工作流运行身份，例如 run-001，用于等待、查询和取消
	RunID string
	// TaskRunID 工作流内的任务执行身份，与整个 RunID 区分
	TaskRunID string
	// WorkerID 本次执行所属 Worker 的注册身份
	WorkerID string
	// WorkerKey 是 SDK 实例逻辑身份，重连保持，重新创建 Worker 则更新
	WorkerKey string
	// RetryCount 失败重试次数；首次执行为 0
	RetryCount int
	// InvocationCount durable handler 调用次数，用于观察重放
	InvocationCount int32
	// ParentRunID 父运行身份；顶层运行时为 nil
	ParentRunID *string
	// FilterPayload 事件过滤匹配后附带的业务数据
	FilterPayload map[string]any
	// AdditionalMetadata 随运行保存的附加 metadata，用于检索和追踪关联
	AdditionalMetadata map[string]string
	// WorkerLabels 执行此任务的 Worker 标签快照
	WorkerLabels map[string]any
}

// EventWait durable 事件等待配置，区分 scope 过滤和历史事件回溯
type EventWait struct {
	// Scope 事件或过滤器的隔离范围；例如 scope-a 与 scope-b 互不匹配
	Scope *string
	// ConsiderEventsSince 事件回溯起点，可接收等待开始前已经发生的匹配事件
	ConsiderEventsSince *time.Time
}

// RunOptions 单次任务提交选项，与注册定义的策略分别配置
type RunOptions struct {
	// Key 任务运行、幂等或限流使用的键
	Key *string
	// Sticky 任务或调用的粘性调度配置；使关联执行优先或强制复用 Worker
	Sticky *bool
	// Priority 调度或驱逐优先级；指针或 Option 用于区分未设置与显式零值
	Priority *int
	// Metadata 请求或运行 metadata；保留键值和多值语义
	Metadata map[string]string
	// Labels Worker 标签或运行亲和性条件；如 owner 标签要求会话路由到同一实例
	Labels map[string]*DesiredWorkerLabel
}

// RunManyInput 批量提交中的一项输入及其独立选项
type RunManyInput struct {
	// Input 提交给任务的业务输入；RPC 入口使用 protobuf envelope
	Input any
	// Options 本次操作的配置选项，按调用顺序合并
	Options RunOptions
}
