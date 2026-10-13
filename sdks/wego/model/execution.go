package model

import (
	"encoding/json"
	"fmt"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/message"
	"time"
)

// Input 为任务及触发器携带业务值与调度字段；业务 struct 的同名字段不会被解释为协议。
type Input struct {
	// Value 是 protobuf 消息或可序列化的 JSON 值。
	Value any
	// Routing 是显式供 CEL 使用的字段，独立于业务 codec。
	Routing map[string]any
}

// MarshalJSON 为显式 Input 生成明文协议，值接收者支持直接 json.Marshal(Input{...})。
// protobuf oneof 使用 ProtoJSON；不选择 Runtime，也不执行任何 codec I/O。
func (i Input) MarshalJSON() ([]byte, error) {
	value, _, err := message.Marshal(i.Value, 1<<20)
	if err != nil {
		return nil, err
	}
	header := map[string]any{"version": message.Version}
	if !message.Portable(value) {
		header["encoding"] = "base64"
		value, err = json.Marshal([]byte(value))
		if err != nil {
			return nil, err
		}
	}
	packet := struct {
		// Header 只声明明文 Input 的版本和可选字节表示。
		Header map[string]any `json:"_wego"`
		// Value 已用 ProtoJSON 或 JSON 完成序列化。
		Value json.RawMessage `json:"value"`
		// Routing 独立承载服务端可计算字段。
		Routing map[string]any `json:"routing,omitempty"`
	}{header, value, i.Routing}
	if len(i.Routing) > 0 {
		data, err := json.Marshal(i.Routing)
		if err != nil {
			return nil, err
		}
		if !message.Portable(data) {
			return nil, fmt.Errorf("wego: routing contains an unsafe JSON number")
		}
	}
	return json.Marshal(packet)
}

// DefaultFilter 默认事件过滤器，包括条件、scope 和业务附加 payload
type DefaultFilter struct {
	// Expression CEL 或触发表达式；语法由对应管理或执行功能解释
	Expression string `json:"expression"`
	// Scope 事件或过滤器的隔离范围；例如 scope-a 与 scope-b 互不匹配
	Scope string `json:"scope"`
	// Payload 业务载荷；wire 中为 protobuf 字节，JSON 编码时自动转为 base64
	Payload any `json:"payload,omitempty"`
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

// RunOptions 对应官方 Go SDK 的运行选项字段，类型由 wego 自己定义。
type RunOptions struct {
	// Routing 保存单次提交的独立调度快照。
	Routing map[string]any
	// StreamIdempotencyKey 仅用于流任务的逻辑提交身份。
	StreamIdempotencyKey string
	// ValidationError 保存构造选项时的错误，提交前统一检查。
	ValidationError error
	// Key 稳定子调用键，例如 child-greet；用于父任务关联，不是提交幂等键。
	Key *string
	// Sticky 请求子任务留在父 Worker，软硬约束由定义级 Sticky 策略决定。
	Sticky *bool
	// Priority 本次提交的调度优先级；nil 表示未覆盖，显式设置必须为 1–3，0 也会校验报错。
	Priority *int
	// AdditionalMetadata 随运行保存的附加字段，例如 source=api；不同于 gRPC metadata。
	AdditionalMetadata map[string]string
	// DesiredWorkerLabels 本次提交的 Worker 标签约束，例如 region=local、Required=true。
	DesiredWorkerLabels map[string]*DesiredWorkerLabel
}

// RunManyInput 批量提交中的一项输入及其独立选项
type RunManyInput struct {
	// Input 提交给任务的业务输入；RPC 入口使用 protobuf envelope
	Input any
	// Options 本次操作的配置选项，按调用顺序合并
	Options RunOptions
}
