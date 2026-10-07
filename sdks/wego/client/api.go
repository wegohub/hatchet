package client

import (
	"context"

	core "github.com/hatchet-dev/hatchet/sdks/wego/internal/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// Option 表达连接配置函数。
type Option = core.Option

// Conn 表达实现 gRPC 客户端及任务执行入口的连接。
type Conn = core.Conn

// WorkerOption 表达原生 Worker 的定义及实例配置函数。
type WorkerOption = core.WorkerOption

// Worker 表达原生 Worker 生命周期入口。
type Worker = core.Worker

// RunOption 表达单次提交的执行选项。
type RunOption = core.RunOption

// RunManyInput 表达一项批量输入与执行选项。
type RunManyInput = core.RunManyInput

// RunRef 表达可查询实际 RunID 的任务句柄。
type RunRef = core.RunRef

// Result 表达拥有明确任务输出的结果视图。
type Result = core.Result

// Definition 表达独立任务或工作流定义契约。
type Definition = core.Definition

// Task 表达原生任务定义。
type Task = core.Task

// Workflow 表达包含多个有依赖任务的工作流定义。
type Workflow = core.Workflow

// New 创建拥有资源的连接；实例装配和借用连接工厂只对内部适配层开放。
func New(options ...Option) (*Conn, error) {
	return core.New(options...)
}

// WithRuntime 配置连接与执行实例，共享的业务 handler 不需要持有内部引擎。
func WithRuntime(options ...runtime.Option) Option {
	return core.WithRuntime(options...)
}

// WithWorkflows 注册原生任务和工作流定义，用于 batch 与必要的 JSON 入口。
func WithWorkflows(definitions ...Definition) WorkerOption {
	return core.WithWorkflows(definitions...)
}

// WithWorkerRuntime 只覆盖一个原生 Worker 的 Slots / DurableSlots、标签和日志。
// 连接、namespace、投影、载荷、观测与关闭策略由 Conn 统一拥有，改变这些实例配置会返回错误。
func WithWorkerRuntime(options ...runtime.Option) WorkerOption {
	return core.WithWorkerRuntime(options...)
}

// WithPanicHandler 在业务 panic 时回调；context 仅携带 wego 的执行能力。
func WithPanicHandler(handler func(context.Context, any)) WorkerOption {
	return core.WithPanicHandler(handler)
}

// WithRunKey 设置稳定 child key，durable 重放必须复用相同业务身份。
func WithRunKey(key string) RunOption {
	return core.WithRunKey(key)
}

// WithRunSticky 控制子调用是否优先留在父 Worker。
func WithRunSticky(sticky bool) RunOption {
	return core.WithRunSticky(sticky)
}

// WithRunPriority 设置本次提交优先级，不修改任务注册默认值。
func WithRunPriority(priority int) RunOption {
	return core.WithRunPriority(priority)
}

// WithRunMetadata 设置运行级 metadata；protobuf RPC 的 gRPC metadata 另由 envelope 传输。
func WithRunMetadata(values map[string]string) RunOption {
	return core.WithRunMetadata(values)
}

// WithDesiredWorkerLabels 设置明确的亲和性约束，例如 capacity=8、Required=true。
func WithDesiredWorkerLabels(labels map[string]*model.DesiredWorkerLabel) RunOption {
	return core.WithDesiredWorkerLabels(labels)
}
