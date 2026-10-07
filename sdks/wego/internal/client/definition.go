package client

import (
	"context"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// Definition 表示可注册的 wego 定义，具体任务在 NewWorker 时转换和校验。
type Definition interface {
	// GetName 返回注册名称，供任务提交和 Worker 定义收集使用。
	GetName() string
	// definitions 把当前定义展开为内部任务列表，供 Worker 一次性注册。
	definitions() []ports.Definition
}

// Task 任务定义及提交入口，保存所属连接、handler 和执行策略。
type Task struct {
	// conn 所属连接；用于提交任务和复用实例配置。
	conn *Conn
	// definition 任务定义，包含名称、handler 与执行策略。
	definition ports.Definition
	// workflow 所属工作流；nil 表示独立任务。
	workflow *Workflow
}

// NewStandaloneTask 定义一个独立普通任务；单次运行执行一个 handler，不需要 DAG。
// 例如 handler 接收 {"message":"hello"}，一个 Run 对应一次独立任务执行。
func (c *Conn) NewStandaloneTask(name string, fn any, opts ...spec.TaskOption) *Task {
	return c.newTask(name, fn, false, nil, opts)
}

// NewStandaloneDurableTask 定义一个独立 durable 任务，支持持久化 Sleep、事件等待及稳定子调用。
// 例如先 Sleep 2 秒再等待 event-1；重启后复用已记录的等待和稳定子运行。
func (c *Conn) NewStandaloneDurableTask(name string, fn any, opts ...spec.TaskOption) *Task {
	return c.newTask(name, fn, true, nil, opts)
}

// NewStandaloneBatchTask 保留原生批量入口；handler 接收批次输入，输出按任务键映射或广播。
// 例如 MaxSize=3，把同组的三个输入作为一批；BroadcastOutput=true 时三项取得同一个批结果。
func (c *Conn) NewStandaloneBatchTask(name string, fn any, batch model.BatchConfig, opts ...spec.TaskOption) *Task {
	return c.newTask(name, fn, false, &batch, opts)
}

// newTask 创建独立任务定义，合并提供的任务选项并绑定业务 handler。
func (c *Conn) newTask(name string, fn any, durable bool, batch *model.BatchConfig, opts []spec.TaskOption) *Task {
	// p 构造当前步骤的数据对象，字段在提交、编码或断言之前一次性初始化。
	p := spec.Task{Durable: durable, Batch: batch}
	// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序。
	for _, o := range opts {
		o(&p)
	}
	return &Task{conn: c, definition: ports.Definition{Name: name, Function: fn, Policy: p}}
}

// GetName 返回注册名称，供任务提交和 Worker 定义收集使用。
func (t *Task) GetName() string {
	return t.definition.Name
}

// definitions 把当前定义展开为内部任务列表，供 Worker 一次性注册。
func (t *Task) definitions() []ports.Definition {
	return []ports.Definition{t.definition}
}

// Run 提交任务并按此入口的结果类型等待或返回执行结果；输入和执行策略共同决定调度。
// 例如 RPCInput 指定 SayHello 和 Request{Message:"hello"}，同步返回对应 protobuf envelope 的结果视图。
func (t *Task) Run(ctx context.Context, input any, opts ...RunOption) (*Result, error) {
	return t.conn.Run(ctx, t.GetName(), input, opts...)
}

// RunNoWait 提交任务后返回运行句柄，不等待业务完成；例如得到 RunID 后可另行等待或取消。
func (t *Task) RunNoWait(ctx context.Context, input any, opts ...RunOption) (*RunRef, error) {
	return t.conn.RunNoWait(ctx, t.GetName(), input, opts...)
}

// RunMany 批量提交输入；例如三条输入得到三个对应运行句柄，逐条保存各自的选项。
func (t *Task) RunMany(ctx context.Context, inputs []RunManyInput) ([]RunRef, error) {
	return t.conn.RunMany(ctx, t.GetName(), inputs)
}

// Workflow 有依赖关系的任务集合；例如 charge 完成后才调度 ship。
type Workflow struct {
	// conn 所属连接；用于提交任务和复用实例配置。
	conn *Conn
	// name 注册或查询时使用的名称；必须与提交任务的名称对应。
	name string
	// tasks 工作流中的任务定义，依赖关系由各任务的 Parents 指定。
	tasks []*Task
	// policy 任务执行策略，保存重试、并发及调度限制。
	policy spec.Task
}

// NewWorkflow 定义由多个任务及依赖关系组成的工作流。
func (c *Conn) NewWorkflow(name string, options ...spec.TaskOption) *Workflow {
	// p 构造当前步骤的数据对象，字段在提交、编码或断言之前一次性初始化。
	p := spec.Task{}
	// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序。
	for _, o := range options {
		o(&p)
	}
	return &Workflow{conn: c, name: name, policy: p}
}

// GetName 返回注册名称，供任务提交和 Worker 定义收集使用。
func (w *Workflow) GetName() string {
	return w.name
}

// NewTask 在当前工作流中加入普通任务，依赖关系由 WithParents 配置。
func (w *Workflow) NewTask(name string, fn any, opts ...spec.TaskOption) *Task {
	// t 构造原生独立任务定义，业务输入保持 JSON 语义。
	t := w.conn.NewStandaloneTask(name, fn, opts...)
	t.workflow = w
	t.definition.Workflow = w.name
	t.definition.WorkflowPolicy = w.policy
	w.tasks = append(w.tasks, t)
	return t
}

// NewDurableTask 在当前工作流中加入具备可重放等待能力的任务。
func (w *Workflow) NewDurableTask(name string, fn any, opts ...spec.TaskOption) *Task {
	// t 注册工作流内任务及依赖关系，当前步骤尚未执行业务。
	t := w.NewTask(name, fn, opts...)
	t.definition.Policy.Durable = true
	return t
}

// OnFailure 配置工作流失败处理任务，接收失败上下文完成补偿或记录。
func (w *Workflow) OnFailure(fn any) {
	// t 注册工作流内任务及依赖关系，当前步骤尚未执行业务。
	t := w.NewTask("on-failure", fn)
	t.definition.OnFailure = true
}

// definitions 把当前定义展开为内部任务列表，供 Worker 一次性注册。
func (w *Workflow) definitions() []ports.Definition {
	// out 逐项收集的定义或配置列表，保留注册顺序及每项独立策略。
	out := []ports.Definition{}
	// 逐项处理 w.tasks，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, t := range w.tasks {
		out = append(out, t.definition)
	}
	return out
}

// Run 提交任务并按此入口的结果类型等待或返回执行结果；输入和执行策略共同决定调度。
// 例如 RPCInput 指定 SayHello 和 Request{Message:"hello"}，同步返回对应 protobuf envelope 的结果视图。
func (w *Workflow) Run(ctx context.Context, input any, opts ...RunOption) (*Result, error) {
	return w.conn.Run(ctx, w.name, input, opts...)
}

// RunNoWait 提交任务后返回运行句柄，不等待业务完成；例如得到 RunID 后可另行等待或取消。
func (w *Workflow) RunNoWait(ctx context.Context, input any, opts ...RunOption) (*RunRef, error) {
	return w.conn.RunNoWait(ctx, w.name, input, opts...)
}

// WithParents 定义 DAG 依赖，父任务须先于子任务加入 Workflow。
func (t *Task) WithParents(parents ...*Task) *Task {
	// 逐项处理 parents，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, p := range parents {
		t.definition.Parents = append(t.definition.Parents, p.GetName())
	}
	return t
}
