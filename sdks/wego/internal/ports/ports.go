package ports

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// Definition 任务定义或收集定义的契约，供注册层展开为后端可执行入口
type Definition struct {
	// Name 资源或任务名称，注册与调用必须使用相同值
	Name string
	// RPCMethod 显式标记 protobuf RPC；原生 JSON 定义为空
	RPCMethod string
	// RPCShape 保存 unary、client、server 或 bidi，避免同方法错误地混用输入形态
	RPCShape string
	// RPCMode 固定流的 reliable/realtime 输出协议；unary 为空
	RPCMode string
	// Function 业务 handler 或验收片段的函数名
	Function any
	// Policy 任务级执行策略
	Policy spec.Task
	// Workflow 所属工作流名称，独立任务不要求工作流依赖
	Workflow string
	// WorkflowPolicy 工作流层面的调度策略
	WorkflowPolicy spec.Task
	// Parents 父任务名称集合，用于定义 DAG 的先后执行关系
	Parents []string
	// OnFailure 是否作为失败处理任务，在工作流失败时执行
	OnFailure bool
	// BeforeStart 为可靠输出建立 CLAIM；确认之前不启动业务或报告 START
	// 只支持普通 standalone，不支持 durable、batch 或失败任务
	BeforeStart func(context.Context, StartInfo) (Admission, error)
}

// StartInfo 保存私有分发动作的自有快照，业务无法取得官方类型
type StartInfo struct {
	// Task 携带真实运行身份和重试次数
	Task model.TaskInfo
	// WorkerKey 是随机进程逻辑身份，与展示名称无关
	WorkerKey string
	// Input 是任务 JSON 原文，供 CLAIM 校验输入身份
	Input json.RawMessage
}

// Admission 表示持久门禁的明确结论，落选或不明确不能伪报成功
type Admission struct {
	// Owned 仅在读回本次规范 CLAIM 后为 true
	Owned bool
	// Writer 是持久门禁确认的本次 nonce，定向取消必须同时匹配它
	Writer string
	// Context 可增加有限历史恢复能力；nil 使用门禁原上下文
	Context context.Context
}

// Result wego 自有结果视图，可按任务选择并解码业务输出
type Result struct {
	// RunID 工作流运行身份，例如 run-001，用于等待、查询和取消
	RunID string
	// Outputs 任务名到输出值的映射；多任务结果须按名字选择
	Outputs map[string]any
}

// Run 内部运行句柄，向上层提供自有身份和等待结果
type Run struct {
	// ID 当前资源或运行身份
	ID string
	// InputIndex 保存成功运行对应的原始批量输入下标，跳过失败分块也不串结果
	InputIndex int
	// Wait 在指定上下文预算内等待此运行的最终结果
	Wait func(context.Context) (Result, error)
}

// Worker Worker 生命周期或内部端口契约，封装注册、消费、暂停和关闭
type Worker interface {
	// Start 启动 Worker 入口；运行期间的异步错误通过错误通道报告
	Start(context.Context) error
	// Errors 返回 Worker 异步错误通道
	Errors() <-chan error
	// WaitReady 等待 Worker 注册并具备消费能力；context 到期时返回等待错误
	WaitReady(context.Context) error
	// Pause 停止新增任务分配，已有执行和控制消息按生命周期继续处理
	Pause(context.Context) error
	// Close 释放此对象拥有的资源；借用资源必须由所有者关闭
	Close(context.Context) error
	// ID 返回资源的注册身份；未找到合法身份时返回空字符串
	ID() string
}

// Backend 后端适配边界；Hatchet 实现只在 internal/backend 中使用
type Backend interface {
	// Run 提交任务并按此入口的结果类型等待或返回执行结果；输入和执行策略共同决定调度
	Run(context.Context, string, any, model.RunOptions) (Run, error)
	// RunMany 批量提交输入；例如三条输入得到三个对应运行句柄，逐条保存各自的选项
	RunMany(context.Context, string, []model.RunManyInput) ([]Run, error)
	// Worker 根据内部定义创建 Worker；实例事件订阅复用连接，不创建控制任务
	Worker(context.Context, string, []Definition, spec.Runtime, func(context.Context, any)) (Worker, error)
	// Feature 执行类型化管理命令并写入自有结果目标，不泄漏后端 DTO
	Feature(context.Context, FeatureRequest, any) error
	// Stream 订阅运行输出或发布执行输出，传递字节与订阅错误
	Stream(context.Context, string, func(string) error) error
	// Publish 向任务运行发布流字节，调用返回发布是否被接受
	Publish(context.Context, string, []byte) error
	// Close 释放此对象拥有的资源；借用资源必须由所有者关闭
	Close() error
}

// Execution 任务执行能力接口，网络 handler 不自动拥有 durable 或父子身份
type Execution interface {
	// Info 读取当前任务或实例的信息；任务身份只存在于 Worker 入口
	Info() model.TaskInfo
	// Sleep 使用 durable 记录执行等待，重放复用原等待序号而非再次开始计时
	Sleep(context.Context, time.Duration) error
	// WaitEvent 将 durable 事件等待映射到后端条件监听器，保留原执行的等待序号
	WaitEvent(context.Context, string, string, model.EventWait) (map[string]any, error)
	// Now 读取可重放时间；重放读取对应的已记录值，避免业务使用新的墙上时钟
	Now(context.Context) (time.Time, error)
	// Refresh 向后端延长当前任务执行预算，调用失败必须结束续期
	Refresh(context.Context, time.Duration) error
	// Stream 订阅运行输出或发布执行输出，传递字节与订阅错误
	Stream(context.Context, []byte) error
	// ParentOutput 按父任务名称解码已完成的父输出，避免把整个结果映射作为单个业务值
	ParentOutput(string, any) error
}

// RPCInput 明确标记 RPC 输入，普通 JSON 对象不因 method 字段而改变调度
type RPCInput interface {
	// RPCMethod 返回生成桩的完整方法名
	RPCMethod() string
	// RPCShape 返回本次调用的标准 RPC 形态
	RPCShape() string
	// RPCMode 返回流模式，unary 为空
	RPCMode() string
}

// WorkerIdentity 提供内部实例身份供事件装配，不暴露官方对象
type WorkerIdentity interface {
	// WorkerKey 返回本次创建的随机 UUID
	WorkerKey() string
	// InstanceName 返回完整展示名称，与寻址身份无关
	InstanceName() string
}
