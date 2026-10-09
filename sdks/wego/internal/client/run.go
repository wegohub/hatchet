package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/rpc"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// RunOption 构造期配置函数或自有别名，按传入顺序合并到对应配置
type RunOption func(*model.RunOptions)

// WithRunKey 设置此次运行的键，与定义级幂等表达式分别配置
func WithRunKey(v string) RunOption {
	return func(o *model.RunOptions) {
		o.Key = &v
	}
}

// WithRunSticky 设置此次调用的粘性调度请求
func WithRunSticky(v bool) RunOption {
	return func(o *model.RunOptions) {
		o.Sticky = &v
	}
}

// WithRunPriority 设置此次提交的调度优先级，指针保留显式零值
// 例如显式优先级 0 通过指针保留，不能被当成没有设置而丢弃
func WithRunPriority(v int) RunOption {
	return func(o *model.RunOptions) {
		o.Priority = &v
	}
}

// WithRunMetadata 设置此次运行的附加 metadata，供查询和追踪关联使用
// 例如 {"case":"case-1"} 随该运行保存，日志或结果查询可据此关联当前验收
func WithRunMetadata(v map[string]string) RunOption {
	return func(o *model.RunOptions) {
		o.Metadata = v
	}
}

// WithDesiredWorkerLabels 设置运行亲和性要求；如 owner 标签 Required=true 可禁止跨实例调度
func WithDesiredWorkerLabels(v map[string]*model.DesiredWorkerLabel) RunOption {
	return func(o *model.RunOptions) {
		o.Labels = v
	}
}

// RunManyInput 批量提交中的一项输入及其独立选项
type RunManyInput struct {
	// Input 提交给任务的业务输入；RPC 入口使用 protobuf envelope
	Input any
	// Options 本次操作的配置选项，按调用顺序合并
	Options []RunOption
}

// RunRef 只保存运行标识和结果读取能力，不持有独立连接
type RunRef struct {
	// RunID 工作流运行身份，例如 run-001，用于等待、查询和取消
	RunID string
	// wait 在给定 context 的预算内等待任务结果的函数
	wait func(context.Context) (ports.Result, error)
	// engine 本实例的执行引擎，协调调用、Worker 与资源关闭
	engine *engine.Engine
	// method 当前 RPC 方法或绑定信息；完整方法名用于查询投影、handler 与任务名称
	method string
}

// Result 等待同一个运行；等待 context 主动取消时也请求取消远端运行，其他观察者会受到影响
// 未传入 context 时使用后台上下文，业务错误不会被误判为观察者主动取消
func (r *RunRef) Result(contexts ...context.Context) (*Result, error) {
	// ctx 选取不带业务 deadline 的资源上下文，具体清理步骤另有明确预算
	ctx := context.Background()
	if len(contexts) > 0 {
		ctx = contexts[0]
	}
	// ctx, done, err 接收 r.engine.Begin 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	ctx, done, err := r.engine.BeginIO(ctx, "result.wait")
	if err != nil {
		return nil, err
	}

	defer done()

	// result, err 接收 r.wait 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	result, err := r.wait(ctx)
	if err != nil {
		if ctx.Err() != nil && r.RunID != "" {
			budget, stop := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			cancelErr := r.engine.Backend.Feature(budget, ports.RunsCancel{Request: map[string]any{"externalIds": []string{r.RunID}}}, nil)
			stop()
			if cancelErr != nil {
				r.engine.Config.Logger.Warn("result wait cancellation request failed", "run_id", r.RunID, "error", cancelErr)
			}
		}
		return nil, err
	}

	return &Result{
		RunID:   result.RunID,
		outputs: result.Outputs,
		engine:  r.engine,
		method:  r.method,
	}, nil
}

// Result wego 自有结果视图，可按任务选择并解码业务输出
type Result struct {
	// RunID 工作流运行身份，例如 run-001，用于等待、查询和取消
	RunID string
	// outputs 任务名称到已转换输出的映射，按任务名读取对应数据
	outputs map[string]any
	// value 当前选定的业务输出；必须完成解码后再交给调用方
	value any
	// selected 是否已经选择具体任务输出，防止多任务结果被误当作单个值
	selected bool
	// found 单独记录键存在性，合法 null 的 found 仍为 true
	found bool
	// engine 本实例的执行引擎，协调调用、Worker 与资源关闭
	engine *engine.Engine
	// method 当前 RPC 方法或绑定信息；完整方法名用于查询投影、handler 与任务名称
	method string
}

// TaskOutput 按任务名称选择输出；例如 DAG 包含 charge、ship 时读取 charge 的输出
func (r *Result) TaskOutput(name string) *Result {
	// value, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值
	value, ok := r.outputs[name]
	// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值
	if !ok {
		value, ok = r.outputs[strings.ToLower(name)]
	}
	return &Result{
		RunID:    r.RunID,
		value:    value,
		engine:   r.engine,
		method:   r.method,
		selected: true,
		found:    ok,
	}
}

// Into 根据目标类型还原结果：protobuf 使用协议及载荷变换，其他类型使用 JSON
func (r *Result) Into(target any) error {
	// 默认解码预算与实例清理预算一致，对象下载不能无限等待
	timeout := 30 * time.Second
	if r.engine != nil && r.engine.Config.Shutdown.Timeout > 0 {
		timeout = r.engine.Config.Shutdown.Timeout
	}
	// ctx 给没有显式预算的调用提供有限 deadline
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return r.IntoContext(ctx, target)
}

// IntoContext 在调用方预算内解码结果；载荷 I/O 纳入 Conn 排空和取消
// 例如对象存储下载设置 2 秒预算，到期会取消下载而不阻止其他结果读取
func (r *Result) IntoContext(ctx context.Context, target any) error {
	// 已到期的调用不开始载荷下载或修改解码目标，JSON 与 protobuf 使用相同规则
	if err := ctx.Err(); err != nil {
		return err
	}

	// value 当前已选定的结果值；按任务名选择后再解码，避免把整个 DAG 映射当作一个响应
	value := r.value
	if r.selected && !r.found {
		return fmt.Errorf("wego: task output is missing")
	}
	if !r.selected {
		// err 当前操作产生的错误；nil 表示该步骤成功
		var err error
		value, err = rpc.SingleOutput(r.outputs)
		if err != nil {
			return err
		}
	}
	// message 为 protobuf 解码目标，原生 JSON 结果走另一条明确解码路径
	if message, ok := target.(proto.Message); ok {
		// envelope, err 接收 wire.AsEnvelope 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		envelope, err := wire.AsEnvelope(value)
		if err != nil {
			return err
		}

		// 纯内存解码可读取已取得的结果；带 payload 变换的调用必须登记生命周期
		managed := false
		// option 只要包含载荷变换，就可能执行外部 I/O，需登记实例生命周期
		for _, option := range r.engine.Config.Middleware {
			if option.Payload != nil {
				managed = true
				break
			}
		}
		if managed {
			// done 直到对象读取及所有反向变换结束才释放，不沿用 Result 返回时已取消的上下文
			budget, done, err := r.engine.BeginIO(ctx, "result.decode")
			if err != nil {
				return err
			}
			defer done()
			ctx = budget
		}
		return wire.Decode(ctx, r.method, envelope, message, r.engine.Config.Middleware, r.engine.Config.Stream.MaxMessageBytes)
	}

	return rpc.DecodeJSON(value, target)
}

// Raw 返回结果的独立副本，调用方修改它不会改变句柄内部状态
func (r *Result) Raw() map[string]any {
	// out 当前结果的按键映射，逐项写入转换后的输出；不会把后端对象直接放入公开返回值
	out := map[string]any{}
	_ = rpc.DecodeJSON(r.outputs, &out)
	return out
}
