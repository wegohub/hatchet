package client

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/rpc"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// RunNoWait 在引擎接受调度后返回句柄，不等待业务执行完成
// 例如先保存 run-001，再用 Result(ctx) 等待；提交返回不表示业务已经执行完毕
func (c *Conn) RunNoWait(ctx context.Context, name string, input any, options ...RunOption) (*RunRef, error) {
	// ctx, done, err 登记本次调用并取得结束回调，返回前释放登记以允许实例排空
	ctx, done, err := c.engine.Begin(ctx)
	if err != nil {
		return nil, err
	}

	defer done()

	// method 本次调用用于 trace、投影及任务路由的方法名；RPCInput 提供完整方法时覆盖普通任务名
	method := name
	// value 提供完整 RPC 方法名，任务命名和结果解码据此采用 protobuf 路径
	if value, ok := input.(model.RPCInput); ok && value.Method != "" {
		method = value.Method
	}
	// ctx, finish 开始属于当前实例的 span，结束时按实际执行错误标记状态
	ctx, finish := c.engine.StartSpan(ctx, method, trace.SpanKindClient)
	defer func() {
		finish(err)
	}()

	name, input, err = rpc.Input(ctx, c.engine.Config, name, input)
	if err != nil {
		return nil, err
	}

	// opts 本次任务提交的 wego 运行选项，包含键、优先级、metadata 和亲和性
	opts := model.RunOptions{}
	// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序
	for _, o := range options {
		o(&opts)
	}
	// ref, err 接收 c.engine.Backend.Run 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	ref, err := c.engine.Backend.Run(ctx, name, input, opts)
	if err != nil {
		return nil, err
	}

	return &RunRef{
		RunID:  ref.ID,
		wait:   ref.Wait,
		engine: c.engine,
		method: method,
	}, nil
}

// Run 使用调用方上下文等待结果；关闭实例时该等待计入排空范围
// 例如 RPCInput 指定 SayHello 和 Request{Message:"hello"}，同步返回对应 protobuf envelope 的结果视图
func (c *Conn) Run(ctx context.Context, name string, input any, options ...RunOption) (*Result, error) {
	// ctx, done, err 登记本次调用并取得结束回调，返回前释放登记以允许实例排空
	ctx, done, err := c.engine.Begin(ctx)
	if err != nil {
		return nil, err
	}

	defer done()

	// ref, err 接收 c.RunNoWait 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	ref, err := c.RunNoWait(ctx, name, input, options...)
	if err != nil {
		return nil, err
	}

	return ref.Result(ctx)
}

// RunMany 批量提交同一目标的输入，返回顺序与输入顺序一致
// 例如输入 [A,B,C] 使用各自的优先级和 metadata，返回对应的三个运行句柄
func (c *Conn) RunMany(ctx context.Context, name string, inputs []RunManyInput) ([]RunRef, error) {
	// ctx, done, err 登记本次调用并取得结束回调，返回前释放登记以允许实例排空
	ctx, done, err := c.engine.Begin(ctx)
	if err != nil {
		return nil, err
	}

	defer done()

	// compiled 分配当前步骤的结果列表，容量只用于聚合，不代表业务已经成功完成
	compiled := make([]model.RunManyInput, len(inputs))
	// target 当前步骤使用的字符串值 ""，用于路由、请求或断言
	target := ""
	// 逐项处理 inputs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for i, v := range inputs {
		// n, data, err 接收 rpc.Input 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		n, data, err := rpc.Input(ctx, c.engine.Config, name, v.Input)
		if err != nil {
			return nil, err
		}
		if target != "" && n != target {
			return nil, fmt.Errorf("wego: batch targets must agree")
		}

		target = n
		// opts 本次任务提交的 wego 运行选项，包含键、优先级、metadata 和亲和性
		opts := model.RunOptions{}
		// 逐项处理 v.Options，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for _, o := range v.Options {
			o(&opts)
		}
		compiled[i] = model.RunManyInput{Input: data, Options: opts}
	}
	if len(inputs) == 0 {
		return []RunRef{}, nil
	}

	// refs, err 接收 c.engine.Backend.RunMany 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	refs, submitErr := c.engine.Backend.RunMany(ctx, target, compiled)

	// out 分配当前步骤的结果列表，容量只用于聚合，不代表业务已经成功完成
	out := make([]RunRef, len(refs))
	// 逐项处理 refs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for i, r := range refs {
		// method 本次调用用于 trace、投影及任务路由的方法名；RPCInput 提供完整方法时覆盖普通任务名
		method := name
		// value 是当前批量条目的 RPC 输入，方法身份不能被相邻条目覆盖
		if value, ok := inputs[r.InputIndex].Input.(model.RPCInput); ok && value.Method != "" {
			method = value.Method
		}
		out[i] = RunRef{
			RunID:  r.ID,
			wait:   r.Wait,
			engine: c.engine,
			method: method,
		}
	}
	return out, submitErr
}
