package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/rpc"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// submit 共享提交链路；同步执行保留 deadline，异步只限制本次提交 I/O。
func (c *Conn) submit(ctx context.Context, name string, input any, synchronous bool, options ...RunOption) (_ *RunRef, err error) {
	opts, err := spec.ApplyRunOptions(options...)
	if err != nil {
		return nil, err
	}
	if opts.StreamIdempotencyKey != "" {
		return nil, fmt.Errorf("wego: stream idempotency key requires a streaming RPC")
	}
	ctx, done, err := c.engine.BeginIO(ctx, "run.submit")
	if err != nil {
		return nil, err
	}
	defer done()
	ctx, finish := c.engine.StartSpan(ctx, name, trace.SpanKindClient)
	defer func() { finish(err) }()
	ctx, err = ports.PrepareTarget(ctx, c.engine.Backend, name, "unary", "", wire.ValueType(input))
	if err != nil {
		return nil, err
	}
	encodingContext := ctx
	if !synchronous {
		encodingContext = wire.ForTrigger(ctx)
	}
	target, encoded, err := rpc.Input(encodingContext, c.engine.Config, name, input, opts)
	if err != nil {
		return nil, err
	}
	ref, err := c.engine.Backend.Run(ctx, target, encoded, opts)
	if err != nil {
		return nil, err
	}
	return &RunRef{RunID: ref.ID, InputIndex: -1, wait: ref.Wait, engine: c.engine, method: name}, nil
}

// RunNoWait 返回引擎确认的身份，调用 context 不绑定后续执行或结果等待。
func (c *Conn) RunNoWait(ctx context.Context, name string, input any, options ...RunOption) (*RunRef, error) {
	return c.submit(ctx, name, input, false, options...)
}

// Run 拥有同步调用的取消责任；durable 父执行中断不取消已记录的子任务。
func (c *Conn) Run(ctx context.Context, name string, input any, options ...RunOption) (*Result, error) {
	ctx, done, err := c.engine.BeginIO(ctx, "run.synchronous")
	if err != nil {
		return nil, err
	}
	defer done()
	ref, err := c.submit(ctx, name, input, true, options...)
	if err != nil {
		return nil, err
	}
	result, err := ref.Result(ctx)
	state, _ := callctx.Get(ctx)
	durable := state != nil && state.Execution != nil && state.Execution.Info().Durable
	if err != nil && ctx.Err() != nil && !durable {
		budget, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		cancelErr := ref.Cancel(budget)
		cancel()
		return nil, errors.Join(err, cancelErr)
	}
	return result, err
}

// RunMany 全批完成本地校验后才执行 codec；部分成功时保留已证明的输入位置。
func (c *Conn) RunMany(ctx context.Context, name string, inputs []RunManyInput) ([]RunRef, error) {
	if len(inputs) == 0 {
		return []RunRef{}, nil
	}
	ctx, done, err := c.engine.BeginIO(ctx, "run.bulk")
	if err != nil {
		return nil, err
	}
	defer done()
	compiled := make([]model.RunManyInput, len(inputs))
	snapshots := make([]wire.Envelope, len(inputs))
	for i, item := range inputs {
		envelope, opts, err := prepareBulkInput(ctx, c.engine.Config, name, item)
		if err != nil {
			return nil, fmt.Errorf("wego: input %d: %w", i, err)
		}
		snapshots[i], compiled[i].Options = envelope, opts
	}
	ctx, err = ports.PrepareTarget(ctx, c.engine.Backend, name, "unary", "", inputTypes(snapshots)...)
	if err != nil {
		return nil, err
	}
	limits := c.engine.Config.PayloadLimits
	for i, value := range snapshots {
		budget, cancel := context.WithTimeout(ctx, limits.EncodeTimeout)
		envelope, err := wire.Seal(budget, value, c.engine.Config, limits.MaxMessageBytes, limits.MaxMessageBytes+(64<<10), limits.MaxEncodedBytes)
		cancel()
		if err != nil {
			return nil, err
		}
		compiled[i].Input = envelope
	}
	refs, submitErr := c.engine.Backend.RunMany(ctx, name, compiled)
	out := make([]RunRef, len(refs))
	for i, ref := range refs {
		out[i] = RunRef{RunID: ref.ID, InputIndex: ref.InputIndex, wait: ref.Wait, engine: c.engine, method: name}
	}
	return out, submitErr
}

// prepareBulkInput 只进行本地参数与序列化检查，整批成功后才允许 codec I/O。
func prepareBulkInput(ctx context.Context, config spec.Runtime, name string, item RunManyInput) (wire.Envelope, model.RunOptions, error) {
	opts, err := spec.ApplyRunOptions(item.Options...)
	if err != nil {
		return wire.Envelope{}, model.RunOptions{}, err
	}
	if opts.StreamIdempotencyKey != "" {
		return wire.Envelope{}, model.RunOptions{}, fmt.Errorf("wego: bulk input uses stream-only option")
	}
	value, defaults, err := wire.InputValue(item.Input)
	if err != nil {
		return wire.Envelope{}, model.RunOptions{}, err
	}
	routing := map[string]any{}
	for key, item := range defaults {
		routing[key] = item
	}
	for key, item := range opts.Routing {
		routing[key] = item
	}
	merged, err := config.Routing(name, routing)
	if err != nil {
		return wire.Envelope{}, model.RunOptions{}, err
	}
	data, typeName, err := wire.MarshalValue(value, config.PayloadLimits.MaxMessageBytes)
	if err != nil {
		return wire.Envelope{}, model.RunOptions{}, err
	}
	method := ""
	if strings.HasPrefix(name, "/") {
		method = name
		if typeName == "" {
			return wire.Envelope{}, model.RunOptions{}, fmt.Errorf("wego: RPC input must be protobuf")
		}
	}
	envelope := wire.NewEnvelope(wire.ForTrigger(ctx), method, typeName, data, merged)
	return envelope, opts, nil
}

// inputTypes 从已完成序列化的批次读取类型约束，不再次访问可变业务对象。
func inputTypes(inputs []wire.Envelope) []string {
	types := make([]string, len(inputs))
	for index, input := range inputs {
		types[index] = input.Type
	}
	return types
}
