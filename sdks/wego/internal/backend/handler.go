package backend

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"

	oldworker "github.com/hatchet-dev/hatchet/pkg/worker"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/logging"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// executionContext 绑定 wego 执行能力，并从调度元数据恢复父 trace
func (b *Backend) executionContext(ctx oldworker.HatchetContext, config spec.Runtime) context.Context {
	// e 当前执行或引擎视图，复用所属后端和实例资源，不创建另一份 durable 状态
	e := &execution{original: ctx, backend: b, workerKey: b.workerKey(ctx.WorkerId()), lifetime: ctx.GetContext()}
	// state 任务上下文能力视图，绑定当前执行和实例 logger，之后再补充借用连接与追踪器
	info := e.Info()
	state := &callctx.State{Execution: e}
	if config.LogReport {
		state.Report = func(ctx context.Context, record slog.Record) error {
			return b.logRecord(ctx, info.TaskRunID, info.RetryCount, config.LogReportTimeout, record)
		}
	}
	// 官方执行 metadata 的 traceparent 是引擎选择的父链路，不能被输入封包覆盖。
	bound := logging.WithTask(callctx.Bind(executionTraceContext(ctx.GetContext(), ctx.AdditionalMetadata()), state), info)
	if b.Bind != nil {
		return b.Bind(bound)
	}

	return bound
}

// bindResponseContract 一次固定错误出口；混合 Batch 的成功成员在输出阶段各自选择编码。
func bindResponseContract(bound context.Context, input any, batch bool) context.Context {
	// 响应模式在执行入口固定，错误和实时输出沿用同一契约。
	if !batch {
		packet, parseErr := wire.AsEnvelope(input)
		if parseErr == nil {
			return wire.WithResponse(bound, packet.Response == "wego")
		}
	} else if members, ok := input.(map[string]any); ok {
		// 全原生批次的整体业务失败使用官方错误；混合批次的成员成功结果各自编码。
		wego := false
		for _, raw := range members {
			packet, err := wire.AsEnvelope(raw)
			if err != nil || packet.Response == "wego" {
				wego = true
				break
			}
		}
		return wire.WithResponse(bound, wego)
	}
	return bound
}

// wrap 把普通任务 handler 包装为后端要求的执行入口，注入 wego 上下文并转换错误
func (b *Backend) wrap(fn any, config spec.Runtime, name, rpcMethod string, policy spec.Task, workflowName string) func(oldworker.HatchetContext, any) (any, error) {
	return func(ctx oldworker.HatchetContext, input any) (result any, err error) {
		// bound, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
		bound, cancel := context.WithCancel(b.executionContext(ctx, config))
		bound = bindResponseContract(bound, input, policy.Batch != nil)

		defer func() {
			if wire.WegoResponse(bound) {
				err = toBackendError(err)
			}
		}()
		// 解码 Filter 也可能执行下载，必须先登记取消句柄，Stop 才能终止此阶段。
		b.observer.mu.Lock()
		// 批量 handler 的 TaskRunID 可能相同或为空，取消句柄必须使用独立的执行编号
		b.observer.nextExecution++
		// executionID 当前执行的本地递增身份，登记取消回调时不与其他执行共用键
		executionID := b.observer.nextExecution
		b.observer.cancels[executionID] = executionCancel{workerID: ctx.WorkerId(), cancel: cancel}
		b.observer.mu.Unlock()
		defer func() {
			// interrupted 记录业务预算是否结束，结果和日志确认另用资源清理预算
			interrupted := bound.Err() != nil
			cancel()
			b.observer.mu.Lock()
			delete(b.observer.cancels, executionID)
			if interrupted {
				delete(b.observer.pending, attemptKey(ctx.StepRunId(), int32(ctx.RetryCount())))
			}
			b.observer.mu.Unlock()
		}()
		// 共享监听属于本次 handler；正常返回也取消剩余观察器，不继承执行器的较长资源上下文
		if state, ok := callctx.Get(bound); ok {
			// 仅 backend 自有执行视图继承 handler 生命周期，其他能力对象不被改写
			if execution, ok := state.Execution.(*execution); ok {
				execution.lifetime = bound
			}
		}
		if state, ok := callctx.Get(bound); ok {
			if execution, ok := state.Execution.(*execution); ok && len(ctx.FilterPayload()) > 0 {
				envelope, decodeErr := wire.AsEnvelope(ctx.FilterPayload())
				if decodeErr != nil {
					return nil, decodeErr
				}
				limits := config.PayloadLimits
				budget, stop := context.WithTimeout(bound, limits.DecodeTimeout)
				decodeErr = wire.DecodeValue(budget, "", envelope, &execution.filterPayload, config, limits.MaxMessageBytes, limits.MaxEncodedBytes)
				stop()
				if decodeErr != nil {
					return nil, decodeErr
				}
			}
		}
		if b.StartExecution != nil && rpcMethod == "" {
			// finish 执行观测的结束回调，业务返回时记录成功或失败并结束 span
			var finish func(error)
			bound, finish = b.StartExecution(bound, name)
			defer func() {
				finish(err)
			}()
		}

		if rpcMethod == "" {
			result, err = invokePayload(fn, bound, input, config, name, policy, workflowName)
		} else {
			result, err = invoke(fn, bound, input)
		}
		return result, err
	}
}

// invoke 通过反射创建准确的请求类型，把 JSON 输入解码后调用标准 handler，并拆分业务结果和错误
func invoke(fn any, ctx context.Context, input any) (any, error) {
	// err 接收 validateHandler 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
	if err := validateHandler(fn); err != nil {
		return nil, err
	}

	// v 在私有适配边界检查原生 handler 形状，业务签名仍保持自有类型
	v := reflect.ValueOf(fn)
	// t 检查业务 handler 或返回值的实际类型，不把任意值当作合法响应
	t := v.Type()
	// value 按 handler 的输入参数类型分配解码目标；反射值本身不持有连接或缓存
	value := reflect.New(t.In(1))
	// err 接收 convert 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
	if err := convert(input, value.Interface()); err != nil {
		return nil, err
	}

	// results 调用已校验的原生业务 handler，返回值随后转换为 wego 自有结果
	results := v.Call([]reflect.Value{reflect.ValueOf(ctx), value.Elem()})
	if !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}

	return results[0].Interface(), nil
}

// validateHandler 在注册时限定原生 handler 的签名，避免反射调用时才发现类型不匹配
func validateHandler(fn any) error {
	// v 在私有适配边界检查原生 handler 形状，业务签名仍保持自有类型
	v := reflect.ValueOf(fn)
	if !v.IsValid() || v.Kind() != reflect.Func || v.IsNil() {
		return fmt.Errorf("wego: handler must be a non-nil function")
	}

	// t 检查业务 handler 或返回值的实际类型，不把任意值当作合法响应
	t := v.Type()
	if t.NumIn() != 2 || t.NumOut() != 2 || t.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() || t.Out(1) != reflect.TypeOf((*error)(nil)).Elem() {
		return fmt.Errorf("wego: handler must be func(context.Context, Input) (Output, error)")
	}

	return nil
}
