package task

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// execution 取出当前任务的执行能力；普通网络上下文返回 ErrTaskContext
func execution(ctx context.Context) (ports.Execution, error) {
	// s, ok 获取任务上下文能力与存在标记；普通网络入口没有任务身份，必须检查后再使用
	s, ok := callctx.Get(ctx)
	if !ok || s.Execution == nil {
		return nil, model.ErrTaskContext
	}

	return s.Execution, nil
}

// Info 读取当前任务或实例的信息；任务身份只存在于 Worker 入口
func Info(ctx context.Context) (model.TaskInfo, bool) {
	// e, err 获取任务上下文能力与存在标记；普通网络入口没有任务身份，必须检查后再使用
	e, err := execution(ctx)
	if err != nil {
		return model.TaskInfo{}, false
	}

	return e.Info(), true
}

// Conn 返回当前实例的借用连接，可调用子任务；Close 和 Shutdown 返回 ErrBorrowedResource。
func Conn(ctx context.Context) (*client.Conn, error) {
	// s, ok 获取任务上下文能力与存在标记；普通网络入口没有任务身份，必须检查后再使用
	s, ok := callctx.Get(ctx)
	// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值
	if !ok {
		return nil, model.ErrTaskContext
	}

	// c, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值
	c, ok := s.Borrowed.(*client.Conn)
	// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值
	if !ok {
		return nil, model.ErrTaskContext
	}

	return c, nil
}

// Tracer 返回本实例追踪器；无任务观测上下文时使用 noop 追踪器
func Tracer(ctx context.Context) trace.Tracer {
	// s, ok 获取任务上下文能力与存在标记；普通网络入口没有任务身份，必须检查后再使用
	s, ok := callctx.Get(ctx)
	if ok && s.Tracer != nil {
		return s.Tracer
	}

	return noop.NewTracerProvider().Tracer("wego")
}

// WithChildKey 为子任务指定稳定身份；durable 重放时相同键复用已有子运行
// 例如两次重放均使用 key="step-1"，查找的是相同的子调用位置而不是创建另一条子运行
func WithChildKey(ctx context.Context, key string) context.Context {
	return callctx.WithChildKey(ctx, key)
}

// NonRetryable 给非 nil 错误添加禁止重试标记；nil 继续表示成功
// 例如参数校验失败包装为 NonRetryable(err)，即使任务配置 Retries=3 也不能重新执行该失败
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}

	return &model.NonRetryableError{Err: err}
}

// Sleep 使用引擎记录的 durable 等待，重放时复用原等待序号
func Sleep(ctx context.Context, d time.Duration) error {
	// e, err 获取任务上下文能力与存在标记；普通网络入口没有任务身份，必须检查后再使用
	e, err := execution(ctx)
	if err != nil {
		return err
	}

	return e.Sleep(ctx, d)
}

// WaitForEvent 等待指定事件键及 CEL 条件匹配的事件；支持 scope 和回溯时间
// 例如 key="event-1"、scope="scope-a"，scope-b 的同键事件不能满足此等待
func WaitForEvent(ctx context.Context, key, expression string, opts ...EventWaitOption) (map[string]any, error) {
	// e, err 获取任务上下文能力与存在标记；普通网络入口没有任务身份，必须检查后再使用
	e, err := execution(ctx)
	if err != nil {
		return nil, err
	}

	// 每次调用独立解析选项；不复用其他等待的条件或序号。
	options, err := eventWaitOptions(opts)
	if err != nil {
		return nil, err
	}
	return e.WaitEvent(ctx, key, expression, options)
}

// Now 返回可重放的记录时间；同一 durable 执行中的后续读取复用该值
// 例如首次记录的时间为 T，重放对应读取仍得到 T，不能改为重启时的新时间
func Now(ctx context.Context) (time.Time, error) {
	// e, err 获取任务上下文能力与存在标记；普通网络入口没有任务身份，必须检查后再使用
	e, err := execution(ctx)
	if err != nil {
		return time.Time{}, err
	}

	return e.Now(ctx)
}

// StreamEvent 发布业务任务的原生流事件，载荷仍由调用方提供字节
func StreamEvent(ctx context.Context, payload []byte) error {
	// e, err 获取任务上下文能力与存在标记；普通网络入口没有任务身份，必须检查后再使用
	e, err := execution(ctx)
	if err != nil {
		return err
	}

	return e.Stream(ctx, payload)
}

// ParentOutput 按父任务名称解码已完成的父输出，避免把整个结果映射作为单个业务值
func ParentOutput(ctx context.Context, name string, out any) error {
	// e, err 获取任务上下文能力与存在标记；普通网络入口没有任务身份，必须检查后再使用
	e, err := execution(ctx)
	if err != nil {
		return err
	}

	return e.ParentOutput(ctx, name, out)
}

// WaitForEventInto 用单次 durable 等待还原任意业务目标；例如事件中的 protobuf bytes 保持原值。
func WaitForEventInto(ctx context.Context, key, expression string, target any, opts ...EventWaitOption) error {
	execution, err := execution(ctx)
	if err != nil {
		return err
	}
	capability, ok := execution.(interface {
		// WaitEventInto 使用现有持久等待并在本次预算内解码。
		WaitEventInto(context.Context, string, string, any, model.EventWait) error
	})
	if !ok {
		return model.ErrDurableContext
	}
	options, err := eventWaitOptions(opts)
	if err != nil {
		return err
	}
	return capability.WaitEventInto(ctx, key, expression, target, options)
}
