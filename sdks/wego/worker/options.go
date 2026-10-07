package worker

import (
	"context"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// Config 当前包的配置集合，分离实例资源配置与业务执行策略。
type Config struct {
	// Defaults Worker 全部方法共享的默认任务策略。
	Defaults spec.Task
	// Methods 完整方法名到任务策略的映射，用于覆盖默认值。
	Methods map[string]spec.Task
	// PanicHandler 业务 panic 回调，在对应任务上下文中执行。
	PanicHandler func(context.Context, any)
}

// Option 配置函数或自有配置视图；按传入顺序应用，相同字段后者覆盖前者。
type Option func(*Config)

// WithTaskDefaults 配置此 Worker 的默认任务策略，具体方法配置可覆盖显式字段。
func WithTaskDefaults(options ...task.Option) Option {
	return func(c *Config) {
		// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序。
		for _, o := range options {
			o(&c.Defaults)
		}
	}
}

// WithTask 为指定完整 RPC 方法配置普通任务策略。
// 例如只为 /wego.example.v1.UnaryGreeter/SayHello 设置 Retries=2，不影响同服务的其他方法。
func WithTask(method string, options ...task.Option) Option {
	return methodOption(method, false, options)
}

// WithDurableTask 为指定完整 RPC 方法配置 durable 执行能力及任务策略。
// 例如 WaitHello 使用 Sleep(ctx, 2*time.Second) 后，再通过稳定 child key 提交子任务。
func WithDurableTask(method string, options ...task.Option) Option {
	return methodOption(method, true, options)
}

// methodOption 把方法配置合并到 Worker 默认策略之上，显式零值必须覆盖默认值。
func methodOption(method string, durable bool, options []task.Option) Option {
	return func(c *Config) {
		// p 构造当前步骤的数据对象，字段在提交、编码或断言之前一次性初始化。
		p := spec.Task{Durable: durable}
		// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序。
		for _, o := range options {
			o(&p)
		}
		c.Methods[method] = p
	}
}

// WithPanicHandler 设置业务 panic 的观测回调，回调接收标准 context 和 panic 值。
func WithPanicHandler(handler func(context.Context, any)) Option {
	return func(c *Config) {
		c.PanicHandler = handler
	}
}
