package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"reflect"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// workerConfig 原生 Worker 构造期配置，收集 Runtime、任务定义和 panic 回调。
type workerConfig struct {
	// runtime 实例运行配置，与单个任务的执行策略分离。
	runtime spec.Runtime
	// definitions 交给 Worker 注册的任务定义集合。
	definitions []ports.Definition
	// panicHandler 接收业务 panic 的回调；执行上下文用于定位对应运行。
	panicHandler func(context.Context, any)
}

// WorkerOption 构造期配置函数或自有别名，按传入顺序合并到对应配置。
type WorkerOption func(*workerConfig)

// WithWorkflows 收集当前 Worker 需要注册的独立任务和工作流定义。
func WithWorkflows(definitions ...Definition) WorkerOption {
	return func(c *workerConfig) {
		// 逐项处理 definitions，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
		for _, d := range definitions {
			c.definitions = append(c.definitions, d.definitions()...)
		}
	}
}

// WithWorkerRuntime 仅调整原生 Worker 的容量、标签和日志。
// 连接、namespace、投影、载荷和实例关闭配置由 Conn 统一拥有；普通字段重复设置相同值允许；连接能力对象不允许替换。
func WithWorkerRuntime(opts ...runtime.Option) WorkerOption {
	return func(c *workerConfig) {
		// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序。
		for _, o := range opts {
			o(&c.runtime)
		}
	}
}

// WithPanicHandler 设置业务 panic 的观测回调，回调接收标准 context 和 panic 值。
func WithPanicHandler(handler func(context.Context, any)) WorkerOption {
	return func(c *workerConfig) {
		c.panicHandler = handler
	}
}

// Worker 是原生任务的运行句柄，与创建它的 Conn 共用实例资源。
type Worker struct {
	// native 封装的内部 Worker；通过适配层管理启动和关闭。
	native ports.Worker
	// conn 所属连接；用于提交任务和复用实例配置。
	conn *Conn
}

// NewWorker 合并此 Worker 的 Runtime 和定义，验证容量及 DisableWorker 冲突后创建内部 Worker。
func (c *Conn) NewWorker(name string, options ...WorkerOption) (*Worker, error) {
	// config 当前实例使用的配置快照。
	config := workerConfig{runtime: c.engine.Config.Clone()}
	// inheritedTLS 是局部 TLS 容器的身份，公开选项只允许替换配置，不能修改 Conn 的连接能力。
	inheritedTLS := config.runtime.TLS
	// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序。
	for _, o := range options {
		o(&config)
	}
	if config.runtime.DisableWorker {
		return nil, fmt.Errorf("wego: Worker creation conflicts with DisableWorker")
	}
	// Conn 统一拥有连接及业务协议，Worker 局部配置不能被静默忽略。
	if err := validateWorkerRuntime(c.engine.Config, config.runtime, inheritedTLS); err != nil {
		return nil, err
	}
	// err 接收 config.runtime.Validate 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
	if err := config.runtime.Validate(); err != nil {
		return nil, err
	}
	if len(config.definitions) == 0 {
		return nil, fmt.Errorf("wego: no task definitions")
	}

	// w, err 接收 c.engine.Worker 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	w, err := c.engine.Worker(context.Background(), name, config.definitions, config.runtime, config.panicHandler)
	if err != nil {
		return nil, err
	}

	return &Worker{native: w, conn: c}, nil
}

// Start 注册监听器后返回；清理函数使用实例的关闭策略排空任务。
func (w *Worker) Start() (func() error, error) {
	return w.start(context.Background())
}

// start 在指定预算内建立监听，成功后的资源仍按实例关闭策略排空。
func (w *Worker) start(ctx context.Context) (func() error, error) {
	// err 保存入口启动结果，取消发生在建立监听阶段时也立即返回。
	if err := w.native.Start(ctx); err != nil {
		return nil, err
	}

	return func() error {
		// ctx 选取不带业务 deadline 的资源上下文，具体清理步骤另有明确预算。
		ctx := context.Background()
		// cancel 取消当前对象所属的执行上下文。
		cancel := func() {}
		if w.conn.engine.Config.Shutdown.Mode == spec.DrainWithTimeout {
			ctx, cancel = context.WithTimeout(ctx, w.conn.engine.Config.Shutdown.Timeout)
		}
		defer cancel()

		return w.Shutdown(ctx)
	}, nil
}

// StartBlocking 启动 Worker 并等待上下文取消或入口错误，再按关闭预算释放资源。
func (w *Worker) StartBlocking(ctx context.Context) error {
	// cleanup 与 err 在调用方预算内启动；尚未就绪时取消也必须结束真实初始化。
	cleanup, err := w.start(ctx)
	if err != nil {
		return err
	}

	// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
	select {
	case <-ctx.Done():
		return cleanup()
	// err 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功。
	case err := <-w.native.Errors():
		return errors.Join(err, cleanup())
	}
}

// WaitReady 等待 Worker 注册并具备消费能力；context 到期时返回等待错误。
func (w *Worker) WaitReady(ctx context.Context) error {
	return w.native.WaitReady(ctx)
}

// Shutdown 只关闭当前 Worker，预算由调用方传入；共享连接仍由 Conn 管理。
// 例如调用方只有 100ms 预算，超时后取消剩余业务，连接和结果确认另用配置的清理预算。
func (w *Worker) Shutdown(ctx context.Context) error {
	return w.native.Close(ctx)
}

// ID 返回资源的注册身份；未找到合法身份时返回空字符串。
func (w *Worker) ID() string {
	return w.native.ID()
}

// validateWorkerRuntime 拒绝不属于原生 Worker 的选项，防止创建成功却使用另一份连接或协议。
// 例如 Conn namespace=first，Worker 设置 second 时直接报错，而不是仍向 first 注册任务。
func validateWorkerRuntime(base, local spec.Runtime, inheritedTLS *tls.Config) error {
	// connectionChanged 检查授权、地址及命名空间，不能成功注册到另一份实例配置。
	connectionChanged := base.Token != local.Token || base.Address != local.Address ||
		base.ServerURL != local.ServerURL || base.TenantID != local.TenantID ||
		base.Namespace != local.Namespace || base.TLSSet != local.TLSSet || local.TLS != inheritedTLS
	// protocolChanged 检查调度投影与实例生命周期，原生 Worker 不能改写 Conn 协议。
	protocolChanged := base.ControlSlots != local.ControlSlots || base.Shutdown != local.Shutdown ||
		base.ResultPollInterval != local.ResultPollInterval ||
		base.Stream != local.Stream || !reflect.DeepEqual(base.Embedded, local.Embedded) ||
		!reflect.DeepEqual(base.Projections, local.Projections)
	// capabilitiesChanged 拦截器、载荷和观测链仍由共享实例管理。
	capabilitiesChanged := base.Telemetry.Trace.DisableWorkerExporter != local.Telemetry.Trace.DisableWorkerExporter ||
		base.Telemetry.Metrics != local.Telemetry.Metrics ||
		len(base.Telemetry.Trace.Exporters) != len(local.Telemetry.Trace.Exporters) ||
		len(base.Middleware) != len(local.Middleware)
	if connectionChanged || protocolChanged || capabilitiesChanged {
		return fmt.Errorf("wego: native Worker runtime may only override Slots, DurableSlots, Labels, Logger and LogReport; configure instance options on Conn")
	}
	// index 按对象身份比较能力；切片复制不复制 exporter、payload 或闭包内部状态。
	for index, exporter := range base.Telemetry.Trace.Exporters {
		if reflect.ValueOf(exporter) != reflect.ValueOf(local.Telemetry.Trace.Exporters[index]) {
			return fmt.Errorf("wego: Worker cannot replace Conn trace exporters")
		}
	}
	// index 对应同一业务变换和拦截器链，Worker 不得私下替换协议能力。
	for index, original := range base.Middleware {
		// replacement 必须复用已有能力对象，新配置需要在 Conn 构造时设置。
		replacement := local.Middleware[index]
		if reflect.ValueOf(original.Payload) != reflect.ValueOf(replacement.Payload) ||
			reflect.ValueOf(original.UnaryClient) != reflect.ValueOf(replacement.UnaryClient) ||
			reflect.ValueOf(original.UnaryServer) != reflect.ValueOf(replacement.UnaryServer) ||
			reflect.ValueOf(original.StreamClient) != reflect.ValueOf(replacement.StreamClient) ||
			reflect.ValueOf(original.StreamServer) != reflect.ValueOf(replacement.StreamServer) {
			return fmt.Errorf("wego: Worker cannot replace Conn middleware")
		}
	}
	return nil
}
