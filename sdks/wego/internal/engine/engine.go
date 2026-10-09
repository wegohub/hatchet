package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/backend"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	observation "github.com/hatchet-dev/hatchet/sdks/wego/internal/telemetry"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// Engine 实例拥有的后端、Worker、在途调用及观测资源，统一管理关闭顺序
type Engine struct {
	// Backend 本实例的内部后端，只有 Engine 拥有其关闭权，借用连接不得释放
	Backend ports.Backend
	// Config 创建实例时确定的 Runtime 快照，控制容量、协议预算和关闭策略
	Config spec.Runtime
	// telemetry 实例拥有的追踪和指标资源，关闭时执行 flush
	telemetry *observation.Resources

	// 生命周期、Worker 列表和在途调用均由 mu 保护
	mu sync.Mutex
	// workers 由此引擎创建的 Worker，实例关闭时统一释放
	workers []ports.Worker
	// closing 是否已进入排空阶段；true 时拒绝新增顶层调用
	closing bool
	// releasing 表示业务排空已结束，开始释放资源；此后嵌套调用也不得重新登记
	releasing bool
	// closed 是否已关闭或禁止接收新工作；已有工作按关闭策略处理
	closed bool
	// done 操作完成通知；关闭 channel 后所有等待者同时被唤醒
	done chan struct{}
	// closeErr 一次关闭过程中保存的错误，重复关闭返回同一结果
	closeErr error
	// Borrow 为任务构造借用连接的工厂
	Borrow func() any

	// 在途调用结束后唤醒排空等待者；预算耗尽时统一取消
	calls map[uint64]context.CancelFunc
	// ownedIO 保存 SDK 拥有的订阅及载荷 I/O；强制排空后仍确认其资源退出
	ownedIO map[uint64]ownedOperation
	// nextCall 生成本地调用身份的递增计数
	nextCall uint64
	// changed 状态变化通知；发送方关闭当前 channel 并换成新 channel，等待方重新检查状态
	changed chan struct{}
}

// activeCallKey 标记当前 context 已属于在途调用，排空时允许此调用继续内部操作
type activeCallKey struct{}

// New 创建实例后端与观测资源，注入执行上下文桥接并管理资源所有权
func New(config spec.Runtime) (*Engine, error) {
	// err 接收 config.Validate 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// b, err 接收 backend.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	b, err := backend.New(config)
	if err != nil {
		return nil, err
	}

	// e 由同一个内部构造器建立完整生命周期，后端注入不会绕过观测和关闭管理
	e, err := NewWithBackend(b.Config(), b)
	if err != nil {
		return nil, err
	}

	b.Bind = e.Context
	b.ProtocolMetrics = e.ObserveProtocol
	b.BeginLogIO = func(ctx context.Context) (context.Context, func(), error) {
		return e.BeginIO(ctx, "task.log.report")
	}
	b.StartExecution = func(ctx context.Context, name string) (context.Context, func(error)) {
		return e.StartSpan(ctx, name, trace.SpanKindInternal)
	}
	return e, nil
}

// NewWithBackend 组装已拥有的内部后端，初始化调用表、观测资源和关闭屏障
// backend 工厂与离线协议测试使用同一构造路径，避免手工拼出不完整 Engine
func NewWithBackend(config spec.Runtime, b ports.Backend) (*Engine, error) {
	// 此构造器取得后端所有权，配置失败同样需要释放它
	if err := config.Validate(); err != nil {
		return nil, errors.Join(err, b.Close())
	}
	config = config.Clone()
	// resources, err 接收 observation.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	resources, err := observation.New(context.Background(), config.Telemetry, config.Address, config.Token, config.TLS)
	if err != nil {
		return nil, errors.Join(err, b.Close())
	}

	// e 组合当前后端、Runtime 与观测资源，并初始化在途调用表和关闭通知，所有连接视图复用此实例
	e := &Engine{
		Backend:   b,
		Config:    config,
		telemetry: resources,
		done:      make(chan struct{}),
		calls:     map[uint64]context.CancelFunc{},
		changed:   make(chan struct{}),
	}
	return e, nil
}

// Begin 跟踪一个在途调用，并返回幂等的结束函数
// 排空期间拒绝新的外部调用，允许已接收调用的嵌套请求和正在执行的任务发起子调用
func (e *Engine) Begin(ctx context.Context) (context.Context, func(), error) {
	return e.begin(ctx, false, "")
}

// BeginIO 登记 SDK 拥有的传输或载荷操作，取消后必须在清理预算内确认退出
// 业务 handler 不使用此登记，强制停止不会无限等待不响应取消的业务
func (e *Engine) BeginIO(ctx context.Context, operation ...string) (context.Context, func(), error) {
	// name 由 SDK 调用点提供稳定操作类别；默认类别用于兼容未命名的内部调用
	name := "sdk.io"
	if len(operation) > 0 && operation[0] != "" {
		name = operation[0]
	}
	return e.begin(ctx, true, name)
}

// begin 在同一把锁内登记取消与资源所有权，避免关闭与追加 I/O 登记竞争
func (e *Engine) begin(ctx context.Context, owned bool, operation string) (context.Context, func(), error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// state, _ 获取任务上下文能力与存在标记；普通网络入口没有任务身份，必须检查后再使用
	state, _ := callctx.Get(ctx)
	if e.closed || e.releasing {
		return nil, nil, model.ErrClosed
	}
	if e.closing && ctx.Value(activeCallKey{}) != e && (state == nil || state.Execution == nil) {
		return nil, nil, model.ErrClosed
	}

	// derived, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	derived, cancel := context.WithCancel(ctx)
	derived = context.WithValue(derived, activeCallKey{}, e)
	e.nextCall++
	// id 当前资源的唯一身份，用于查找对应运行或会话
	id := e.nextCall
	e.calls[id] = cancel
	if owned {
		if e.ownedIO == nil {
			e.ownedIO = make(map[uint64]ownedOperation)
		}
		e.ownedIO[id] = ownedOperation{name: operation, started: time.Now()}
	}
	// once 确保资源释放或完成通知只执行一次，防止重复关闭 channel
	var once sync.Once
	return derived, func() {
		once.Do(func() {
			cancel()
			e.mu.Lock()
			delete(e.calls, id)
			delete(e.ownedIO, id)
			close(e.changed)
			e.changed = make(chan struct{})
			e.mu.Unlock()
		})
	}, nil
}

// Tracer 返回本实例追踪器；无任务观测上下文时使用 noop 追踪器
func (e *Engine) Tracer() trace.Tracer {
	return e.telemetry.Provider.Tracer("wego")
}

// Context 复制上下文视图，注入实例 tracer 和借用连接，避免并发调用修改共享状态
func (e *Engine) Context(ctx context.Context) context.Context {
	// state, ok 获取任务上下文能力与存在标记；普通网络入口没有任务身份，必须检查后再使用
	state, ok := callctx.Get(ctx)
	// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值
	if !ok {
		state = &callctx.State{}
	}
	// copy 当前记录的独立结构体副本，用于改写本调用状态而不并发修改共享原对象
	copy := *state
	copy.Tracer = e.Tracer()
	if e.Borrow != nil {
		copy.Borrowed = e.Borrow()
	}
	return callctx.Bind(ctx, &copy)
}

// Check 检查实例生命周期，拒绝已进入关闭阶段的新工作
func (e *Engine) Check() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closing {
		return model.ErrClosed
	}

	return nil
}

// Worker 根据内部定义注册业务任务，启动实例事件订阅并接管 Worker 生命周期
func (e *Engine) Worker(ctx context.Context, name string, definitions []ports.Definition, config spec.Runtime, panicHandler func(context.Context, any)) (ports.Worker, error) {
	// 注册也属于在途操作；关闭预算耗尽时必须取消版本探测和 PutWorkflow
	registration, done, err := e.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	// 网络注册不能持有生命周期锁，否则 Shutdown 无法发布取消信号
	w, err := e.Backend.Worker(registration, name, definitions, config, panicHandler)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	if e.closing {
		e.mu.Unlock()
		// 未发布的 Worker 由本次创建负责清理，不能留给已截取列表的 Shutdown
		_ = w.Close(registration)
		return nil, model.ErrClosed
	}
	e.workers = append(e.workers, w)
	e.mu.Unlock()
	return w, nil
}

// Shutdown 按依赖顺序排空资源；并发关闭等待同一次关闭结果
// 调用方预算耗尽后取消剩余调用，并继续释放观测资源及连接
// 例如调用方只有 100ms 预算，超时后取消剩余业务，连接和结果确认另用配置的清理预算
func (e *Engine) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	if e.closing {
		// done 操作完成通知；关闭 channel 后所有等待者同时被唤醒
		done := e.done
		e.mu.Unlock()
		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		case <-done:
			return e.closeErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e.closing = true
	close(e.changed)
	e.changed = make(chan struct{})
	// workers 复制或扩展当前列表，避免把内部可变容器直接交给调用方修改
	workers := append([]ports.Worker(nil), e.workers...)
	e.mu.Unlock()
	// errs 关闭或多项验收中的错误集合，结束时合并报告而不丢失后续清理失败
	var errs []error
	// 停止新增任务分配后排空所有业务，实例订阅在输出和结果确认之后关闭
	for _, w := range workers {
		errs = append(errs, w.Close(ctx))
	}
	// 调用方排空预算到期后统一取消业务，资源 I/O 另外确认实际退出
	errs = append(errs, e.drainCalls(ctx))
	// 执行和上报结束后才刷新 trace，最后关闭共享传输
	cleanupCtx := ctx
	// cancelCleanup 独立清理预算的取消函数，默认空操作；创建清理上下文后必须在退出时调用
	cancelCleanup := func() {}
	// 调用方预算已耗尽，剩余业务取消后资源清理使用独立预算，避免上报直接继承过期 context
	if ctx.Err() != nil {
		// timeout 实例配置的资源清理预算，未提供正值时回退到 30 秒
		timeout := e.Config.CleanupTimeout()
		cleanupCtx, cancelCleanup = context.WithTimeout(context.Background(), timeout)
	}
	defer cancelCleanup()
	// 自有 I/O 结束或预算耗尽后设置释放屏障，随后才关闭观测与后端
	errs = append(errs, e.drainOwnedIO(cleanupCtx))
	errs = append(errs, e.telemetry.Close(cleanupCtx), e.Backend.Close())
	e.mu.Lock()
	e.closeErr = errors.Join(errs...)
	e.closed = true
	close(e.done)
	e.mu.Unlock()
	return e.closeErr
}

// Close 释放此对象拥有的资源；借用资源必须由所有者关闭
func (e *Engine) Close() error {
	// ctx 选取不带业务 deadline 的资源上下文，具体清理步骤另有明确预算
	ctx := context.Background()
	// cancel 取消当前对象所属的执行上下文
	cancel := func() {}
	// 按配置创建有限关闭预算；例如 30 秒到期后取消仍未结束的业务
	if e.Config.Shutdown.Mode == spec.DrainWithTimeout {
		ctx, cancel = context.WithTimeout(ctx, e.Config.Shutdown.Timeout)
	}
	defer cancel()

	return e.Shutdown(ctx)
}

// WaitReady 等待 Worker 注册并具备消费能力；context 到期时返回等待错误
func (e *Engine) WaitReady(ctx context.Context) error {
	e.mu.Lock()
	// workers 复制或扩展当前列表，避免把内部可变容器直接交给调用方修改
	workers := append([]ports.Worker(nil), e.workers...)
	e.mu.Unlock()
	// 实例没有已启动 Worker，不把空集合当作真实消费就绪
	if len(workers) == 0 {
		return fmt.Errorf("wego: workers not started")
	}

	// 逐项处理 workers，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, w := range workers {
		// err 接收 w.WaitReady 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
		if err := w.WaitReady(ctx); err != nil {
			return err
		}
	}
	return nil
}
