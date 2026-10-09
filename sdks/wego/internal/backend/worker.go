package backend

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	clientconfig "github.com/hatchet-dev/hatchet/pkg/config/client"
	oldworker "github.com/hatchet-dev/hatchet/pkg/worker"
	sdk "github.com/hatchet-dev/hatchet/sdks/go"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// worker 封装后端 Worker 的私有生命周期，跟踪注册身份、结果确认和监听器退出
type worker struct {
	// backend 内部后端接口；业务层不能取出其具体实现
	backend *Backend
	// config 保存当前 Worker 的事件与日志配置，不与 Conn 的可变视图共享
	config spec.Runtime
	// events 是不占业务 slots 的两个持久通知订阅
	events *workerEvents
	// name 是展示名称；相同名称的 Worker 仍各自拥有 key、注册 ID 和监听器
	name string
	// key 是本地实例身份；同名 Worker 的监听器和关闭预算不得共享
	key string
	// runner 是此实例的持久门禁执行路径；没有可靠任务时为 nil
	runner *gatedRunner
	// native 封装的内部 Worker；通过适配层管理启动和关闭
	native *nativeWorker
	// cleanup 释放 Worker 注册及传输资源的回调，预算由传入上下文控制
	cleanup func(context.Context) error
	// errors 异步错误通道，用于把入口故障交给生命周期协调器
	errors <-chan error
	// once 确保资源释放或完成通知只执行一次，防止重复关闭 channel
	once sync.Once
	// err 当前操作产生的错误；nil 表示该步骤成功
	err error
	// started 入口是否已经启动；与停止并发时在锁内读写
	started bool
	// closed 禁止停止后的启动；初始化完成后也不能重新启动
	closed bool
	// startCancel 只在初始化阶段由 Close 使用，已运行的控制通道先排空再结束
	startCancel context.CancelFunc
	// startDone 是初始化完成屏障，Close 等待其后再读取 cleanup
	startDone chan struct{}
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁
	mu sync.Mutex
}

// Worker 在后端边界将 wego 定义转换为官方任务，并在注册前校验 handler 和策略
func (b *Backend) Worker(
	ctx context.Context,
	name string,
	definitions []ports.Definition,
	config spec.Runtime,
	panicHandler func(context.Context, any),
) (result ports.Worker, err error) {
	defer func() {
		// recovered 捕获当前执行的 panic，边界应返回可诊断错误并继续资源清理
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("wego: registration: %v", recovered)
		}
	}()

	// 使用公开 Worker 执行器；注册通过实例拥有的官方协议连接完成
	key := uuid.NewString()
	name = instanceName(config, name, key)
	// worker_name 始终反映最终展示名称；例如 WithInstanceName("processor") 得到同名标签
	// 每个实例独立复制标签，避免两个 Worker 复用配置时相互覆盖或改写调用方 map
	config.Labels = maps.Clone(config.Labels)
	if config.Labels == nil {
		config.Labels = make(map[string]any)
	}
	config.Labels["worker_name"] = name
	// ready 取得 make 的结果，确认成功后才进入下一处理阶段
	ready := make(chan struct{})
	// rpcNames 只由已验证绑定生成，随机 Worker 身份不进入定义
	rpcNames := map[string]string{}
	for _, definition := range definitions {
		if definition.RPCMethod != "" {
			if err := binding.ValidateName(b.config.Namespace, definition.RPCMethod); err != nil {
				return nil, err
			}
			if definition.Workflow != "" || definition.Name != binding.Name(definition.RPCMethod) {
				return nil, fmt.Errorf("wego: inconsistent standalone RPC binding")
			}
			rpcNames[definition.Name] = binding.WorkflowName(b.config.Namespace, definition.RPCMethod)
			rpcNames[rpcNames[definition.Name]] = rpcNames[definition.Name]
		}
	}
	for _, definition := range definitions {
		if definition.RPCMethod == "" && rpcNames[definition.Name] != "" {
			return nil, fmt.Errorf("wego: native task conflicts with RPC workflow %s", definition.Name)
		}
	}
	// view 取得 &workerTransport{Client: b.observer, dispatcher: &dispatcher{DispatcherClient: b.raw.Dispatcher 的结果，确认成功后才进入下一处理阶段
	view := &workerTransport{Client: b.observer, dispatcher: &dispatcher{DispatcherClient: b.raw.Dispatcher(), owner: b.observer, key: key, ready: ready}, admin: &registrationAdmin{AdminClient: b.raw.Admin(), backend: b, ctx: ctx, rpcNames: rpcNames}}
	// runner 只接管配置持久门禁的任务，注册和容量仍使用同一官方 Worker
	view.dispatcher.runner = newGatedRunner(view.dispatcher, config, panicHandler)
	// native 公开执行器，仅通过已发布的方法使用，不读取私有字段
	native, err := oldworker.NewWorker(oldworker.WithClient(view), oldworker.WithName(name), oldworker.WithSlotConfig(map[string]int32{"default": int32(config.Slots), "durable": int32(config.DurableSlots)}), oldworker.WithLabels(config.Labels), oldworker.WithLogger(b.raw.Logger()))
	if err != nil {
		return nil, Normalize(err)
	}
	if panicHandler != nil {
		native.SetPanicHandler(func(ctx oldworker.HatchetContext, recovered any) {
			panicHandler(b.executionContext(ctx, config), fmt.Sprint(recovered))
		})
	}
	// lifecycle 本实例拥有的 Worker 运行及关闭协调器，不与同名 Worker 共享
	lifecycle := newNativeWorker(b, native, ready, config)
	// 服务版本决定 durable 协议能力，探测也受本次启动预算限制
	version, err := b.raw.Dispatcher().GetVersion(ctx)
	if err != nil {
		return nil, Normalize(err)
	}
	lifecycle.supported, err = sdk.SupportsDurableEviction(version)
	if err != nil {
		return nil, Normalize(err)
	}
	// groups 工作流名称到定义请求的注册表，同一工作流只提交一次
	groups := map[string]*v1.CreateWorkflowVersionRequest{}
	// order 首次登记的工作流顺序，避免 map 迭代改变注册顺序
	order := []string{}
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
	for _, definition := range definitions {
		if err = validateHandler(definition.Function); err != nil {
			return nil, err
		}
		if err = definition.Policy.Validate(); err != nil {
			return nil, err
		}
		// workflowName 同一组任务共享的工作流名称，独立任务使用自己的名称
		workflowName, policy := definition.Name, definition.Policy
		if definition.Workflow != "" {
			workflowName, policy = definition.Workflow, definition.WorkflowPolicy
		}
		// req 当前协议请求，编码完整后才发送，不能使用未初始化的调度字段
		req := groups[workflowName]
		if req == nil {
			req, err = workflowDefinition(workflowName, policy, b.config.Namespace)
			if err != nil {
				return nil, err
			}
			if definition.RPCMethod != "" {
				req.Name = rpcNames[workflowName]
				// InputJsonSchema 持久记录绑定，客户端只读管理引用不依赖本地注册表
				req.InputJsonSchema, err = rpcSchema(b.config.Namespace, definition.RPCMethod, definition.RPCShape, definition.RPCMode)
				if err != nil {
					return nil, err
				}
			}
			groups[workflowName] = req
			order = append(order, workflowName)
		}
		// task 协议任务定义，action 与 readable_id 各自保持调度和结果语义
		task, err := taskDefinition(req.Name, definition)
		if err != nil {
			return nil, err
		}
		if definition.RPCMethod != "" {
			task.Action = binding.Action(b.config.Namespace, definition.RPCMethod)
		}
		if definition.OnFailure {
			req.OnFailureTask = task
		} else {
			req.Tasks = append(req.Tasks, task)
		}
		if definition.BeforeStart != nil {
			if definition.Policy.Durable || definition.Policy.Batch != nil || definition.Workflow != "" || definition.OnFailure {
				return nil, fmt.Errorf("wego: persistent admission requires a non-durable standalone task")
			}
			view.dispatcher.runner.definitions[task.Action] = definition
		}
		// wrapped 注入 wego 上下文和错误协议的业务入口，不向业务暴露后端对象
		wrapped := b.wrap(definition.Function, config, task.Action, definition.RPCMethod)
		// handler 注册给官方执行器的适配入口，按普通、durable、batch 恢复输入
		handler := func(ctx oldworker.HatchetContext) (any, error) {
			// input 从执行上下文读取的业务输入，转换失败时禁止调用 handler
			var input any
			if definition.Policy.Batch != nil {
				// err 当前操作错误，失败时不继续使用对应结果
				if err := ctx.BatchInputInto(&input); err != nil {
					return nil, err
				}
				// } else if err 取得 ctx.WorkflowInput 的结果，确认成功后才进入下一处理阶段
			} else if err := ctx.WorkflowInput(&input); err != nil {
				return nil, err
			}
			if definition.Policy.Durable {
				return lifecycle.invokeDurable(ctx, definition.Policy, func(ctx oldworker.HatchetContext) (any, error) { return wrapped(ctx, input) })
			}
			// output, err 取得 wrapped 的结果，确认成功后才进入下一处理阶段
			output, err := wrapped(ctx, input)
			if err != nil {
				return nil, err
			}
			// batch 取得 definition.Policy.Batch 的结果，确认成功后才进入下一处理阶段
			if batch := definition.Policy.Batch; batch != nil {
				return batchResults(input, output, batch.BroadcastOutput)
			}
			return output, nil
		}
		// err 当前操作错误，失败时不继续使用对应结果
		if err := native.RegisterAction(task.Action, handler); err != nil {
			return nil, Normalize(err)
		}
	}
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
	for _, group := range order {
		// 登记最终 workflow 名称；执行器不再自行添加 namespace
		req := groups[group]
		qualified := rpcNames[group]
		if qualified == "" {
			qualified = clientconfig.ApplyNamespace(strings.ToLower(group), &b.config.Namespace)
		}
		req.Name = qualified
		// err 当前操作错误，失败时不继续使用对应结果
		if err := native.RegisterWorkflowV1(req); err != nil {
			return nil, Normalize(err)
		}
		// durable 子事件对单任务直接保存业务输出；登记任务名以恢复统一的 ports.Result 形状
		names := make([]string, len(req.Tasks))
		// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
		for i, task := range req.Tasks {
			names[i] = task.ReadableId
		}
		b.featureMu.Lock()
		b.workflowTasks[qualified] = names
		b.featureMu.Unlock()
	}

	if len(view.dispatcher.runner.definitions) == 0 {
		view.dispatcher.runner = nil
	}
	return &worker{backend: b, config: config.Clone(), name: name, key: key, native: lifecycle, runner: view.dispatcher.runner}, nil
}

// Start 启动 Worker 入口；运行期间的异步错误通过错误通道报告
func (w *worker) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.closed || w.startDone != nil {
		w.mu.Unlock()
		return fmt.Errorf("wego: worker stopped or already starting")
	}
	// lifetime 发布后仅由 cleanup 取消；启动 context 只限制注册和建立监听的阶段
	lifetime, cancel := context.WithCancel(context.WithoutCancel(ctx))
	// stopStartup 让启动失败可取消真实 I/O，成功后解除与启动预算的连接
	stopStartup := context.AfterFunc(ctx, cancel)
	w.startCancel = cancel
	w.startDone = make(chan struct{})
	w.mu.Unlock()

	// cleanup、errs 与 err 从真正的启动返回，初始化 I/O 不持有状态锁
	notifications, err := newWorkerEvents(w, w.config)
	// cleanup 和 errs 仅在订阅屏障成功后由 native 生命周期填充
	var cleanup func(context.Context) error
	// errs 保存任务入口错误，通知错误随后加入同一个实例通道
	var errs <-chan error
	if err == nil {
		err = notifications.start(lifetime)
	}
	if err == nil {
		cleanup, errs, err = w.native.StartContext(lifetime)
	}
	if err != nil {
		cancel()
		if notifications != nil {
			budget, release := context.WithTimeout(context.Background(), 30*time.Second)
			_ = notifications.close(budget)
			release()
		}
	}
	stopStartup()
	w.mu.Lock()
	// closing 在锁内取得停止状态；停止先到时不能把新监听器交给业务
	closing := w.closed || lifetime.Err() != nil
	if err == nil {
		// cleanupOnce 使迟到启动与 Close 并发清理时，底层资源仍只释放一次
		var cleanupOnce sync.Once
		// cleanupErr 保留第一次注销结果，后续清理调用复用它
		var cleanupErr error
		w.cleanup = func(budget context.Context) error {
			cleanupOnce.Do(func() {
				cleanupErr = cleanup(budget)
				cancel()
				cleanupErr = errors.Join(cleanupErr, notifications.close(budget))
			})
			return cleanupErr
		}
		w.events = notifications
		w.errors = mergeErrors(lifetime, errs, notifications.errors)
		w.started = true
	}
	close(w.startDone)
	// release 是已发布的清理回调，锁外执行实际注销
	release := w.cleanup
	w.mu.Unlock()
	if err != nil {
		cancel()
		// 启动桥接通过取消实际 RPC 结束 I/O，返回时仍保留调用方的 deadline 原因
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return Normalize(err)
	}
	if closing {
		// budget 给停止之前已建立的监听器独立清理时间，不能遗留迟到资源
		// timeout 没有显式清理预算时回退到 30 秒，业务无限排空不等于资源零预算
		timeout := w.backend.config.Shutdown.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		// budget 与 finish 限制迟到监听器的资源释放，并在清理后停止计时器
		budget, finish := context.WithTimeout(context.Background(), timeout)
		defer finish()
		_ = release(budget)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return context.Canceled
	}
	return nil
}

// Errors 返回 Worker 异步错误通道
func (w *worker) Errors() <-chan error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.errors
}

// ID 返回资源的注册身份；未找到合法身份时返回空字符串
func (w *worker) ID() string {
	w.backend.observer.mu.Lock()
	defer w.backend.observer.mu.Unlock()

	return w.backend.observer.ids[w.key]
}

// WaitReady 等待 Worker 注册并具备消费能力；context 到期时返回等待错误
func (w *worker) WaitReady(ctx context.Context) error {
	// ticker 周期计时器，驱动轮询或续期；当前步骤结束时停止以释放资源
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		if w.ID() != "" {
			return nil
		}

		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Pause 停止新增任务分配，已有执行和控制消息按生命周期继续处理
func (w *worker) Pause(ctx context.Context) error {
	return w.backend.Feature(ctx, ports.WorkersPause{ID: w.ID()}, nil)
}

// Close 先暂停分配，再等待所有终态确认；预算耗尽时取消该 Worker 的每个执行
// 排空受调用方预算限制；取消后注销和结果确认使用独立的清理预算
func (w *worker) Close(ctx context.Context) error {
	w.once.Do(func() {
		w.mu.Lock()
		w.closed = true
		// initializing 只在启动尚未发布时取消；排空中的控制 Worker 保持监听
		initializing := w.startDone != nil && !w.started
		// startDone 与 cancelStartup 属于本次启动，在锁外等待和取消
		startDone, cancelStartup := w.startDone, w.startCancel
		w.mu.Unlock()
		if initializing {
			cancelStartup()
			select {
			case <-startDone:
			case <-ctx.Done():
				w.err = ctx.Err()
				return
			}
		}
		w.mu.Lock()
		// cleanup 在初始化完成后取得快照，不与启动发布并发读写
		cleanup := w.cleanup
		w.mu.Unlock()
		if w.ID() != "" {
			w.err = w.Pause(ctx)
		}
		// ticker 周期计时器，驱动轮询或续期；当前步骤结束时停止以释放资源
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
		for {
			w.backend.observer.mu.Lock()
			// pending 是本实例尚未收到终态确认的执行数，不统计其他同名 Worker
			pending := 0
			// 按 Worker 注册 ID 统计尚未确认的执行；例如 A 有 2 项、B 有 1 项，关闭 A 只等待 A 的 2 项
			for _, id := range w.backend.observer.pending {
				if id == w.backend.observer.ids[w.key] {
					pending++
				}
			}
			w.backend.observer.mu.Unlock()
			if pending == 0 {
				break
			}
			// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
			select {
			case <-ctx.Done():
				w.err = ctx.Err()
				w.backend.observer.mu.Lock()
				// 预算耗尽只取消此注册 ID 的业务；另一同名实例的 handler 继续执行
				for _, execution := range w.backend.observer.cancels {
					if execution.workerID == w.backend.observer.ids[w.key] {
						execution.cancel()
					}
				}
				w.backend.observer.mu.Unlock()
				goto stop
			case <-ticker.C:
			}
		}
	stop:
		// 业务预算只控制排空；强制取消后仍给注销和结果确认独立的清理预算
		cleanupCtx := ctx
		// cancelCleanup 独立清理预算的取消函数，默认空操作；创建清理上下文后必须在退出时调用
		cancelCleanup := func() {}
		// 调用方预算已耗尽，剩余业务取消后资源清理使用独立预算，避免上报直接继承过期 context
		if ctx.Err() != nil {
			// timeout 实例配置的资源清理预算，未提供正值时回退到 30 秒
			timeout := w.backend.config.Shutdown.Timeout
			// 没有可用清理预算时使用默认 30 秒，保证资源关闭本身有机会执行
			if timeout <= 0 {
				timeout = 30 * time.Second
			}
			cleanupCtx, cancelCleanup = context.WithTimeout(context.Background(), timeout)
			_ = w.Pause(cleanupCtx)
		}
		defer cancelCleanup()
		w.backend.observer.mu.Lock()
		w.backend.observer.unregisterBudgets[w.key] = cleanupCtx
		// cancel 仅结束当前 WorkerID 的上报资源，其他同名 Worker 的结果确认保持有效
		if cancel := w.backend.observer.reportCancels[w.backend.observer.ids[w.key]]; cancel != nil {
			cancel()
		}
		w.backend.observer.mu.Unlock()
		if cleanup != nil {
			// e 是注销结果；已有排空错误优先保留，不能被成功清理覆盖
			if e := cleanup(cleanupCtx); w.err == nil {
				w.err = Normalize(e)
			}
			w.backend.observer.mu.Lock()
			// done 操作完成通知；关闭 channel 后所有等待者同时被唤醒
			done := w.backend.observer.listenerDone[w.key]
			w.backend.observer.mu.Unlock()
			if done != nil {
				// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
				select {
				case <-done:
				case <-cleanupCtx.Done():
					w.err = cleanupCtx.Err()
				}
			}
		}
	})
	return w.err
}
