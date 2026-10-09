package backend

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	dispatcherpb "github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// executionCancel 为单次业务执行记录 Worker 身份与取消函数
type executionCancel struct {
	// workerID 已注册 Worker 的身份，用于查询和注销
	workerID string
	// cancel 取消当前对象所属的执行上下文
	cancel context.CancelFunc
}

// transport 观察任务分发和终态确认，为 Worker 排空提供依据
type transport struct {
	// v0.Client 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	v0.Client
	// dispatcher 负责监听动作和确认执行结果的内部调度客户端
	dispatcher *dispatcher
	// backend 提供带预算的官方注销协议入口
	backend *Backend
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁
	mu sync.Mutex

	// 注册身份、尚未确认终态的任务，以及注销完成信号
	ids map[string]string
	// pending 尚未确认终态的任务登记，关闭时先完成结果上报
	pending map[string]string
	// listenerDone Worker 名称到监听器退出通知的映射
	listenerDone map[string]chan struct{}

	// 执行编号不依赖任务编号，批量任务的每个 handler 都能被独立取消
	cancels map[uint64]executionCancel
	// nextExecution 单调递增的本地执行编号，避免不同执行共用取消记录
	nextExecution uint64

	// 结果上报生命周期及调用方传入的注销预算
	reports map[string]context.Context
	// reportCancels 与结果上报上下文配对的取消函数
	reportCancels map[string]context.CancelFunc
	// unregisterBudgets Worker 注销操作的预算，限制底层等待时间
	unregisterBudgets map[string]context.Context
}

// Dispatcher 返回跟踪动作和结果确认的 dispatcher 视图，不对外返回底层调度连接
func (t *transport) Dispatcher() v0.DispatcherClient {
	return t.dispatcher
}

// dispatcher 调度客户端视图，拦截动作监听与结果上报以管理资源生命周期
type dispatcher struct {
	// v0.DispatcherClient 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	v0.DispatcherClient
	// owner 保存所属实例的监听和结果确认，不在 Worker 间共享取消表
	owner *transport
	// key 唯一标识一次 Worker 实例，不能使用允许重复的显示名称
	key string
	// runner 为配置了持久门禁的普通任务提供按代次隔离的执行路径
	runner *gatedRunner
	// ready 在监听器具备消费能力后关闭，只属于当前 Worker
	ready chan struct{}
}

// GetActionListener 建立动作监听并保存 Worker 注册身份、执行取消与监听器结束记录，为注销阶段提供确认条件
func (d *dispatcher) GetActionListener(ctx context.Context, req *v0.GetActionListenerRequest) (v0.WorkerActionListener, *string, error) {
	// l, id, err 接收 d.DispatcherClient.GetActionListener 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	l, id, err := d.DispatcherClient.GetActionListener(ctx, req)
	// done 是此监听器独占的结束通知，创建时随实例身份一起发布
	var done chan struct{}
	if err == nil && id != nil {
		d.owner.mu.Lock()
		d.owner.ids[d.key] = *id
		// reportCtx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
		reportCtx, cancel := context.WithCancel(context.Background())
		d.owner.reports[*id] = reportCtx
		d.owner.reportCancels[*id] = cancel
		done = make(chan struct{})
		d.owner.listenerDone[d.key] = done
		d.owner.mu.Unlock()
	}
	if err == nil && (id == nil || l == nil) {
		return nil, nil, fmt.Errorf("wego: listener missing registration identity")
	}
	if err == nil {
		l = &actionListener{WorkerActionListener: l, owner: d.owner, name: d.key, done: done, ready: d.ready, backend: d.owner.backend, workerID: *id, runner: d.runner}
	}
	return l, id, err
}

// actionListener 具有单次注销语义的动作监听器
type actionListener struct {
	// v0.WorkerActionListener 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	v0.WorkerActionListener
	// owner 保存所属实例的监听和结果确认，不在 Worker 间共享取消表
	owner *transport
	// name 注册或查询时使用的名称；必须与提交任务的名称对应
	name string
	// done 由此监听器独占，其他同名实例不能覆盖它
	done chan struct{}
	// once 确保资源释放或完成通知只执行一次，防止重复关闭 channel
	once sync.Once
	// ready 消费能力通知，仅在 Actions 成功后发布
	ready chan struct{}
	// readyOnce 防止重复取得 Actions 时重复关闭就绪通知
	readyOnce sync.Once
	// backend 拥有注销 RPC 使用的连接
	backend *Backend
	// runner 与当前 Worker 实例共享，负责可靠输出任务及精确代次取消
	runner *gatedRunner
	// workerID 是当前监听器注册身份
	workerID string
}

// Unregister 以记录的注销预算结束当前 Worker 注册，监听退出只通知一次
func (l *actionListener) Unregister() error {
	l.owner.mu.Lock()
	// budget 此 Worker 注销时使用的明确预算，不能用未受限的默认上下文等待
	budget := l.owner.unregisterBudgets[l.name]
	l.owner.mu.Unlock()
	// err 当前操作产生的错误；nil 表示该步骤成功
	var err error
	if budget == nil {
		// cancel 与派生预算配对的取消函数，退出时释放等待资源
		var cancel context.CancelFunc
		budget, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
	}
	if l.backend != nil {
		_, err = l.backend.rpcDispatcher.Unsubscribe(l.backend.auth(budget), &dispatcherpb.WorkerUnsubscribeRequest{WorkerId: l.workerID})
	} else {
		err = l.WorkerActionListener.Unregister()
	}

	l.once.Do(func() {
		l.owner.mu.Lock()
		if l.done != nil {
			close(l.done)
		}
		l.owner.mu.Unlock()
	})
	return err
}

// Actions 转发动作流，并为普通与批处理执行登记可取消上下文；监听结束时通知关闭方
func (l *actionListener) Actions(ctx context.Context) (<-chan *v0.Action, <-chan error, error) {
	// in, errs, err 接收 l.WorkerActionListener.Actions 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	in, errs, err := l.WorkerActionListener.Actions(ctx)
	if err != nil {
		return nil, nil, err
	}

	if l.runner != nil {
		errs = l.runner.errorsWith(ctx, errs)
	}
	if l.ready != nil {
		l.readyOnce.Do(func() { close(l.ready) })
	}
	// out 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	out := make(chan *v0.Action)
	go func() {
		defer close(out)

		// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
		for {
			// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
			select {
			case <-ctx.Done():
				return
			// action, ok 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功
			case action, ok := <-in:
				// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值
				if !ok {
					return
				}
				if action == nil {
					continue
				}
				// 持久门禁在任何 START 上报之前执行；可靠任务不进入按 TaskRunID 取消的官方执行器
				if l.runner != nil && l.runner.handle(ctx, action) {
					continue
				}
				// 先登记任务，再交给执行器；关闭流程不能漏掉刚收到但尚未开始执行的任务
				l.owner.mu.Lock()
				if action.ActionType == v0.ActionTypeStartStepRun {
					l.owner.pending[attemptKey(action.StepRunId, action.RetryCount)] = action.WorkerId
				}
				if action.ActionType == v0.ActionTypeStartBatch {
					// 逐项处理 action.BatchItems，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
					for id := range action.BatchItems {
						l.owner.pending[attemptKey(id, 0)] = action.WorkerId
					}
				}
				l.owner.mu.Unlock()
				// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
				select {
				case out <- action:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, errs, nil
}

// SendBatchActionEvent 按批内任务逐条移除已确认的终态，避免批量结果尚未上报就关闭
func (d *dispatcher) SendBatchActionEvent(ctx context.Context, event *v0.BatchActionEvent) (*v0.ActionEventResponse, error) {
	if event == nil {
		return nil, status.Error(codes.InvalidArgument, "wego: missing batch report")
	}
	// ctx, stop 为当前 Worker 的结果上报绑定独立关闭信号，其他实例不受影响
	ctx, stop := d.reportContext(ctx, event.WorkerId)
	defer stop()

	limit := backendMessageLimit
	if d.owner.backend != nil {
		limit = d.owner.backend.messageLimit()
	}
	chunks, err := boundedBatchReports(event, limit)
	if err != nil {
		return nil, err
	}
	// result 保存最近一个实际获得确认的分块响应，不将第二块失败改写为整体成功
	var result *v0.ActionEventResponse
	for _, chunk := range chunks {
		result, err = d.DispatcherClient.SendBatchActionEvent(ctx, chunk)
		if err != nil {
			return result, err
		}
		// 只回收已确认分块；第二块失败不能丢失剩余运行的排空记录
		if chunk.EventType == v0.ActionEventTypeCompleted || chunk.EventType == v0.ActionEventTypeFailed {
			d.owner.mu.Lock()
			for _, item := range chunk.Items {
				delete(d.owner.pending, attemptKey(item.TaskRunExternalId, itemRetry(item)))
			}
			d.owner.mu.Unlock()
		}
	}
	return result, nil
}

// SendStepActionEvent 在上报 RPC 确认接收后移除本次代次的待排空记录
// 接收确认不等于引擎已采用该终态；最终状态仍由持久查询确认
func (d *dispatcher) SendStepActionEvent(ctx context.Context, event *v0.ActionEvent) (*v0.ActionEventResponse, error) {
	if event == nil || event.Action == nil {
		return nil, status.Error(codes.InvalidArgument, "wego: missing task report")
	}
	// ctx, stop 为当前 Worker 的结果上报绑定独立关闭信号，其他实例不受影响
	ctx, stop := d.reportContext(ctx, event.WorkerId)
	defer stop()

	limit := backendMessageLimit
	if d.owner.backend != nil {
		limit = d.owner.backend.messageLimit()
	}
	bounded, err := boundedStepReport(event, limit)
	if err != nil {
		return nil, err
	}
	event = bounded
	result, err := d.DispatcherClient.SendStepActionEvent(ctx, event)
	if err == nil && (event.EventType == v0.ActionEventTypeCompleted || event.EventType == v0.ActionEventTypeFailed) {
		d.owner.mu.Lock()
		delete(d.owner.pending, attemptKey(event.StepRunId, event.RetryCount))
		d.owner.mu.Unlock()
	}
	return result, err
}

// reportContext 将结果上报限制在 Worker 生命周期内，关闭预算耗尽时可中断上报
func (d *dispatcher) reportContext(ctx context.Context, workerID string) (context.Context, func()) {
	d.owner.mu.Lock()
	// lifetime 此 Worker 的结果上报上下文，业务取消后仍可完成终态确认
	lifetime := d.owner.reports[workerID]
	d.owner.mu.Unlock()
	if lifetime == nil {
		return ctx, func() {}
	}

	// derived, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	derived, cancel := context.WithCancel(ctx)
	// stop 把所属资源的取消信号传播到当前 I/O，结束时移除取消钩子
	stop := context.AfterFunc(lifetime, cancel)
	return derived, func() {
		stop()
		cancel()
	}
}

// workerTransport 共享连接与执行确认表，仅 Dispatcher 使用实例独立的注册身份
type workerTransport struct {
	// Client 委托共享管理、日志和 durable 能力
	v0.Client
	// dispatcher 属于此 Worker 的监听器视图
	dispatcher *dispatcher
	// admin 在注册期间使用不可变的启动预算
	admin v0.AdminClient
}

// Admin 返回本 Worker 独占的注册视图，普通客户端不借用启动预算
func (t *workerTransport) Admin() v0.AdminClient { return t.admin }

// Dispatcher 返回本实例的身份视图；任务确认仍通过共享 owner 记录
func (t *workerTransport) Dispatcher() v0.DispatcherClient { return t.dispatcher }

// attemptKey 防止旧代次的迟到确认删除新代次的排空记录，例如 task:0 与 task:1 各自释放
func attemptKey(taskID string, retry int32) string {
	return taskID + ":" + strconv.FormatInt(int64(retry), 10)
}

// itemRetry 使用上报中明确的代次；官方缺省为零，不能猜测当前运行代次
func itemRetry(item *v0.BatchActionEventItem) int32 {
	if item.RetryCount != nil {
		return *item.RetryCount
	}
	return 0
}

// Namespace 对执行器返回空前缀：workflow 和 action 已由 backend 绑定为最终名称
// 这样官方 NewService 不会再次给摘要 service 加 namespace；事件和管理作用域仍由 backend 显式处理
func (w *workerTransport) Namespace() string { return "" }
