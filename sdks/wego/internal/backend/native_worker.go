package backend

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	clientconfig "github.com/hatchet-dev/hatchet/pkg/config/client"
	oldworker "github.com/hatchet-dev/hatchet/pkg/worker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// auth 只复制并添加当前实例凭证；例如 A 的 Worker 不继承 B 的连接配置
func (b *Backend) auth(ctx context.Context) context.Context {
	// md 当前 outgoing metadata 的副本，只替换本实例的授权头
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set("authorization", "Bearer "+b.config.Token)
	return metadata.NewOutgoingContext(ctx, md)
}

// registrationAdmin 借用官方 Admin 的其余能力，仅注册 RPC 使用固定启动预算
type registrationAdmin struct {
	// AdminClient 委托不需要修改预算的官方管理方法，不拥有独立连接
	v0.AdminClient
	// backend 提供未经修改的官方协议客户端
	backend *Backend
	// ctx 属于本次构造过程，不在多个 Worker 之间复用
	ctx context.Context
	// rpcNames 是本次注册的显式名称映射；不会对原生任务套用 RPC 规则
	rpcNames map[string]string
}

// PutWorkflowV1 在 RPC 层贯通取消；不修改原请求，以免重复添加 namespace
func (a *registrationAdmin) PutWorkflowV1(req *v1.CreateWorkflowVersionRequest, _ ...v0.PutOptFunc) error {
	// copy 独立请求或错误副本，修改它不会改变调用方对象
	copy := proto.Clone(req).(*v1.CreateWorkflowVersionRequest)
	copy.Name = a.rpcNames[req.Name]
	if copy.Name == "" {
		copy.Name = clientconfig.ApplyNamespace(strings.ToLower(req.Name), &a.backend.config.Namespace)
	}
	// 正式版首次并发 upsert 可能以 Internal 报唯一键竞争；相同定义重试会复用 checksum 版本
	// 有限次数和调用方预算共同约束，永久 Internal 仍作为实际启动失败返回
	budget, cancel := context.WithTimeout(a.ctx, 30*time.Second)
	defer cancel()
	for attempt := 0; ; attempt++ {
		_, err := a.backend.v1admin.PutWorkflow(a.backend.auth(budget), copy)
		if err == nil {
			return nil
		}
		if budget.Err() != nil {
			return budget.Err()
		}
		code := status.Code(err)
		if attempt >= 4 || code != codes.Internal && code != codes.Unavailable && code != codes.DeadlineExceeded {
			return Normalize(err)
		}
		if err := waitObservationRetry(budget, time.Duration(1<<attempt)*50*time.Millisecond); err != nil {
			return err
		}
	}
}

// durableRun 保存当前 invocation 的等待引用数及驱逐策略，不能用同名任务代替运行身份
type durableRun struct {
	// invocation 区分同一任务的恢复执行
	invocation int32
	// waiting 引用计数允许同时等待多个子结果
	waiting int
	// since 进入等待状态的时间，MarkActive 归零后清空
	since time.Time
	// policy 当前任务显式驱逐策略
	policy spec.Task
	// cancel 只取消本 invocation 的执行预算，不能按 TaskRunID 查找后续执行
	cancel context.CancelFunc
	// evicting 阻止对同一个 invocation 重复请求驱逐
	evicting bool
}

// nativeWorker 管理公开 Worker.Run、durable listener 与排空资源
// 官方执行器负责 protobuf action 的业务执行，wego 负责启动确认和独立预算
type nativeWorker struct {
	// backend 提供本实例的协议与配置
	backend *Backend
	// worker 只通过官方公开方法使用的执行器
	worker *oldworker.Worker
	// ready 来自动作监听器成功建立后的通知
	ready <-chan struct{}
	// config 是实例配置快照
	config spec.Runtime
	// once 只为当前注册身份创建一个 durable listener
	once sync.Once
	// listener 持有官方 durable 协议收发器，停止时先回收再注销
	listener *v0.DurableTaskListener
	// mu 保护 listener 发布与运行记录
	mu sync.Mutex
	// runs 以 TaskRunID 保存驱逐记录
	runs map[string]*durableRun
	// supported 由启动阶段版本探测确定
	supported bool
	// lifetime 属于 Worker，durable listener 不继承单次业务 deadline
	lifetime context.Context
	// closing 防止关闭后的迟到 handler 创建新 listener
	closing bool
}

// newNativeWorker 创建私有生命周期，不启动后台任务
func newNativeWorker(b *Backend, w *oldworker.Worker, ready <-chan struct{}, config spec.Runtime) *nativeWorker {
	return &nativeWorker{backend: b, worker: w, ready: ready, config: config, runs: map[string]*durableRun{}}
}

// StartContext 使用 Run 的返回错误，禁止通过官方 Start 的后台 panic 表示失败
func (n *nativeWorker) StartContext(ctx context.Context) (func(context.Context) error, <-chan error, error) {
	// lifetime 与执行器配对，成功启动后只由 cleanup 取消
	lifetime, cancel := context.WithCancel(ctx)
	n.mu.Lock()
	n.lifetime = lifetime
	n.mu.Unlock()
	// done 本次 goroutine 的退出确认，关闭函数返回前需完成资源回收
	done := make(chan struct{})
	// errs 只交付执行器入口故障，容量为一避免退出报告阻塞
	errs := make(chan error, 1)
	// runErr 执行器的最终错误，退出屏障之前不能读取它
	var runErr error
	go func() {
		defer close(done)
		defer close(errs)
		runErr = n.worker.Run(lifetime)
		if runErr != nil {
			errs <- runErr
		}
	}()
	select {
	case <-n.ready:
	case <-done:
		cancel()
		if runErr == nil {
			runErr = fmt.Errorf("wego: worker exited before ready")
		}
		return nil, errs, runErr
	case <-ctx.Done():
		cancel()
		// Run 的注销也有独立预算；等待退出确认，避免迟到资源逃逸
		<-done
		return nil, errs, ctx.Err()
	}
	// evictionCtx 独立控制定期驱逐，清理先停止它再处理最后一轮驱逐
	evictionCtx, stopEviction := context.WithCancel(lifetime)
	// evictionDone 确认轮询已退出，避免清理与轮询同时使用协议确认键
	evictionDone := make(chan struct{})
	go func() {
		defer close(evictionDone)
		n.evictionLoop(evictionCtx)
	}()
	// once 只执行一次的初始化或关闭保护，避免重复释放资源
	var once sync.Once
	// cleanupErr 首次清理的结果，重复关闭复用同一个错误
	var cleanupErr error
	// cleanup 返回单次资源回收回调；多个关闭者共享一次结果
	cleanup := func(budget context.Context) error {
		once.Do(func() {
			// 先结束定期驱逐，防止它与关闭驱逐争用确认键
			stopEviction()
			<-evictionDone
			n.mu.Lock()
			n.closing = true
			n.mu.Unlock()
			n.evictWaiting(budget)
			n.mu.Lock()
			// listener 根 Worker 拥有的 durable 监听器，子等待复用相同执行账本
			listener := n.listener
			n.mu.Unlock()
			if listener != nil {
				listener.Stop()
			}
			cancel()
			select {
			case <-done:
				if !errors.Is(runErr, context.Canceled) {
					cleanupErr = runErr
				}
			case <-budget.Done():
				cleanupErr = budget.Err()
			}
		})
		return cleanupErr
	}
	return cleanup, errs, nil
}

// invokeDurable 为根执行初始化一次基础设施；子调用沿用 listener，不重建父 durable 状态
func (n *nativeWorker) invokeDurable(ctx oldworker.HatchetContext, p spec.Task, fn func(oldworker.HatchetContext) (any, error)) (any, error) {
	n.once.Do(func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.closing || n.lifetime == nil || n.lifetime.Err() != nil {
			return
		}
		// listener 根 Worker 拥有的 durable 监听器，子等待复用相同执行账本
		listener := v0.NewDurableTaskListener(ctx.WorkerId(), n.backend.raw.Dispatcher().DurableTaskStream, n.backend.raw.Logger())
		listener.SetServerEvictCallback(func(id string, invocation int32, _ string) {
			n.mu.Lock()
			// run 取得 n.runs[id] 的结果，确认成功后才进入下一处理阶段
			run := n.runs[id]
			// matches 只接受当前 invocation 的服务端驱逐，迟到通知不能取消恢复执行
			matches := run != nil && run.invocation == invocation
			n.mu.Unlock()
			if matches {
				n.cancelInvocation(run)
			}
		})
		n.listener = listener
		listener.Start(n.lifetime)
	})
	n.mu.Lock()
	// listener 根 Worker 拥有的 durable 监听器，子等待复用相同执行账本
	listener := n.listener
	if listener == nil || n.closing {
		n.mu.Unlock()
		return nil, context.Canceled
	}
	// invocationContext 在业务 handler 启动前串行绑定一次，之后不再修改原始上下文
	// 官方执行器必须观察同一取消状态，才能把已确认驱逐与业务失败分开
	invocationContext, cancelInvocation := context.WithCancel(ctx.GetContext())
	ctx.SetContext(invocationContext)
	// 正常返回不能提前 cancel，否则执行器会跳过结果上报；父上下文由官方执行器释放
	// run 固定持有这一代预算，迟到旧驱逐确认不能取消恢复执行
	run := &durableRun{invocation: max(1, ctx.DurableTaskInvocationCount()), policy: p, cancel: cancelInvocation}
	n.runs[ctx.StepRunId()] = run
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		if n.runs[ctx.StepRunId()] == run {
			delete(n.runs, ctx.StepRunId())
		}
		n.mu.Unlock()
	}()
	oldworker.SetContextDurableHooks(ctx, &invocationHook{worker: n, id: ctx.StepRunId(), run: run}, listener, n.supported)
	defer oldworker.SetContextDurableHooks(ctx, nil, nil, false)
	// 根执行退出后清除这一代协议登记，唤醒取消后等待迟到 ACK 的协调器
	defer listener.CleanupTaskState(ctx.StepRunId(), run.invocation)
	// 只在根 handler 入口建立 durable 类型；提交/等待使用 e.original 的相同执行身份
	return fn(oldworker.NewDurableHatchetContext(ctx))
}

// invocationHook 捕获具体执行记录，旧 handler 的迟到退出不能修改恢复执行
type invocationHook struct {
	// worker 拥有运行表和锁
	worker *nativeWorker
	// id 是根任务身份，调用钩子不能用于其他任务
	id string
	// run 是这一代执行的记录指针，不按 TaskRunID 重新选取下一代
	run *durableRun
}

// MarkWaiting 只登记仍属于当前 invocation 的等待，例如两次并行子等待计数为 2
func (h *invocationHook) MarkWaiting(id, _, _ string) {
	h.worker.mu.Lock()
	defer h.worker.mu.Unlock()
	if id != h.id || h.worker.runs[id] != h.run {
		return
	}
	if h.run.waiting == 0 {
		h.run.since = time.Now()
	}
	h.run.waiting++
}

// MarkActive 只释放本钩子登记的等待；旧 invocation 退出时对新记录无影响
func (h *invocationHook) MarkActive(id string) {
	h.worker.mu.Lock()
	defer h.worker.mu.Unlock()
	if id != h.id || h.worker.runs[id] != h.run {
		return
	}
	h.run.waiting = max(0, h.run.waiting-1)
	if h.run.waiting == 0 {
		h.run.since = time.Time{}
	}
}

// evictionLoop 定期选择 TTL 或容量候选，不持有运行表锁执行网络请求
func (n *nativeWorker) evictionLoop(ctx context.Context) {
	// ticker 固定频率的状态检查器，退出时停止以释放计时资源
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.evictEligible(ctx, false)
		}
	}
}

// evictWaiting 关闭时释放可重放的挂起执行，排空完成前不关闭 durable 协议
func (n *nativeWorker) evictWaiting(ctx context.Context) { n.evictEligible(ctx, true) }

// evictEligible 保留禁止容量驱逐的任务；TTL=0 表示没有时间驱逐
func (n *nativeWorker) evictEligible(ctx context.Context, all bool) {
	if !n.supported {
		return
	}
	// candidates 保存锁内快照；每轮最多请求一个容量候选，TTL 可同时到期
	type candidate struct {
		// id 真实运行或成员身份，例如 run-1，不能使用显示名称代替
		id string
		// invocation 根执行的恢复次数，不能将旧 invocation 的确认用于当前运行
		invocation int32
		// run 固定选择时的执行记录，迟到 ACK 不得重新查找恢复执行
		run *durableRun
	}
	// candidates 在锁内收集的驱逐快照；网络请求必须在解锁后执行
	var candidates []candidate
	n.mu.Lock()
	// listener 根 Worker 拥有的 durable 监听器，子等待复用相同执行账本
	listener := n.listener
	// full 当前 durable 执行是否占满实例容量，容量驱逐仅选择允许的等待任务
	full := len(n.runs) >= n.config.DurableSlots
	// capacityChosen 本轮是否已选择容量候选，避免一次轮询驱逐所有等待任务
	capacityChosen := false
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
	// ordered 优先选择较低驱逐优先级，同级先处理等待更久的执行，避免 map 顺序改变决策
	ordered := make([]string, 0, len(n.runs))
	// 登记每个真实 TaskRunID，不能把同名任务合并为一个候选
	for id := range n.runs {
		ordered = append(ordered, id)
	}
	sort.Slice(ordered, func(i, j int) bool {
		// left/right 是锁内记录，排序期间不会被其他 goroutine 改写
		left, right := n.runs[ordered[i]], n.runs[ordered[j]]
		if left.policy.Eviction.Value.Priority.Value != right.policy.Eviction.Value.Priority.Value {
			return left.policy.Eviction.Value.Priority.Value < right.policy.Eviction.Value.Priority.Value
		}
		return left.since.Before(right.since)
	})
	// 按稳定优先级检查候选，网络请求留到解锁后
	for _, id := range ordered {
		// run 属于当前候选身份，不能使用另一 invocation 的等待状态
		run := n.runs[id]
		if run.waiting == 0 || run.evicting {
			continue
		}
		// ttl 连续等待的时间上限，显式零表示禁用 TTL 驱逐
		ttl := 15 * time.Minute
		// allow 是否允许容量驱逐；禁止的任务只能按其明确 TTL 处理
		allow := true
		if run.policy.Eviction.Set {
			// p 取得 run.policy.Eviction.Value 的结果，确认成功后才进入下一处理阶段
			p := run.policy.Eviction.Value
			if p.TTL.Set {
				ttl = p.TTL.Value
			}
			if p.AllowCapacityEviction.Set {
				allow = p.AllowCapacityEviction.Value
			}
		}
		// elapsed 当前执行连续挂起的时长，活跃后重新开始计时
		elapsed := time.Since(run.since)
		// eligible 当前 invocation 是否满足驱逐条件，活跃业务不能作为候选
		eligible := all || (ttl > 0 && elapsed >= ttl)
		if !eligible && full && allow && !capacityChosen && elapsed >= 10*time.Second {
			eligible = true
			capacityChosen = true
		}
		if eligible {
			run.evicting = true
			candidates = append(candidates, candidate{id, run.invocation, run})
		}
	}
	n.mu.Unlock()
	if listener == nil {
		return
	}
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
	for _, c := range candidates {
		// budget 此步骤独立的网络或资源清理预算，不继承已到期的业务 deadline
		budget, cancel := context.WithTimeout(ctx, 5*time.Second)
		// err 当前操作错误，失败时不继续使用对应结果
		err := listener.SendEvictionRequest(budget, c.id, int(c.invocation))
		cancel()
		if err == nil {
			n.cancelInvocation(c.run)
		} else {
			n.mu.Lock()
			// run 取得 n.runs[c.id] 的结果，确认成功后才进入下一处理阶段
			if run := n.runs[c.id]; run != nil && run.invocation == c.invocation {
				run.evicting = false
			}
			n.mu.Unlock()
		}
	}
}

// cancelInvocation 只取消已经确认驱逐的具体执行预算；取消函数在发布记录前固定
func (n *nativeWorker) cancelInvocation(run *durableRun) {
	if run.cancel != nil {
		run.cancel()
	}
}
