package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"time"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	clientconfig "github.com/hatchet-dev/hatchet/pkg/config/client"
	oldworker "github.com/hatchet-dev/hatchet/pkg/worker"
	"github.com/hatchet-dev/hatchet/pkg/worker/condition"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// execution 私有保存原始执行上下文，复用 durable 等待序号、memo 和子任务身份
type execution struct {
	// original 后端原始执行上下文，保留 durable 等待序号和父子运行身份
	original oldworker.HatchetContext
	// backend 内部后端接口；业务层不能取出其具体实现
	backend *Backend
	// workerKey 在执行入口固定，注销后仍保留本次执行的逻辑身份
	workerKey string

	// 同一次 durable 调用中的子任务确认共享键；提交需串行到确认完成
	// 结果等待不持有此锁，业务仍可并行等待多个子任务
	spawnMu submissionGate
	// ackGate 隔离共享确认槽，只在确认阶段持有
	ackGate submissionGate
	// lifetime 是根 handler 的生命周期，子观察者不拥有它
	lifetime context.Context
	// callbacksMu 保护稳定事件节点到共享观察器的映射
	callbacksMu sync.Mutex
	// callbacks 为同一节点保留一次底层等待及不可变结果
	callbacks map[callbackKey]*sharedCallback
	// now 保存本次 invocation 已确认的时间，由 spawnMu 保护
	// 同一 handler 多次 Now 不新增 memo 节点；恢复执行重新查询引擎记录
	now *time.Time
}

// Info 读取当前任务或实例的信息；任务身份只存在于 Worker 入口
func (e *execution) Info() model.TaskInfo {
	// count 保留原执行的 durable invocation 次数，普通和恢复执行不能共用新建状态
	count := e.original.DurableTaskInvocationCount()
	// _, durable 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值
	_, durable := e.original.(oldworker.DurableHatchetContext)
	if durable && count == 0 {
		count = 1
	}
	return model.TaskInfo{
		Durable:            durable,
		RunID:              e.original.WorkflowRunId(),
		TaskRunID:          e.original.StepRunId(),
		WorkerID:           e.original.WorkerId(),
		WorkerKey:          e.workerKey,
		RetryCount:         e.original.RetryCount(),
		InvocationCount:    count,
		ParentRunID:        e.original.ParentWorkflowRunId(),
		FilterPayload:      cloneMap(e.original.FilterPayload()),
		AdditionalMetadata: maps.Clone(e.original.AdditionalMetadata()),
		WorkerLabels:       cloneMap(e.original.Worker().GetLabels()),
	}
}

// cloneMap 复制 map 容器，使调用方修改返回值不会改写内部记录
func cloneMap(input map[string]any) map[string]any {
	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
	var out map[string]any
	_ = convert(input, &out)
	return out
}

// durableProvider 只读取根执行已有的基础设施，不替换或并发修改原始 context
type durableProvider interface {
	// DurableTaskListener 返回根执行共享的 durable 协议监听器
	DurableTaskListener() *v0.DurableTaskListener
	// DurableEvictionSupported 返回启动探测确认的协议能力
	DurableEvictionSupported() bool
	// DurableEvictionHook 返回引用计数式等待钩子，驱逐只处理挂起执行
	DurableEvictionHook() oldworker.DurableEvictionHook
}

// durableListener 返回根执行共享的监听器；例如并行子结果使用不同 branch/node 注册回调
func (e *execution) durableListener() *v0.DurableTaskListener {
	// p, ok 取得 e.original. 的结果，确认成功后才进入下一处理阶段
	if p, ok := e.original.(durableProvider); ok {
		return p.DurableTaskListener()
	}
	return nil
}

// durableSupported 检查服务能力，不能把本地临时计时冒充可重放等待
func (e *execution) durableSupported() bool {
	// p, ok 取得 e.original. 的结果，确认成功后才进入下一处理阶段
	p, ok := e.original.(durableProvider)
	return ok && p.DurableEvictionSupported()
}

// waiting 引用计数保留并行等待；返回回调必须与每次登记配对
func (e *execution) waiting(kind, resource string) func() {
	// p, ok 取得 e.original. 的结果，确认成功后才进入下一处理阶段
	if p, ok := e.original.(durableProvider); ok {
		// hook 取得 p.DurableEvictionHook 的结果，确认成功后才进入下一处理阶段
		if hook := p.DurableEvictionHook(); hook != nil {
			hook.MarkWaiting(e.original.StepRunId(), kind, resource)
			return func() { hook.MarkActive(e.original.StepRunId()) }
		}
	}
	return func() {}
}

// waitCondition 直接使用公开 durable 协议并贯通调用方预算；不重建父执行的重放状态
func (e *execution) waitCondition(ctx context.Context, c condition.Condition, kind, key string) ([]byte, error) {
	if !e.Info().Durable || e.durableListener() == nil || !e.durableSupported() {
		return nil, model.ErrDurableContext
	}
	// err 当前操作错误，失败时不继续使用对应结果
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	defer e.waiting(kind, key)()
	// conditions 取得 c.ToPB 的结果，确认成功后才进入下一处理阶段
	conditions := c.ToPB(v1.Action_CREATE)
	// ack 只确认等待节点已经记录，长期等待在释放 ACK 登记权后执行
	ack, err := e.eventAck(ctx, &v1.DurableTaskRequest{Message: &v1.DurableTaskRequest_WaitFor{WaitFor: &v1.DurableTaskWaitForRequest{
		DurableTaskExternalId: e.original.StepRunId(), InvocationCount: int32(e.Info().InvocationCount),
		WaitForConditions: &v1.DurableEventListenerConditions{SleepConditions: conditions.SleepConditions, UserEventConditions: conditions.UserEventConditions},
	}}})
	if err != nil {
		return nil, err
	}
	// ref 是引擎分配的稳定节点身份，例如 branch=1、node=2
	ref, err := waitReference(ack)
	if err != nil {
		return nil, err
	}
	return e.callback(callbackKey{ref.BranchId, ref.NodeId}).wait(ctx, e, callbackKey{ref.BranchId, ref.NodeId})
}

// Sleep 使用引擎记录的等待，重放时恢复同一个事件日志节点；例如 2 秒等待不因驱逐重新计时
func (e *execution) Sleep(ctx context.Context, d time.Duration) error {
	// _, err 取得 e.waitCondition 的结果，确认成功后才进入下一处理阶段
	_, err := e.waitCondition(ctx, condition.SleepCondition(d), "sleep", d.String())
	return err
}

// WaitEvent 复制 scope/lookback 条件，并解码对应事件的首个匹配数据
func (e *execution) WaitEvent(ctx context.Context, key, expression string, opts model.EventWait) (map[string]any, error) {
	// options 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障
	var options []condition.UserEventConditionOpt
	if opts.Scope != nil {
		options = append(options, condition.WithEventScope(*opts.Scope))
	}
	if opts.ConsiderEventsSince != nil {
		options = append(options, condition.WithConsiderEventsSince(*opts.ConsiderEventsSince))
	}
	key = clientconfig.ApplyNamespace(key, &e.backend.config.Namespace)
	// data 编码后的业务或协议字节，只有编码成功才可交付
	data, err := e.waitCondition(ctx, condition.UserEventCondition(key, expression, options...), "wait_for_event", key)
	if err != nil {
		return nil, err
	}
	// 引擎结果按条件组、可读键、匹配列表分层；例如 {group:{ready:[{value:7}]}} 返回 {value:7}
	var groups map[string]map[string][]map[string]any
	// err 当前操作错误，失败时不继续使用对应结果
	if err := json.Unmarshal(data, &groups); err != nil {
		return nil, err
	}
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
	for _, group := range groups {
		// rows 取得 group[key] 的结果，确认成功后才进入下一处理阶段
		if rows := group[key]; len(rows) > 0 {
			return rows[0], nil
		}
	}
	return nil, fmt.Errorf("wego: event wait result missing key %s", key)
}

// Now 用固定 memo 键持久化 UTC 时间；恢复执行读取原值而不访问新的墙上时钟
func (e *execution) Now(ctx context.Context) (time.Time, error) {
	if !e.Info().Durable || e.durableListener() == nil || !e.durableSupported() {
		return time.Time{}, model.ErrDurableContext
	}
	// err 当前操作错误，失败时不继续使用对应结果
	if err := e.spawnMu.Lock(ctx); err != nil {
		return time.Time{}, err
	}
	defer e.spawnMu.Unlock()
	// 例如首次得到 12:00:00，Sleep 后再次调用仍返回 12:00:00
	// 每个 invocation 独立缓存，不能用进程级缓存代替引擎重放
	if e.now != nil {
		return *e.now, nil
	}
	// listener 根 Worker 拥有的 durable 监听器，子等待复用相同执行账本
	listener := e.durableListener()
	// key 当前事件、memo 或子调用的稳定键，不能随重放随机改变
	key := []byte("wego.now")
	// response 通过统一 ACK 协调器查询 memo，不能与 Sleep 登记争用同一槽
	response, err := e.eventAck(ctx, &v1.DurableTaskRequest{Message: &v1.DurableTaskRequest_Memo{Memo: &v1.DurableTaskMemoRequest{
		DurableTaskExternalId: e.original.StepRunId(), InvocationCount: int32(e.Info().InvocationCount), Key: key,
	}}})
	if err != nil {
		return time.Time{}, err
	}
	// ack 必须含 memo 引用；其他类型的确认不能当作时间记录
	ack := response.GetMemoAck()
	if ack == nil || ack.Ref == nil {
		return time.Time{}, fmt.Errorf("wego: memo acknowledgment missing reference")
	}
	// data 编码后的业务或协议字节，只有编码成功才可交付
	data := ack.MemoResultPayload
	// 已建立但未完成的 memo 仍需计算；例如请求确认后发生驱逐，恢复时可能只有引用而没有载荷
	if !ack.MemoAlreadyExisted || len(data) == 0 {
		data, err = json.Marshal(time.Now().UTC())
		if err == nil {
			err = listener.SendMemoCompleted(ctx, ack.Ref, key, data)
		}
		if err != nil {
			return time.Time{}, Normalize(err)
		}
	}
	// out 当前操作的转换目标，验证成功后才向调用者返回
	var out time.Time
	err = json.Unmarshal(data, &out)
	if err == nil {
		e.now = &out
	}
	return out, err
}

// childRef 保存稳定的引擎 branch/node 身份，子结果等待不占用提交锁
func (e *execution) childRef(entry v0.TriggerRunAckEntry, name string) ports.Run {
	return ports.Run{ID: entry.WorkflowRunID, Wait: func(ctx context.Context) (ports.Result, error) {
		defer e.waiting("spawn_child", name)()
		// data 编码后的业务或协议字节，只有编码成功才可交付
		data, err := e.callback(callbackKey{entry.BranchID, entry.NodeID}).wait(ctx, e, callbackKey{entry.BranchID, entry.NodeID})
		if err != nil {
			return ports.Result{}, Normalize(err)
		}
		// payload 保留完整 JSON 值，独立任务可以返回对象、数值、字符串或 null
		var payload any
		// err 当前操作错误，失败时不继续使用对应结果
		if err := json.Unmarshal(data, &payload); err != nil {
			return ports.Result{}, err
		}
		// 同步订阅提供 task-name→output，durable 事件的单任务结果直接是业务对象
		e.backend.featureMu.Lock()
		// names 注册任务的结果键；单任务 durable 输出需要恢复这一层映射
		names := append([]string(nil), e.backend.workflowTasks[name]...)
		e.backend.featureMu.Unlock()
		// 远端定义没有本地任务名读取同一个已完成 RunID 的持久结果，
		// 不能把 {Count:6, ParentRunID:...} 的业务字段猜成多个任务输出
		// 这里只恢复结果形状，不重新提交子任务，也不重建 durable 上下文
		if len(names) == 0 {
			return e.backend.runRef(entry.WorkflowRunID).Wait(ctx)
		}
		if len(names) == 1 {
			return ports.Result{RunID: entry.WorkflowRunID, Outputs: map[string]any{names[0]: payload}}, nil
		}
		// outputs 多任务结果必须有显式任务键，不能把标量猜成 DAG 输出
		outputs, ok := payload.(map[string]any)
		if !ok {
			return ports.Result{}, fmt.Errorf("wego: durable workflow output is not a task map")
		}
		return ports.Result{RunID: entry.WorkflowRunID, Outputs: outputs}, nil
	}}
}

// Refresh 向后端延长当前任务执行预算，调用失败必须结束续期
func (e *execution) Refresh(ctx context.Context, d time.Duration) error {
	return Normalize(e.backend.raw.Dispatcher().RefreshTimeout(ctx, e.original.StepRunId(), d.String()))
}

// Stream 将业务字节发布到当前 TaskRunID 的流出口，调用方不需要取得后端执行对象
func (e *execution) Stream(ctx context.Context, payload []byte) error {
	return e.backend.Publish(ctx, e.original.StepRunId(), payload)
}

// ParentOutput 按父任务名称解码已完成的父输出，避免把整个结果映射作为单个业务值
func (e *execution) ParentOutput(name string, out any) error {
	return Normalize(e.original.StepOutput(name, out))
}
