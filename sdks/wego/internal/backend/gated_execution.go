package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"time"

	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/logging"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// gatedAttempt 的取消记录只属于一个实际投递；迟到返回不能删除较新代次
type gatedAttempt struct {
	// action 是分发消息的独立快照，结果上报始终携带原始 RetryCount
	action *v0.Action
	// writer 是规范 CLAIM 确认后安装的本次 nonce
	writer string
	// cancel 结束门禁、业务及发布，不取消整个逻辑 Run
	cancel context.CancelFunc
}

// gatedRunner 仅执行有持久门禁的非 durable、非 batch 任务
// 每个 Worker 独立拥有此表；TaskRunID 相同的 epoch 0/1 不共享取消句柄
type gatedRunner struct {
	// dispatcher 委托官方上报协议，保留同一注册 Worker 和真实 slots
	dispatcher *dispatcher
	// config 是此 Worker 的独立配置快照
	config spec.Runtime
	// definitions 按已注册 action 路由，不通过展示名称前缀判断
	definitions map[string]ports.Definition
	// panicHandler 接收业务 panic 的规范化视图
	panicHandler func(context.Context, any)
	// mu 保护投递去重及当前执行身份，网络和业务 I/O 不持有它
	mu sync.Mutex
	// latest 只保留活跃投递；完成后的重投通过新的 nonce 读回持久 CLAIM 判定落选
	latest map[string]*gatedAttempt
	// inflight 包含尚未退出的旧代次业务，防止不配合取消的 handler 无界积累
	inflight int
	// errors 只报告门禁不明确、容量超限或上报故障，业务错误通过任务失败路径报告
	errors chan error
}

// newGatedRunner 不启动 goroutine，定义注册完成后才由监听器使用
func newGatedRunner(d *dispatcher, config spec.Runtime, panicHandler func(context.Context, any)) *gatedRunner {
	return &gatedRunner{dispatcher: d, config: config, panicHandler: panicHandler,
		definitions: map[string]ports.Definition{}, latest: map[string]*gatedAttempt{}, errors: make(chan error, 1)}
}

// handle 在官方执行器接收动作前处理持久任务；true 表示该动作不能继续转发
func (r *gatedRunner) handle(ctx context.Context, action *v0.Action) bool {
	definition, registered := r.definitions[action.ActionId]
	r.mu.Lock()
	previous := r.latest[action.StepRunId]
	// CANCEL 的 action 标识可能不完整，取消定位只使用任务与确切代次
	if action.ActionType == v0.ActionTypeCancelStepRun && previous != nil {
		if action.RetryCount == previous.action.RetryCount {
			previous.cancel()
		}
		r.mu.Unlock()
		return true
	}
	if !registered {
		r.mu.Unlock()
		return false
	}
	if action.ActionType != v0.ActionTypeStartStepRun {
		r.mu.Unlock()
		return true
	}
	if previous != nil && action.RetryCount <= previous.action.RetryCount {
		r.mu.Unlock()
		return true
	}
	// 预算含旧代次孤立业务；新代次各有独立取消句柄，但不允许无限积累 goroutine
	if r.inflight >= max(64, r.config.Slots*2) {
		r.mu.Unlock()
		r.fail(status.Error(codes.ResourceExhausted, "wego: reliable active execution budget exhausted"))
		return true
	}
	if previous != nil {
		previous.cancel()
	}
	copy := *action
	copy.ActionPayload = append([]byte(nil), action.ActionPayload...)
	copy.AdditionalMetadata = maps.Clone(action.AdditionalMetadata)
	lifetime, cancel := context.WithCancel(ctx)
	attempt := &gatedAttempt{action: &copy, cancel: cancel}
	r.latest[action.StepRunId] = attempt
	r.inflight++
	r.mu.Unlock()
	owner := r.dispatcher.owner
	owner.mu.Lock()
	owner.pending[attemptKey(copy.StepRunId, copy.RetryCount)] = copy.WorkerId
	owner.nextExecution++
	id := owner.nextExecution
	owner.cancels[id] = executionCancel{workerID: copy.WorkerId, cancel: cancel}
	owner.mu.Unlock()
	go r.execute(lifetime, attempt, definition, id)
	return true
}

// execute 的门禁失败不能变成 FAILED；否则同代次落选者会终结获胜执行
func (r *gatedRunner) execute(ctx context.Context, attempt *gatedAttempt, definition ports.Definition, executionID uint64) {
	owner, action := r.dispatcher.owner, attempt.action
	defer func() {
		attempt.cancel()
		r.mu.Lock()
		if r.latest[action.StepRunId] == attempt {
			delete(r.latest, action.StepRunId)
		}
		r.inflight--
		r.mu.Unlock()
		owner.mu.Lock()
		delete(owner.cancels, executionID)
		delete(owner.pending, attemptKey(action.StepRunId, action.RetryCount))
		owner.mu.Unlock()
	}()
	// payload 只解析官方动作中的任务输入，不把包装字段交给业务
	var payload struct {
		// Input 是官方 START 的任务 JSON；完整动作包装不能直接作为业务输入
		Input json.RawMessage `json:"input"`
	}
	if len(action.ActionPayload) > backendMessageLimit {
		r.fail(status.Error(codes.ResourceExhausted, "wego: dispatched action exceeds backend message limit"))
		return
	}
	if err := json.Unmarshal(action.ActionPayload, &payload); err != nil {
		r.fail(status.Error(codes.DataLoss, "wego: invalid dispatched task input"))
		return
	}
	info := model.TaskInfo{RunID: action.WorkflowRunId, TaskRunID: action.StepRunId, WorkerID: action.WorkerId, WorkerKey: r.dispatcher.key,
		RetryCount: int(action.RetryCount), AdditionalMetadata: maps.Clone(action.AdditionalMetadata), WorkerLabels: cloneMap(r.config.Labels)}
	if action.ParentWorkflowRunId != nil {
		value := *action.ParentWorkflowRunId
		info.ParentRunID = &value
	}
	execution := &gatedExecution{backend: owner.backend, info: info}
	bound := r.bind(ctx, execution)
	admission, err := admit(definition.BeforeStart, bound, ports.StartInfo{Task: execution.Info(), WorkerKey: r.dispatcher.key, Input: payload.Input})
	if err != nil {
		if ctx.Err() == nil {
			r.config.Logger.Warn("reliable execution ownership unconfirmed", "task_id", action.StepRunId, "epoch", action.RetryCount, "error", err)
			if status.Code(err) == codes.Internal {
				r.fail(err)
			}
		}
		return
	}
	if !admission.Owned || ctx.Err() != nil {
		return
	}
	r.mu.Lock()
	attempt.writer = admission.Writer
	r.mu.Unlock()
	if admission.Context != nil {
		bound = admission.Context
	}
	if err := r.report(ctx, action, v0.ActionEventTypeStarted, nil, false); err != nil {
		r.fail(err)
		return
	}
	// input 在门禁确认之后才作为准确的 handler 参数解码
	var input any
	if ctx.Err() != nil {
		return
	}
	if err := json.Unmarshal(payload.Input, &input); err != nil {
		r.fail(status.Error(codes.DataLoss, "wego: dispatched task input is not JSON"))
		return
	}
	result, taskErr := r.invoke(bound, definition.Function, input)
	// 新代次接管或取消后不报告旧结果；它不能释放新代次的 slot 或覆盖终态
	if ctx.Err() != nil {
		return
	}
	r.mu.Lock()
	current := r.latest[action.StepRunId] == attempt
	r.mu.Unlock()
	if !current {
		return
	}
	eventType, value, nonretry := v0.ActionEventTypeCompleted, result, false
	if taskErr != nil {
		eventType, value = v0.ActionEventTypeFailed, wire.EncodeError(taskErr)
		// marker 保留业务明确禁止重试的策略，不以成功结果包装错误
		var marker *model.NonRetryableError
		nonretry = errors.As(taskErr, &marker)
	}
	if err := r.report(ctx, action, eventType, value, nonretry); err != nil {
		r.fail(err)
	}
}

// bind 建立自有普通执行上下文，不伪造 durable 原始上下文或修改全局 trace provider
func (r *gatedRunner) bind(ctx context.Context, execution *gatedExecution) context.Context {
	state := &callctx.State{Execution: execution}
	if r.config.LogReport {
		state.Report = func(ctx context.Context, record slog.Record) error {
			return execution.backend.logRecord(ctx, execution.info.TaskRunID, execution.info.RetryCount, r.config.LogReportTimeout, record)
		}
	}
	carrier := propagation.MapCarrier{}
	for _, key := range []string{"traceparent", "tracestate"} {
		carrier[key] = execution.info.AdditionalMetadata["wego."+key]
	}
	ctx = logging.WithTask(callctx.Bind(propagation.TraceContext{}.Extract(ctx, carrier), state), execution.info)
	if execution.backend.Bind != nil {
		ctx = execution.backend.Bind(ctx)
	}
	return ctx
}

// admit 把用户扩展门禁的 panic 变成实例故障，禁止继续启动或报告业务成功
func admit(gate func(context.Context, ports.StartInfo) (ports.Admission, error), ctx context.Context, info ports.StartInfo) (result ports.Admission, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = status.Errorf(codes.Internal, "wego: admission panic: %v", recovered)
		}
	}()
	return gate(ctx, info)
}

// invoke 保持业务 panic 的失败/重试语义；panic 回调自身失败不能破坏终态上报
func (r *gatedRunner) invoke(ctx context.Context, fn, input any) (result any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = status.Errorf(codes.Internal, "wego: handler panic: %v", recovered)
			if r.panicHandler != nil {
				func() { defer func() { _ = recover() }(); r.panicHandler(ctx, fmt.Sprint(recovered)) }()
			}
		}
	}()
	return invoke(fn, ctx, input)
}

// report 使用原分发身份和明确 RetryCount，确认接收后仍需由运行观察器判断权威终态
func (r *gatedRunner) report(ctx context.Context, action *v0.Action, eventType v0.ActionEventType, payload any, nonretry bool) error {
	budget, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	now := time.Now().UTC()
	event := &v0.ActionEvent{Action: action, EventTimestamp: &now, EventType: eventType, EventPayload: payload}
	if nonretry {
		yes := true
		event.ShouldNotRetry = &yes
	}
	_, err := r.dispatcher.SendStepActionEvent(budget, event)
	return Normalize(err)
}

// fail 只保留首个入口故障，避免故障处理本身阻塞 CANCEL 接收
func (r *gatedRunner) fail(err error) {
	select {
	case r.errors <- err:
	default:
	}
}

// errorsWith 合并官方监听和门禁故障；取消时无需业务 handler 配合即可退出
func (r *gatedRunner) errorsWith(ctx context.Context, source <-chan error) <-chan error {
	out := make(chan error, 1)
	go func() {
		defer close(out)
		for {
			// err 同时承接监听和门禁故障，取消后不再交付
			var err error
			select {
			case <-ctx.Done():
				return
			case err = <-r.errors:
			case value, ok := <-source:
				if !ok {
					source = nil
					continue
				}
				err = value
			}
			if err != nil {
				select {
				case out <- err:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// gatedExecution 仅携带普通任务能力；其取消和子调用计数由所属执行独占
type gatedExecution struct {
	// backend 借用实例连接，不拥有独立网络资源
	backend *Backend
	// info 是真实分发身份，外部读取时复制所有可变字段
	info model.TaskInfo
	// spawnMu 保证子调用索引分配与提交确认顺序稳定
	spawnMu submissionGate
	// childIndex 只在 spawnMu 内访问，同一执行从零开始
	childIndex int32
}

// Info 返回独立身份快照，业务改写 labels 不影响其他调用
func (e *gatedExecution) Info() model.TaskInfo {
	info := e.info
	info.AdditionalMetadata, info.WorkerLabels = maps.Clone(info.AdditionalMetadata), cloneMap(info.WorkerLabels)
	return info
}

// Sleep 不把普通流执行伪装为 durable；持久历史恢复使用流 checkpoint
func (e *gatedExecution) Sleep(context.Context, time.Duration) error { return model.ErrDurableContext }

// WaitEvent 保持普通任务与 durable 等待的明确边界
func (e *gatedExecution) WaitEvent(context.Context, string, string, model.EventWait) (map[string]any, error) {
	return nil, model.ErrDurableContext
}

// Now 普通任务不提供可重放时间
func (e *gatedExecution) Now(context.Context) (time.Time, error) {
	return time.Time{}, model.ErrDurableContext
}

// Refresh 延长真实 TaskRunID 的服务端执行预算
func (e *gatedExecution) Refresh(ctx context.Context, d time.Duration) error {
	return Normalize(e.backend.raw.Dispatcher().RefreshTimeout(ctx, e.info.TaskRunID, d.String()))
}

// Stream 发布实时输出的兼容能力；可靠流使用持久输出发布器
func (e *gatedExecution) Stream(ctx context.Context, data []byte) error {
	return e.backend.Publish(ctx, e.info.TaskRunID, data)
}

// ParentOutput 可靠流是单独任务，本次不允许以 DAG 输出伪造历史恢复
func (e *gatedExecution) ParentOutput(string, any) error {
	return status.Error(codes.FailedPrecondition, "wego: reliable output task has no DAG parent output")
}

// 编译检查确保自有执行能力满足任务端口
var _ ports.Execution = (*gatedExecution)(nil)

// cancelNotice 只取消当前实例内完全匹配的旧执行；迟到消息不影响接管后的 writer
func (r *gatedRunner) cancelNotice(frame *wire.LogFrame) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	attempt := r.latest[frame.TaskRunId]
	if attempt == nil || frame.WorkerKey != r.dispatcher.key || frame.OldEpoch != attempt.action.RetryCount || frame.OldWriter == "" || frame.OldWriter != attempt.writer || frame.Epoch <= frame.OldEpoch {
		return false
	}
	attempt.cancel()
	return true
}
