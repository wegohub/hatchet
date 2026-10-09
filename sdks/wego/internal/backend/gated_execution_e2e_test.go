//go:build e2e

package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/option"
)

// p0ClaimKey 在候选执行中传递门禁结果，不把后端对象放进公开上下文
type p0ClaimKey struct{}

// p0ClaimContext 保留当前 writer 与已确认的历史前缀
type p0ClaimContext struct {
	// request 是本次执行独占的 producer 身份
	request stream.ClaimRequest
	// result 是持久日志中的所有权和前缀确认
	result stream.ClaimResult
}

// p0Backend 为每个真实验收创建唯一 namespace；凭证仅从环境读入连接配置
func p0Backend(t *testing.T) (*Backend, spec.Runtime, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	config := spec.Defaults()
	config.Token = os.Getenv("HATCHET_CLIENT_TOKEN")
	if config.Token == "" {
		t.Fatal("required real-engine token missing; P0 cannot skip")
	}
	config.Address, config.ServerURL = "localhost:7077", "http://localhost:8080"
	config.TLS, config.TLSSet = nil, true
	config.Namespace = "p0-execution-" + uuid.NewString()
	config.Slots = 2
	config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	b, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, config, ctx
}

// TestDurableStreamsGatedWorkerP0 真实调度一个任务并触发应用重试，同时人工注入同代次重复动作
// 人工动作验证协议防御，不冒充引擎自然重复；任务完成和 slot 释放仍由真实引擎负责
func TestDurableStreamsGatedWorkerP0(t *testing.T) {
	b, config, ctx := p0Backend(t)
	codec, err := wire.NewFrameCodec(nil, 4<<20, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := stream.InputDigest("/fixture/Probe", []json.RawMessage{json.RawMessage(`{"value":"one"}`)}, map[string]any{"cost": 1})
	if err != nil {
		t.Fatal(err)
	}
	callInput := map[string]any{"version": 4, "method": "/fixture/Probe", "input_digest": digest, "key": config.Namespace + ".operation", "payload": map[string]any{"value": "one"}, "routing": map[string]any{"cost": 1}}
	claims := make(chan p0ClaimContext, 8)
	firstStarted := make(chan model.TaskInfo, 1)
	releaseFirst := make(chan struct{})
	secondStarted := make(chan model.TaskInfo, 1)
	releaseSecond := make(chan struct{})
	// invocations 统计真正进入业务的执行，落选分发不得增加
	var invocations atomic.Int32
	definition := ports.Definition{Name: "reliable-probe", Policy: spec.Task{Retries: option.Some(1), IdempotencyExpression: "input.key", IdempotencyTTL: 24 * time.Hour}}
	definition.BeforeStart = func(ctx context.Context, info ports.StartInfo) (ports.Admission, error) {
		request := stream.ClaimRequest{Namespace: config.Namespace, TaskID: info.Task.TaskRunID, RunID: info.Task.RunID, Method: "/fixture/Probe",
			WorkerKey: info.WorkerKey, Writer: uuid.NewString(), Epoch: int32(info.Task.RetryCount), InputDigest: digest}
		result, err := stream.Claim(ctx, b, codec, request)
		if err != nil {
			return ports.Admission{}, err
		}
		claims <- p0ClaimContext{request, result}
		return ports.Admission{Owned: result.Owned, Context: context.WithValue(ctx, p0ClaimKey{}, p0ClaimContext{request, result})}, nil
	}
	definition.Function = func(ctx context.Context, input map[string]any) (any, error) {
		invocations.Add(1)
		state, _ := callctx.Get(ctx)
		info := state.Execution.Info()
		claim := ctx.Value(p0ClaimKey{}).(p0ClaimContext)
		if claim.result.Prefix.LastOutput != uint64(info.RetryCount) {
			return nil, errors.New("replay prefix mismatch")
		}
		// endID 与引擎最终成功结果中的结束清单保持同一确切帧身份
		var endID string
		publish := func(sequence int64, kind string) error {
			frame := &wire.LogFrame{Version: wire.LogVersion, Kind: kind, TaskRunId: info.TaskRunID, RunId: info.RunID,
				Method: claim.request.Method, InputDigest: claim.request.InputDigest, Epoch: int32(info.RetryCount), Writer: claim.request.Writer,
				WorkerKey: claim.request.WorkerKey, FrameId: uuid.NewString(), OutputSeq: uint64(info.RetryCount + 1)}
			if kind == "ATTEMPT_END" {
				endID = frame.FrameId
			}
			encoded, err := codec.Encode(ctx, frame.Method, frame)
			if err != nil {
				return err
			}
			return stream.PublishFixed(ctx, b, ports.DurableMessage{Namespace: config.Namespace, Topic: stream.Topic(info.TaskRunID),
				Producer: fmt.Sprintf("%s:%d", info.TaskRunID, info.RetryCount), Sequence: sequence, Payload: encoded})
		}
		if err := publish(1, "HEADERS"); err != nil {
			return nil, err
		}
		if err := publish(2, "DATA"); err != nil {
			return nil, err
		}
		if err := publish(3, "ATTEMPT_END"); err != nil {
			return nil, err
		}
		if info.RetryCount == 0 {
			select {
			case firstStarted <- info:
			default:
			}
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			detailed, err := status.New(codes.Aborted, "retry fixture").WithDetails(&errdetails.ErrorInfo{Reason: "ATTEMPT_ZERO"})
			if err != nil {
				return nil, err
			}
			return nil, detailed.Err()
		}
		select {
		case secondStarted <- info:
		default:
		}
		select {
		case <-releaseSecond:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return stream.Completion{Version: wire.LogVersion, TaskID: info.TaskRunID, Epoch: int32(info.RetryCount), Writer: claim.request.Writer, LastOutput: uint64(info.RetryCount + 1), EndID: endID}, nil
	}
	workers := make([]*worker, 2)
	for i := range workers {
		w, err := b.Worker(ctx, fmt.Sprintf("probe-%d", i), []ports.Definition{definition}, config, nil)
		if err != nil {
			t.Fatal(err)
		}
		workers[i] = w.(*worker)
		if err := w.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := w.WaitReady(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			budget, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = w.Close(budget)
		})
	}
	run, err := b.Run(ctx, definition.Name, callInput, model.RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// info 从业务启动屏障取得真实运行身份
	var info model.TaskInfo
	select {
	case info = <-firstStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	winner := <-claims
	// original 保存旧 attempt 的真实动作，故障注入仍使用明确原始 RetryCount
	var original *v0.Action
	for _, w := range workers {
		if w.ID() == info.WorkerID {
			w.runner.mu.Lock()
			copy := *w.runner.latest[info.TaskRunID].action
			original = &copy
			w.runner.mu.Unlock()
		}
	}
	if !winner.result.Owned {
		t.Fatal("scheduled execution lost initial claim")
	}
	// END 已持久写入时引擎仍在运行，不能仅根据 END 宣告成功
	var detail model.RunStatus
	if err := b.Feature(ctx, ports.RunsGetStatus{ID: run.ID}, &detail); err != nil {
		t.Fatal(err)
	}
	if detail == model.Completed {
		t.Fatal("attempt end was interpreted as final engine success")
	}
	// 将同代次 START 注入另一个真实注册实例；落选者不得启动或报告终态
	loser := workers[0]
	if loser.ID() == info.WorkerID {
		loser = workers[1]
	}
	workflow, err := workflowDefinition(definition.Name, definition.Policy, config.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	task, err := taskDefinition(workflow.Name, definition)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"input": callInput})
	loser.runner.handle(ctx, &v0.Action{ActionType: v0.ActionTypeStartStepRun, ActionId: task.Action, WorkerId: loser.ID(),
		StepRunId: info.TaskRunID, WorkflowRunId: run.ID, RetryCount: 0, ActionPayload: payload})
	select {
	case rejected := <-claims:
		if rejected.result.Owned {
			t.Fatal("duplicate scheduled writer won")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(releaseFirst)
	select {
	case <-secondStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// 绕过本地过滤注入旧成功/失败，验证官方真实终结路径按 retry 隔离
	now := time.Now().UTC()
	for _, eventType := range []v0.ActionEventType{v0.ActionEventTypeCompleted, v0.ActionEventTypeFailed} {
		// value 注入旧执行结果，不能污染当前代次的持久结果
		var value any = map[string]any{"epoch": -1}
		if eventType == v0.ActionEventTypeFailed {
			value = "injected stale failure"
		}
		if _, err := b.raw.Dispatcher().SendStepActionEvent(ctx, &v0.ActionEvent{Action: original, EventType: eventType, EventTimestamp: &now, EventPayload: value}); err != nil {
			t.Fatal(err)
		}
	}
	close(releaseSecond)
	result, err := run.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// output 是最终 retry=1 的持久结果，不接受首次 attempt 的结束帧替代
	var output map[string]any
	for _, value := range result.Outputs {
		output, _ = value.(map[string]any)
	}
	if output["epoch"] != float64(1) || output["last_output"] != float64(2) || invocations.Load() != 2 {
		t.Fatalf("retry result mismatch: output=%v executions=%d", output, invocations.Load())
	}
	// 真实持久日志与最终引擎清单必须闭合，而不是只检查客户端得到一个成功值
	state := stream.State{TaskID: info.TaskRunID, RunID: run.ID, Method: "/fixture/Probe", InputDigest: digest}
	delivered := uint64(0)
	complete := errors.New("effective output read complete")
	err = b.SubscribeDurable(ctx, ports.DurableSubscription{Namespace: config.Namespace, Topic: stream.Topic(info.TaskRunID)}, func(entry ports.DurableEntry) error {
		frame, err := codec.Decode(ctx, state.Method, entry.Payload)
		if err != nil {
			return err
		}
		accepted, err := state.Apply(frame)
		if err != nil {
			return err
		}
		if accepted {
			delivered++
		}
		if state.End != nil && state.Epoch == 1 {
			return complete
		}
		return nil
	})
	if !errors.Is(err, complete) {
		t.Fatal(err)
	}
	finalData, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}
	// final 是权威成功结果中保存的 exact attempt 清单
	var final stream.Completion
	if err := json.Unmarshal(finalData, &final); err != nil {
		t.Fatal(err)
	}
	if err := stream.VerifyCompletion(state, delivered, final); err != nil {
		t.Fatal(err)
	}
	// 幂等冲突必须保留已存在的真实 RunID，不能只剩错误文本
	_, err = b.Run(ctx, definition.Name, callInput, model.RunOptions{})
	// collision 必须携带原 RunID，证明恢复身份未丢失
	var collision *model.IdempotencyCollisionError
	if !errors.As(err, &collision) || collision.ExistingRunID != run.ID {
		t.Fatalf("idempotency identity: %v", err)
	}
	// 冲突后核对官方持久输入；不同 payload 或 routing 不能复用已有运行
	persisted, err := b.RunInput(ctx, collision.ExistingRunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.VerifyRunIdentity(persisted, "/fixture/Probe", digest); err != nil {
		t.Fatal(err)
	}
	otherDigest, err := stream.InputDigest("/fixture/Probe", []json.RawMessage{json.RawMessage(`{"value":"two"}`)}, map[string]any{"cost": 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.VerifyRunIdentity(persisted, "/fixture/Probe", otherDigest); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("different input accepted after collision: %v", err)
	}
	// 批量部分冲突仍必须保留两项成功运行与原冲突 RunID
	first, second := cloneMap(callInput), cloneMap(callInput)
	first["key"], second["key"] = config.Namespace+".batch-a", config.Namespace+".batch-b"
	_, err = b.RunMany(ctx, definition.Name, []model.RunManyInput{{Input: first}, {Input: callInput}, {Input: second}})
	// bulk 保留真实部分成功身份，不能将整批归类为无运行
	var bulk *model.BulkIdempotencyCollisionError
	if !errors.As(err, &bulk) || len(bulk.SuccessfulRunIDs) != 2 || len(bulk.Collisions) != 1 || bulk.Collisions[0].ExistingRunID != run.ID {
		t.Fatalf("partial bulk identities lost: %#v %v", bulk, err)
	}
	for _, id := range bulk.SuccessfulRunIDs {
		if _, err := b.runRef(id).Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, w := range workers {
		if err := waitP0Slots(ctx, b, w.ID(), config.Slots); err != nil {
			t.Fatal(err)
		}
	}
	// 同一 namespace 的两份实例定义注册后只产生一个逻辑任务入口
	writeP0Report(t, b, "v020-p0-execution", map[string]any{"namespace": config.Namespace, "run_id": run.ID, "task_run_id": info.TaskRunID, "winner_worker_id": info.WorkerID, "worker_key": winner.request.WorkerKey, "final_epoch": final.Epoch, "final_writer": final.Writer, "end_id": final.EndID, "outputs": delivered, "handler_calls_total": invocations.Load(), "bulk_successful_run_ids": bulk.SuccessfulRunIDs, "checks": []string{"same_epoch_gate", "real_retry_prefix", "stale_terminal_rpc_filter", "matching_persisted_completion", "single_and_bulk_idempotency_identity", "different_input_rejected", "slots_released"}, "cleanup": "workers drained and closed; definitions and run/topic history retained under server policy"})
	t.Logf("verified real RunID=%s TaskRunID=%s epoch=1 executions=%d; duplicate same-epoch writer rejected", run.ID, info.TaskRunID, invocations.Load())
	// 等待所有实例的本地上报资源排空，Close 会确认自己的 pending 表不包含其他实例
	var closeGroup sync.WaitGroup
	for _, w := range workers {
		closeGroup.Add(1)
		go func() {
			defer closeGroup.Done()
			budget, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := w.Close(budget); err != nil {
				t.Errorf("worker cleanup: %v", err)
			}
		}()
	}
	closeGroup.Wait()
}

// TestGatedFinalFailureAndCancellationP0 验证业务详情、panic、控制台取消和无结束帧的超时终结
func TestGatedFinalFailureAndCancellationP0(t *testing.T) {
	for _, scenario := range []string{"status_details", "panic", "console_cancel", "timeout_without_end"} {
		t.Run(scenario, func(t *testing.T) {
			b, config, ctx := p0Backend(t)
			codec, err := wire.NewFrameCodec(nil, 4<<20, 4<<20)
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan struct{})
			// 控制台取消使用独立宽预算，避免把引擎超时与取消竞赛混为同一断言
			// 只有 timeout_without_end 必须由 2 秒引擎预算形成真实超时终态
			executionTimeout := 30 * time.Second
			if scenario == "timeout_without_end" {
				executionTimeout = 2 * time.Second
			}
			definition := ports.Definition{Name: "terminal-probe", Policy: spec.Task{Retries: option.Some(0), ExecutionTimeout: option.Some(executionTimeout)}}
			definition.BeforeStart = func(ctx context.Context, info ports.StartInfo) (ports.Admission, error) {
				result, err := stream.Claim(ctx, b, codec, stream.ClaimRequest{Namespace: config.Namespace, TaskID: info.Task.TaskRunID,
					RunID: info.Task.RunID, Method: "/fixture/Terminal", WorkerKey: info.WorkerKey, Writer: uuid.NewString(), Epoch: int32(info.Task.RetryCount), InputDigest: "fixture"})
				return ports.Admission{Owned: result.Owned}, err
			}
			definition.Function = func(ctx context.Context, _ map[string]any) (any, error) {
				close(started)
				switch scenario {
				case "status_details":
					detailed, err := status.New(codes.Aborted, "fixture terminal failure").WithDetails(&errdetails.ErrorInfo{Reason: "ATTEMPT_ZERO", Metadata: map[string]string{"epoch": "0"}})
					if err != nil {
						return nil, err
					}
					return nil, detailed.Err()
				case "panic":
					panic("fixture business panic")
				default:
					<-ctx.Done()
					return nil, ctx.Err()
				}
			}
			w, err := b.Worker(ctx, "terminal-probe", []ports.Definition{definition}, config, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Start(ctx); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				budget, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = w.Close(budget)
			})
			if err := w.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			run, err := b.Run(ctx, definition.Name, map[string]any{}, model.RunOptions{})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if scenario == "console_cancel" {
				if err := b.Feature(ctx, ports.RunsCancel{Request: model.Resource{"externalIds": []string{run.ID}}}, nil); err != nil {
					t.Fatal(err)
				}
			}
			_, err = run.Wait(ctx)
			if err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("missing or indefinitely awaited engine terminal: %v", err)
			}
			switch scenario {
			case "status_details":
				if status.Code(err) != codes.Aborted {
					t.Fatal(err)
				}
				details := status.Convert(err).Details()
				if len(details) != 1 {
					t.Fatalf("status details lost: %v", details)
				}
				info, ok := details[0].(*errdetails.ErrorInfo)
				if !ok || info.Reason != "ATTEMPT_ZERO" || info.Metadata["epoch"] != "0" {
					t.Fatal(details)
				}
			case "panic":
				if status.Code(err) != codes.Internal {
					t.Fatal(err)
				}
			case "console_cancel":
				if status.Code(err) != codes.Canceled {
					t.Fatal(err)
				}
			}
			if err := waitP0Slots(ctx, b, w.ID(), config.Slots); err != nil {
				t.Fatal(err)
			}
			writeP0Report(t, b, "v020-p0-terminal-"+scenario, map[string]any{"namespace": config.Namespace, "run_id": run.ID, "outcome": scenario, "code": status.Code(err).String(), "checks": []string{"authoritative_terminal", "no_indefinite_wait", "slots_released"}, "cleanup": "worker cleanup scheduled; definition and run/topic history retained under server policy"})
			t.Logf("verified terminal=%s RunID=%s code=%s and released slots", scenario, run.ID, status.Code(err))
		})
	}
}

// waitP0Slots 读取真实注册 Worker 的容量，迟到旧报告不能重复释放使 available 超过 limit
func waitP0Slots(ctx context.Context, b *Backend, workerID string, expected int) error {
	for {
		// resource 使用 wego 自有动态 DTO，slotConfig 必须完整提供真实容量
		var resource model.Resource
		if err := b.Feature(ctx, ports.WorkersGet{ID: workerID}, &resource); err != nil {
			return err
		}
		config, ok := resource["slotConfig"].(map[string]any)
		if !ok {
			return errors.New("worker slot config missing")
		}
		slot, ok := config["default"].(map[string]any)
		if !ok {
			return errors.New("worker default slot missing")
		}
		available, limit := slot["available"], slot["limit"]
		if limit != float64(expected) {
			return fmt.Errorf("worker slot limit mismatch: %v", slot)
		}
		if value, ok := available.(float64); ok && value > float64(expected) {
			return fmt.Errorf("stale terminal overreleased slot: %v", slot)
		}
		if available == float64(expected) {
			return nil
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// TestGatedRejectedAdmissionSlotsP0 门禁落选或确认不明确时，真实任务靠引擎终结释放容量，不能假报成功
func TestGatedRejectedAdmissionSlotsP0(t *testing.T) {
	for _, scenario := range []string{"lost", "uncertain"} {
		t.Run(scenario, func(t *testing.T) {
			b, config, ctx := p0Backend(t)
			codec, err := wire.NewFrameCodec(nil, 4<<20, 4<<20)
			if err != nil {
				t.Fatal(err)
			}
			gateFinished := make(chan struct{})
			// business 只统计实际启动的 handler，两个拒绝路径都应为零
			var business atomic.Int32
			definition := ports.Definition{Name: "rejected-probe", Policy: spec.Task{Retries: option.Some(0), ExecutionTimeout: option.Some(2 * time.Second)}}
			definition.BeforeStart = func(ctx context.Context, info ports.StartInfo) (ports.Admission, error) {
				defer close(gateFinished)
				request := stream.ClaimRequest{Namespace: config.Namespace, TaskID: info.Task.TaskRunID, RunID: info.Task.RunID, Method: "/fixture/Rejected", WorkerKey: info.WorkerKey, Writer: uuid.NewString(), InputDigest: "fixture"}
				if scenario == "uncertain" {
					request.Timeout = 50 * time.Millisecond
					_, err := stream.Claim(ctx, &failedReadback{DurableStreams: b}, codec, request)
					return ports.Admission{}, err
				}
				if _, err := stream.Claim(ctx, b, codec, request); err != nil {
					return ports.Admission{}, err
				}
				request.Writer = uuid.NewString()
				result, err := stream.Claim(ctx, b, codec, request)
				return ports.Admission{Owned: result.Owned}, err
			}
			definition.Function = func(context.Context, map[string]any) (any, error) {
				business.Add(1)
				return map[string]any{"unexpected": true}, nil
			}
			w, err := b.Worker(ctx, "rejected-probe", []ports.Definition{definition}, config, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Start(ctx); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				budget, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = w.Close(budget)
			})
			if err := w.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			run, err := b.Run(ctx, definition.Name, map[string]any{}, model.RunOptions{})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-gateFinished:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// 不明确候选只退出本地执行；运行由引擎执行超时终结，其他任务仍可由此 Worker 消费
			_, err = run.Wait(ctx)
			if err == nil || errors.Is(err, context.DeadlineExceeded) || business.Load() != 0 {
				t.Fatalf("rejected execution leaked or failed to terminate: calls=%d error=%v", business.Load(), err)
			}
			if err := waitP0Slots(ctx, b, w.ID(), config.Slots); err != nil {
				t.Fatal(err)
			}
			writeP0Report(t, b, "v020-p0-admission-"+scenario, map[string]any{"namespace": config.Namespace, "run_id": run.ID, "outcome": scenario, "business_calls": business.Load(), "checks": []string{"no_handler", "no_successful_result", "slots_released"}, "cleanup": "worker cleanup scheduled; definition and run/topic history retained under server policy"})
			t.Logf("verified admission=%s RunID=%s no handler, no successful result, slots released", scenario, run.ID)
		})
	}
}
