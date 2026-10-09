package backend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// gateReporter 记录实际进入官方上报接口的动作；gate 落选不能留下 START 或终态
type gateReporter struct {
	// DispatcherClient 的其他协议方法不属于此隔离测试
	v0.DispatcherClient
	// events 有界记录当前测试的上报，不引入真实网络或固定 sleep
	events chan *v0.ActionEvent
}

// SendStepActionEvent 复制事件容器，测试读取期间不借用执行线程的可变指针
func (g *gateReporter) SendStepActionEvent(ctx context.Context, event *v0.ActionEvent) (*v0.ActionEventResponse, error) {
	copy := *event
	select {
	case g.events <- &copy:
		return &v0.ActionEventResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// gateFixture 只创建自有能力与上报替身，取消记录仍走正式的 gatedRunner 路径
func gateFixture(t *testing.T, definition ports.Definition) (*gatedRunner, *gateReporter, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	b := &Backend{}
	owner := &transport{backend: b, pending: map[string]string{}, cancels: map[uint64]executionCancel{}, reports: map[string]context.Context{}}
	b.observer = owner
	reporter := &gateReporter{events: make(chan *v0.ActionEvent, 8)}
	d := &dispatcher{DispatcherClient: reporter, owner: owner, key: "instance-key"}
	runner := newGatedRunner(d, spec.Defaults(), nil)
	runner.definitions["fixture:invoke"] = definition
	return runner, reporter, ctx
}

// gateAction 为相同 TaskRunID 构造明确的 retry 代次，输入为受控 JSON
func gateAction(epoch int32) *v0.Action {
	return &v0.Action{StepRunId: "task", WorkflowRunId: "run", WorkerId: "worker", ActionId: "fixture:invoke",
		RetryCount: epoch, ActionType: v0.ActionTypeStartStepRun, ActionPayload: []byte(`{"input":{"n":1}}`)}
}

// nextGateEvent 等待事实屏障，上报缺失会在测试预算内失败
func nextGateEvent(t *testing.T, ctx context.Context, reporter *gateReporter) *v0.ActionEvent {
	t.Helper()
	select {
	case event := <-reporter.events:
		return event
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return nil
	}
}

// TestGatedRunnerDuplicateAndCancellation 验证旧执行延迟退出不会取消、清除或完成较新代次
func TestGatedRunnerDuplicateAndCancellation(t *testing.T) {
	oldReturned := make(chan struct{})
	releaseOld := make(chan struct{})
	releaseNew := make(chan struct{})
	started := make(chan int, 4)
	// calls 等待两个已经开始的业务退出，结束断言不靠延时猜测
	var calls sync.WaitGroup
	definition := ports.Definition{
		BeforeStart: func(context.Context, ports.StartInfo) (ports.Admission, error) {
			return ports.Admission{Owned: true}, nil
		},
		Function: func(ctx context.Context, _ map[string]any) (any, error) {
			calls.Add(1)
			defer calls.Done()
			state, _ := callctx.Get(ctx)
			epoch := state.Execution.Info().RetryCount
			started <- epoch
			if epoch == 0 {
				<-releaseOld
				close(oldReturned)
				return nil, errors.New("late old failure")
			}
			select {
			case <-releaseNew:
				return map[string]any{"epoch": epoch}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	r, reporter, ctx := gateFixture(t, definition)
	r.handle(ctx, gateAction(0))
	if event := nextGateEvent(t, ctx, reporter); event.EventType != v0.ActionEventTypeStarted {
		t.Fatal(event)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// 同代次重投应在门禁之前合并，不新增 handler 或 START
	r.handle(ctx, gateAction(0))
	r.handle(ctx, gateAction(1))
	if event := nextGateEvent(t, ctx, reporter); event.RetryCount != 1 || event.EventType != v0.ActionEventTypeStarted {
		t.Fatal(event)
	}
	select {
	case epoch := <-started:
		if epoch != 1 {
			t.Fatal(epoch)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancelOld := gateAction(0)
	cancelOld.ActionType = v0.ActionTypeCancelStepRun
	r.handle(ctx, cancelOld)
	close(releaseOld)
	select {
	case <-oldReturned:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(releaseNew)
	event := nextGateEvent(t, ctx, reporter)
	if event.RetryCount != 1 || event.EventType != v0.ActionEventTypeCompleted {
		t.Fatalf("old cancel reached new execution: %+v", event)
	}
	calls.Wait()
	select {
	case extra := <-reporter.events:
		t.Fatalf("duplicate or old terminal: %+v", extra)
	default:
	}
}

// TestGatedRunnerAdmissionPaths 验证落选、门禁不明确与业务 panic 分别走不同结果路径
func TestGatedRunnerAdmissionPaths(t *testing.T) {
	for _, scenario := range []string{"loser", "uncertain", "gate_panic", "panic"} {
		t.Run(scenario, func(t *testing.T) {
			gateDone := make(chan struct{})
			definition := ports.Definition{
				BeforeStart: func(context.Context, ports.StartInfo) (ports.Admission, error) {
					defer close(gateDone)
					if scenario == "gate_panic" {
						panic("fixture gate panic")
					}
					if scenario == "uncertain" {
						return ports.Admission{}, status.Error(codes.Unavailable, "unconfirmed claim")
					}
					return ports.Admission{Owned: scenario == "panic"}, nil
				},
				Function: func(context.Context, map[string]any) (any, error) { panic("fixture panic") },
			}
			r, reporter, ctx := gateFixture(t, definition)
			r.handle(ctx, gateAction(0))
			select {
			case <-gateDone:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if scenario == "panic" {
				if e := nextGateEvent(t, ctx, reporter); e.EventType != v0.ActionEventTypeStarted {
					t.Fatal(e)
				}
				e := nextGateEvent(t, ctx, reporter)
				if e.EventType != v0.ActionEventTypeFailed || status.Code(wire.DecodeError(e.EventPayload.(string))) != codes.Internal {
					t.Fatal(e)
				}
			} else {
				if scenario == "gate_panic" {
					select {
					case <-r.errors:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				// 门禁返回不是资源释放屏障，等候本地候选退出后再断言没有错误终态
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					r.mu.Lock()
					idle := r.inflight == 0
					r.mu.Unlock()
					if idle {
						break
					}
					select {
					case <-ticker.C:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				select {
				case e := <-reporter.events:
					t.Fatalf("unowned execution reported: %+v", e)
				default:
				}
			}
		})
	}
}

// TestDispatcherLateAckKeepsNewAttempt 旧代次 RPC 接收确认只清理自己的排空记录
func TestDispatcherLateAckKeepsNewAttempt(t *testing.T) {
	r, reporter, ctx := gateFixture(t, ports.Definition{})
	owner := r.dispatcher.owner
	owner.pending[attemptKey("task", 0)] = "worker"
	owner.pending[attemptKey("task", 1)] = "worker"
	_, err := r.dispatcher.SendStepActionEvent(ctx, &v0.ActionEvent{Action: gateAction(0), EventType: v0.ActionEventTypeFailed})
	if err != nil {
		t.Fatal(err)
	}
	_ = nextGateEvent(t, ctx, reporter)
	if owner.pending[attemptKey("task", 1)] != "worker" || len(owner.pending) != 1 {
		t.Fatalf("new attempt erased: %v", owner.pending)
	}
}

// TestCancellationNoticeMatchesActualWriter 验证延迟通知不能按 TaskRunID 误取消新执行
func TestCancellationNoticeMatchesActualWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &gatedRunner{dispatcher: &dispatcher{key: "current-key"}, latest: map[string]*gatedAttempt{"task": {action: gateAction(1), writer: "current-writer", cancel: cancel}}}
	valid := &wire.LogFrame{TaskRunId: "task", WorkerKey: "current-key", OldEpoch: 1, OldWriter: "current-writer", Epoch: 2}
	for _, scenario := range []string{"other_worker", "old_epoch", "other_writer", "no_new_epoch", "other_task"} {
		t.Run(scenario, func(t *testing.T) {
			copy := proto.Clone(valid).(*wire.LogFrame)
			switch scenario {
			case "other_worker":
				copy.WorkerKey = "other"
			case "old_epoch":
				copy.OldEpoch = 0
			case "other_writer":
				copy.OldWriter = "other"
			case "no_new_epoch":
				copy.Epoch = 1
			case "other_task":
				copy.TaskRunId = "other"
			}
			if runner.cancelNotice(copy) || ctx.Err() != nil {
				t.Fatal("unmatched notice canceled current execution")
			}
		})
	}
	if !runner.cancelNotice(valid) || ctx.Err() != context.Canceled {
		t.Fatal("matching notice did not cancel execution")
	}
	if !runner.cancelNotice(valid) {
		t.Fatal("duplicate notice lost idempotent cancellation")
	}
}
