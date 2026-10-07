package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// syncPeer 将发送和确认分别交给测试控制，固定并发边界的顺序。
type syncPeer struct {
	// ClientStream 满足协议流接口；未使用的方法由嵌入接口占位。
	grpc.ClientStream
	// ctx 是 listener 分配的传输上下文，停止后 Send 和 Recv 都退出。
	ctx context.Context
	// waitCtx 限制测试等待，缺失协议请求时明确失败。
	waitCtx context.Context
	// sent 将客户端请求交给测试驱动，接收后才发送对应确认。
	sent chan *v1.DurableTaskRequest
	// replies 注入服务端确认及完成通知，保持测试指定的顺序。
	replies chan *v1.DurableTaskResponse
}

// Send 交付请求到受控出口，传输取消时不能继续发送。
func (p *syncPeer) Send(r *v1.DurableTaskRequest) error {
	select {
	case p.sent <- r:
		return nil
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

// Recv 按测试指定顺序交付响应，listener 停止时立即结束接收。
func (p *syncPeer) Recv() (*v1.DurableTaskResponse, error) {
	select {
	// 当前请求或响应由受控通道交付，按消息类型推进本次测试协议。
	case r := <-p.replies:
		return r, nil
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	}
}

// syncExecution 创建真实协议 listener 与受控对端，所有等待由测试预算限制。
func syncExecution(t *testing.T) (*execution, *syncPeer, context.Context) {
	t.Helper()
	// ctx 和 cancel 为测试设置有限预算，退出时取消全部受控等待。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	// p 为协议出口或受控载荷转换，通道明确区分进入和释放。
	p := &syncPeer{waitCtx: ctx, sent: make(chan *v1.DurableTaskRequest, 20), replies: make(chan *v1.DurableTaskResponse, 20)}
	// l 使用静默 logger，协议测试不依赖输出日志推断时序。
	l := zerolog.Nop()
	// listener 使用官方公开协议接口，测试验证真实 ACK/回调键规则。
	listener := v0.NewDurableTaskListener("memo-worker", func(c context.Context) (v1.V1Dispatcher_DurableTaskClient, error) { p.ctx = c; return p, nil }, &l)
	listener.Start(ctx)
	t.Cleanup(listener.Stop)
	return &execution{original: &syncParent{memoContext: memoContext{listener: listener}}, lifetime: ctx, backend: &Backend{workflowTasks: map[string][]string{"child": {"value"}}}}, p, ctx
}

// syncParent 使用真实父运行身份，并记录子提交序号。
type syncParent struct {
	// memoContext 提供稳定任务身份及真实 durable listener。
	memoContext
	// index 记录下一子任务序号，同一父执行的子调用依次递增。
	index int
}

// CurChildIndex 返回父执行的当前子序号，用于生成稳定子键。
func (p *syncParent) CurChildIndex() int { return p.index }

// IncChildIndex 在子提交保留位置后推进序号，重放相同顺序得到相同键。
func (p *syncParent) IncChildIndex() { p.index++ }

// nextRequest 等待指定类型的协议请求，不以固定延迟猜测请求是否已发出。
func nextRequest(t *testing.T, p *syncPeer, kind string) *v1.DurableTaskRequest {
	t.Helper()
	for {
		select {
		// 当前请求或响应由受控通道交付，按消息类型推进本次测试协议。
		case r := <-p.sent:
			// matches 区分等待、memo 查询、子提交和 memo 完成通知。
			matches := kind == "wait" && r.GetWaitFor() != nil ||
				kind == "memo" && r.GetMemo() != nil ||
				kind == "trigger" && r.GetTriggerRuns() != nil ||
				kind == "complete_memo" && r.GetCompleteMemo() != nil
			if matches {
				return r
			}
		case <-p.waitCtx.Done():
			t.Fatal("missing request", kind)
			return nil
		}
	}
}

// TestSyncReviewConcurrentSleepAndNow 验证 Sleep 确认后释放 ACK 槽，长期等待期间仍可完成 Now。
func TestSyncReviewConcurrentSleepAndNow(t *testing.T) {
	// e、p、ctx 分别提供执行视图、受控对端和有限测试预算。
	e, p, ctx := syncExecution(t)
	// sleep 接收 durable 等待结果，Now 完成不应取消此等待。
	sleep := make(chan error, 1)
	// now 接收 memo 查询结果，确认不能被 Sleep 的登记截获。
	now := make(chan error, 1)
	go func() { sleep <- e.Sleep(ctx, time.Second) }()
	nextRequest(t, p, "wait")
	// queuedBudget 仅允许 memo 排队 20ms；Sleep 未确认时它不能覆盖 ACK 槽。
	queuedBudget, stopQueued := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopQueued()
	// queuedErr 必须来自获取确认权的预算，而非重写 Sleep 的 pending ACK。
	_, queuedErr := e.Now(queuedBudget)
	if !errors.Is(queuedErr, context.DeadlineExceeded) {
		t.Fatalf("queued memo: %v", queuedErr)
	}
	// ref 保存真实子运行或事件节点身份，多个结果观察者复用它。
	ref := &v1.DurableEventLogEntryRef{DurableTaskExternalId: "memo-task", InvocationCount: 1, BranchId: 1, NodeId: 1}
	p.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_WaitForAck{WaitForAck: &v1.DurableTaskEventWaitForAckResponse{Ref: ref}}}
	// 后台观察者只等待已有身份，完成或取消均通过独立结果通道发布。
	go func() { _, err := e.Now(ctx); now <- err }()
	nextRequest(t, p, "memo")
	p.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_EntryCompleted{EntryCompleted: &v1.DurableTaskEventLogEntryCompletedResponse{Ref: ref, Payload: []byte(`{}`)}}}
	p.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_MemoAck{MemoAck: &v1.DurableTaskEventMemoAckResponse{Ref: ref, MemoAlreadyExisted: true, MemoResultPayload: []byte(`"2026-10-07T00:00:00Z"`)}}}
	// err 等待该观察者真实结束，任何取消或失败都进入断言。
	if err := <-now; err != nil {
		t.Errorf("Now received wrong ACK: %v", err)
	}
	// err 等待该观察者真实结束，任何取消或失败都进入断言。
	if err := <-sleep; err != nil {
		t.Errorf("Sleep lost ACK: %v", err)
	}
}

// TestSyncReviewDurablePartialSubmission 验证第二块超时时仍交付前 100 个确认句柄。
func TestSyncReviewDurablePartialSubmission(t *testing.T) {
	// e、p、ctx 分别提供执行视图、受控对端和有限测试预算。
	e, p, ctx := syncExecution(t)
	// budget 只限制批量提交 250ms，根 listener 仍拥有独立生命周期。
	budget, stop := context.WithTimeout(ctx, 250*time.Millisecond)
	defer stop()
	// bound 保存父执行身份，RunMany 必须进入 durable 子提交分支。
	bound := callctx.Bind(budget, &callctx.State{Execution: e})
	go func() {
		// r 是受控协议请求或自有结果视图，测试只检查其当前入口的行为。
		r := nextRequest(t, p, "trigger")
		// entries 一一对应首块输入，模拟引擎返回 100 个真实子身份。
		entries := make([]*v1.DurableTaskRunAckEntry, len(r.GetTriggerRuns().TriggerOpts))
		// i 对应首块 100 个输入，生成互不重叠的确认身份。
		for i := range entries {
			entries[i] = &v1.DurableTaskRunAckEntry{NodeId: int64(i + 1), BranchId: 1, WorkflowRunExternalId: fmt.Sprintf("accepted-%d", i)}
		}
		p.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_TriggerRunsAck{TriggerRunsAck: &v1.DurableTaskEventTriggerRunsAckResponse{DurableTaskExternalId: "memo-task", InvocationCount: 1, RunEntries: entries}}}
		nextRequest(t, p, "trigger")
	}()
	// refs 与 err 同时返回部分成功身份和后续失败，错误不能清空已接受的句柄。
	refs, err := e.backend.RunMany(bound, "child", make([]model.RunManyInput, 101))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second chunk should preserve deadline: %v", err)
	}
	// i 与 ref 保留原输入下标及确认身份，部分成功不能重新编号。
	for i, ref := range refs {
		if ref.InputIndex != i || ref.ID != fmt.Sprintf("accepted-%d", i) {
			t.Fatalf("partial identity at %d: %+v", i, ref)
		}
	}
	if len(refs) != 100 {
		t.Errorf("engine confirmed 100 child runs; returned %d refs: %v", len(refs), err)
	}
	// partial 保留前 100 项已确认身份，避免整批重提。
	var partial *model.PartialSubmissionError
	if !errors.As(err, &partial) || len(partial.SuccessfulRunIDs) != 100 {
		t.Errorf("accepted child IDs absent from error: %T %v", err, err)
	}
}

// TestSyncReviewOldInvocationWaitRelease 验证 invocation=1 的退出不能清除 invocation=2 的等待。
func TestSyncReviewOldInvocationWaitRelease(t *testing.T) {
	// n 的运行表保存当前 invocation，用指针区分旧执行与恢复执行。
	n := &nativeWorker{runs: map[string]*durableRun{"task": {invocation: 1}}}
	// oldHook 固定绑定 invocation=1 的执行记录，不能重新查找当前记录。
	oldHook := &invocationHook{worker: n, id: "task", run: n.runs["task"]}
	oldHook.MarkWaiting("task", "sleep", "")
	n.runs["task"] = &durableRun{invocation: 2}
	// newHook 固定绑定 invocation=2，用于断言旧回调不会减掉它的等待计数。
	newHook := &invocationHook{worker: n, id: "task", run: n.runs["task"]}
	newHook.MarkWaiting("task", "sleep", "")
	oldHook.MarkActive("task") // invocation=1 的迟到退出，不得释放 invocation=2 的等待。
	if n.runs["task"].waiting != 1 {
		t.Fatalf("old invocation cleared new waiting: %+v", n.runs["task"])
	}
}

// TestCancelledAckDrainsBeforeNextRequest 验证已发送请求取消后隔离迟到确认。
func TestCancelledAckDrainsBeforeNextRequest(t *testing.T) {
	// e 与 peer 保留同一 invocation，测试分别注入旧 ACK 和新 ACK。
	e, peer, root := syncExecution(t)
	// budget 控制首个已经发送的等待请求。
	budget, cancel := context.WithCancel(root)
	// done 接收首个等待的取消原因。
	done := make(chan error, 1)
	go func() { done <- e.Sleep(budget, time.Second) }()
	nextRequest(t, peer, "wait")
	cancel()
	// err 说明调用方及时返回，ACK 槽仍隔离旧请求。
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// nextBudget 的短预算证明迟到 ACK 前不能发送新的 memo。
	nextBudget, stop := context.WithTimeout(root, 20*time.Millisecond)
	defer stop()
	// err 获取确认权到期，不能把旧 Sleep 确认作为新 memo 处理。
	if _, err := e.Now(nextBudget); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("next request: %v", err)
	}
	peer.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_WaitForAck{WaitForAck: &v1.DurableTaskEventWaitForAckResponse{
		Ref: &v1.DurableEventLogEntryRef{DurableTaskExternalId: "memo-task", InvocationCount: 1, BranchId: 1, NodeId: 1},
	}}}
	// 新 memo 必须等旧确认被排空后才登记，结果由独立通道发布。
	go func() { _, err := e.Now(root); done <- err }()
	nextRequest(t, peer, "memo")
	peer.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_MemoAck{MemoAck: &v1.DurableTaskEventMemoAckResponse{
		Ref: &v1.DurableEventLogEntryRef{DurableTaskExternalId: "memo-task", InvocationCount: 1, BranchId: 1, NodeId: 2}, MemoAlreadyExisted: true, MemoResultPayload: []byte(`"2026-10-07T00:00:00Z"`),
	}}}
	// err 必须来自真正的新 memo 完成，确认类型不能串位。
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestDurableMissingIdentityAck 验证损坏 ACK 不引发 panic，也不能伪造成功句柄。
func TestDurableMissingIdentityAck(t *testing.T) {
	// e 和 peer 只替换确认出口，子提交仍执行完整生产实现。
	e, peer, ctx := syncExecution(t)
	// bound 使调用使用父 durable 身份，而不是普通 Admin 提交。
	bound := callctx.Bind(ctx, &callctx.State{Execution: e})
	go func() {
		nextRequest(t, peer, "trigger")
		peer.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_TriggerRunsAck{TriggerRunsAck: &v1.DurableTaskEventTriggerRunsAckResponse{
			DurableTaskExternalId: "memo-task", InvocationCount: 1, RunEntries: []*v1.DurableTaskRunAckEntry{nil},
		}}}
	}()
	// refs 与 err 必须区分引擎确认的成功身份和损坏响应。
	refs, err := e.backend.RunMany(bound, "child", []model.RunManyInput{{}})
	if err == nil || len(refs) != 0 {
		t.Fatalf("invalid ACK accepted: %v %v", refs, err)
	}
}

// TestLateEvictionCancelsOnlyCapturedInvocation 验证迟到驱逐确认不取消后续 invocation。
func TestLateEvictionCancelsOnlyCapturedInvocation(t *testing.T) {
	// worker 使用真实记录表，业务取消函数由两个独立 context 提供。
	worker := &nativeWorker{runs: map[string]*durableRun{}}
	// oldBudget 和 oldCancel 只属于 invocation=1。
	oldBudget, oldCancel := context.WithCancel(context.Background())
	defer oldCancel()
	// newBudget 和 newCancel 只属于 invocation=2，复用相同 TaskRunID。
	newBudget, newCancel := context.WithCancel(context.Background())
	defer newCancel()
	// old 与 current 表示驱逐请求和新执行分别捕获的记录。
	old, current := &durableRun{invocation: 1, cancel: oldCancel}, &durableRun{invocation: 2, cancel: newCancel}
	worker.runs["task"] = old
	worker.runs["task"] = current
	worker.cancelInvocation(old)
	if oldBudget.Err() != context.Canceled || newBudget.Err() != nil {
		t.Fatal("late eviction cancelled new invocation")
	}
}

// TestNowCompletesPendingMemo 验证恢复到已建立但尚未完成的 memo 时仍提交可重放 UTC 时间。
func TestNowCompletesPendingMemo(t *testing.T) {
	// execution、peer、ctx 分别提供根执行、真实 listener 的受控对端及有限预算。
	execution, peer, ctx := syncExecution(t)
	// memoResult 将恢复执行的时间及错误作为一个不可变结果发布。
	type memoResult struct {
		// value 是真正提交到 memo 的 UTC 时间。
		value time.Time
		// err 保留解析和发送错误，不能用时间零值表示成功。
		err error
	}
	// result 发布 Now 的时间和错误，测试先确认协议请求才注入响应。
	result := make(chan memoResult, 1)
	go func() {
		// value、err 接收恢复执行的 Now 结果。
		value, err := execution.Now(ctx)
		result <- memoResult{value: value, err: err}
	}()
	nextRequest(t, peer, "memo")
	// ref 是引擎已经建立的 memo；空 payload 表示记录尚未完成，而非合法业务 null。
	ref := &v1.DurableEventLogEntryRef{DurableTaskExternalId: "memo-task", InvocationCount: 1, BranchId: 1, NodeId: 1}
	peer.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_MemoAck{MemoAck: &v1.DurableTaskEventMemoAckResponse{Ref: ref, MemoAlreadyExisted: true}}}
	// completed 必须包含同一引用和非空时间，不得静默返回新时间而不记录它。
	completed := nextRequest(t, peer, "complete_memo").GetCompleteMemo()
	// observed 是此次业务观察到的时间，用于对照引擎完成通知。
	observed := <-result
	if observed.err != nil || observed.value.IsZero() {
		t.Fatalf("pending memo result: %v %v", observed.value, observed.err)
	}
	// persisted 还原实际发送的 JSON 时间，确保返回值具有可重放记录。
	var persisted time.Time
	// err 检查完成通知可解码为时间，且引用与返回值没有被替换。
	if err := json.Unmarshal(completed.Payload, &persisted); err != nil || !persisted.Equal(observed.value) || !proto.Equal(completed.Ref, ref) {
		t.Fatalf("pending memo completion: %v %v", persisted, err)
	}
	// repeated 在同一 invocation 内使用缓存，不再建立额外 memo 节点。
	repeated, err := execution.Now(ctx)
	if err != nil || !repeated.Equal(observed.value) {
		t.Fatalf("pending memo repeated result: %v %v", repeated, err)
	}
}
