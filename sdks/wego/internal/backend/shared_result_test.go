package backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	adminpb "github.com/hatchet-dev/hatchet/internal/services/admin/contracts"
	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// cardinalityAdmin 返回覆盖该块全部输入的结果：末项冲突，其余均成功
// 1002 个输入形成 1000 和 2 两块，应累计 1000 个成功身份和 2 个冲突
type cardinalityAdmin struct {
	// WorkflowServiceClient 提供未被测试替换的协议方法占位
	adminpb.WorkflowServiceClient
	// offset 保留跨块输入位置，使成功和冲突的身份互不重叠
	offset int
}

// BulkTriggerWorkflow 返回真实 gRPC 幂等冲突详情，成功和冲突总数等于当前块输入数
func (a *cardinalityAdmin) BulkTriggerWorkflow(_ context.Context, req *adminpb.BulkTriggerWorkflowRequest, _ ...grpc.CallOption) (*adminpb.BulkTriggerWorkflowResponse, error) {
	// details 的成功数与冲突数之和必须恰好等于当前块输入数
	details := &v1.BulkTriggerIdempotencyCollisionError{}
	// i 是当前块内位置，末项冲突，其余项均已被引擎接受
	for i := range req.Workflows {
		// id 结合累计块偏移生成唯一身份，第二块不会覆盖第一块
		id := fmt.Sprintf("run-%d", a.offset+i)
		if i == len(req.Workflows)-1 {
			details.Collisions = append(details.Collisions, &v1.IdempotencyCollisionError{ExistingRunExternalId: id})
		} else {
			details.SuccessfulWorkflowRunExternalIds = append(details.SuccessfulWorkflowRunExternalIds, id)
		}
	}
	a.offset += len(req.Workflows)
	// 按真实 gRPC 错误详情进入 Normalize 和跨块合并路径
	s, err := status.New(codes.AlreadyExists, "collision").WithDetails(details)
	if err != nil {
		return nil, err
	}
	return nil, s.Err()
}

// TestIndependentReviewValidBulkCollisionChunks 验证 1002 项产生 1000 个成功身份和 2 个冲突
func TestIndependentReviewValidBulkCollisionChunks(t *testing.T) {
	// 后端只替换受控的协议出口，分块、错误转换与合并均执行生产实现
	b := &Backend{admin: &cardinalityAdmin{}, config: spec.Defaults()}
	// err 保留调用的失败原因；本场景只验证错误或合并详情，不使用普通返回值
	_, err := b.RunMany(context.Background(), "child", make([]model.RunManyInput, 1002))
	// collision 接收跨块合并的成功身份和冲突详情
	var collision *model.BulkIdempotencyCollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("missing collision details: %v", err)
	}
	if len(collision.SuccessfulRunIDs) != 1000 || len(collision.Collisions) != 2 {
		t.Fatalf("1002 inputs: want 1000 successes and 2 collisions, got %d successes and %d collisions", len(collision.SuccessfulRunIDs), len(collision.Collisions))
	}
}

// nextWaitingStatus 等待真实 listener 报告某个等待已经登记，避免以 sleep 猜测时序
func nextWaitingStatus(t *testing.T, p *syncPeer) {
	t.Helper()
	for {
		select {
		// 当前请求或响应由受控通道交付，按消息类型推进本次测试协议
		case request := <-p.sent:
			if request.GetWorkerStatus() != nil && len(request.GetWorkerStatus().WaitingEntries) > 0 {
				return
			}
		case <-p.waitCtx.Done():
			t.Fatal("listener did not register callback")
		}
	}
}

// TestIndependentReviewConcurrentDurableResultWaiters 验证一次完成通知交付全部本地观察者
func TestIndependentReviewConcurrentDurableResultWaiters(t *testing.T) {
	// 同一 child RunRef 被两个调用者并发等待，完成事件应使二者都得到结果
	e, peer, ctx := syncExecution(t)
	// ref 保存真实子运行或事件节点身份，多个结果观察者复用它
	ref := e.childRef(v0.TriggerRunAckEntry{WorkflowRunID: "child-run", BranchID: 1, NodeID: 1}, "child")
	// first 和 second 分别接收两个观察者的结果，缓冲避免结果交付反向阻塞 listener
	first, second := make(chan error, 1), make(chan error, 1)
	// 后台观察者只等待已有身份，完成或取消均通过独立结果通道发布
	go func() { _, err := ref.Wait(ctx); first <- err }()
	nextWaitingStatus(t, peer)
	// 后台观察者只等待已有身份，完成或取消均通过独立结果通道发布
	go func() { _, err := ref.Wait(ctx); second <- err }()
	// 只登记一次底层等待；两个本地观察者都加入之后才发送完成事件
	shared := e.callback(callbackKey{branch: 1, node: 1})
	for {
		shared.mu.Lock()
		// joined 在锁内确认两个观察者已经加入同一代等待，完成事件随后发送
		joined := shared.current != nil && shared.current.observers == 2
		shared.mu.Unlock()
		if joined {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("second observer did not join")
		}
		runtime.Gosched()
	}
	// 引擎只需交付一次真实完成事件，SDK 应协调同一身份的多个本地观察者
	peer.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_EntryCompleted{
		EntryCompleted: &v1.DurableTaskEventLogEntryCompletedResponse{
			Ref:     &v1.DurableEventLogEntryRef{DurableTaskExternalId: "memo-task", InvocationCount: 1, BranchId: 1, NodeId: 1},
			Payload: []byte(`{"count":7}`),
		},
	}}
	// err 等待该观察者真实结束，任何取消或失败都进入断言
	if err := <-first; err != nil {
		t.Errorf("first waiter lost completion: %v", err)
	}
	// err 等待该观察者真实结束，任何取消或失败都进入断言
	if err := <-second; err != nil {
		t.Errorf("second waiter lost completion: %v", err)
	}
}

// TestSharedResultCancellationAndCache 验证观察者取消独立性、完成缓存和字节副本
func TestSharedResultCancellationAndCache(t *testing.T) {
	// e 和 peer 使用真实 listener，测试只控制服务端协议消息
	e, peer, root := syncExecution(t)
	// key 对应同一 child 的稳定节点
	key := callbackKey{branch: 1, node: 4}
	// shared 是两个观察者共享的协议登记
	shared := e.callback(key)
	// observer 单独取消，不取消 root 或其他观察者
	observer, cancel := context.WithCancel(root)
	defer cancel()
	// cancelled 接收首个观察者的结果，buffer 防止发送方在断言前阻塞
	cancelled := make(chan error, 1)
	// 后台观察者只等待已有身份，完成或取消均通过独立结果通道发布
	go func() { _, err := shared.wait(observer, e, key); cancelled <- err }()
	nextWaitingStatus(t, peer)
	// finished 接收仍在等待的另一个观察者的输出
	finished := make(chan []byte, 1)
	go func() {
		// data 与 err 来自同一共享完成通知，观察者拿到独立字节副本
		data, err := shared.wait(root, e, key)
		if err != nil {
			t.Errorf("remaining observer: %v", err)
		}
		finished <- data
	}()
	// 等到两者加入同一代后再取消首个，固定取消与完成的先后关系
	for {
		shared.mu.Lock()
		// joined 在锁内确认两个观察者已经加入同一代等待，完成事件随后发送
		joined := shared.current != nil && shared.current.observers == 2
		shared.mu.Unlock()
		if joined {
			break
		}
		if root.Err() != nil {
			t.Fatal("observer did not join")
		}
		runtime.Gosched()
	}
	cancel()
	// err 必须保留首个观察者的取消身份
	if err := <-cancelled; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	peer.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_EntryCompleted{EntryCompleted: &v1.DurableTaskEventLogEntryCompletedResponse{
		Ref: &v1.DurableEventLogEntryRef{DurableTaskExternalId: "memo-task", InvocationCount: 1, BranchId: 1, NodeId: 4}, Payload: []byte("result"),
	}}}
	// data 是独立副本，修改它不能改变后续读取的缓存
	data := <-finished
	if !bytes.Equal(data, []byte("result")) {
		t.Fatalf("payload %q", data)
	}
	data[0] = 'X'
	// cached 不再要求第二次引擎完成通知，并保留不可变内容
	cached, err := shared.wait(root, e, key)
	if err != nil || string(cached) != "result" {
		t.Fatalf("cached result %q: %v", cached, err)
	}
}

// TestSharedResultAllObserversCancel 验证全部观察者取消后保留唯一根监听，重新等待不丢完成
func TestSharedResultAllObserversCancel(t *testing.T) {
	// e 和 peer 共享根预算，子预算可提前结束
	e, peer, root := syncExecution(t)
	// key 固定事件节点，重新等待时不重新提交 child
	key := callbackKey{branch: 1, node: 5}
	// observer 是可单独取消的调用预算
	observer, cancel := context.WithCancel(root)
	// done 发布观察者退出，根监听保留直到完成或根取消
	done := make(chan error, 1)
	// 后台观察者只等待已有身份，完成或取消均通过独立结果通道发布
	go func() { _, err := e.callback(key).wait(observer, e, key); done <- err }()
	nextWaitingStatus(t, peer)
	cancel()
	// err 保留取消身份，不能伪造成功空结果
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	// 后台观察者只等待已有身份，完成或取消均通过独立结果通道发布
	go func() { _, err := e.callback(key).wait(root, e, key); done <- err }()
	peer.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_EntryCompleted{EntryCompleted: &v1.DurableTaskEventLogEntryCompletedResponse{
		Ref: &v1.DurableEventLogEntryRef{DurableTaskExternalId: "memo-task", InvocationCount: 1, BranchId: 1, NodeId: 5}, Payload: []byte("result"),
	}}}
	// err 是新一代观察者的真实完成结果
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestSharedResultCompletionCancellationRace 验证最后观察者取消与完成竞争时结果不会丢失
// 完成只发送一次；随后重新读取必须使用缓存或 listener 的早到完成缓冲
func TestSharedResultCompletionCancellationRace(t *testing.T) {
	// e 与 peer 使用真实 listener，根预算不会随局部观察者取消
	e, peer, root := syncExecution(t)
	// node 为每轮提供互不重叠的真实事件身份
	for node := int64(10); node < 42; node++ {
		// key 是当前轮次的稳定节点，不让上一轮缓存替代本轮完成
		key := callbackKey{branch: 1, node: node}
		// observer 专属取消预算，与完成通知竞争
		observer, cancel := context.WithCancel(root)
		// done 确认首个观察者退出，并完成底层等待引用释放
		done := make(chan error, 1)
		// err 可以是取消或成功，两种结果都不能吞掉已经收到的完成
		go func() { _, err := e.callback(key).wait(observer, e, key); done <- err }()
		nextWaitingStatus(t, peer)
		peer.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_EntryCompleted{EntryCompleted: &v1.DurableTaskEventLogEntryCompletedResponse{
			Ref: &v1.DurableEventLogEntryRef{DurableTaskExternalId: "memo-task", InvocationCount: 1, BranchId: 1, NodeId: node}, Payload: []byte("result"),
		}}}
		cancel()
		// firstErr 保留观察者取消身份，真正业务失败必须单独报告
		if firstErr := <-done; firstErr != nil && !errors.Is(firstErr, context.Canceled) {
			t.Fatal(firstErr)
		}
		// data 必须来自唯一完成通知，读取过程中不会重新提交任务
		data, err := e.callback(key).wait(root, e, key)
		if err != nil || string(data) != "result" {
			t.Fatalf("node %d lost completion: %q %v", node, data, err)
		}
	}
}

// mixedSubmissionAdmin 让首块返回真实幂等冲突，后续单项遇到传输超时
type mixedSubmissionAdmin struct {
	// cardinalityAdmin 复用首块的完整成功和冲突基数
	cardinalityAdmin
}

// BulkTriggerWorkflow 模拟后续分块超时，不返回未经确认的运行身份
func (a *mixedSubmissionAdmin) BulkTriggerWorkflow(ctx context.Context, request *adminpb.BulkTriggerWorkflowRequest, opts ...grpc.CallOption) (*adminpb.BulkTriggerWorkflowResponse, error) {
	if a.offset >= 1000 {
		return nil, context.DeadlineExceeded
	}
	return a.cardinalityAdmin.BulkTriggerWorkflow(ctx, request, opts...)
}

// TestBulkConflictPreservesOtherChunkError 验证冲突汇总保留其他块超时的错误链
func TestBulkConflictPreservesOtherChunkError(t *testing.T) {
	// backend 使用真实分块与 Normalize，仅协议出口由 fixture 控制
	backend := &Backend{admin: &mixedSubmissionAdmin{}, config: spec.Defaults()}
	// err 包含首块 999 个成功、1 个冲突及后续单项超时
	_, err := backend.RunMany(context.Background(), "child", make([]model.RunManyInput, 1001))
	// collision 是自有合并错误，不能携带官方错误实例
	var collision *model.BulkIdempotencyCollisionError
	if !errors.As(err, &collision) || len(collision.SuccessfulRunIDs) != 999 || len(collision.Collisions) != 1 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("mixed chunk details lost: %v", err)
	}
}
