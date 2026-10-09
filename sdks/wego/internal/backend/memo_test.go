package backend

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	oldworker "github.com/hatchet-dev/hatchet/pkg/worker"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

// memoPeer 模拟引擎的 memo 查询和完成出口，不执行任务调度
type memoPeer struct {
	// ClientStream 提供测试未使用的标准流方法；收发及取消由下面的方法实现
	grpc.ClientStream
	// ctx 限制流的存活期，测试退出后不能继续等待响应
	ctx context.Context
	// replies 保存引擎确认，容量足够容纳本用例的两次查询
	replies chan *v1.DurableTaskResponse
	// completed 确认首次 memo 已交付给引擎，再验证新 invocation 的恢复
	completed chan struct{}
	// mu 保护协议发送计数及引擎保存的业务字节
	mu sync.Mutex
	// queries 是真实查询次数，同一次执行的多次 Now 只能产生一次查询
	queries int
	// data 是模拟引擎已记录的 UTC 时间，新执行必须读取相同值
	data []byte
}

// TestDurableStandaloneKeepsJSONValue 验证任务输出边界不将标量、null 或对象字段当成任务映射
func TestDurableStandaloneKeepsJSONValue(t *testing.T) {
	// cases 覆盖独立任务支持的 JSON 形状；7 必须作为 value 的输出，不能解析成 map
	cases := []struct {
		// name 描述数据的结构，不包含环境或业务身份
		name string
		// payload 是引擎完成回调实际携带的 JSON 字节
		payload string
		// want 按 JSON 的动态类型比较，数值必须是 float64
		want any
	}{
		{"number", "7", float64(7)},
		{"string", `"done"`, "done"},
		{"null", "null", nil},
		{"object", `{"count":7}`, map[string]any{"count": float64(7)}},
	}
	// current 独立运行每种输出形状，回调不能跨测试共享
	for _, current := range cases {
		t.Run(current.name, func(t *testing.T) {
			// ctx 限制协议等待，失败时不会留下等待回调
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			// peer 通过正式 listener 的完成分发路径交付结果，不直接调用解码辅助函数
			peer := &memoPeer{replies: make(chan *v1.DurableTaskResponse, 1)}
			// logger 不输出协议日志，断言保留业务值的差异
			logger := zerolog.Nop()
			// listener 复用真实回调缓冲与分发代码，完成先于订阅时也必须正确
			listener := v0.NewDurableTaskListener("memo-worker", func(streamCtx context.Context) (v1.V1Dispatcher_DurableTaskClient, error) {
				peer.ctx = streamCtx
				return peer, nil
			}, &logger)
			listener.Start(ctx)
			defer listener.Stop()
			peer.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_EntryCompleted{
				EntryCompleted: &v1.DurableTaskEventLogEntryCompletedResponse{
					Ref:     &v1.DurableEventLogEntryRef{DurableTaskExternalId: "memo-task", InvocationCount: 1, BranchId: 1, NodeId: 1},
					Payload: []byte(current.payload),
				},
			}}
			// executionState 只知道一个任务键 value，业务类型在结果读取时决定
			executionState := &execution{original: &memoContext{listener: listener}, backend: &Backend{
				workflowTasks: map[string][]string{"standalone": {"value"}},
			}}
			// result 等待真实分发结果，不能把解析成功但类型错误计为通过
			result, err := executionState.childRef(v0.TriggerRunAckEntry{WorkflowRunID: "child-run", BranchID: 1, NodeID: 1}, "standalone").Wait(ctx)
			if err != nil || len(result.Outputs) != 1 || !reflect.DeepEqual(result.Outputs["value"], current.want) {
				t.Fatalf("durable output shape changed: %#v, err=%v", result.Outputs, err)
			}
		})
	}
}

// Send 在查询时返回确认，在完成时持久化字节；状态通报不影响 memo 计数
func (p *memoPeer) Send(req *v1.DurableTaskRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// memo 保留请求中的任务和恢复身份，确认不能交给另一个执行
	if memo := req.GetMemo(); memo != nil {
		p.queries++
		p.replies <- &v1.DurableTaskResponse{Message: &v1.DurableTaskResponse_MemoAck{
			MemoAck: &v1.DurableTaskEventMemoAckResponse{
				Ref: &v1.DurableEventLogEntryRef{DurableTaskExternalId: memo.DurableTaskExternalId,
					InvocationCount: memo.InvocationCount, BranchId: 1, NodeId: 1},
				MemoAlreadyExisted: len(p.data) > 0,
				MemoResultPayload:  append([]byte(nil), p.data...),
			},
		}}
	}
	// completed 携带首次算出的时间，后续查询只能返回这份记录
	if completed := req.GetCompleteMemo(); completed != nil {
		p.data = append([]byte(nil), completed.Payload...)
		close(p.completed)
	}
	return nil
}

// Recv 交付确认或保留取消原因，不能让官方监听器在测试结束后阻塞
func (p *memoPeer) Recv() (*v1.DurableTaskResponse, error) {
	select {
	// reply 是官方 listener 所需的协议确认，不能用直接赋值绕过分发
	case reply := <-p.replies:
		return reply, nil
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	}
}

// memoWorker 只提供 Info 所读取的标签，其他 Worker 能力不会在此测试调用
type memoWorker struct{ oldworker.HatchetWorkerContext }

// GetLabels 返回空标签，不把身份构造与 memo 行为混在同一用例中
func (*memoWorker) GetLabels() map[string]any { return nil }

// memoContext 提供真实 listener 与最小任务身份，嵌入接口中的其他方法不可调用
type memoContext struct {
	// DurableHatchetContext 使测试执行具有 durable 类型，未使用的方法保持未实现
	oldworker.DurableHatchetContext
	// listener 是根执行共享的官方监听器，恢复执行仍通过它访问引擎
	listener *v0.DurableTaskListener
}

// DurableTaskListener 返回本测试拥有的监听器
func (c *memoContext) DurableTaskListener() *v0.DurableTaskListener { return c.listener }

// DurableEvictionSupported 明确启用引擎 memo 能力，不允许退化到墙上时钟
func (*memoContext) DurableEvictionSupported() bool { return true }

// DurableEvictionHook 此用例不触发驱逐，恢复由新的 execution 对象模拟
func (*memoContext) DurableEvictionHook() oldworker.DurableEvictionHook { return nil }

// DurableTaskInvocationCount 返回当前恢复序号，与协议确认中的 invocation 保持一致
func (*memoContext) DurableTaskInvocationCount() int32 { return 1 }

// WorkflowRunId 返回测试根运行身份
func (*memoContext) WorkflowRunId() string { return "memo-run" }

// StepRunId 返回 memo 请求和确认共用的任务身份
func (*memoContext) StepRunId() string { return "memo-task" }

// WorkerId 返回测试实例身份，不能用业务名称代替
func (*memoContext) WorkerId() string { return "memo-worker" }

// RetryCount 初次业务执行没有重试
func (*memoContext) RetryCount() int { return 0 }

// ParentWorkflowRunId 根任务不含父运行身份
func (*memoContext) ParentWorkflowRunId() *string { return nil }

// FilterPayload 本测试未配置事件过滤载荷
func (*memoContext) FilterPayload() map[string]any { return nil }

// AdditionalMetadata 本测试不依赖调度元数据
func (*memoContext) AdditionalMetadata() map[string]string { return nil }

// Worker 返回不拥有后台资源的最小身份视图
func (*memoContext) Worker() oldworker.HatchetWorkerContext { return &memoWorker{} }

// TestNowMemoizesWithinInvocationAndRestores 验证并发重复调用不增加 memo 节点，恢复仍读取引擎记录
func TestNowMemoizesWithinInvocationAndRestores(t *testing.T) {
	// ctx 统一限制协议和业务等待，测试失败也能终止监听器
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// peer 对首次查询返回未命中，收到完成字节后才允许恢复查询命中
	peer := &memoPeer{replies: make(chan *v1.DurableTaskResponse, 2), completed: make(chan struct{})}
	// logger 关闭测试日志，断言直接检查协议计数与时间值
	logger := zerolog.Nop()
	// listener 使用正式收发与确认分发逻辑，测试不会绕过官方 ACK 通道
	listener := v0.NewDurableTaskListener("memo-worker", func(streamCtx context.Context) (v1.V1Dispatcher_DurableTaskClient, error) {
		peer.ctx = streamCtx
		return peer, nil
	}, &logger)
	listener.Start(ctx)
	defer listener.Stop()
	// executionState 代表首次 invocation，32 次并发查询必须共享它的本地记忆
	executionState := &execution{original: &memoContext{listener: listener}}
	// values 收集各调用的时间，错误不能以零时间冒充有效结果
	values := make(chan time.Time, 32)
	// group 确认全部调用已结束，再检查查询计数
	var group sync.WaitGroup
	for range 32 {
		group.Go(func() {
			// value 是实际 Now 结果，每个 goroutine 都需要独立检查错误
			value, err := executionState.Now(ctx)
			if err != nil {
				t.Error(err)
			}
			values <- value
		})
	}
	group.Wait()
	close(values)
	// first 保留第一次结果，后续结果必须逐值相等
	var first time.Time
	// value 是每个并发调用真实返回的时间，逐项校验而非只检查没有错误
	for value := range values {
		if first.IsZero() {
			first = value
		}
		if value.IsZero() || !value.Equal(first) {
			t.Fatalf("Now changed within invocation: %v != %v", value, first)
		}
	}
	select {
	case <-peer.completed:
	case <-ctx.Done():
		t.Fatal("memo completion was not delivered")
	}
	peer.mu.Lock()
	if peer.queries != 1 {
		t.Errorf("repeated Now allocated %d memo nodes", peer.queries)
	}
	peer.mu.Unlock()
	// restored 不含本地缓存，必须通过引擎返回首次保存的时间
	restored := &execution{original: &memoContext{listener: listener}}
	// value 来自官方 memo 确认，而非测试直接注入 execution.now
	value, err := restored.Now(ctx)
	if err != nil || !value.Equal(first) {
		t.Fatalf("replay changed Now: %v != %v: %v", value, first, err)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	// stored 解码引擎保留的时间，证明完成出口携带了实际业务值
	var stored time.Time
	// err 必须明确暴露损坏的记录，不能将零时间当成成功恢复
	if err := json.Unmarshal(peer.data, &stored); err != nil || !stored.Equal(first) || peer.queries != 2 {
		t.Fatalf("engine memo mismatch: %v, queries=%d, err=%v", stored, peer.queries, err)
	}
}
