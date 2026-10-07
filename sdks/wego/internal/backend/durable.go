package backend

import (
	"context"
	"fmt"
	"sync"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// eventAck 串行登记并确认当前 invocation 的协议操作。
// listener 按任务和 invocation 匹配 ACK，不能同时登记 Sleep、memo 或子提交。
// 请求已发送后若调用方取消，继续接收迟到 ACK 后才归还登记权，防止它被下次请求误收。
func (e *execution) eventAck(ctx context.Context, request *v1.DurableTaskRequest) (*v1.DurableTaskResponse, error) {
	// 根 handler 已退出时禁止再次登记，调用方不能延长 invocation 的生命。
	if e.lifetime != nil && e.lifetime.Err() != nil {
		return nil, e.lifetime.Err()
	}
	// gate 只覆盖登记和 ACK；事件完成或子结果等待不持有它。
	if err := e.ackGate.Lock(ctx); err != nil {
		return nil, err
	}
	// 获取登记权期间根执行可能已退出；必须在登记前再次确认，不能复用已终止的 ACK 槽。
	if e.lifetime != nil && e.lifetime.Err() != nil {
		e.ackGate.Unlock()
		return nil, e.lifetime.Err()
	}
	// lifetimeDone 为 nil 时仅由 listener 的 Stop/CleanupTaskState 结束等待。
	var lifetimeDone <-chan struct{}
	if e.lifetime != nil {
		lifetimeDone = e.lifetime.Done()
	}
	// listener 的 Stop 和根执行清理都会结束登记的等待，不遗留后台接收者。
	listener := e.durableListener()
	if listener == nil {
		e.ackGate.Unlock()
		return nil, model.ErrDurableContext
	}
	// channel 独占当前 invocation 的确认槽，后续请求必须等待此确认完成。
	channel := listener.AddPendingEventAck(v0.PendingAckKey{TaskID: e.original.StepRunId(), SignalKey: int64(e.Info().InvocationCount)})
	// 发送失败不能进入确认等待，未入队请求不会产生迟到 ACK。
	if err := listener.SendRequest(ctx, request); err != nil {
		// SendRequest 返回错误表示请求未入队，不会产生需要隔离的迟到确认。
		e.ackGate.Unlock()
		return nil, Normalize(err)
	}
	select {
	// ack 对应当前独占登记，先释放确认锁再交付结果。
	case ack := <-channel:
		e.ackGate.Unlock()
		return ack.Resp, Normalize(ack.Err)
	case <-lifetimeDone:
		e.ackGate.Unlock()
		return nil, e.lifetime.Err()
	case <-ctx.Done():
		// 独立调用取消不能释放槽；根退出则禁止任何新登记，可结束后台接收。
		// 不能以任意超时释放活跃 invocation 的槽，否则迟到 Sleep ACK 可能被 Now 误收。
		go func() {
			defer e.ackGate.Unlock()
			select {
			case <-channel:
			case <-lifetimeDone:
			}
		}()
		return nil, ctx.Err()
	}
}

// callbackKey 是 invocation 内的稳定事件节点身份，不使用业务任务名作为共享键。
type callbackKey struct {
	// branch 区分并行 durable 分支。
	branch int64
	// node 是引擎记录的事件序号。
	node int64
}

// callbackWait 保存根 invocation 的唯一共享等待；只有一个 goroutine 向 listener 登记结果。
type callbackWait struct {
	// done 发布不可变结果并确认底层等待退出。
	done chan struct{}
	// observers 是仍在等待的调用者数量，由 sharedCallback.mu 保护。
	observers int
	// payload 在 done 关闭前写入，读取者取得独立字节副本。
	payload []byte
	// err 是底层等待的结果，done 关闭后才读取。
	err error
}

// sharedCallback 为同一 durable 节点分发结果，观察者预算不会取消其他观察者。
type sharedCallback struct {
	// mu 保护唯一监听及本地观察者数量。
	mu sync.Mutex
	// current 持有根 invocation 的唯一监听；暂时没有观察者也保留完成通知。
	current *callbackWait
}

// callback 返回同一事件节点的共享观察器，多个 RunRef 也不能覆盖 listener 的登记。
func (e *execution) callback(key callbackKey) *sharedCallback {
	e.callbacksMu.Lock()
	defer e.callbacksMu.Unlock()
	if e.callbacks == nil {
		e.callbacks = make(map[callbackKey]*sharedCallback)
	}
	// value 属于当前节点，例如 branch=1、node=7 的所有观察者共享它。
	value := e.callbacks[key]
	if value == nil {
		value = &sharedCallback{}
		e.callbacks[key] = value
	}
	return value
}

// wait 接收一次完成事件并向所有观察者交付副本；监听随根退出清理，缓存随执行视图释放。
// 暂时没有观察者时保留一次监听，避免取消与完成竞争后再读取永久等待。
func (s *sharedCallback) wait(ctx context.Context, e *execution, key callbackKey) ([]byte, error) {
	// 先检查观察者预算，已取消的调用不新增 listener 登记。
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	// run 是此节点唯一的根监听，观察者只共享其完成结果。
	run := s.current
	if run == nil {
		// listener 在 handler 生命周期内捕获；根退出会撤销 hooks，后台等待不能再次查找它。
		listener := e.durableListener()
		if listener == nil {
			s.mu.Unlock()
			return nil, model.ErrDurableContext
		}
		// taskID 和 invocation 是本代稳定身份，不在后台读取后续 hook 状态。
		taskID, invocation := e.original.StepRunId(), int32(e.Info().InvocationCount)
		// lifetime 由根执行控制；测试独立 listener 时由 listener.Stop 唤醒结果等待。
		lifetime := e.lifetime
		if lifetime == nil {
			lifetime = context.Background()
		}
		// budget 属于根 invocation，观察者取消不能丢弃引擎只交付一次的完成。
		budget, cancel := context.WithCancel(lifetime)
		run = &callbackWait{done: make(chan struct{})}
		s.current = run
		go func() {
			// data 仅由该 goroutine 写入，done 是所有结果读取者的发布屏障。
			data, err := listener.WaitForCallback(budget, taskID, invocation, key.branch, key.node)
			cancel()
			s.mu.Lock()
			run.payload, run.err = data, Normalize(err)
			close(run.done)
			s.mu.Unlock()
		}()
	}
	run.observers++
	s.mu.Unlock()
	// 等待完成或本观察者取消；所有路径都归还本地观察者计数。
	var data []byte
	// err 保存此观察者的完成或取消原因，不影响其他观察者的预算。
	var err error
	select {
	case <-run.done:
		data, err = append([]byte(nil), run.payload...), run.err
	case <-ctx.Done():
		err = ctx.Err()
	}
	s.mu.Lock()
	run.observers--
	// 没有观察者时仍保留根监听，后续 Result 不需要引擎重复交付完成。
	s.mu.Unlock()
	return data, err
}

// waitReference 检查 ACK 形状，损坏或错类型的确认不能推进事件账本。
func waitReference(response *v1.DurableTaskResponse) (*v1.DurableEventLogEntryRef, error) {
	if response == nil || response.GetWaitForAck().GetRef() == nil {
		return nil, fmt.Errorf("wego: wait acknowledgment missing reference")
	}
	return response.GetWaitForAck().GetRef(), nil
}
