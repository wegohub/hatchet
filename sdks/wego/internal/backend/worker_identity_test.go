package backend

import (
	"context"
	"testing"

	v0 "github.com/hatchet-dev/hatchet/pkg/client"
)

// 评审专用：同名 Worker 拥有各自监听器，注销不得共享 channel。
type reviewListener struct{ v0.WorkerActionListener }

// Unregister 模拟无网络副作用的注销，仅观察监听器自身完成通知的所有权。
func (*reviewListener) Unregister() error { return nil }

// TestReviewSameNameListenersCanBothClose 两个监听器分别注销并关闭自身通知通道，同名覆盖不得引发 panic。
func TestReviewSameNameListenersCanBothClose(t *testing.T) {
	// owner 保存本实例的完成或故障通知，关闭通道不代表业务成功。
	owner := &transport{listenerDone: map[string]chan struct{}{"worker": make(chan struct{})}, unregisterBudgets: map[string]context.Context{}}
	// first 保存首次注册的独占通知；后续名称映射变化不能改变它的所有权。
	first := &actionListener{WorkerActionListener: &reviewListener{}, owner: owner, name: "worker", done: owner.listenerDone["worker"]}
	owner.listenerDone["worker"] = make(chan struct{})
	// second 保存自己的结束通知，即使显示名称相同也不能引用第一个监听器的 channel。
	second := &actionListener{WorkerActionListener: &reviewListener{}, owner: owner, name: "worker", done: owner.listenerDone["worker"]}
	defer func() {
		// r 捕获当前执行的 panic，边界应返回可诊断错误并继续资源清理。
		if r := recover(); r != nil {
			t.Errorf("two same-name workers panic on close: %v", r)
		}
	}()
	// err 结束本实例的监听注册，两个同名 Worker 不得关闭同一个通知通道。
	if err := first.Unregister(); err != nil {
		t.Fatal(err)
	}
	// err 结束本实例的监听注册，两个同名 Worker 不得关闭同一个通知通道。
	if err := second.Unregister(); err != nil {
		t.Fatal(err)
	}
}

// TestSubmissionQueueHonorsCancellation 前一子调用提交阻塞时，后一个调用的 deadline 仍须生效。
func TestSubmissionQueueHonorsCancellation(t *testing.T) {
	// gate 为 execution 使用的零值提交门禁，第一项先占用唯一 token。
	var gate submissionGate
	// err 检查首次占用是否成功，后续断言只能在 token 已持有时执行。
	if err := gate.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer gate.Unlock()
	// ctx 在排队前取消，不能等待持有者手动释放。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// err 必须保留调用方的取消原因，而非等待或占用稳定提交序号。
	if err := gate.Lock(ctx); err != context.Canceled {
		t.Fatal(err)
	}
}

// TestAffinityIntegerBounds 允许协议整数类型，拒绝 JSON 浮点和 int32 溢出。
func TestAffinityIntegerBounds(t *testing.T) {
	// value 包含各支持的数值表示及协议两端边界，不能因为 Go 平台字长不同而截断。
	for _, value := range []any{int(8), int32(8), int64(8), int64(-2147483648), int64(2147483647)} {
		// converted 与 err 检查值和类型，真实 dispatcher 只识别 int32 及 string。
		converted, err := affinityValue(value)
		if err != nil {
			t.Fatal(err)
		}
		// ok 确认输出没有退化成 float64，避免底层忽略整数亲和条件。
		if _, ok := converted.(int32); !ok {
			t.Fatalf("integer type lost: %T", converted)
		}
	}
	// value 包含越界整数、浮点和非数值；每项必须在发送请求之前报错。
	for _, value := range []any{int64(-2147483649), int64(2147483648), float64(8), true, nil} {
		// err 必须显式拒绝非法动态值，不能静默丢弃 Required 条件。
		if _, err := affinityValue(value); err == nil {
			t.Fatalf("invalid affinity accepted: %v", value)
		}
	}
}
