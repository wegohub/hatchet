package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	observation "github.com/hatchet-dev/hatchet/sdks/wego/internal/telemetry"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/telemetry"
)

// closingBackend 记录关闭次数的后端 fixture，验证并发 Shutdown 只释放一次
type closingBackend struct {
	// ports.Backend 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	ports.Backend
	// count 原子记录实际 Close 次数，断言并发关闭只释放一次资源
	count atomic.Int32
}

// Close 释放此对象拥有的资源；借用资源必须由所有者关闭
func (b *closingBackend) Close() error {
	b.count.Add(1)
	return nil
}

// isolatedEngine 构造不依赖真实后端的生命周期测试引擎，注入可观察的关闭 fixture
func isolatedEngine(t *testing.T) (*Engine, *closingBackend) {
	t.Helper()
	// b 当前后端或测试后端对象，在所属实例内管理运行与资源，不经公开 API 暴露
	b := &closingBackend{}
	// c 创建默认配置副本，后续显式选项可以覆盖 0、false 或 nil
	c := spec.Defaults()
	// r, err 接收 observation.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	r, err := observation.New(context.Background(), telemetry.Config{Trace: telemetry.TraceConfig{DisableWorkerExporter: true}}, "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	// e 当前执行或引擎视图，复用所属后端和实例资源，不创建另一份 durable 状态
	e := &Engine{
		Backend:   b,
		Config:    c,
		telemetry: r,
		done:      make(chan struct{}),
		calls:     map[uint64]context.CancelFunc{},
		changed:   make(chan struct{}),
	}
	return e, b
}

// TestConcurrentShutdownCancelsWithinBudget 并发调用 Shutdown，验证等待在预算内结束且底层资源仅关闭一次
func TestConcurrentShutdownCancelsWithinBudget(t *testing.T) {
	// e, b 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值
	e, b := isolatedEngine(t)
	// call, done, err 登记本次调用并取得结束回调，返回前释放登记以允许实例排空
	call, done, err := e.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer done()

	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	// started 读取本机时钟，用于耗时或超时判断，不参与 durable 重放
	started := time.Now()
	// wg 等待本组并发步骤结束，资源关闭前必须确认所有已启动步骤退出
	var wg sync.WaitGroup
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// err 保存资源释放结果，清理错误同样必须报告，不能因业务已成功而忽略
			if err := e.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if time.Since(started) > time.Second || call.Err() != context.Canceled {
		t.Fatal("shutdown did not cancel active operation within its budget")
	}
	if b.count.Load() != 1 {
		t.Fatalf("backend closed %d times", b.count.Load())
	}
	if _, _, err = e.Begin(context.Background()); !errors.Is(err, model.ErrClosed) {
		t.Fatal(err)
	}
}

// TestDrainKeepsNestedOperationAlive 先登记一个顶层调用再开始排空，验证其内部操作仍可执行而新顶层调用被拒绝
func TestDrainKeepsNestedOperationAlive(t *testing.T) {
	// e, b 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值
	e, b := isolatedEngine(t)
	// call, done, err 登记本次调用并取得结束回调，返回前释放登记以允许实例排空
	call, done, err := e.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// closed 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	closed := make(chan error, 1)
	go func() {
		closed <- e.Shutdown(context.Background())
	}()
	// 等待真实生命周期状态变化，避免用固定 sleep 代替排空就绪
	for {
		e.mu.Lock()
		closing := e.closing
		changed := e.changed
		e.mu.Unlock()
		if closing {
			break
		}
		<-changed
	}
	if _, _, err = e.Begin(context.Background()); !errors.Is(err, model.ErrClosed) {
		t.Fatal("external operation admitted during drain")
	}
	// nested, finish, err 登记本次调用并取得结束回调，返回前释放登记以允许实例排空
	nested, finish, err := e.Begin(call)
	if err != nil {
		t.Fatal(err)
	}
	if nested.Err() != nil {
		t.Fatal("in-flight context canceled before completion")
	}
	done()
	// 根据已经就绪的通知选择下一步，不通过轮询固定延迟猜测执行时机
	select {
	case <-closed:
		t.Fatal("nested operation was not drained")
	default:
	}
	finish()
	if err = <-closed; err != nil {
		t.Fatal(err)
	}
	if b.count.Load() != 1 {
		t.Fatal("backend not closed once")
	}
	if _, _, err = e.Begin(call); !errors.Is(err, model.ErrClosed) {
		t.Fatal("completed instance admitted stale nested context")
	}
}

// waitingWorker 等待取消信号的 Worker fixture，验证排空可被强制中断
type waitingWorker struct {
	// ports.Worker 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	ports.Worker
	// entered 进入关闭步骤的通知，让测试在确定时刻取消预算，而非靠 sleep 猜测
	entered chan struct{}
}

// Close 释放此对象拥有的资源；借用资源必须由所有者关闭
func (w *waitingWorker) Close(ctx context.Context) error {
	close(w.entered)
	<-ctx.Done()
	return ctx.Err()
}

// TestCancelableDrainInterruptsWorkerAndCalls 注入等待型 Worker 和在途调用，取消排空 context 后两者都收到取消
func TestCancelableDrainInterruptsWorkerAndCalls(t *testing.T) {
	// e, b 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值
	e, b := isolatedEngine(t)
	// w 当前工作流或 Worker 对象，后续绑定依赖或执行生命周期操作
	w := &waitingWorker{entered: make(chan struct{})}
	e.workers = []ports.Worker{w}
	// call, finish, err 登记本次调用并取得结束回调，返回前释放登记以允许实例排空
	call, finish, err := e.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	// drain, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	drain, cancel := context.WithCancel(context.Background())
	defer cancel()
	// done 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	done := make(chan error, 1)
	go func() { done <- e.Shutdown(drain) }()
	<-w.entered
	cancel()
	// 等待排空状态或强制停止信号；Stop 可以打断 GracefulStop 的持续等待
	select {
	// err 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("force cancellation did not interrupt drain")
	}
	if call.Err() != context.Canceled || b.count.Load() != 1 {
		t.Fatal("resources did not close exactly once")
	}
}

// TestOwnedIOCleanupPrecedesBackendRelease 验证取消通知后仍等待 SDK 自有资源退出
func TestOwnedIOCleanupPrecedesBackendRelease(t *testing.T) {
	// instance 和 backend 仅模拟资源释放，不连接真实任务引擎
	instance, backend := isolatedEngine(t)
	// ioContext 与 finish 在同一锁内登记自有 I/O，结束函数用于发布实际退出
	ioContext, finish, err := instance.BeginIO(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	// budget 已取消，业务排空不能等待但资源释放使用独立预算
	budget, cancel := context.WithCancel(context.Background())
	cancel()
	// stopped 确认关闭返回，finish 之前此通道不得得到结果
	stopped := make(chan error, 1)
	go func() { stopped <- instance.Shutdown(budget) }()
	<-ioContext.Done()
	select {
	// earlyErr 若到达表示只发出取消就提前关闭了后端，SDK goroutine 仍拥有资源
	case earlyErr := <-stopped:
		t.Fatalf("released backend before I/O exit: %v", earlyErr)
	case <-time.After(30 * time.Millisecond):
	}
	if backend.count.Load() != 0 {
		t.Fatal("backend released while I/O owns transport")
	}
	finish()
	select {
	// closeErr 在实际退出后仍保留调用方排空取消，不能伪造正常排空成功
	case closeErr := <-stopped:
		if !errors.Is(closeErr, context.Canceled) {
			t.Fatal(closeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("resource cleanup did not finish")
	}
	if backend.count.Load() != 1 {
		t.Fatal("backend not released exactly once")
	}
}
