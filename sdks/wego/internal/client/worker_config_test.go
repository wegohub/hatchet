package client

import (
	"context"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// TestDisableWorkerRejectsNativeCreation 在 Runtime 禁用 Worker 后创建原生 Worker，断言得到配置冲突且不会创建后端 Worker
func TestDisableWorkerRejectsNativeCreation(t *testing.T) {
	// config 创建默认配置副本，后续显式选项可以覆盖 0、false 或 nil
	config := spec.Defaults()
	if config.DisableWorker {
		t.Fatal("Worker should be enabled by default")
	}
	runtime.WithDisableWorker()(&config)
	// conn 所属连接；用于提交任务和复用实例配置
	conn := &Conn{engine: &engine.Engine{Config: config}}
	// _, err 接收 conn.NewWorker 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
	if _, err := conn.NewWorker("test"); err == nil {
		t.Fatal("disabled Worker created")
	}
}

// blockedStartup 模拟初始化阶段阻塞的入口；只接受实际 context 取消，不主动释放
type blockedStartup struct {
	// Worker 委托当前测试不调用的方法
	ports.Worker
}

// Start 在监听尚未就绪前等待预算，用于验证 StartBlocking 的取消覆盖启动阶段
func (*blockedStartup) Start(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

// TestStartBlockingBudgetCoversInitialization 启动时尚无 cleanup，deadline 仍须结束调用
func TestStartBlockingBudgetCoversInitialization(t *testing.T) {
	// ctx 给受控初始化 20ms，不能等到启动后才开始观察预算
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	// worker 不需要连接：失败启动不得运行已启动资源的关闭路径
	worker := &Worker{native: &blockedStartup{}}
	// err 保留 deadline 原因，不要求额外 Stop 或释放信号
	if err := worker.StartBlocking(ctx); err != context.DeadlineExceeded {
		t.Fatal(err)
	}
}
