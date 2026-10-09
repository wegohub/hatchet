package telemetry

import (
	"context"
	"net"
	"testing"

	"github.com/hatchet-dev/hatchet/sdks/wego/telemetry"
)

// TestExpiredShutdownClosesMetricsListener 传入已到期的关闭 context，断言指标端口仍会释放
func TestExpiredShutdownClosesMetricsListener(t *testing.T) {
	// l, err 接收 net.Listen 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// address 保存实际绑定地址，测试使用空闲端口避免固定端口冲突
	address := l.Addr().String()
	l.Close()
	// r, err 接收 context.Background 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	r, err := New(
		context.Background(),
		telemetry.Config{
			Trace:   telemetry.TraceConfig{DisableWorkerExporter: true},
			Metrics: telemetry.MetricsConfig{Enabled: true, Addr: address},
		},
		"",
		"",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = r.Close(ctx)
	// listener, err 接收 net.Listen 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal("expired budget leaked metrics listener:", err)
	}
	listener.Close()
}
