package engine

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestUnfinishedIOHasDiagnostics 验证不响应取消的 SDK I/O 会报告身份、类别与数量，日志调用不持锁
func TestUnfinishedIOHasDiagnostics(t *testing.T) {
	// instance 使用离线后端，测试只覆盖资源退出超时及重复关闭
	instance, backend := isolatedEngine(t)
	// logs 保存结构化诊断，不涉及实际业务 payload 或授权信息
	var logs bytes.Buffer
	instance.Config.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	instance.Config.Shutdown.Timeout = 20 * time.Millisecond
	// finish 故意延迟到关闭返回后，模拟违反取消契约的下载实现
	_, finish, err := instance.BeginIO(context.Background(), "result.decode")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	// budget 已取消，强制关闭使用单独配置的清理预算
	budget, cancel := context.WithCancel(context.Background())
	cancel()
	err = instance.Shutdown(budget)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "id=1 kind=result.decode") {
		t.Fatal("missing pending I/O identity:", err)
	}
	if !strings.Contains(logs.String(), "count=1") || !strings.Contains(logs.String(), "result.decode") || backend.count.Load() != 1 {
		t.Fatal("missing shutdown diagnostics or duplicate release:", logs.String())
	}
}
