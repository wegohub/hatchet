//go:build e2e

package backend

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/option"
)

// TestOversizedResultReport 验证真实运行快速失败并释放容量，不重试永久超大结果
func TestOversizedResultReport(t *testing.T) {
	_, config, ctx := p0Backend(t)
	config.BackendMessageLimit = 4096
	b, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	// executions 证明永久尺寸错误不会触发配置的三次业务重试
	var executions atomic.Int32
	definition := ports.Definition{Name: "oversized-report", Policy: spec.Task{Retries: option.Some(3)}, Function: func(context.Context, any) (any, error) { executions.Add(1); return strings.Repeat("x", 16<<10), nil }}
	w, err := b.Worker(ctx, "", []ports.Definition{definition}, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		budget, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := w.Close(budget); err != nil {
			t.Error(err)
		}
	})
	ref, err := b.Run(ctx, definition.Name, map[string]any{}, model.RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ref.Wait(ctx); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("oversized result did not reach failed terminal", err)
	}
	if executions.Load() != 1 {
		t.Fatal("permanent report failure retried", executions.Load())
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal("failed report prevented drain", err)
	}
	writeP0Report(t, b, "v020-report-budget", map[string]any{"run_id": ref.ID, "backend_message_limit": config.BackendMessageLimit, "handler_executions": executions.Load(), "assertions": []string{"real report converted to ResourceExhausted failure", "ShouldNotRetry prevents configured application retries", "result ACK permits normal worker drain"}, "cleanup": "worker closed; probe definition and run history retained"})
}
