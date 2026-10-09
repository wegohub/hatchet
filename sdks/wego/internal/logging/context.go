// Package logging 实现统一日志输出和不可变上下文属性，不持有后端连接。
package logging

import (
	"context"
	"log/slog"
	"slices"
	"sync/atomic"

	"go.opentelemetry.io/otel/trace"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// output 是 log 包共享的输出实例；nil 表示每次读取当前 slog.Default。
var output atomic.Pointer[slog.Logger]

// attrsKey 保存业务上下文属性，与执行身份的保护字段分开存储。
type attrsKey struct{}

// identityKey 保存本次执行的标量快照，避免每条日志复制 TaskInfo 中的业务 map。
type identityKey struct{}

// SetOutput 原子替换输出；nil 只供内部恢复默认出口，Runtime 不发布 nil 配置。
func SetOutput(logger *slog.Logger) {
	if logger == nil {
		output.Store(nil)
		return
	}
	output.Store(slog.New(Handler(logger.Handler())))
}

// Output 一次读取固定的出口；并发替换不会拆开同一次日志的过滤和写入。
func Output() *slog.Logger {
	if logger := output.Load(); logger != nil {
		return logger
	}
	return slog.New(Handler(slog.Default().Handler()))
}

// With 复制并继承属性；例如父 count=10、子 count=20，父仍保持 10。
func With(ctx context.Context, attrs ...slog.Attr) context.Context {
	if len(attrs) == 0 {
		return ctx
	}
	merged := append(slices.Clone(Attrs(ctx)), attrs...)
	// 动态 LogValuer 在 Enabled 之后解析；这里只消除已知的普通同名键。
	return context.WithValue(ctx, attrsKey{}, compact(merged, false))
}

// Attrs 返回内部只读属性；返回值不能修改或交给会修改切片的 Handler。
func Attrs(ctx context.Context) []slog.Attr {
	attrs, _ := ctx.Value(attrsKey{}).([]slog.Attr)
	return attrs
}

// WithTask 在执行入口冻结身份，不保存 payload、labels 或完整业务 metadata。
func WithTask(ctx context.Context, info model.TaskInfo) context.Context {
	attrs := []slog.Attr{
		slog.String("run_id", info.RunID), slog.String("task_run_id", info.TaskRunID),
		slog.String("worker_id", info.WorkerID), slog.String("worker_key", info.WorkerKey),
		slog.Int("retry_count", info.RetryCount),
	}
	if info.Durable {
		attrs = append(attrs, slog.Int64("invocation_count", int64(info.InvocationCount)))
	}
	return context.WithValue(ctx, identityKey{}, attrs)
}

// identity 返回身份和当前有效 span；业务同名属性不能覆盖真实执行关联。
func identity(ctx context.Context) []slog.Attr {
	attrs, _ := ctx.Value(identityKey{}).([]slog.Attr)
	result := slices.Clone(attrs)
	if span := trace.SpanContextFromContext(ctx); span.IsValid() {
		result = append(result, slog.String("trace_id", span.TraceID().String()), slog.String("span_id", span.SpanID().String()))
	}
	return result
}

// FromContext 固定当前出口并绑定 context，支持标准 Info 和 InfoContext 及派生日志器。
func FromContext(ctx context.Context) *slog.Logger {
	h := *Output().Handler().(*handler)
	h.bound = ctx
	return slog.New(&h)
}
