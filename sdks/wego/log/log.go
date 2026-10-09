package log

import (
	"context"
	"log/slog"
	goruntime "runtime"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/logging"
)

// With 继承上下文属性；同层同名字段后写覆盖前写，父 context 不变。
func With(ctx context.Context, attrs ...slog.Attr) context.Context {
	return logging.With(ctx, attrs...)
}

// Handler 为标准 slog 输出增加上下文属性和执行身份；重复包装保持幂等。
func Handler(h slog.Handler) slog.Handler { return logging.Handler(h) }

// FromContext 获取标准日志器，可使用 With、WithGroup、Info 和 InfoContext。
// 需要跨普通日志及 R 系列共享的字段，使用 With 绑定到 context。
func FromContext(ctx context.Context) *slog.Logger { return logging.FromContext(ctx) }

// Enabled 只判断统一输出实例的级别，不解析字段或进行网络操作。
func Enabled(ctx context.Context, level slog.Level) bool {
	return logging.Output().Enabled(ctx, level)
}

// write 获取业务调用位置；输出和上报使用同一个内部记录管线。
func write(ctx context.Context, level slog.Level, report bool, msg string, args []any, attrs []slog.Attr) error {
	// 三层栈分别为 Callers、write 和公开日志函数；PC 指向业务调用者。
	var pcs [1]uintptr
	goruntime.Callers(3, pcs[:])
	return logging.Write(ctx, level, pcs[0], report, msg, args, attrs)
}

// Log 记录自定义 slog 级别，只写本地出口。
func Log(ctx context.Context, level slog.Level, msg string, args ...any) {
	_ = write(ctx, level, false, msg, args, nil)
}

// LogAttrs 记录标准属性，例如 slog.Int("count", 3)，只写本地出口。
func LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	_ = write(ctx, level, false, msg, nil, attrs)
}

// Debug 按当前输出级别记录调试日志，普通 context 也可使用。
func Debug(ctx context.Context, msg string, args ...any) {
	_ = write(ctx, slog.LevelDebug, false, msg, args, nil)
}

// Info 记录普通信息日志，不上报任务日志。
func Info(ctx context.Context, msg string, args ...any) {
	_ = write(ctx, slog.LevelInfo, false, msg, args, nil)
}

// Warn 记录警告日志，不上报任务日志。
func Warn(ctx context.Context, msg string, args ...any) {
	_ = write(ctx, slog.LevelWarn, false, msg, args, nil)
}

// Error 记录错误日志，取消后的 context 仍能打印退出原因。
func Error(ctx context.Context, msg string, args ...any) {
	_ = write(ctx, slog.LevelError, false, msg, args, nil)
}

// RDebug 按级别打印并同步上报；普通 context 返回 ErrTaskContext。
func RDebug(ctx context.Context, msg string, args ...any) error {
	return write(ctx, slog.LevelDebug, true, msg, args, nil)
}

// RInfo 打印并上报同一条信息日志，返回上报或本地 Handler 的错误。
func RInfo(ctx context.Context, msg string, args ...any) error {
	return write(ctx, slog.LevelInfo, true, msg, args, nil)
}

// RWarn 打印并上报警告；未启用上报返回 ErrLogReportDisabled。
func RWarn(ctx context.Context, msg string, args ...any) error {
	return write(ctx, slog.LevelWarn, true, msg, args, nil)
}

// RError 打印并上报错误；本地输出失败仍尝试上报，合并返回失败原因。
func RError(ctx context.Context, msg string, args ...any) error {
	return write(ctx, slog.LevelError, true, msg, args, nil)
}
