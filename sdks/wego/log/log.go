package log

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// write 追加任务身份写入实例 slog logger；report=true 时再向引擎上报，缺少任务上下文返回 ErrTaskContext。
func write(ctx context.Context, level slog.Level, report bool, message string, args ...any) error {
	// s, ok 获取任务上下文能力与存在标记；普通网络入口没有任务身份，必须检查后再使用。
	s, ok := callctx.Get(ctx)
	// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值。
	if !ok {
		if report {
			return model.ErrTaskContext
		}

		return nil
	}

	// attrs 复制或扩展当前列表，避免把内部可变容器直接交给调用方修改。
	attrs := append([]any{}, args...)
	// 任务日志追加 RunID、TaskRunID、WorkerID 与 RetryCount，方便关联具体执行。
	if s.Execution != nil {
		// info 读取实际执行身份，例如 RunID 与 WorkerID；普通网络 context 没有任务身份。
		info := s.Execution.Info()
		attrs = append(attrs, "run_id", info.RunID, "task_run_id", info.TaskRunID, "worker_id", info.WorkerID, "retry_count", info.RetryCount)
	}
	s.Logger.Log(ctx, level, message, attrs...)
	// 只有上报型日志且实例提供 Report 回调时才写入引擎，普通日志只写实例 logger。
	if report && s.Report != nil {
		return s.Report(ctx, level.String(), fmt.Sprint(message))
	}

	return nil
}

// Debug 记录 Debug 级别实例日志，不向引擎上报。
func Debug(ctx context.Context, msg string, args ...any) {
	_ = write(ctx, slog.LevelDebug, false, msg, args...)
}

// Info 记录 Info 级别实例日志，不向引擎上报。
func Info(ctx context.Context, msg string, args ...any) {
	_ = write(ctx, slog.LevelInfo, false, msg, args...)
}

// Warn 记录 Warn 级别实例日志，不向引擎上报。
func Warn(ctx context.Context, msg string, args ...any) {
	_ = write(ctx, slog.LevelWarn, false, msg, args...)
}

// Error 记录 Error 级别实例日志，不向引擎上报。
func Error(ctx context.Context, msg string, args ...any) {
	_ = write(ctx, slog.LevelError, false, msg, args...)
}

// RDebug 记录 Debug 日志并上报引擎，返回上报错误。
func RDebug(ctx context.Context, msg string, args ...any) error {
	return write(ctx, slog.LevelDebug, true, msg, args...)
}

// RInfo 记录 Info 日志并上报引擎，返回上报错误。
func RInfo(ctx context.Context, msg string, args ...any) error {
	return write(ctx, slog.LevelInfo, true, msg, args...)
}

// RWarn 记录 Warn 日志并上报引擎，返回上报错误。
func RWarn(ctx context.Context, msg string, args ...any) error {
	return write(ctx, slog.LevelWarn, true, msg, args...)
}

// RError 记录 Error 日志并上报引擎，返回上报错误。
func RError(ctx context.Context, msg string, args ...any) error {
	return write(ctx, slog.LevelError, true, msg, args...)
}
