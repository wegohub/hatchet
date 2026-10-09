package logging

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// Write 固定出口，先按级别打印，再显式上报；两个出口共享同一份已合并记录。
func Write(ctx context.Context, level slog.Level, pc uintptr, report bool, message string, args []any, attrs []slog.Attr) error {
	if ctx == nil {
		ctx = context.Background()
	}
	state, task := callctx.Get(ctx)
	task = task && state != nil && state.Execution != nil
	// capabilityErr 独立保存上报能力错误，普通日志仍先执行本地输出。
	var capabilityErr error
	if report {
		if !task {
			capabilityErr = model.ErrTaskContext
		} else if state.Report == nil {
			capabilityErr = model.ErrLogReportDisabled
		}
	}
	h := Output().Handler().(*handler)
	if !h.Enabled(ctx, level) {
		return capabilityErr
	}
	// PC 和时间在两个出口间保持一致；Clone 隔离底层 Handler 的记录修改。
	record := slog.NewRecord(time.Now(), level, message, pc)
	record.Add(args...)
	record.AddAttrs(attrs...)
	record = h.record(ctx, record)
	localErr := h.base.Handle(ctx, record.Clone())
	if !report || capabilityErr != nil {
		return errors.Join(localErr, capabilityErr)
	}
	return errors.Join(localErr, state.Report(ctx, record))
}
