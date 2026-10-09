package task

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// Checkpoint 返回可靠 server/bidi stream 在本次 CLAIM 之前的有效输出
// 业务读取至 EOF 后重建进度，例如历史已有输出 1、2，本次从输出 3 继续
func Checkpoint(ctx context.Context) (model.StreamHistory, error) {
	history := callctx.Checkpoint(ctx)
	if history == nil {
		return nil, status.Error(codes.FailedPrecondition, "wego: reliable output context required")
	}
	return history, nil
}
