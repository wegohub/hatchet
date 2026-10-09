package client

import (
	"context"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// StreamCheckpoint 返回最后成功交付给业务的断点；预取和解码失败不会推进此位置
func StreamCheckpoint(ctx context.Context) (model.StreamCheckpoint, error) {
	return callctx.StreamCheckpoint(ctx)
}
