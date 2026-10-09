package callctx

import (
	"context"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// checkpointKey 隔离 handler 的历史恢复能力，网络入口不自动具备此能力
type checkpointKey struct{}

// WithCheckpoint 只在可靠输出的实际执行上下文中安装有限历史
func WithCheckpoint(ctx context.Context, history model.StreamHistory) context.Context {
	return context.WithValue(ctx, checkpointKey{}, history)
}

// Checkpoint 读取当前执行的有限历史；nil 表示此传输不支持恢复
func Checkpoint(ctx context.Context) model.StreamHistory {
	history, _ := ctx.Value(checkpointKey{}).(model.StreamHistory)
	return history
}
