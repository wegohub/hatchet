package client

import (
	"context"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
)

// WithRouting 设置本次 RPC 的调度字段，同名顶层键覆盖 Runtime 默认值
// 例如 WithRouting(ctx, map[string]any{"group":"group-a","cost":3})，CEL 读取 input.routing.group
// 一次 client/bidi 输入流只保存一份 routing，Send 不改变它
func WithRouting(ctx context.Context, values map[string]any) context.Context {
	return callctx.WithRouting(ctx, values)
}
