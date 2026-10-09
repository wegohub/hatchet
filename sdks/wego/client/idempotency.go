package client

import (
	"context"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
)

// WithIdempotencyKey 指定一次逻辑流调用的稳定键；业务应在提交前保存此值
func WithIdempotencyKey(ctx context.Context, key string) context.Context {
	return callctx.WithIdempotencyKey(ctx, key)
}
