package client

import (
	"context"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// WithStreamIdempotencyKey 指定流提交身份；业务应先持久保存 key，再发起新调用。
func WithStreamIdempotencyKey(key string) RunOption { return spec.WithStreamIdempotencyKey(key) }

// StreamResumeToken 返回 Client 的消费恢复凭证，供 Conn.ResumeStream 继续读取已有可靠输出。
// 使用流返回的 Context；例如已交付 60 条但预取到 64 条时，凭证仍停在第 60 条。
// 仅支持 Reliable server/bidi stream；Worker 恢复已发布历史使用 task.Checkpoint。
func StreamResumeToken(ctx context.Context) (model.StreamResumeToken, error) {
	return callctx.StreamResumeToken(ctx)
}
