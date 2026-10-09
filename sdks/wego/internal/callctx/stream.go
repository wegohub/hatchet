package callctx

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// consumerKey 将消费位置读取器绑定到标准 ClientStream.Context
type consumerKey struct{}

// WithConsumer 在独立流上下文中安装线程安全的断点快照能力
func WithConsumer(ctx context.Context, read func() (model.StreamCheckpoint, error)) context.Context {
	return context.WithValue(ctx, consumerKey{}, read)
}

// StreamCheckpoint 读取当前流的已交付断点，普通 context 不拥有此能力
func StreamCheckpoint(ctx context.Context) (model.StreamCheckpoint, error) {
	read, ok := ctx.Value(consumerKey{}).(func() (model.StreamCheckpoint, error))
	if !ok {
		return model.StreamCheckpoint{}, status.Error(codes.FailedPrecondition, "wego: reliable client stream context required")
	}
	return read()
}
