package backend

import (
	"context"
	"strings"

	v0 "github.com/hatchet-dev/hatchet/pkg/client"
)

// Stream 订阅指定运行的真实流输出，把后端事件消息转成 string 并规范化订阅错误。
func (b *Backend) Stream(ctx context.Context, id string, fn func(string) error) error {
	return Normalize(b.raw.Subscribe().Stream(ctx, id, func(e v0.StreamEvent) error {
		return fn(string(e.Message))
	}))
}

// Publish 向指定任务执行发布流字节，供订阅端读取；错误在返回前转成 wego 状态。
func (b *Backend) Publish(ctx context.Context, id string, data []byte) error {
	return Normalize(b.raw.Event().PutStreamEvent(ctx, id, data))
}

// Log 上报任务日志，将等级转为后端需要的大写值并携带重试编号。
func (b *Backend) Log(ctx context.Context, id, level, message string, retry int) error {
	// r 转换为 int32 的重试计数，作为日志上报参数传给后端。
	r := int32(retry)
	level = strings.ToUpper(level)
	return Normalize(b.raw.Event().PutLog(ctx, id, message, &level, &r))
}
