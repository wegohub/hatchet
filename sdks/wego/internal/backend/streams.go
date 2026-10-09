package backend

import (
	"context"
	"errors"
	"io"
	"unicode/utf8"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

const (
	// durablePayloadLimit 为正式版的内联上限，预留完整 RPC 的元数据空间
	durablePayloadLimit = 4*1024*1024 - 5*1024
	// backendMessageLimit 与默认 gRPC 消息上限一致，检查实际请求编码尺寸
	backendMessageLimit = 4 * 1024 * 1024
)

// PublishDurable 使用实例自有连接，不采用官方客户端自动轮换 producer 的恢复策略
func (b *Backend) PublishDurable(ctx context.Context, message ports.DurableMessage) error {
	if err := validateTopic(message.Namespace, message.Topic); err != nil {
		return err
	}
	if message.Producer == "" || message.Sequence < 0 {
		return status.Error(codes.InvalidArgument, "wego: durable producer must be nonempty and sequence nonnegative")
	}
	if len(message.Payload) == 0 || len(message.Payload) > durablePayloadLimit {
		return status.Errorf(codes.ResourceExhausted, "wego: durable_payload_bytes=%d exceeds valid range 1..%d", len(message.Payload), durablePayloadLimit)
	}
	request := &v1.PublishStreamMessageRequest{
		Namespace:   message.Namespace,
		Topic:       message.Topic,
		ProducerId:  message.Producer,
		ProducerSeq: message.Sequence,
		Payload:     message.Payload,
	}
	if size := proto.Size(request); size > b.messageLimit() {
		return status.Errorf(codes.ResourceExhausted, "wego: durable_request_bytes=%d exceeds backend_message_bytes=%d", size, b.messageLimit())
	}
	_, err := b.durableStreams.Publish(b.auth(ctx), request)
	return Normalize(err)
}

// SubscribeDurable 不吞掉 hangup 或 EOF；重连和最终运行状态由日志消费者判断
func (b *Backend) SubscribeDurable(ctx context.Context, subscription ports.DurableSubscription, consume func(ports.DurableEntry) error) error {
	if err := validateTopic(subscription.Namespace, subscription.Topic); err != nil {
		return err
	}
	if consume == nil {
		return status.Error(codes.InvalidArgument, "wego: durable consumer cannot be nil")
	}
	// 回调提前完成时也立即释放远端订阅，不等待调用方的长 deadline
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// cursor 保存订阅起点的独立值，回调返回后释放对应订阅
	var cursor *string
	if subscription.Cursor != nil {
		// 使用独立值，调用方在订阅期间修改原变量不会改变重连身份
		value := *subscription.Cursor
		cursor = &value
	}
	stream, err := b.durableStreams.Subscribe(b.auth(ctx), &v1.SubscribeStreamRequest{
		Namespace: subscription.Namespace, Topic: subscription.Topic, Cursor: cursor,
	})
	if err != nil {
		return Normalize(err)
	}
	for {
		message, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, io.EOF) {
				return status.Error(codes.Unavailable, "wego: durable subscription ended before consumer completion")
			}
			return Normalize(err)
		}
		// 若服务端同时携带最后一批消息和 hangup，先交付消息再请求重连
		for _, entry := range message.Entries {
			if entry == nil || entry.Cursor == "" {
				return status.Error(codes.DataLoss, "wego: durable entry missing cursor")
			}
			converted := ports.DurableEntry{Payload: entry.Payload, Cursor: entry.Cursor}
			if entry.CreatedAt != nil {
				converted.CreatedAt = entry.CreatedAt.AsTime()
			}
			if err := consume(converted); err != nil {
				return err
			}
		}
		if message.Hangup {
			return status.Error(codes.Unavailable, "wego: durable subscription hangup; resume from last interpreted cursor")
		}
	}
}

// validateTopic 在连接前拒绝非法名称，避免不同 UTF-8 处理方式产生隐式别名
func validateTopic(namespace, topic string) error {
	for _, name := range []string{namespace, topic} {
		if !utf8.ValidString(name) || utf8.RuneCountInString(name) > 255 {
			return status.Error(codes.InvalidArgument, "wego: durable namespace/topic must be UTF-8 and at most 255 characters")
		}
		for _, character := range name {
			if character == 0 {
				return status.Error(codes.InvalidArgument, "wego: durable namespace/topic contains NUL")
			}
		}
	}
	if topic == "" {
		return status.Error(codes.InvalidArgument, "wego: durable topic cannot be empty")
	}
	return nil
}

// 编译检查确保 backend 的动态能力视图不暴露官方协议类型
var _ ports.DurableStreams = (*Backend)(nil)

// messageLimit 为真实实例使用显式配置；未构造实例的协议 fixture 沿用 4 MiB 默认值
func (b *Backend) messageLimit() int {
	if b.config.BackendMessageLimit > 0 {
		return b.config.BackendMessageLimit
	}
	return backendMessageLimit
}
