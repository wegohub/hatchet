package wire

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// LogVersion 是单任务持久日志协议，与客户端和 Worker envelope 一致
const LogVersion = 6

// FrameCodec 统一处理业务和控制帧，不在 DATA 内部再执行第二次变换
type FrameCodec struct {
	// registry 包含接收能力；历史消息可使用与当前发送链不同的组件顺序。
	registry *spec.PayloadCodecs
	// chain 保存经过有界解码契约校验的 codec，按 Encode 顺序排列
	chain []middleware.Payload
	// ids 是持久格式标识，不记录密钥内容
	ids []string
	// maxPlain 限制 codec 前后完整 protobuf 帧的字节数
	maxPlain int
	// maxEncoded 限制每层变换和最终封装的实际字节数
	maxEncoded int
	// observer 在构造后保持不变；指标关闭时为 nil，不读取时钟或创建时间序列
	observer ports.ProtocolObserver
}

// NewFrameCodec 在订阅或发布前拒绝缺少有界还原能力的自定义 codec
func NewFrameCodec(config spec.Runtime, maxPlain, maxEncoded int, observers ...ports.ProtocolObserver) (*FrameCodec, error) {
	if maxPlain <= 0 || maxEncoded <= 0 {
		return nil, status.Error(codes.InvalidArgument, "wego: frame limits must be positive")
	}
	registry, err := config.Codecs()
	if err != nil {
		return nil, err
	}
	codec := &FrameCodec{registry: registry, ids: registry.EncodingIDs(), maxPlain: maxPlain, maxEncoded: maxEncoded}
	if len(observers) > 1 {
		return nil, status.Error(codes.InvalidArgument, "wego: frame codec accepts one instance observer")
	}
	if len(observers) == 1 {
		codec.observer = observers[0]
	}
	for _, id := range codec.ids {
		component, _ := registry.Lookup(id)
		codec.chain = append(codec.chain, component)
	}
	return codec, nil
}

// Encode 返回可以用于同序号重试的独立字节快照
func (c *FrameCodec) Encode(ctx context.Context, method string, frame *LogFrame) (encoded []byte, err error) {
	if c.observer != nil {
		started := time.Now()
		defer func() { c.observe(method, "encode", started, err) }()
	}
	if frame == nil || frame.Version != LogVersion {
		return nil, status.Error(codes.FailedPrecondition, "wego: log frame protocol must be 6")
	}
	size := proto.Size(frame)
	if size > c.maxPlain {
		return nil, status.Errorf(codes.ResourceExhausted, "wego: plain_frame_bytes=%d exceeds limit=%d", size, c.maxPlain)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(c.chain) == 0 {
		// 无变换时直接把 protobuf 追加到封装的 payload 字段，一次分配保留独立快照
		// 字段号与 stream.proto 一致：format_version=1、codec_ids=2、payload=3
		prefix, err := c.envelopePrefix(size, nil)
		if err != nil {
			return nil, err
		}
		return proto.MarshalOptions{}.MarshalAppend(prefix, frame)
	}
	data, err := proto.Marshal(frame)
	if err != nil {
		return nil, err
	}
	// applied 是当前消息实际执行的链，各并发调用独立保存。
	applied := make([]string, 0, len(c.ids))
	for index, codec := range c.chain {
		if !c.registry.ShouldEncode(c.ids[index], len(data)) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err = codec.Encode(ctx, data)
		applied = append(applied, c.ids[index])
		if err != nil {
			return nil, err
		}
		if len(data) > c.maxEncoded {
			return nil, status.Errorf(codes.ResourceExhausted, "wego: codec_output_bytes=%d exceeds limit=%d", len(data), c.maxEncoded)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix, err := c.envelopePrefix(len(data), applied)
	if err != nil {
		return nil, err
	}
	return append(prefix, data...), nil
}

// envelopePrefix 一次分配最终封装，codec 仍获得独立输入，返回的结果不引用其缓冲区
// 不池化逃逸到队列或自定义 codec 的字节，避免后续编码覆盖已经发布的重试快照
func (c *FrameCodec) envelopePrefix(payloadBytes int, ids []string) ([]byte, error) {
	size := 2 + protowire.SizeBytes(payloadBytes) + 1
	for _, id := range ids {
		size += 1 + protowire.SizeBytes(len(id))
	}
	if size > c.maxEncoded {
		return nil, status.Errorf(codes.ResourceExhausted, "wego: encoded_frame_bytes=%d exceeds limit=%d", size, c.maxEncoded)
	}
	data := make([]byte, 0, size)
	data = protowire.AppendTag(data, 1, protowire.VarintType)
	data = protowire.AppendVarint(data, 1)
	for _, id := range ids {
		data = protowire.AppendTag(data, 2, protowire.BytesType)
		data = protowire.AppendString(data, id)
	}
	data = protowire.AppendTag(data, 3, protowire.BytesType)
	return protowire.AppendVarint(data, uint64(payloadBytes)), nil
}

// Decode 先还原完整帧，再由日志解释器判断 CLAIM、序号和取消目标
func (c *FrameCodec) Decode(ctx context.Context, method string, data []byte) (decoded *LogFrame, err error) {
	if c.observer != nil {
		started := time.Now()
		defer func() { c.observe(method, "decode", started, err) }()
	}
	if len(data) > c.maxEncoded {
		return nil, status.Errorf(codes.ResourceExhausted, "wego: encoded_frame_bytes=%d exceeds limit=%d", len(data), c.maxEncoded)
	}
	// envelope 在编码大小校验后解析，只包含格式和 codec 后字节
	var envelope EncodedLogFrame
	if err := proto.Unmarshal(data, &envelope); err != nil {
		return nil, status.Error(codes.DataLoss, "wego: malformed encoded log frame")
	}
	if envelope.FormatVersion != 1 {
		return nil, status.Error(codes.FailedPrecondition, "wego: log codec format mismatch")
	}
	if err := c.validateDecoders(envelope.CodecIds); err != nil {
		return nil, err
	}
	data = envelope.Payload
	for i := len(envelope.CodecIds) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		limit := c.maxEncoded
		if i == 0 {
			limit = c.maxPlain
		}
		// err 保留当前反向变换错误，不能跳过失败层解析协议
		var err error
		component, _ := c.registry.Lookup(envelope.CodecIds[i])
		data, err = component.DecodeLimit(ctx, data, limit)
		if err != nil {
			return nil, err
		}
		if len(data) > limit {
			return nil, status.Errorf(codes.ResourceExhausted, "wego: restored_frame_bytes=%d exceeds limit=%d", len(data), limit)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(data) > c.maxPlain {
		return nil, status.Errorf(codes.ResourceExhausted, "wego: plain_frame_bytes=%d exceeds limit=%d", len(data), c.maxPlain)
	}
	frame := new(LogFrame)
	if err := proto.Unmarshal(data, frame); err != nil {
		return nil, status.Error(codes.DataLoss, "wego: malformed decoded log frame")
	}
	if frame.Version != LogVersion {
		return nil, status.Errorf(codes.FailedPrecondition, "wego: log protocol expected=%d received=%d", LogVersion, frame.Version)
	}
	return frame, nil
}

// observe 只记录固定操作及成功/失败，输入内容、writer 和游标均不参与指标标签
func (c *FrameCodec) observe(method, operation string, started time.Time, err error) {
	outcome := "success"
	if err != nil {
		outcome = "failure"
	}
	c.observer.ObserveProtocol(ports.ProtocolMetric{Name: "wego_frame_" + operation + "_total", Method: method, Mode: "codec", Outcome: outcome, Value: 1})
	c.observer.ObserveProtocol(ports.ProtocolMetric{Name: "wego_frame_" + operation + "_duration_seconds", Method: method, Mode: "codec", Outcome: outcome, Value: time.Since(started).Seconds()})
}

// validateDecoders 先核验全链；不能下载外层对象后才发现内层组件缺失。
func (c *FrameCodec) validateDecoders(ids []string) error {
	// 先检查整条解码链，再执行任何 I/O；例如缺少 gzip 时不先下载外层 S3 对象。
	for _, id := range ids {
		if _, ok := c.registry.Lookup(id); !ok {
			return status.Errorf(codes.FailedPrecondition, "wego: decoding CodecID %q is not registered", id)
		}
	}
	if len(ids) > 16 {
		return status.Error(codes.ResourceExhausted, "wego: codec chain exceeds 16 steps")
	}

	return nil
}
