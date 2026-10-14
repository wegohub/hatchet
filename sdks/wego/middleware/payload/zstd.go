package payload

import (
	"bytes"
	"context"
	"errors"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ZstdCodecID 标识标准 zstd 帧格式，不包含压缩级别或业务数据。
const ZstdCodecID = "zstd.v1"

// Zstd 使用独立压缩器处理每次调用，无共享可变状态，可并发注册使用。
type Zstd struct {
	// encoding 保存组件自己的发送策略；注册后不得修改。
	encoding encodingConfig
}

// CodecID 返回接收端查找组件所使用的稳定格式标识。
func (*Zstd) CodecID() string { return ZstdCodecID }

// Encode 限制原始输入大小；单并发编码避免每条小消息创建多个工作 goroutine。
func (*Zstd) Encode(ctx context.Context, data []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(data) > maxObjectBytes {
		return nil, status.Error(codes.ResourceExhausted, "codec: compression input exceeds limit")
	}
	writer, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(maxObjectBytes))
	if err != nil {
		return nil, err
	}
	defer writer.Close()
	encoded := writer.EncodeAll(data, nil)
	return encoded, ctx.Err()
}

// Decode 使用绝对上限还原；例如 9 MiB 解压结果会被拒绝。
func (p *Zstd) Decode(ctx context.Context, data []byte) ([]byte, error) {
	return p.DecodeLimit(ctx, data, maxObjectBytes)
}

// DecodeLimit 同时限制解码窗口和输出读取，防止短压缩帧触发无界内存分配。
func (*Zstd) DecodeLimit(ctx context.Context, data []byte, limit int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 || len(data) > maxObjectBytes {
		return nil, status.Error(codes.ResourceExhausted, "codec: invalid decode budget or oversized input")
	}
	reader, err := zstd.NewReader(bytes.NewReader(data),
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(maxObjectBytes),
		zstd.WithDecoderMaxWindow(maxObjectBytes),
	)
	if err != nil {
		return nil, status.Error(codes.DataLoss, "codec: invalid zstd payload")
	}
	defer reader.Close()
	decoded, err := readLimit(ctx, reader, limit)
	if errors.Is(err, zstd.ErrWindowSizeExceeded) || errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		return nil, status.Error(codes.ResourceExhausted, "codec: zstd frame exceeds memory budget")
	}
	if err != nil && status.Code(err) == codes.Unknown && ctx.Err() == nil {
		return nil, status.Error(codes.DataLoss, "codec: invalid zstd payload")
	}
	return decoded, err
}

// EncodingThreshold 默认仅处理至少 256 KiB 的当前步骤输入。
func (p *Zstd) EncodingThreshold() int { return p.encoding.threshold() }
