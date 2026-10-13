package wire

import (
	"context"
	"encoding/base64"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// EncodeNativeStream 为原生字节输出使用同一整帧 codec，不提供持久回放语义。
func EncodeNativeStream(ctx context.Context, config spec.Runtime, data []byte) ([]byte, error) {
	limits := config.PayloadLimits
	if len(data) > limits.MaxMessageBytes {
		return nil, status.Error(codes.ResourceExhausted, "wego: native stream message exceeds limit")
	}
	codec, err := NewFrameCodec(config, limits.MaxMessageBytes+(64<<10), limits.MaxEncodedBytes)
	if err != nil {
		return nil, err
	}
	budget, cancel := context.WithTimeout(ctx, limits.EncodeTimeout)
	defer cancel()
	encoded, err := codec.Encode(budget, "native-stream:"+config.Namespace, &LogFrame{Version: LogVersion, Kind: "NATIVE_STREAM", Payload: data})
	if err != nil {
		return nil, err
	}
	if base64.StdEncoding.EncodedLen(len(encoded)) > limits.MaxEncodedBytes {
		return nil, status.Error(codes.ResourceExhausted, "wego: encoded native stream exceeds limit")
	}
	return []byte(base64.StdEncoding.EncodeToString(encoded)), nil
}

// DecodeNativeStream 拒绝 RPC 控制帧，调用方只会得到原生业务字节。
func DecodeNativeStream(ctx context.Context, config spec.Runtime, value string) ([]byte, error) {
	limits := config.PayloadLimits
	if len(value) > limits.MaxEncodedBytes {
		return nil, status.Error(codes.ResourceExhausted, "wego: encoded native stream exceeds limit")
	}
	data, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, status.Error(codes.DataLoss, "wego: invalid native stream frame")
	}
	codec, err := NewFrameCodec(config, limits.MaxMessageBytes+(64<<10), limits.MaxEncodedBytes)
	if err != nil {
		return nil, err
	}
	budget, cancel := context.WithTimeout(ctx, limits.DecodeTimeout)
	defer cancel()
	frame, err := codec.Decode(budget, "native-stream:"+config.Namespace, data)
	if err != nil {
		return nil, err
	}
	if frame.Kind != "NATIVE_STREAM" {
		return nil, status.Error(codes.FailedPrecondition, "wego: RPC frames are not native stream messages")
	}
	if len(frame.Payload) > limits.MaxMessageBytes {
		return nil, status.Error(codes.ResourceExhausted, "wego: native stream message exceeds limit")
	}
	return append([]byte(nil), frame.Payload...), nil
}
