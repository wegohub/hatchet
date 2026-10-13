package wire

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/hatchet-dev/hatchet/sdks/wego/examples/codec/payload"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// countedPayload 记录实际解码次数，确保未知 ID 在任何变换执行前被拒绝。
type countedPayload struct {
	// Payload 委托真实 gzip 实现，大小和取消限制不由测试假造。
	middleware.Payload
	// calls 在并行解码测试中也可安全读取。
	calls atomic.Int64
}

// DecodeLimit 仅记录组件执行，编码能力继续使用被嵌入的真实组件。
func (p *countedPayload) DecodeLimit(ctx context.Context, data []byte, limit int) ([]byte, error) {
	p.calls.Add(1)
	return p.Payload.DecodeLimit(ctx, data, limit)
}

// TestCodecRegistryMessageOrder 验证注册顺序、发送顺序和历史解码是三项独立状态。
func TestCodecRegistryMessageOrder(t *testing.T) {
	cipher, err := payload.NewAESGCM(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	frame := &LogFrame{Version: LogVersion, Kind: "DATA", Payload: []byte(`{"value":"hello"}`)}
	senderConfig := spec.Runtime{Middleware: []middleware.Option{middleware.WithPayload(&payload.Gzip{}), middleware.WithPayload(cipher)}}
	sender, err := NewFrameCodec(senderConfig, 4096, 8192)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := sender.Encode(context.Background(), "/fixture/Call", frame)
	if err != nil {
		t.Fatal(err)
	}
	receiverConfig := spec.Runtime{
		Middleware:        []middleware.Option{middleware.WithPayload(cipher), middleware.WithPayload(&payload.Gzip{}), middleware.WithPayload(&profileCodec{id: "fixture.extra.v1"})},
		EncodingCodecsSet: true,
	}
	receiver, err := NewFrameCodec(receiverConfig, 4096, 8192)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := receiver.Decode(context.Background(), "/fixture/Call", encoded)
	if err != nil || !proto.Equal(frame, decoded) {
		t.Fatal("receiver used registration/encoding order", err)
	}
	// 显式空发送链仍产生正式 protobuf 封包，原发送方同样能还原。
	plain, err := receiver.Encode(context.Background(), "/fixture/Call", frame)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err = sender.Decode(context.Background(), "/fixture/Call", plain)
	if err != nil || !proto.Equal(frame, decoded) {
		t.Fatal("empty chain was not preserved", err)
	}
	// 重复 gzip 是两个真实步骤，接收端只注册一次组件也必须执行两次。
	receiverConfig.EncodingCodecs = []string{payload.GzipCodecID, payload.GzipCodecID}
	repeated, err := NewFrameCodec(receiverConfig, 4096, 8192)
	if err != nil {
		t.Fatal(err)
	}
	data, err := repeated.Encode(context.Background(), "/fixture/Call", frame)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err = sender.Decode(context.Background(), "/fixture/Call", data)
	if err != nil || !proto.Equal(frame, decoded) {
		t.Fatal("duplicate steps were deduplicated", err)
	}
}

// TestUnknownCodecBeforeIO 即使未知组件位于最内层，也不能先执行已知的外层组件。
func TestUnknownCodecBeforeIO(t *testing.T) {
	counted := &countedPayload{Payload: &payload.Gzip{}}
	codec, err := NewFrameCodec(spec.Runtime{Middleware: []middleware.Option{middleware.WithPayload(counted)}}, 4096, 8192)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := proto.Marshal(&EncodedLogFrame{FormatVersion: 1, CodecIds: []string{"fixture.missing.v1", payload.GzipCodecID}, Payload: []byte("invalid gzip")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(context.Background(), "", packet); status.Code(err) != codes.FailedPrecondition || counted.calls.Load() != 0 {
		t.Fatal("unknown codec caused partial I/O", err, counted.calls.Load())
	}
}
