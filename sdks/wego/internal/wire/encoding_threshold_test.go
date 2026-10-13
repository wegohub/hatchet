package wire

import (
	"context"
	"strings"
	"testing"

	"github.com/hatchet-dev/hatchet/sdks/wego/examples/codec/payload"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"google.golang.org/protobuf/proto"
)

// TestEncodingThresholds 验证边界、实际步骤记录及接收端不同阈值，不依赖 S3。
func TestEncodingThresholds(t *testing.T) {
	for _, threshold := range []int{0, 5, 6} {
		compressed, err := payload.NewGzip(payload.WithThreshold(threshold))
		if err != nil {
			t.Fatal(err)
		}
		config := spec.Runtime{
			Middleware: []middleware.Option{middleware.WithPayload(compressed)},
		}
		input := Envelope{Version: Version, Payload: []byte(`"abc"`)}
		sealed, err := Seal(context.Background(), input, config, 4096, 8192, 8192)
		if err != nil {
			t.Fatal(err)
		}
		expected := 0
		if threshold <= len(input.Payload) {
			expected = 1
		}
		if len(sealed.CodecIDs) != expected {
			t.Fatalf("threshold=%d ids=%v", threshold, sealed.CodecIDs)
		}
		// 接收端即便设置更高发送阈值，也必须还原消息中已执行的 gzip。
		receiver, err := payload.NewGzip(payload.WithThreshold(1 << 20))
		if err != nil {
			t.Fatal(err)
		}
		config.Middleware = []middleware.Option{middleware.WithPayload(receiver)}
		opened, err := Open(context.Background(), "", sealed, config, 4096, 8192)
		if err != nil || string(opened.Payload) != string(input.Payload) {
			t.Fatalf("roundtrip: %v", err)
		}
	}
	frame := &LogFrame{Version: LogVersion, Kind: "DATA", Payload: []byte("hello")}
	for _, threshold := range []int{0, proto.Size(frame), proto.Size(frame) + 1} {
		compressed, err := payload.NewZstd(payload.WithThreshold(threshold))
		if err != nil {
			t.Fatal(err)
		}
		config := spec.Runtime{Middleware: []middleware.Option{middleware.WithPayload(compressed)}}
		codec, err := NewFrameCodec(config, 4096, 8192)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := codec.Encode(context.Background(), "", frame)
		if err != nil {
			t.Fatal(err)
		}
		// packet 的 ID 必须与当前帧实际执行步骤完全一致。
		var packet EncodedLogFrame
		if err := proto.Unmarshal(encoded, &packet); err != nil {
			t.Fatal(err)
		}
		expected := 0
		if threshold <= proto.Size(frame) {
			expected = 1
		}
		if len(packet.CodecIds) != expected {
			t.Fatalf("frame threshold=%d ids=%v", threshold, packet.CodecIds)
		}
		decoded, err := codec.Decode(context.Background(), "", encoded)
		if err != nil || !proto.Equal(decoded, frame) {
			t.Fatalf("frame roundtrip: %v", err)
		}
	}
}

// TestThresholdAfterCompression 压缩后的小值不得触发后续卸载步骤；步骤顺序不能使用原始大小判断。
func TestThresholdAfterCompression(t *testing.T) {
	config := spec.Runtime{
		Middleware: []middleware.Option{middleware.WithPayload(&payload.Gzip{}), middleware.WithPayload(&thresholdTestCodec{profileCodec: profileCodec{id: "fixture.upload"}})},
	}
	input := Envelope{Version: Version, Payload: []byte(`"` + strings.Repeat("a", 300<<10) + `"`)}
	sealed, err := Seal(context.Background(), input, config, 1<<20, 2<<20, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed.CodecIDs) != 1 || sealed.CodecIDs[0] != payload.GzipCodecID {
		t.Fatalf("steps after compression: %v", sealed.CodecIDs)
	}
	opened, err := Open(context.Background(), "", sealed, config, 1<<20, 2<<20)
	if err != nil || string(opened.Payload) != string(input.Payload) {
		t.Fatalf("compressed roundtrip: %v", err)
	}
}

// thresholdTestCodec 模拟具有上传阈值的组件，跳过时不会进入它的 Encode。
type thresholdTestCodec struct {
	// profileCodec 保留有界零变换，便于只验证步骤是否执行。
	profileCodec
}

// EncodingThreshold 按默认 256 KiB 判定后续步骤是否执行。
func (*thresholdTestCodec) EncodingThreshold() int { return 256 << 10 }
