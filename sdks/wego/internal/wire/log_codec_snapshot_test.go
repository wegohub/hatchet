package wire

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// frameMetrics 只保存固定指标，验证成功和错误操作都会进入实例出口
type frameMetrics struct {
	// values 按指标名与结果汇总，测试不生成包含运行身份的标签
	values map[string]float64
}

// profileCodec 是零变换的完整帧 codec，专门验证持久格式标识的边界
type profileCodec struct {
	// id 必须是 protobuf string 可表示的 UTF-8，不能发布解码器永远拒绝的封装
	id string
}

// CodecID 提供测试指定的格式标识
func (c *profileCodec) CodecID() string { return c.id }

// Encode 保留输入，最终封装仍必须拥有独立结果
func (c *profileCodec) Encode(_ context.Context, _ string, data []byte) ([]byte, error) {
	return data, nil
}

// Decode 对业务入口提供相同的零变换语义
func (c *profileCodec) Decode(_ context.Context, _ string, data []byte) ([]byte, error) {
	return data, nil
}

// DecodeLimit 在有界还原入口拒绝超出预算的字节
func (c *profileCodec) DecodeLimit(_ context.Context, _ string, data []byte, limit int) ([]byte, error) {
	if len(data) > limit {
		return nil, status.Error(codes.ResourceExhausted, "fixture payload exceeds limit")
	}
	return data, nil
}

// TestFrameCodecRejectsInvalidUTF8Profile 在发布前发现非法标识，不把错误延迟到订阅解码
func TestFrameCodecRejectsInvalidUTF8Profile(t *testing.T) {
	for _, id := range []string{"", string([]byte{0xff})} {
		if _, err := NewFrameCodec([]middleware.Option{middleware.WithPayload(&profileCodec{id: id})}, 4096, 8192); status.Code(err) != codes.FailedPrecondition {
			t.Fatal("invalid profile accepted", err)
		}
	}
}

// ObserveProtocol 累计指标值，标签范围由测试逐项验证
func (m *frameMetrics) ObserveProtocol(metric ports.ProtocolMetric) {
	if metric.Mode != "codec" || metric.Method != "/fixture/Call" {
		panic("unexpected codec metric labels")
	}
	m.values[metric.Name+":"+metric.Outcome] += metric.Value
}

// TestFrameEncodingSnapshotAndCompatibility 验证直接封装与生成的 protobuf 外层格式一致
// 127/128、16383/16384 覆盖长度 varint 边界，后续编码及消息修改不能覆盖先前结果
func TestFrameEncodingSnapshotAndCompatibility(t *testing.T) {
	codec, err := NewFrameCodec(nil, 1<<20, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{0, 127, 128, 16383, 16384, 65536} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			frame := &LogFrame{Version: LogVersion, Kind: "DATA", Payload: bytes.Repeat([]byte{'x'}, size)}
			inner, err := proto.Marshal(frame)
			if err != nil {
				t.Fatal(err)
			}
			want, err := proto.Marshal(&EncodedLogFrame{FormatVersion: 1, Payload: inner})
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := codec.Encode(context.Background(), "/fixture/Call", frame)
			if err != nil || !bytes.Equal(encoded, want) {
				t.Fatal("wire format changed", err)
			}
			if size > 0 {
				frame.Payload[0] = 'z'
			}
			if _, err := codec.Encode(context.Background(), "/fixture/Call", frame); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded, want) {
				t.Fatal("encoded snapshot aliases reusable input")
			}
		})
	}
	// 最终封装的字段 tag 和长度必须包含在预算中，不能仅检查内层帧
	frame := &LogFrame{Version: LogVersion}
	limited, err := NewFrameCodec(nil, 128, proto.Size(frame))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := limited.Encode(context.Background(), "", frame); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("outer framing escaped budget", err)
	}
}

// TestFrameMetricsFailures 验证非法版本、损坏帧也能诊断，关闭观测时不需要 observer
func TestFrameMetricsFailures(t *testing.T) {
	metrics := &frameMetrics{values: map[string]float64{}}
	codec, err := NewFrameCodec(nil, 4096, 8192, metrics)
	if err != nil {
		t.Fatal(err)
	}
	data, err := codec.Encode(context.Background(), "/fixture/Call", &LogFrame{Version: LogVersion})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(context.Background(), "/fixture/Call", data); err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Encode(context.Background(), "/fixture/Call", nil); err == nil {
		t.Fatal("invalid frame accepted")
	}
	if _, err := codec.Decode(context.Background(), "/fixture/Call", []byte{0xff}); err == nil {
		t.Fatal("invalid encoded frame accepted")
	}
	for _, operation := range []string{"encode", "decode"} {
		for _, outcome := range []string{"success", "failure"} {
			if metrics.values["wego_frame_"+operation+"_total:"+outcome] != 1 {
				t.Fatal("missing outcome metric", operation, outcome)
			}
		}
	}
}

// TestBinaryFrameMetadataIsolation 验证二进制值和重复键往返，业务修改返回值不影响协议帧
func TestBinaryFrameMetadataIsolation(t *testing.T) {
	input := metadata.MD{"key-bin": {string([]byte{0xff, 0, 0xfe}), "second"}}
	encoded := Metadata(input)
	encoded["nil"] = nil
	decoded := FromMetadata(encoded)
	if decoded["key-bin"][0] != input["key-bin"][0] || len(decoded["key-bin"]) != 2 {
		t.Fatal("metadata lost binary/repeated values")
	}
	encoded["key-bin"].Values[0][0] = 0
	if decoded["key-bin"][0] != input["key-bin"][0] {
		t.Fatal("decoded metadata aliases frame")
	}
}
