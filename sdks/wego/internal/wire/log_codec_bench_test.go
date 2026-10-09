package wire

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// BenchmarkFrameCodec 区分小控制帧、常见输出及大输出，报告独立编码快照的真实分配
// plain 与 AES-GCM 使用相同协议；加密随机 nonce 的成本包含在结果中
func BenchmarkFrameCodec(b *testing.B) {
	block, err := aes.NewCipher(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		b.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{0, 1024, 65536} {
		for _, encrypted := range []bool{false, true} {
			profile := "plain"
			// options 只在加密组安装业务变换
			var options []middleware.Option
			if encrypted {
				profile = "aes-gcm"
				options = []middleware.Option{middleware.WithPayload(&encryptedCodec{aead})}
			}
			codec, err := NewFrameCodec(options, 1<<20, 2<<20)
			if err != nil {
				b.Fatal(err)
			}
			frame := &LogFrame{Version: LogVersion, Kind: "DATA", Method: "/fixture/Call", TaskRunId: "task", Writer: "writer", FrameId: "frame", OutputSeq: 1, Payload: bytes.Repeat([]byte{'x'}, size)}
			encoded, err := codec.Encode(context.Background(), frame.Method, frame)
			if err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("%s/%d/encode", profile, size), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(proto.Size(frame)))
				for b.Loop() {
					if _, err := codec.Encode(context.Background(), frame.Method, frame); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(fmt.Sprintf("%s/%d/decode", profile, size), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(encoded)))
				for b.Loop() {
					if _, err := codec.Decode(context.Background(), frame.Method, encoded); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkProtoJSONEnvelope 测量完整任务 envelope，默认路径不增加 base64 封装
func BenchmarkProtoJSONEnvelope(b *testing.B) {
	message := wrapperspb.String(string(bytes.Repeat([]byte{'x'}, 1024)))
	for b.Loop() {
		if _, err := Encode(context.Background(), "/fixture/Call", message, nil, nil, 1<<20); err != nil {
			b.Fatal(err)
		}
	}
}
