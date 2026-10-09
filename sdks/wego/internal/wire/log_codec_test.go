package wire

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// encryptedCodec 用公开测试密钥验证随机 nonce，不使用任何环境凭证
type encryptedCodec struct{ aead cipher.AEAD }

// CodecID 为恢复读取提供稳定格式标识
func (c *encryptedCodec) CodecID() string { return "fixture-aes-gcm-v1" }

// Encode 对完整帧加密，每次独立 Encode 都产生不同密文
func (c *encryptedCodec) Encode(_ context.Context, method string, data []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return c.aead.Seal(nonce, nonce, data, []byte(method)), nil
}

// Decode 用于满足消息 codec 契约，持久帧使用 DecodeLimit
func (c *encryptedCodec) Decode(ctx context.Context, method string, data []byte) ([]byte, error) {
	return c.DecodeLimit(ctx, method, data, 4<<20)
}

// DecodeLimit 在解密分配前校验最终明文大小
func (c *encryptedCodec) DecodeLimit(_ context.Context, method string, data []byte, limit int) ([]byte, error) {
	n := c.aead.NonceSize()
	if len(data) < n+c.aead.Overhead() {
		return nil, fmt.Errorf("truncated encrypted frame")
	}
	if len(data)-n-c.aead.Overhead() > limit {
		return nil, status.Error(codes.ResourceExhausted, "plaintext exceeds limit")
	}
	return c.aead.Open(nil, data[:n], data[n:], []byte(method))
}

// TestControlAndDataUseIdenticalCodec 验证全部帧类型往返，控制字段不以明文留在外层
func TestControlAndDataUseIdenticalCodec(t *testing.T) {
	block, err := aes.NewCipher(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := NewFrameCodec([]middleware.Option{middleware.WithPayload(&encryptedCodec{aead})}, 4096, 8192)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"CLAIM", "HEADERS", "DATA", "ATTEMPT_END", "JOIN", "CANCEL", "EVENT"} {
		t.Run(kind, func(t *testing.T) {
			frame := &LogFrame{Version: LogVersion, Kind: kind, Writer: "private-writer", Payload: []byte(`{"message":"hello"}`)}
			first, err := codec.Encode(context.Background(), "/fixture/Call", frame)
			if err != nil {
				t.Fatal(err)
			}
			second, err := codec.Encode(context.Background(), "/fixture/Call", frame)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(first, second) || bytes.Contains(first, []byte(frame.Writer)) {
				t.Fatal("randomized full-frame encryption was bypassed")
			}
			decoded, err := codec.Decode(context.Background(), "/fixture/Call", first)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(frame, decoded) {
				t.Fatal("frame did not round-trip")
			}
			if _, err := codec.Decode(context.Background(), "/fixture/Other", first); err == nil {
				t.Fatal("incorrect method context decrypted frame")
			}
		})
	}
}

// TestFrameCodecBudgetsAndProfile 验证完整帧预算和外层 codec 配置不匹配
func TestFrameCodecBudgetsAndProfile(t *testing.T) {
	codec, err := NewFrameCodec(nil, 64, 128)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Encode(context.Background(), "", &LogFrame{Version: LogVersion, Payload: bytes.Repeat([]byte{1}, 65)}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("plain limit: %v", err)
	}
	encoded, err := proto.Marshal(&EncodedLogFrame{FormatVersion: 1, CodecIds: []string{"unknown"}, Payload: []byte{1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(context.Background(), "", encoded); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("profile mismatch: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := codec.Encode(ctx, "", &LogFrame{Version: LogVersion}); err != context.Canceled {
		t.Fatalf("cancellation: %v", err)
	}
}

// FuzzFrameCodecDecode 用不可信字节验证封装解码没有 panic，并只返回协议 4 帧
func FuzzFrameCodecDecode(f *testing.F) {
	codec, _ := NewFrameCodec(nil, 4096, 8192)
	valid, _ := codec.Encode(context.Background(), "", &LogFrame{Version: LogVersion, Kind: "CLAIM"})
	f.Add(valid)
	f.Add([]byte{0xff, 0x01})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		frame, err := codec.Decode(context.Background(), "", data)
		if err == nil && frame.Version != LogVersion {
			t.Fatal("decoder accepted incompatible protocol")
		}
	})
}
