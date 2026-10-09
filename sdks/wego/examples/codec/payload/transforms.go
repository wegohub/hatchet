// Package payload 演示压缩、加密和 S3 卸载的有界 codec；不属于 SDK 核心 API
package payload

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxObjectBytes 限制每层变换的内存和对象大小；例如 9 MiB 输入不会上传到 S3
const maxObjectBytes = 8 << 20

// Gzip 压缩 ProtoJSON 输入和完整协议帧，解压必须遵守调用方预算
type Gzip struct{}

// CodecID 是持久格式身份，不随消息内容变化
func (*Gzip) CodecID() string { return "example.gzip.v1" }

// Encode 完整关闭压缩器后返回字节，避免上传缺少尾部校验和的对象
func (*Gzip) Encode(ctx context.Context, _ string, data []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(data) > maxObjectBytes {
		return nil, status.Error(codes.ResourceExhausted, "codec: compression input exceeds limit")
	}
	// output 只属于当前调用，多个 handler 可并发使用同一 codec
	var output bytes.Buffer
	writer := gzip.NewWriter(&output)
	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), ctx.Err()
}

// Decode 使用示例的绝对上限；SDK 流路径通过 DecodeLimit 提供更小的预算
func (p *Gzip) Decode(ctx context.Context, method string, data []byte) ([]byte, error) {
	return p.DecodeLimit(ctx, method, data, maxObjectBytes)
}

// DecodeLimit 边解压边限制字节，例如压缩炸弹在 1 MiB 预算内停止读取
func (*Gzip) DecodeLimit(ctx context.Context, _ string, data []byte, limit int) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, status.Error(codes.DataLoss, "codec: invalid gzip payload")
	}
	defer reader.Close()
	return readLimit(ctx, reader, limit)
}

// AESGCM 对字节加密并认证方法身份；实例持有不可变密钥，可被客户端和 Worker 并发使用
// 此示例只使用一个密钥；实际轮换需在载荷中保存 key ID，并保留历史解密密钥
type AESGCM struct {
	// aead 保存复制后的密钥所创建的认证加密器，不公开原始密钥
	aead cipher.AEAD
}

// NewAESGCM 接受 32 字节的 AES-256 密钥；构造后修改调用方切片不影响实例
func NewAESGCM(key []byte) (*AESGCM, error) {
	if len(key) != 32 {
		return nil, status.Error(codes.InvalidArgument, "codec: AES-256 requires a 32-byte key")
	}
	block, err := aes.NewCipher(bytes.Clone(key))
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &AESGCM{aead: aead}, nil
}

// CodecID 只描述格式；随机 nonce 不影响两端的 codec profile
func (*AESGCM) CodecID() string { return "example.aes256-gcm.v1" }

// Encode 将随机 nonce 前置于密文，例如同样的请求重复编码仍产生不同密文
// 协议发布的重试必须复用 SDK 已冻结的完整编码字节，不能重新生成 nonce
func (p *AESGCM) Encode(ctx context.Context, method string, data []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(data)+p.aead.NonceSize()+p.aead.Overhead() > maxObjectBytes {
		return nil, status.Error(codes.ResourceExhausted, "codec: encrypted payload exceeds limit")
	}
	// nonce 每条消息独立生成，不能按 RunID 或业务序号确定性复用
	nonce := make([]byte, p.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return p.aead.Seal(nonce, nonce, data, []byte(method)), nil
}

// Decode 按默认预算还原密文
func (p *AESGCM) Decode(ctx context.Context, method string, data []byte) ([]byte, error) {
	return p.DecodeLimit(ctx, method, data, maxObjectBytes)
}

// DecodeLimit 在分配明文前检查长度，再认证密文和方法；跨方法移用引用明确失败
func (p *AESGCM) DecodeLimit(ctx context.Context, method string, data []byte, limit int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	plainSize := len(data) - p.aead.NonceSize() - p.aead.Overhead()
	if plainSize < 0 {
		return nil, status.Error(codes.DataLoss, "codec: truncated ciphertext")
	}
	if limit < 1 || plainSize > min(limit, maxObjectBytes) {
		return nil, status.Error(codes.ResourceExhausted, "codec: decrypted payload exceeds limit")
	}
	plain, err := p.aead.Open(nil, data[:p.aead.NonceSize()], data[p.aead.NonceSize():], []byte(method))
	if err != nil {
		return nil, status.Error(codes.DataLoss, "codec: ciphertext authentication failed")
	}
	return plain, nil
}

// readLimit 最多读取预算加一个判定字节；S3 的 Content-Length 或引用长度不能替代此检查
func readLimit(ctx context.Context, input io.Reader, limit int) ([]byte, error) {
	if limit < 1 {
		return nil, status.Error(codes.ResourceExhausted, "codec: invalid decode budget")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(input, int64(min(limit, maxObjectBytes))+1))
	if err != nil {
		return nil, err
	}
	if len(data) > min(limit, maxObjectBytes) {
		return nil, status.Error(codes.ResourceExhausted, "codec: restored payload exceeds limit")
	}
	return data, ctx.Err()
}
