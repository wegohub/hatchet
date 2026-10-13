package spec

import (
	"fmt"
	"time"
)

// PayloadLimits 对序列化后的原文和完整传输封包分别计费；压缩不能绕过原文限制。
type PayloadLimits struct {
	// MaxMessageBytes 是单条业务消息的还原上限，默认 1 MiB。
	MaxMessageBytes int
	// MaxEncodedBytes 包含 base64 和外层 JSON，默认 4 MiB。
	MaxEncodedBytes int
	// EncodeTimeout 限制编码及对象上传，默认 10 秒。
	EncodeTimeout time.Duration
	// DecodeTimeout 限制解密、解压和下载，默认 10 秒。
	DecodeTimeout time.Duration
}

// DefaultPayloadLimits 返回实例独立的默认预算。
func DefaultPayloadLimits() PayloadLimits {
	return PayloadLimits{1 << 20, 4 << 20, 10 * time.Second, 10 * time.Second}
}

// Validate 在创建连接前拒绝无界或负数预算。
func (p PayloadLimits) Validate() error {
	if p.MaxMessageBytes <= 0 || p.MaxEncodedBytes <= 0 || p.EncodeTimeout <= 0 || p.DecodeTimeout <= 0 {
		return fmt.Errorf("wego: payload limits and timeouts must be positive")
	}
	return nil
}
