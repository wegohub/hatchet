// Package payload 为旧示例导入路径保留源码兼容；新代码使用 wego/middleware/payload。
package payload

import (
	"context"

	core "github.com/hatchet-dev/hatchet/sdks/wego/middleware/payload"
)

const (
	// GzipCodecID 与正式组件使用相同的持久格式标识。
	GzipCodecID = core.GzipCodecID
	// ZstdCodecID 与正式组件使用相同的持久格式标识。
	ZstdCodecID = core.ZstdCodecID
	// AESGCMCodecID 与正式组件使用相同的持久格式标识。
	AESGCMCodecID = core.AESGCMCodecID
	// S3ObjectCodecID 与正式组件使用相同的持久格式标识。
	S3ObjectCodecID = core.S3ObjectCodecID
	// DefaultEncodingThreshold 保留既有默认发送阈值。
	DefaultEncodingThreshold = core.DefaultEncodingThreshold
)

// Option 是正式组件的构造选项；旧路径调用不改变发送阈值语义。
type Option = core.Option

// Gzip 是正式 gzip 组件的类型别名。
type Gzip = core.Gzip

// Zstd 是正式 zstd 组件的类型别名。
type Zstd = core.Zstd

// AESGCM 是正式 AES-GCM 组件的类型别名。
type AESGCM = core.AESGCM

// Objects 是正式 S3 组件的类型别名。
type Objects = core.Objects

// Config 是正式 S3 组件的连接配置类型别名。
type Config = core.Config

// WithThreshold 使用正式组件的阈值选项。
func WithThreshold(bytes int) Option { return core.WithThreshold(bytes) }

// NewGzip 构造正式 gzip 组件。
func NewGzip(opts ...Option) (*Gzip, error) { return core.NewGzip(opts...) }

// NewZstd 构造正式 zstd 组件。
func NewZstd(opts ...Option) (*Zstd, error) { return core.NewZstd(opts...) }

// NewAESGCM 构造正式 AES-GCM 组件。
func NewAESGCM(key []byte) (*AESGCM, error) { return core.NewAESGCM(key) }

// ConfigFromEnv 读取与正式组件一致的本机 S3 配置。
func ConfigFromEnv() Config { return core.ConfigFromEnv() }

// NewS3 构造正式 S3 组件，连接和关闭仍由调用方负责。
func NewS3(ctx context.Context, config Config, prefix string, opts ...Option) (*Objects, error) {
	return core.NewS3(ctx, config, prefix, opts...)
}
