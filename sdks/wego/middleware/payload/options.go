package payload

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Option 配置压缩或对象卸载组件；构造完成后策略保持不可变。
type Option func(*encodingConfig)

// encodingConfig 区分默认阈值和显式零值，不在 Runtime 保存第二份配置。
type encodingConfig struct {
	// minBytes 是调用者指定的最小发送字节数，0 表示始终执行。
	minBytes int
	// configured 为 false 时使用默认 256 KiB，使组件零值也可注册。
	configured bool
}

// WithThreshold 设置组件发送阈值，单位字节；0 强制执行，负值在构造时拒绝。
// 例如 WithThreshold(128<<10) 仅处理至少 128 KiB 的当前步骤输入。
func WithThreshold(bytes int) Option {
	return func(config *encodingConfig) {
		config.minBytes, config.configured = bytes, true
	}
}

// threshold 返回组件的有效阈值，解码始终忽略发送阈值。
func (c *encodingConfig) threshold() int {
	if !c.configured {
		return DefaultEncodingThreshold
	}
	return c.minBytes
}

// configureEncoding 构造独立策略快照；重复选项最后一次生效，任何 I/O 前完成校验。
func configureEncoding(opts []Option) (encodingConfig, error) {
	// config 仅属于当前组件，不与调用方或其他组件共享可修改状态。
	var config encodingConfig
	for _, option := range opts {
		if option == nil {
			return config, status.Error(codes.InvalidArgument, "codec: nil encoding option")
		}
		option(&config)
	}
	if config.threshold() < 0 {
		return config, status.Error(codes.InvalidArgument, "codec: encoding threshold cannot be negative")
	}
	return config, nil
}

// NewGzip 配置 gzip 组件的发送策略；默认阈值 256 KiB。
func NewGzip(opts ...Option) (*Gzip, error) {
	config, err := configureEncoding(opts)
	if err != nil {
		return nil, err
	}
	return &Gzip{encoding: config}, nil
}

// NewZstd 配置 zstd 组件的发送策略；默认阈值 256 KiB。
func NewZstd(opts ...Option) (*Zstd, error) {
	config, err := configureEncoding(opts)
	if err != nil {
		return nil, err
	}
	return &Zstd{encoding: config}, nil
}
