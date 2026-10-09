package spec

import (
	"fmt"
	"time"
)

// StreamMode 选择持久恢复或实时输出；只有 Reliable 提供日志回放
type StreamMode uint8

const (
	// Reliable 使用持久 topic、明确 producer 和最终输出清单
	Reliable StreamMode = iota
	// Realtime 使用 PutStream；缺失消息返回错误，不保证重放
	Realtime
)

// StreamOptions 按方法限制消息、整批输入和日志扫描；零值继承默认值
type StreamOptions struct {
	// Mode 默认为持久输出，实时模式必须明确配置
	Mode StreamMode
	// IdempotencyTTL 限制三种流的提交幂等保留期，默认 24 小时
	IdempotencyTTL time.Duration
	// MaxMessageBytes 限制 codec 前的一条业务消息，默认 1 MiB
	MaxMessageBytes int
	// MaxFrameBytes 限制完整协议帧解码后的大小，默认 4 MiB
	MaxFrameBytes int
	// MaxEncodedMessageBytes 限制业务 codec 的中间字节，默认 4 MiB
	MaxEncodedMessageBytes int
	// MaxInputBytes 限制整批原始请求，默认 4 MiB
	MaxInputBytes int
	// MaxEncodedInputBytes 限制整个 JSON 输入 envelope，默认 4 MiB
	MaxEncodedInputBytes int
	// MaxInputMessages 限制零字节请求也能产生的对象数量，默认 4096
	MaxInputMessages int
	// PrefetchMessages 限制尚未交付的业务输出，默认 64 条
	PrefetchMessages int
	// PrefetchBytes 限制尚未交付的业务字节，默认 4 MiB
	PrefetchBytes int
	// ClaimTimeout 限制规范 CLAIM 的确认时间，默认 10 秒
	ClaimTimeout time.Duration
	// CompletionTimeout 限制引擎成功后缺失输出的对账，默认 30 秒
	CompletionTimeout time.Duration
	// PublishTimeout 限制一次冻结字节发布的重试，默认 10 秒
	PublishTimeout time.Duration
	// DecodeTimeout 限制包括对象下载在内的一次帧还原，默认 10 秒
	DecodeTimeout time.Duration
	// RecoveryTimeout 限制恢复扫描和业务历史迭代的总预算，默认 30 秒
	RecoveryTimeout time.Duration
	// MaxCheckpointMessages 限制有效历史消息数量，默认 1000
	MaxCheckpointMessages int
	// MaxCheckpointBytes 限制有效历史 ProtoJSON 累计字节，默认 10 MiB
	MaxCheckpointBytes int
	// MaxReplayFrames 限制恢复扫描的全部帧，包含无效旧输出，默认十万帧
	MaxReplayFrames int
	// MaxReplayBytes 分别限制编码和还原的历史大小，默认 64 MiB
	MaxReplayBytes int
}

// StreamOptionsFor 返回方法独立的有效限制，不改变 Runtime 保存的用户配置
func (r *Runtime) StreamOptionsFor(method string) StreamOptions {
	o := r.StreamMethods[method]
	if o.IdempotencyTTL == 0 {
		o.IdempotencyTTL = 24 * time.Hour
	}
	// 数值限制分别继承；MaxMessageBytes 仍遵循实例的基本消息预算
	if o.MaxMessageBytes == 0 {
		o.MaxMessageBytes = r.Stream.MaxMessageBytes
	}
	if o.MaxFrameBytes == 0 {
		o.MaxFrameBytes = 4 << 20
	}
	if o.MaxEncodedMessageBytes == 0 {
		o.MaxEncodedMessageBytes = 4 << 20
	}
	if o.MaxInputBytes == 0 {
		o.MaxInputBytes = 4 << 20
	}
	if o.MaxEncodedInputBytes == 0 {
		o.MaxEncodedInputBytes = 4 << 20
	}
	if o.MaxInputMessages == 0 {
		o.MaxInputMessages = 4096
	}
	if o.PrefetchMessages == 0 {
		o.PrefetchMessages = r.Stream.Window
	}
	if o.PrefetchBytes == 0 {
		o.PrefetchBytes = r.Stream.BufferBytes
	}
	if o.ClaimTimeout == 0 {
		o.ClaimTimeout = 10 * time.Second
	}
	if o.CompletionTimeout == 0 {
		o.CompletionTimeout = 30 * time.Second
	}
	if o.PublishTimeout == 0 {
		o.PublishTimeout = 10 * time.Second
	}
	if o.DecodeTimeout == 0 {
		o.DecodeTimeout = 10 * time.Second
	}
	if o.RecoveryTimeout == 0 {
		o.RecoveryTimeout = 30 * time.Second
	}
	if o.MaxCheckpointMessages == 0 {
		o.MaxCheckpointMessages = 1000
	}
	if o.MaxCheckpointBytes == 0 {
		o.MaxCheckpointBytes = 10 << 20
	}
	if o.MaxReplayFrames == 0 {
		o.MaxReplayFrames = 100000
	}
	if o.MaxReplayBytes == 0 {
		o.MaxReplayBytes = 64 << 20
	}
	return o
}

// Validate 防止负预算绕过限制；预取必须能容纳一条合法业务消息
func (o *StreamOptions) Validate() error {
	if o.Mode > Realtime || o.IdempotencyTTL <= 0 || o.MaxMessageBytes < 1 || o.MaxFrameBytes < o.MaxMessageBytes ||
		o.MaxEncodedMessageBytes < 1 || o.MaxInputBytes < 1 || o.MaxEncodedInputBytes < 1 ||
		o.MaxInputMessages < 1 || o.PrefetchMessages < 1 || o.PrefetchBytes < o.MaxMessageBytes ||
		o.ClaimTimeout <= 0 || o.CompletionTimeout <= 0 || o.PublishTimeout <= 0 || o.DecodeTimeout <= 0 ||
		o.RecoveryTimeout <= 0 || o.MaxCheckpointMessages < 1 || o.MaxCheckpointBytes < 1 || o.MaxReplayFrames < 1 || o.MaxReplayBytes < 1 {
		return fmt.Errorf("wego: invalid per-method stream limits")
	}
	return nil
}
