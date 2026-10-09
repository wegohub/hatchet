package spec

import (
	"fmt"
	"time"
)

// WorkerEventOptions 限制完整通知传输及业务队列；内部取消不占业务队列容量
type WorkerEventOptions struct {
	// QueueMessages 限制排队回调数，默认 128
	QueueMessages int
	// QueueBytes 包含等待和正在执行回调的载荷，默认 1 MiB
	QueueBytes int
	// MaxMessageBytes 限制完整事件 JSON，默认 64 KiB
	MaxMessageBytes int
	// MaxFrameBytes 限制 codec 还原后的完整协议帧，默认 4 MiB
	MaxFrameBytes int
	// MaxEncodedBytes 限制编码后的完整帧，默认 4 MiB 减协议开销
	MaxEncodedBytes int
	// TTL 是未显式设置 ExpiresAt 时的有效期，默认五分钟
	TTL time.Duration
	// CallbackTimeout 为一个业务回调提供预算，默认 30 秒
	CallbackTimeout time.Duration
	// PublishTimeout 包含完整帧编码和固定字节重试，默认 10 秒
	PublishTimeout time.Duration
	// DecodeTimeout 限制单帧逆 codec，默认 10 秒
	DecodeTimeout time.Duration
	// RecoveryTimeout 限制事件通道一次故障后的重新确认预算，默认 30 秒
	RecoveryTimeout time.Duration
	// JoinTimeout 限制加入屏障和历史扫描，默认 30 秒
	JoinTimeout time.Duration
	// MaxReplayFrames 限制屏障前包括旧广播在内的全部帧，默认十万条
	MaxReplayFrames int
	// MaxReplayBytes 分别限制编码和还原的历史，默认 64 MiB
	MaxReplayBytes int
	// DedupEntries 限制事件 ID 的有效期去重缓存，默认 4096 条
	DedupEntries int
}

// WorkerEventOptionsFor 逐字段合并零值，返回独立有效预算
func (r *Runtime) WorkerEventOptionsFor() WorkerEventOptions {
	o := r.WorkerEvents
	if o.QueueMessages == 0 {
		o.QueueMessages = 128
	}
	if o.QueueBytes == 0 {
		o.QueueBytes = 1 << 20
	}
	if o.MaxMessageBytes == 0 {
		o.MaxMessageBytes = 64 << 10
	}
	if o.MaxFrameBytes == 0 {
		o.MaxFrameBytes = 4 << 20
	}
	if o.MaxEncodedBytes == 0 {
		o.MaxEncodedBytes = (4 << 20) - 5120
	}
	if o.TTL == 0 {
		o.TTL = 5 * time.Minute
	}
	if o.CallbackTimeout == 0 {
		o.CallbackTimeout = 30 * time.Second
	}
	if o.PublishTimeout == 0 {
		o.PublishTimeout = 10 * time.Second
	}
	if o.DecodeTimeout == 0 {
		o.DecodeTimeout = 10 * time.Second
	}
	if o.JoinTimeout == 0 {
		o.JoinTimeout = 30 * time.Second
	}
	if o.RecoveryTimeout == 0 {
		o.RecoveryTimeout = 30 * time.Second
	}
	if o.MaxReplayFrames == 0 {
		o.MaxReplayFrames = 100000
	}
	if o.MaxReplayBytes == 0 {
		o.MaxReplayBytes = 64 << 20
	}
	if o.DedupEntries == 0 {
		o.DedupEntries = 4096
	}
	return o
}

// Validate 在建立连接或订阅前拒绝负预算；队列至少能容纳一条合法事件
func (o *WorkerEventOptions) Validate() error {
	if o.QueueMessages < 1 || o.MaxMessageBytes < 1 || o.QueueBytes < o.MaxMessageBytes || o.MaxFrameBytes < o.MaxMessageBytes || o.MaxEncodedBytes < 1 ||
		o.TTL <= 0 || o.CallbackTimeout <= 0 || o.PublishTimeout <= 0 || o.DecodeTimeout <= 0 || o.JoinTimeout <= 0 || o.RecoveryTimeout <= 0 ||
		o.MaxReplayFrames < 1 || o.MaxReplayBytes < 1 || o.DedupEntries < 1 {
		return fmt.Errorf("wego: invalid worker event limits")
	}
	return nil
}
