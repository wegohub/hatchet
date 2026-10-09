package runtime

import (
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// StreamMode 选择持久或实时输出，属于 wego 自有配置
type StreamMode = spec.StreamMode

// StreamOptions 为一条完整 RPC 方法配置资源预算
type StreamOptions = spec.StreamOptions

const (
	// Reliable 持久发布并允许恢复，默认启用
	Reliable = spec.Reliable
	// Realtime 实时输出，不提供持久回放
	Realtime = spec.Realtime
)

// WithStreamMode 仅修改此方法的输出模式，不清除已经配置的大小或超时
func WithStreamMode(method string, mode StreamMode) Option {
	return func(r *spec.Runtime) {
		if r.StreamMethods == nil {
			r.StreamMethods = map[string]spec.StreamOptions{}
		}
		o := r.StreamMethods[method]
		o.Mode = mode
		r.StreamMethods[method] = o
	}
}

// WithStreamOptions 替换此方法的资源限制；零值继承实例默认限制
func WithStreamOptions(method string, value StreamOptions) Option {
	return func(r *spec.Runtime) {
		if r.StreamMethods == nil {
			r.StreamMethods = map[string]spec.StreamOptions{}
		}
		r.StreamMethods[method] = value
	}
}

// WithStreamIdempotencyTTL 设置输入提交的保留期；显式零值或负值在启动前拒绝
func WithStreamIdempotencyTTL(method string, ttl time.Duration) Option {
	return func(r *spec.Runtime) {
		if r.StreamMethods == nil {
			r.StreamMethods = map[string]spec.StreamOptions{}
		}
		o := r.StreamMethods[method]
		if ttl == 0 {
			ttl = -1
		}
		o.IdempotencyTTL = ttl
		r.StreamMethods[method] = o
	}
}
