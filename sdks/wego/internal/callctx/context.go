package callctx

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

// key 任务上下文私有键，避免与业务 context 的其他键冲突
type key struct{}

// State 通过 context 传递的 wego 任务能力，不改变标准 handler 签名
type State struct {
	// Execution 当前任务执行能力；网络 gRPC 请求不具有此能力
	Execution ports.Execution
	// Borrowed 当前任务可借用的连接视图，不能关闭实例资源
	Borrowed any
	// Tracer 本实例的追踪器，不读取或替换全局 provider
	Tracer trace.Tracer
	// Report 同步上报已合并的日志记录；nil 表示本次执行未启用任务日志上报。
	Report func(context.Context, slog.Record) error
}

// Bind 把任务能力与借用连接绑定到 context，业务 handler 仍接收标准 context.Context
func Bind(ctx context.Context, state *State) context.Context {
	return context.WithValue(ctx, key{}, state)
}

// Get 读取对应资源或上下文值，结果类型由当前入口决定
func Get(ctx context.Context) (*State, bool) {
	// value 是派生 context 中保留的 wego 执行能力，取消或追踪派生不丢失此身份
	value, ok := ctx.Value(key{}).(*State)
	return value, ok
}

// childKey 稳定子调用的私有上下文键，例如 step-1 在 durable 重放时保持相同身份
type childKey struct{}

// WithChildKey 在派生 context 中写入稳定子键；例如 "step-1" 在重放时定位同一个子运行
func WithChildKey(ctx context.Context, value string) context.Context {
	return context.WithValue(ctx, childKey{}, value)
}

// ChildKey 读取派生 context 中的稳定子任务键，未设置时返回空字符串
func ChildKey(ctx context.Context) *string {
	// v 与 ok 表示显式 child key 的存在性，未设置时由父执行分配稳定序号
	v, ok := ctx.Value(childKey{}).(string)
	// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值
	if !ok {
		return nil
	}

	return &v
}
