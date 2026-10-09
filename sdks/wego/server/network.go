package server

import (
	"context"
	"log/slog"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/logging"
)

// networkCalls 在原生传输之外跟踪业务，使强制停止可取消正在排空的 handler
type networkCalls struct {
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁
	mu sync.Mutex
	// closing 是否已进入排空阶段；true 时拒绝新增顶层调用
	closing bool
	// calls 本地调用编号到取消函数的映射，排空等待此表清空
	calls map[uint64]context.CancelFunc
	// next 递增的本地调用编号，例如登记 1、2 后分别取消和释放，不共用同一身份
	next uint64
	// changed 状态变化通知；发送方关闭当前 channel 并换成新 channel，等待方重新检查状态
	changed chan struct{}
}

// begin 登记网络调用并创建可取消上下文，完成回调删除记录并唤醒排空等待者
// 例如登记调用 1、2 后，完成 1 只移除记录 1；排空仍等待调用 2 完成
func (n *networkCalls) begin(ctx context.Context) (context.Context, func(), error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	// 检查 n.closing；不满足协议或配置约束时返回 Unavailable（wego: server stopping）
	if n.closing {
		return nil, nil, status.Error(codes.Unavailable, "wego: server stopping")
	}
	// 首次调用时建立登记表与状态通知，后续调用复用同一生命周期跟踪器
	if n.calls == nil {
		n.calls = map[uint64]context.CancelFunc{}
		n.changed = make(chan struct{})
	}
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	ctx, cancel := context.WithCancel(ctx)
	n.next++
	// id 当前资源的唯一身份，用于查找对应运行或会话
	id := n.next
	n.calls[id] = cancel
	return ctx, func() {
		cancel()
		n.mu.Lock()
		delete(n.calls, id)
		close(n.changed)
		n.changed = make(chan struct{})
		n.mu.Unlock()
	}, nil
}

// unary 为网络 unary 调用登记可取消生命周期，再调用业务 handler
func (n *networkCalls) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	// ctx, done, err 接收 n.begin 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	ctx = logging.With(ctx, slog.String("rpc_method", info.FullMethod), slog.String("rpc_transport", "network"))
	ctx, done, err := n.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return handler(ctx, req)
}

// contextualStream 替换 ServerStream 的 Context，同时委托其他原生流方法
type contextualStream struct {
	// grpc.ServerStream 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	grpc.ServerStream
	// ctx 当前操作上下文，承载取消、截止时间与 metadata
	ctx context.Context
}

// Context 返回当前执行上下文，包含取消与截止时间
func (s *contextualStream) Context() context.Context { return s.ctx }

// stream 包装网络 ServerStream 的上下文，让强制停止可以取消流 handler
func (n *networkCalls) stream(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// ctx, done, err 接收 n.begin 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	ctx := logging.With(stream.Context(), slog.String("rpc_method", info.FullMethod), slog.String("rpc_transport", "network"))
	ctx, done, err := n.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	return handler(service, &contextualStream{ServerStream: stream, ctx: ctx})
}

// drain 禁止新增网络调用并等待在途表清空；强制停止信号到达时取消剩余上下文
func (n *networkCalls) drain(force <-chan struct{}) {
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		n.mu.Lock()
		n.closing = true
		// 没有在途业务，排空立即完成；有调用编号 1、2 时须两者都移除后再返回
		if len(n.calls) == 0 {
			n.mu.Unlock()
			return
		}
		// changed 状态变化通知；发送方关闭当前 channel 并换成新 channel，等待方重新检查状态
		changed := n.changed
		n.mu.Unlock()
		// 等待排空状态或强制停止信号；Stop 可以打断 GracefulStop 的持续等待
		select {
		case <-force:
			n.mu.Lock()
			// 逐项处理 n.calls，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
			for _, cancel := range n.calls {
				cancel()
			}
			n.mu.Unlock()
			return
		case <-changed:
		}
	}
}

// transportCallKey 将传输级跟踪保留到 RPC 终态，覆盖原生单个 interceptor 的外围逻辑
type transportCallKey struct{}

// TagRPC 登记传输层 RPC 的整个生命周期，包含原生 interceptor 外层执行
func (n *networkCalls) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	// derived, done, err 接收 n.begin 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	ctx = logging.With(ctx, slog.String("rpc_method", info.FullMethodName), slog.String("rpc_transport", "network"))
	derived, done, err := n.begin(ctx)
	if err != nil {
		// derived, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
		derived, cancel := context.WithCancel(ctx)
		cancel()
		return derived
	}
	return context.WithValue(derived, transportCallKey{}, done)
}

// HandleRPC 收到 stats.End 后释放传输层调用记录，通知排空等待者
func (n *networkCalls) HandleRPC(ctx context.Context, event stats.RPCStats) {
	// event 只有 stats.End 才表示网络 RPC 生命周期完成，其他统计事件不释放在途登记
	if _, ok := event.(*stats.End); ok {
		// done 是网络调用登记的结束回调，在实际 stats.End 时释放一次，排空才能可靠结束
		if done, ok := ctx.Value(transportCallKey{}).(func()); ok {
			done()
		}
	}
}

// TagConn 保留连接上下文；连接级事件不代表某次业务 RPC 已完成
func (n *networkCalls) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }

// HandleConn 处理连接级观测事件；业务完成由 RPC 结束事件判断
func (n *networkCalls) HandleConn(context.Context, stats.ConnStats) {}
