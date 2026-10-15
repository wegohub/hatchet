package extra

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authtoken "github.com/hatchet-dev/hatchet/pkg/auth/token"
	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// Authenticator 校验 Hatchet token；禁止由 RPC 请求指定 tenant。
type Authenticator interface {
	// Authenticate 必须验证完整 token、遵守 context，返回已确认身份和可诊断错误。
	Authenticate(context.Context, string) (uuid.UUID, error)
}

// AuthenticatorFunc 把自定义鉴权函数适配为 Authenticator，不能提供 nil 函数。
type AuthenticatorFunc func(context.Context, string) (uuid.UUID, error)

// Authenticate 验证 bearer token 并返回租户身份，不从请求参数读取租户。
func (f AuthenticatorFunc) Authenticate(ctx context.Context, t string) (uuid.UUID, error) {
	return f(ctx, t)
}

// tenantKey 是私有身份槽，避免业务 context 键覆盖经过认证的租户。
type tenantKey struct{}

// contextWithTenant 供内部执行与测试注入已经认证的身份；公开 RPC 一律通过拦截器建立身份。
func contextWithTenant(ctx context.Context, id uuid.UUID) context.Context {
	return context.WithValue(ctx, tenantKey{}, id)
}

// tenant 仅接受拦截器写入的私有身份，缺少认证时返回 Unauthenticated。
func tenant(ctx context.Context) (uuid.UUID, error) {
	id, ok := ctx.Value(tenantKey{}).(uuid.UUID)
	if !ok || id == uuid.Nil {
		return uuid.Nil, status.Error(codes.Unauthenticated, "tenant identity required")
	}
	return id, nil
}

// authenticate 解析唯一的 Bearer header，区分鉴权失败、查询故障与调用取消。
func authenticate(ctx context.Context, a Authenticator) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 {
		return nil, status.Error(codes.Unauthenticated, "one bearer token required")
	}
	scheme, t, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "bearer") || t == "" || strings.ContainsAny(t, " \t\r\n") {
		return nil, status.Error(codes.Unauthenticated, "invalid bearer token")
	}
	id, err := a.Authenticate(ctx, t)
	if err != nil {
		if ctx.Err() != nil {
			return nil, rpcError(ctx.Err())
		}
		if errors.Is(err, authtoken.ErrCouldNotReadTokenFromDatabase) {
			return nil, status.Error(codes.Unavailable, "token verification unavailable")
		}
		return nil, status.Error(codes.Unauthenticated, "invalid bearer token")
	}
	if id == uuid.Nil {
		return nil, status.Error(codes.Unauthenticated, "invalid tenant")
	}
	return contextWithTenant(ctx, id), nil
}

// unaryAuth 在进入 unary handler 前完成认证并注入租户。
func unaryAuth(a Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		ctx, err := authenticate(ctx, a)
		if err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

// authenticatedStream 仅替换 context，发送和接收仍由原生 gRPC 流执行。
type authenticatedStream struct {
	grpc.ServerStream
	// ctx 保存本条流的认证租户，并保留原始传输取消信号。
	ctx context.Context
}

// Context 返回包含认证租户的流 context，保持原始取消和 deadline。
func (s *authenticatedStream) Context() context.Context {
	return s.ctx
}

// streamAuth 在流建立时认证一次，所有帧共用已经认证的租户 context。
func streamAuth(a Authenticator) grpc.StreamServerInterceptor {
	return func(srv any, s grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		ctx, err := authenticate(s.Context(), a)
		if err != nil {
			return err
		}
		return next(srv, &authenticatedStream{
			ServerStream: s,
			ctx:          ctx,
		})
	}
}

// 入场预算立即拒绝过载，不能让待入场 RPC 无限持有请求数据。
// unary 与长期订阅使用独立预算，满额 Watch 不能阻塞 ACK 和查询。
func unaryAdmission(slots chan struct{}) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if !admitted(ctx) {
			return nil, status.Error(codes.Unavailable, "server draining")
		}
		select {
		case slots <- struct{}{}:
			defer func() {
				<-slots
			}()
			return next(ctx, req)
		default:
			return nil, exhausted(ReasonRPCCapacity, "RPC capacity exhausted")
		}
	}
}

// streamAdmission 为长期流保留独立预算，订阅满额不能占用普通查询和 ACK 的预算。
func streamAdmission(slots chan struct{}, reads chan struct{}) grpc.StreamServerInterceptor {
	return func(srv any, s grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		if !admitted(s.Context()) {
			return status.Error(codes.Unavailable, "server draining")
		}
		budget, reason := slots, ReasonStreamCapacity
		if info.FullMethod == pb.StreamService_Read_FullMethodName {
			budget, reason = reads, ReasonReadCapacity
		}
		select {
		case budget <- struct{}{}:
			defer func() { <-budget }()
			return next(srv, s)
		default:
			return exhausted(reason, "stream capacity exhausted")
		}
	}
}
