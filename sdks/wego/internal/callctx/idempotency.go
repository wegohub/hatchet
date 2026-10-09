package callctx

import "context"

// idempotencyKeyKey 为逻辑调用保存提交重试中不变的业务键
type idempotencyKeyKey struct{}

// WithIdempotencyKey 保存业务已经持久记录的调用键，不根据输入自动合并调用
func WithIdempotencyKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, idempotencyKeyKey{}, key)
}

// IdempotencyKey 返回显式业务键，空值由一次调用生成随机键
func IdempotencyKey(ctx context.Context) string {
	key, _ := ctx.Value(idempotencyKeyKey{}).(string)
	return key
}
