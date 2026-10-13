package ports

import "context"

// TargetPreparer 为同一次提交返回验证凭据，后续后端提交复用它，不重复查询当前 workflow。
// 凭据属于具体后端与调用上下文，不允许跨目标、跨实例或跨独立调用永久复用。
type TargetPreparer interface {
	// PrepareTarget 验证注册信息并冻结本次调用的目标约束。
	PrepareTarget(context.Context, string, string, string, ...string) (context.Context, error)
}

// PrepareTarget 优先取得后端的本次调用凭据；受控测试后端可以只提供校验接口。
func PrepareTarget(ctx context.Context, backend Backend, name, shape, mode string, types ...string) (context.Context, error) {
	if preparer, ok := backend.(TargetPreparer); ok {
		return preparer.PrepareTarget(ctx, name, shape, mode, types...)
	}
	if validator, ok := backend.(TargetValidator); ok {
		if err := validator.ValidateTarget(ctx, name, shape, mode, types...); err != nil {
			return nil, err
		}
	}
	return ctx, nil
}

// outputTypeKey 的约束来自生成桩；它是本次调用的数据校验，不作为权限凭据。
type outputTypeKey struct{}

// WithExpectedOutput 在目标查询前声明生成桩的响应类型，校验仍复用同一次注册查询。
func WithExpectedOutput(ctx context.Context, typeName string) context.Context {
	return context.WithValue(ctx, outputTypeKey{}, typeName)
}

// ExpectedOutput 读取本次 RPC 的响应约束；普通动态结果入口不预先指定目标 Go 类型。
func ExpectedOutput(ctx context.Context) string {
	name, _ := ctx.Value(outputTypeKey{}).(string)
	return name
}
