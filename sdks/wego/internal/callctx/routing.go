package callctx

import (
	"context"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// routingKey 是单次调用配置的私有 context 键，业务不能覆盖 SDK 的执行身份
type routingKey struct{}

// routingValue 保存完整快照或编码错误；WithRouting 没有错误返回，提交时必须检查 err
type routingValue struct {
	// values 仅保存独立 JSON 容器，不引用调用方的可变 map
	values map[string]any
	// err 延迟到提交前返回，非法配置不得部分创建运行
	err error
}

// WithRouting 为调用保存调度字段快照，后续派生 context 仍继承它
func WithRouting(ctx context.Context, values map[string]any) context.Context {
	snapshot, err := spec.SnapshotRouting(values)
	return context.WithValue(ctx, routingKey{}, routingValue{snapshot, err})
}

// Routing 返回独立调度快照；嵌套对象修改不能影响同一 context 的其他调用
func Routing(ctx context.Context) (map[string]any, error) {
	value, _ := ctx.Value(routingKey{}).(routingValue)
	if value.err != nil {
		return nil, value.err
	}
	return spec.SnapshotRouting(value.values)
}
