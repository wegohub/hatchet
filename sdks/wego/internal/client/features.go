package client

import (
	"context"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

// managedFeatures 将管理请求和订阅纳入实例排空，防止关闭时提前释放连接。
type managedFeatures struct {
	// ports.Backend 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则。
	ports.Backend
	// engine 本实例的执行引擎，协调调用、Worker 与资源关闭。
	engine *engine.Engine
}

// Feature 把管理调用纳入实例在途登记，后端返回前完成登记释放；任务内部调用在排空期间仍可继续。
func (b managedFeatures) Feature(ctx context.Context, request ports.FeatureRequest, out any) error {
	// ctx, done, err 接收 b.engine.Begin 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	ctx, done, err := b.engine.Begin(ctx)
	if err != nil {
		return err
	}

	defer done()

	return b.Backend.Feature(ctx, request, out)
}

// Stream 跟踪流订阅的整个等待周期，订阅退出时释放在途登记，实例排空能确认其结束。
func (b managedFeatures) Stream(ctx context.Context, id string, consume func(string) error) error {
	// ctx、done、err 登记实例拥有的传输 I/O，只有订阅实际退出后才释放资源登记。
	ctx, done, err := b.engine.BeginIO(ctx, "management.request")
	if err != nil {
		return err
	}

	defer done()

	return b.Backend.Stream(ctx, id, consume)
}

// Begin 在 payload 上传之前登记操作，排空或强制关闭覆盖整段编码与提交。
func (b managedFeatures) Begin(ctx context.Context) (context.Context, func(), error) {
	return b.engine.Begin(ctx)
}
