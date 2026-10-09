package features

import (
	"context"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// CronsClient 管理客户端，复用 base 的共享后端，不通过公开返回值暴露后端类型
type CronsClient struct {
	// base 共享的管理调用能力，包含后端和 RPC 输入编码器
	base
}

// SchedulesClient 管理客户端，复用 base 的共享后端，不通过公开返回值暴露后端类型
type SchedulesClient struct {
	// base 共享的管理调用能力，包含后端和 RPC 输入编码器
	base
}

// EventsClient 管理客户端，复用 base 的共享后端，不通过公开返回值暴露后端类型
type EventsClient struct {
	// base 共享的管理调用能力，包含后端和 RPC 输入编码器
	base
}

// Push 不把发布请求的截止时间写入 RPC 载荷，事件触发的任务可在发布结束后运行
func (c *EventsClient) Push(ctx context.Context, key string, input any, scope ...*string) error {
	// ctx 纳入实例在途登记，编码 I/O 与后端提交共享关闭和取消预算
	ctx, done, beginErr := c.begin(ctx)
	if beginErr != nil {
		return beginErr
	}
	defer done()
	// _, data, err 接收 c.encode 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	_, data, err := c.encode(wire.ForTrigger(ctx), key, input)
	if err != nil {
		return err
	}

	// request 明确标记输入和可选作用域，避免位置参数的隐式约定
	request := ports.EventsPush{Key: key, Input: data}
	if len(scope) > 0 {
		request.Scope = scope[0]
	}
	return c.backend.Feature(ctx, request, nil)
}

// Create 编码未来执行的 RPC 输入，注册请求的取消预算仅约束管理调用
func (c *CronsClient) Create(ctx context.Context, name string, trigger model.CreateCronTrigger) (model.Resource, error) {
	// ctx 纳入实例在途登记，编码 I/O 与后端提交共享关闭和取消预算
	ctx, done, beginErr := c.begin(ctx)
	if beginErr != nil {
		return nil, beginErr
	}
	defer done()
	// target, input, err 接收 c.encode 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	target, input, err := c.encode(wire.ForTrigger(ctx), name, trigger.Input)
	if err != nil {
		return nil, err
	}

	trigger.Input = input
	return c.invoke(ctx, ports.CronsCreate{Name: target, Trigger: trigger})
}

// Create 创建一次性调度，通过共享后端调用 Schedules.Create；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *SchedulesClient) Create(ctx context.Context, name string, trigger model.CreateScheduledRunTrigger) (model.Resource, error) {
	// ctx 纳入实例在途登记，编码 I/O 与后端提交共享关闭和取消预算
	ctx, done, beginErr := c.begin(ctx)
	if beginErr != nil {
		return nil, beginErr
	}
	defer done()
	trigger.TriggerAt = trigger.TriggerAt.UTC()
	// target, input, err 接收 c.encode 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	target, input, err := c.encode(wire.ForTrigger(ctx), name, trigger.Input)
	if err != nil {
		return nil, err
	}

	trigger.Input = input
	return c.invoke(ctx, ports.SchedulesCreate{Name: target, Trigger: trigger})
}

// Get 读取Cron 触发器，通过共享后端调用 Crons.Get；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *CronsClient) Get(ctx context.Context, id string) (model.Resource, error) {
	return c.invoke(ctx, ports.CronsGet{ID: id})
}

// List 查询列表Cron 触发器，通过共享后端调用 Crons.List；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *CronsClient) List(ctx context.Context, query model.Query) (model.Resource, error) {
	return c.invoke(ctx, ports.CronsList{Query: query})
}

// Get 读取一次性调度，通过共享后端调用 Schedules.Get；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *SchedulesClient) Get(ctx context.Context, id string) (model.Resource, error) {
	return c.invoke(ctx, ports.SchedulesGet{ID: id})
}

// List 查询列表一次性调度，通过共享后端调用 Schedules.List；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *SchedulesClient) List(ctx context.Context, query model.Query) (model.Resource, error) {
	return c.invoke(ctx, ports.SchedulesList{Query: query})
}

// Delete 删除Cron 触发器，通过共享后端调用 Crons.Delete；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *CronsClient) Delete(ctx context.Context, id string) error {
	return c.backend.Feature(ctx, ports.CronsDelete{ID: id}, nil)
}

// Delete 删除一次性调度，通过共享后端调用 Schedules.Delete；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *SchedulesClient) Delete(ctx context.Context, id string) error {
	return c.backend.Feature(ctx, ports.SchedulesDelete{ID: id}, nil)
}
