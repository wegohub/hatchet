package features

import (
	"context"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// encodeInput 将 RPCInput 转成任务名和 envelope、保留原生 JSON 输入的转换函数。
type encodeInput func(context.Context, string, any) (string, any, error)

// base 管理客户端共享的后端和输入编码能力。
type base struct {
	// backend 内部后端接口；业务层不能取出其具体实现。
	backend ports.Backend
	// encode 把 RPC 输入转换成可提交的任务名称与 protobuf envelope。
	encode encodeInput
}

// invoke 调用共享管理后端并将结果解码为 model.Resource；具体请求固定功能与参数类型。
func (b base) invoke(ctx context.Context, request ports.FeatureRequest) (model.Resource, error) {
	// out 构造当前步骤的数据对象，字段在提交、编码或断言之前一次性初始化。
	out := model.Resource{}
	// err 接收 b.backend.Feature 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	err := b.backend.Feature(ctx, request, &out)
	return out, err
}

// Clients 共用同一后端的管理入口集合，不为每个功能创建独立连接。
type Clients struct {
	// Runs 运行查询、取消、重放和流订阅入口。
	Runs *RunsClient
	// Crons 周期调度的注册与查询入口。
	Crons *CronsClient
	// Schedules 一次性调度的创建、查询和删除入口。
	Schedules *SchedulesClient
	// Events 触发此任务的事件键集合。
	Events *EventsClient
	// Filters 默认事件过滤器及传给业务的 filter payload。
	Filters *FiltersClient
	// Workers Worker 注册状态的查询与暂停入口。
	Workers *WorkersClient
	// Workflows 已注册工作流集合，可作为 Worker 就绪信息的补充。
	Workflows *WorkflowsClient
	// Logs 任务日志查询入口。
	Logs *LogsClient
	// Webhooks Webhook 注册、查询及更新入口。
	Webhooks *WebhooksClient
	// Metrics 指标配置或管理查询入口；实例指标监听默认关闭。
	Metrics *MetricsClient
	// RateLimits 任务消耗的限流额度定义集合。
	RateLimits *RateLimitsClient
	// CEL CEL 表达式调试入口。
	CEL *CELClient
	// Tenant 当前授权范围信息查询入口。
	Tenant *TenantClient
}

// New 为同一个后端创建全部管理入口，RPC 输入通过共享编码函数转换。
func New(backend ports.Backend, encode encodeInput) *Clients {
	// b 构造当前步骤的数据对象，字段在提交、编码或断言之前一次性初始化。
	b := base{backend: backend, encode: encode}
	return &Clients{
		Runs:       &RunsClient{b},
		Crons:      &CronsClient{b},
		Schedules:  &SchedulesClient{b},
		Events:     &EventsClient{b},
		Filters:    &FiltersClient{b},
		Workers:    &WorkersClient{b},
		Workflows:  &WorkflowsClient{b},
		Logs:       &LogsClient{b},
		Webhooks:   &WebhooksClient{b},
		Metrics:    &MetricsClient{b},
		RateLimits: &RateLimitsClient{b},
		CEL:        &CELClient{b},
		Tenant:     &TenantClient{b},
	}
}

// begin 把含载荷 I/O 的触发调用登记为一个生命周期，内部提交可继续使用相同 context。
func (b base) begin(ctx context.Context) (context.Context, func(), error) {
	// managed 由 Conn 注入；直接使用测试后端时只保留调用方预算。
	if managed, ok := b.backend.(interface {
		// Begin 登记一次可取消操作，完成回调必须在所有编码和提交工作结束后执行。
		Begin(context.Context) (context.Context, func(), error)
	}); ok {
		return managed.Begin(ctx)
	}
	return ctx, func() {}, nil
}
