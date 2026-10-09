package backend

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	f "github.com/hatchet-dev/hatchet/sdks/go/features"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// featureClients 只构造没有后台资源的官方功能客户端；Workflow 与 Metrics 使用无缓存适配
type featureClients struct {
	// raw 为 REST 查询及官方 Runs 的只读运行详情提供配置
	raw v0.Client
}

// newFeatureClients 创建无 goroutine 的管理门面
func newFeatureClients(raw v0.Client) *featureClients { return &featureClients{raw: raw} }

// Runs 返回使用当前实例连接和身份的运行管理客户端
func (c *featureClients) Runs() *f.RunsClient {
	return f.NewRunsClient(c.raw.API(), c.raw.TenantId(), c.raw)
}

// Crons 返回 Cron 触发管理客户端，不拥有连接
func (c *featureClients) Crons() *f.CronsClient {
	return f.NewCronsClient(c.raw.API(), c.raw.TenantId())
}

// Schedules 返回同一 namespace 的计划管理客户端
func (c *featureClients) Schedules() *f.SchedulesClient {
	// namespace 取得 c.raw.Namespace 的结果，确认成功后才进入下一处理阶段
	namespace := c.raw.Namespace()
	return f.NewSchedulesClient(c.raw.API(), c.raw.TenantId(), &namespace)
}

// Workers 返回 Worker 管理客户端
func (c *featureClients) Workers() *f.WorkersClient {
	return f.NewWorkersClient(c.raw.API(), c.raw.TenantId())
}

// Filters 返回事件过滤管理客户端
func (c *featureClients) Filters() *f.FiltersClient {
	return f.NewFiltersClient(c.raw.API(), c.raw.TenantId())
}

// Webhooks 返回 Webhook 管理客户端
func (c *featureClients) Webhooks() *f.WebhooksClient {
	return f.NewWebhooksClient(c.raw.API(), c.raw.TenantId())
}

// Logs 返回日志查询客户端
func (c *featureClients) Logs() *f.LogsClient { return f.NewLogsClient(c.raw.API(), c.raw.TenantId()) }

// RateLimits 返回限流查询客户端，写入由协议层处理调用预算
func (c *featureClients) RateLimits() *f.RateLimitsClient {
	return f.NewRateLimitsClient(c.raw.API(), c.raw.TenantId(), c.raw.Admin())
}

// CEL 返回 CEL 调试客户端
func (c *featureClients) CEL() *f.CELClient { return f.NewCELClient(c.raw.API(), c.raw.TenantId()) }

// Workflows 返回无缓存工作流视图，删除再重建时自然读取新的身份
func (c *featureClients) Workflows() *workflowClient {
	return &workflowClient{api: c.raw.API(), tenant: uuid.MustParse(c.raw.TenantId())}
}

// Metrics 返回无缓存 Metrics 视图，避免创建无法通过公开 API 关闭的官方缓存
func (c *featureClients) Metrics() *metricsClient { return &metricsClient{workflows: c.Workflows()} }

// workflowClient 每次查询真实 REST 数据，不拥有缓存计时器
type workflowClient struct {
	// api 为借用的 REST 客户端
	api *rest.ClientWithResponses
	// tenant 限制工作流查询范围
	tenant uuid.UUID
}

// List 检查 HTTP 状态和响应体后返回工作流列表
func (w *workflowClient) List(ctx context.Context, p *rest.WorkflowListParams) (*rest.WorkflowList, error) {
	// r, err 取得 w.api.WorkflowListWithResponse 的结果，确认成功后才进入下一处理阶段
	r, err := w.api.WorkflowListWithResponse(ctx, w.tenant, p)
	if err != nil {
		return nil, err
	}
	if err = responseStatus(r.StatusCode(), r.JSON200 != nil); err != nil {
		return nil, err
	}
	return r.JSON200, nil
}

// Get 按名称查找精确工作流；删除后再次查询不读取旧缓存
func (w *workflowClient) Get(ctx context.Context, name string) (*rest.Workflow, error) {
	// r, err 取得 w.List 的结果，确认成功后才进入下一处理阶段
	r, err := w.List(ctx, &rest.WorkflowListParams{Name: &name})
	if err != nil {
		return nil, err
	}
	if r.Rows != nil {
		// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
		for _, row := range *r.Rows {
			if row.Name == name {
				return &row, nil
			}
		}
	}
	return nil, fmt.Errorf("wego: workflow %s not found", name)
}

// Delete 接受所有成功的 2xx 删除响应，例如无响应体的 204
func (w *workflowClient) Delete(ctx context.Context, name string) (*rest.WorkflowDeleteResponse, error) {
	// workflow, err 取得 w.Get 的结果，确认成功后才进入下一处理阶段
	workflow, err := w.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	// id 真实运行或成员身份，例如 run-1，不能使用显示名称代替
	id, err := uuid.Parse(workflow.Metadata.Id)
	if err != nil {
		return nil, err
	}
	// r, err 取得 w.api.WorkflowDeleteWithResponse 的结果，确认成功后才进入下一处理阶段
	r, err := w.api.WorkflowDeleteWithResponse(ctx, id)
	if err != nil {
		return nil, err
	}
	if r.StatusCode() < 200 || r.StatusCode() >= 300 {
		return nil, fmt.Errorf("wego: delete HTTP status %d", r.StatusCode())
	}
	return r, nil
}

// metricsClient 只借用工作流查询和 REST，不隐式创建 metadata 缓存
type metricsClient struct {
	// workflows 是同一实例、无缓存的工作流查询视图
	workflows *workflowClient
}

// GetWorkflowMetrics 名称先解析为当前工作流 ID，再读取 Metrics
func (m *metricsClient) GetWorkflowMetrics(ctx context.Context, name string, p *rest.WorkflowGetMetricsParams) (*rest.WorkflowMetrics, error) {
	// w, err 取得 m.workflows.Get 的结果，确认成功后才进入下一处理阶段
	w, err := m.workflows.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	// id 真实运行或成员身份，例如 run-1，不能使用显示名称代替
	id, err := uuid.Parse(w.Metadata.Id)
	if err != nil {
		return nil, err
	}
	// r, err 取得 m.workflows.api.WorkflowGetMetricsWithResponse 的结果，确认成功后才进入下一处理阶段
	r, err := m.workflows.api.WorkflowGetMetricsWithResponse(ctx, id, p)
	if err != nil {
		return nil, err
	}
	if err = responseStatus(r.StatusCode(), r.JSON200 != nil); err != nil {
		return nil, err
	}
	return r.JSON200, nil
}

// GetQueueMetrics 查询授权范围的队列统计
func (m *metricsClient) GetQueueMetrics(ctx context.Context, p *rest.TenantGetQueueMetricsParams) (*rest.TenantQueueMetrics, error) {
	// w 取得 m.workflows 的结果，确认成功后才进入下一处理阶段
	w := m.workflows
	// r, err 取得 w.api.TenantGetQueueMetricsWithResponse 的结果，确认成功后才进入下一处理阶段
	r, err := w.api.TenantGetQueueMetricsWithResponse(ctx, w.tenant, p)
	if err != nil {
		return nil, err
	}
	if err = responseStatus(r.StatusCode(), r.JSON200 != nil); err != nil {
		return nil, err
	}
	return r.JSON200, nil
}

// GetTaskQueueMetrics 查询任务队列统计
func (m *metricsClient) GetTaskQueueMetrics(ctx context.Context) (*rest.TenantStepRunQueueMetrics, error) {
	// w 取得 m.workflows 的结果，确认成功后才进入下一处理阶段
	w := m.workflows
	// r, err 取得 w.api.TenantGetStepRunQueueMetricsWithResponse 的结果，确认成功后才进入下一处理阶段
	r, err := w.api.TenantGetStepRunQueueMetricsWithResponse(ctx, w.tenant)
	if err != nil {
		return nil, err
	}
	if err = responseStatus(r.StatusCode(), r.JSON200 != nil); err != nil {
		return nil, err
	}
	return r.JSON200, nil
}

// responseStatus 拒绝空响应和错误 HTTP 状态，不能把零值当作成功结果
func responseStatus(code int, present bool) error {
	if code == http.StatusOK && present {
		return nil
	}
	// REST 拒绝保留可诊断的标准状态，不把权限、不存在或读模型缺失都变成 Unknown
	grpcCode := codes.FailedPrecondition
	switch code {
	case http.StatusOK:
		grpcCode = codes.DataLoss
	case http.StatusBadRequest:
		grpcCode = codes.InvalidArgument
	case http.StatusUnauthorized:
		grpcCode = codes.Unauthenticated
	case http.StatusForbidden:
		grpcCode = codes.PermissionDenied
	case http.StatusNotFound:
		grpcCode = codes.NotFound
	case http.StatusConflict:
		grpcCode = codes.AlreadyExists
	case http.StatusTooManyRequests:
		grpcCode = codes.ResourceExhausted
	case http.StatusNotImplemented:
		grpcCode = codes.Unimplemented
	default:
		if code >= 500 {
			grpcCode = codes.Unavailable
		}
	}
	return status.Errorf(grpcCode, "wego: HTTP status %d or missing response", code)
}
