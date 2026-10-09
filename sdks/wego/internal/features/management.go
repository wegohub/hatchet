package features

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// FiltersClient 管理客户端，复用 base 的共享后端，不通过公开返回值暴露后端类型
type FiltersClient struct {
	// base 共享的管理调用能力，包含后端和 RPC 输入编码器
	base
}

// WorkersClient 管理客户端，复用 base 的共享后端，不通过公开返回值暴露后端类型
type WorkersClient struct {
	// base 共享的管理调用能力，包含后端和 RPC 输入编码器
	base
}

// WorkflowsClient 管理客户端，复用 base 的共享后端，不通过公开返回值暴露后端类型
type WorkflowsClient struct {
	// base 共享的管理调用能力，包含后端和 RPC 输入编码器
	base
}

// LogsClient 管理客户端，复用 base 的共享后端，不通过公开返回值暴露后端类型
type LogsClient struct {
	// base 共享的管理调用能力，包含后端和 RPC 输入编码器
	base
}

// WebhooksClient 管理客户端，复用 base 的共享后端，不通过公开返回值暴露后端类型
type WebhooksClient struct {
	// base 共享的管理调用能力，包含后端和 RPC 输入编码器
	base
}

// MetricsClient 管理客户端，复用 base 的共享后端，不通过公开返回值暴露后端类型
type MetricsClient struct {
	// base 共享的管理调用能力，包含后端和 RPC 输入编码器
	base
}

// RateLimitsClient 管理客户端，复用 base 的共享后端，不通过公开返回值暴露后端类型
type RateLimitsClient struct {
	// base 共享的管理调用能力，包含后端和 RPC 输入编码器
	base
}

// CELClient 管理客户端，复用 base 的共享后端，不通过公开返回值暴露后端类型
type CELClient struct {
	// base 共享的管理调用能力，包含后端和 RPC 输入编码器
	base
}

// TenantClient 管理客户端，复用 base 的共享后端，不通过公开返回值暴露后端类型
type TenantClient struct {
	// base 共享的管理调用能力，包含后端和 RPC 输入编码器
	base
}

// Upsert 使用默认 context 创建或更新限流资源；需要明确取消预算时使用 UpsertContext
func (c *RateLimitsClient) Upsert(opts model.CreateRatelimitOpts) error {
	return c.UpsertContext(context.Background(), opts)
}

// UpsertContext 创建或更新限流资源，通过共享后端调用 RateLimits.Upsert；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *RateLimitsClient) UpsertContext(ctx context.Context, opts model.CreateRatelimitOpts) error {
	return c.backend.Feature(ctx, ports.RateLimitsUpsert{Options: opts}, nil)
}

// Debug 调试CEL 表达式，通过共享后端调用 CEL.Debug；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *CELClient) Debug(ctx context.Context, expression string, input map[string]any, metadata, filterPayload *map[string]any) (model.Resource, error) {
	return c.invoke(ctx, ports.CELDebug{Expression: expression, Input: input, Metadata: metadata, FilterPayload: filterPayload})
}

// Get 读取授权范围，通过共享后端调用 Tenant.Get；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *TenantClient) Get(ctx context.Context) (model.Resource, error) {
	return c.invoke(ctx, ports.TenantGet{})
}

// Get 读取事件过滤器，通过共享后端调用 Filters.Get；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *FiltersClient) Get(ctx context.Context, id string) (model.Resource, error) {
	return c.invoke(ctx, ports.FiltersGet{ID: id})
}

// List 查询列表事件过滤器，通过共享后端调用 Filters.List；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *FiltersClient) List(ctx context.Context, query model.Query) (model.Resource, error) {
	return c.invoke(ctx, ports.FiltersList{Query: query})
}

// Create 创建事件过滤器，通过共享后端调用 Filters.Create；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *FiltersClient) Create(ctx context.Context, request model.Resource) (model.Resource, error) {
	return c.invoke(ctx, ports.FiltersCreate{Request: request})
}

// Update 更新事件过滤器，通过共享后端调用 Filters.Update；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *FiltersClient) Update(ctx context.Context, id string, request model.Resource) (model.Resource, error) {
	return c.invoke(ctx, ports.FiltersUpdate{ID: id, Request: request})
}

// Get 读取Worker，通过共享后端调用 Workers.Get；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *WorkersClient) Get(ctx context.Context, id string) (model.Resource, error) {
	return c.invoke(ctx, ports.WorkersGet{ID: id})
}

// List 查询列表Worker，通过共享后端调用 Workers.List；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *WorkersClient) List(ctx context.Context) (model.Resource, error) {
	return c.invoke(ctx, ports.WorkersList{})
}

// Pause 暂停Worker，通过共享后端调用 Workers.Pause；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *WorkersClient) Pause(ctx context.Context, id string) (model.Resource, error) {
	return c.invoke(ctx, ports.WorkersPause{ID: id})
}

// Unpause 恢复分配Worker，通过共享后端调用 Workers.Unpause；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *WorkersClient) Unpause(ctx context.Context, id string) (model.Resource, error) {
	return c.invoke(ctx, ports.WorkersUnpause{ID: id})
}

// Get 读取工作流，通过共享后端调用 Workflows.Get；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *WorkflowsClient) Get(ctx context.Context, name string) (model.Resource, error) {
	return c.invoke(ctx, ports.WorkflowsGet{ID: name})
}

// List 查询列表工作流，通过共享后端调用 Workflows.List；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *WorkflowsClient) List(ctx context.Context, query model.Query) (model.Resource, error) {
	return c.invoke(ctx, ports.WorkflowsList{Query: query})
}

// Delete 删除工作流，通过共享后端调用 Workflows.Delete；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *WorkflowsClient) Delete(ctx context.Context, name string) (model.Resource, error) {
	return c.invoke(ctx, ports.WorkflowsDelete{ID: name})
}

// List 查询列表日志，通过共享后端调用 Logs.List；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *LogsClient) List(ctx context.Context, id string, query model.Query) (model.Resource, error) {
	return c.invoke(ctx, ports.LogsList{ID: id, Query: query})
}

// Get 读取Webhook，通过共享后端调用 Webhooks.Get；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *WebhooksClient) Get(ctx context.Context, name string) (model.Resource, error) {
	return c.invoke(ctx, ports.WebhooksGet{ID: name})
}

// List 查询列表Webhook，通过共享后端调用 Webhooks.List；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *WebhooksClient) List(ctx context.Context, query model.Query) (model.Resource, error) {
	return c.invoke(ctx, ports.WebhooksList{Query: query})
}

// Create 创建Webhook，通过共享后端调用 Webhooks.Create；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *WebhooksClient) Create(ctx context.Context, request model.CreateWebhookOpts) (model.Resource, error) {
	return c.invoke(ctx, ports.WebhooksCreate{Request: request})
}

// Update 更新Webhook，通过共享后端调用 Webhooks.Update；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *WebhooksClient) Update(ctx context.Context, name string, request model.Resource) (model.Resource, error) {
	return c.invoke(ctx, ports.WebhooksUpdate{ID: name, Request: request})
}

// GetWorkflowMetrics 读取工作流指标指标，通过共享后端调用 Metrics.GetWorkflowMetrics；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *MetricsClient) GetWorkflowMetrics(ctx context.Context, name string, query model.Query) (model.Resource, error) {
	return c.invoke(ctx, ports.MetricsGetWorkflowMetrics{Name: name, Query: query})
}

// GetQueueMetrics 读取队列指标指标，通过共享后端调用 Metrics.GetQueueMetrics；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *MetricsClient) GetQueueMetrics(ctx context.Context, query model.Query) (model.Resource, error) {
	return c.invoke(ctx, ports.MetricsGetQueueMetrics{Query: query})
}

// GetTaskQueueMetrics 读取任务队列指标指标，通过共享后端调用 Metrics.GetTaskQueueMetrics；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *MetricsClient) GetTaskQueueMetrics(ctx context.Context) (model.Resource, error) {
	return c.invoke(ctx, ports.MetricsGetTaskQueueMetrics{})
}

// List 查询列表限流资源，通过共享后端调用 RateLimits.List；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *RateLimitsClient) List(ctx context.Context, query model.Query) (model.Resource, error) {
	return c.invoke(ctx, ports.RateLimitsList{Query: query})
}

// Delete 删除事件过滤器，通过共享后端调用 Filters.Delete；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *FiltersClient) Delete(ctx context.Context, id string) error {
	return c.backend.Feature(ctx, ports.FiltersDelete{ID: id}, nil)
}

// Delete 删除Webhook，通过共享后端调用 Webhooks.Delete；输入和返回结果只使用 wego 或普通 JSON 类型
func (c *WebhooksClient) Delete(ctx context.Context, id string) error {
	return c.backend.Feature(ctx, ports.WebhooksDelete{ID: id}, nil)
}

// SendEvent 向 Worker 实例 key 定向发布，成功仅表示消息已持久存储
func (c *WorkersClient) SendEvent(ctx context.Context, workerKey string, event model.WorkerEvent) error {
	if workerKey == "" {
		return status.Error(codes.InvalidArgument, "wego: directed event requires worker key")
	}
	_, err := c.invoke(ctx, ports.WorkerSendEvent{WorkerKey: workerKey, Event: event})
	return err
}

// BroadcastEvent 向同租户、namespace 内当前在线 Worker 广播，新加入实例跳过旧广播
func (c *WorkersClient) BroadcastEvent(ctx context.Context, event model.WorkerEvent) error {
	_, err := c.invoke(ctx, ports.WorkerSendEvent{Event: event})
	return err
}
