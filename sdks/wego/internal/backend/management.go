package backend

import (
	"context"
	"fmt"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

// Feature 接受封闭的自有命令，在后端内部映射到官方管理能力
// 位置参数只用于此包的官方 DTO 适配，不跨越模块边界
func (b *Backend) Feature(ctx context.Context, request ports.FeatureRequest, out any) error {
	// command 的具体类型决定操作与参数，不通过调用方字符串选择方法
	switch command := request.(type) {
	case ports.WorkerSendEvent:
		return b.publishWorkerEvent(ctx, command)
	case ports.WorkerCancelNotice:
		return b.publishWorkerCancel(ctx, command)
	case ports.CELDebug:
		return b.callFeature(ctx, "CEL.Debug", []any{command.Expression, command.Input, command.Metadata, command.FilterPayload}, out)
	case ports.CronsCreate:
		return b.callFeature(ctx, "Crons.Create", []any{command.Name, command.Trigger}, out)
	case ports.CronsDelete:
		return b.callFeature(ctx, "Crons.Delete", []any{command.ID}, out)
	case ports.CronsGet:
		return b.callFeature(ctx, "Crons.Get", []any{command.ID}, out)
	case ports.CronsList:
		return b.callFeature(ctx, "Crons.List", []any{command.Query}, out)
	case ports.EventsPush:
		return b.callFeature(ctx, "Events.Push", []any{command.Key, command.Input, command.Scope}, out)
	case ports.FiltersCreate:
		return b.callFeature(ctx, "Filters.Create", []any{command.Request}, out)
	case ports.FiltersDelete:
		return b.callFeature(ctx, "Filters.Delete", []any{command.ID}, out)
	case ports.FiltersGet:
		return b.callFeature(ctx, "Filters.Get", []any{command.ID}, out)
	case ports.FiltersList:
		return b.callFeature(ctx, "Filters.List", []any{command.Query}, out)
	case ports.FiltersUpdate:
		return b.callFeature(ctx, "Filters.Update", []any{command.ID, command.Request}, out)
	case ports.LogsList:
		return b.callFeature(ctx, "Logs.List", []any{command.ID, command.Query}, out)
	case ports.MetricsGetQueueMetrics:
		return b.callFeature(ctx, "Metrics.GetQueueMetrics", []any{command.Query}, out)
	case ports.MetricsGetTaskQueueMetrics:
		return b.callFeature(ctx, "Metrics.GetTaskQueueMetrics", nil, out)
	case ports.MetricsGetWorkflowMetrics:
		return b.callFeature(ctx, "Metrics.GetWorkflowMetrics", []any{command.Name, command.Query}, out)
	case ports.RateLimitsList:
		return b.callFeature(ctx, "RateLimits.List", []any{command.Query}, out)
	case ports.RateLimitsUpsert:
		return b.callFeature(ctx, "RateLimits.Upsert", []any{command.Options}, out)
	case ports.RunsCancel:
		return b.callFeature(ctx, "Runs.Cancel", []any{command.Request}, out)
	case ports.RunsGet:
		return b.callFeature(ctx, "Runs.Get", []any{command.ID}, out)
	case ports.RunsGetDetails:
		return b.callFeature(ctx, "Runs.GetDetails", []any{command.ID}, out)
	case ports.RunsGetStatus:
		return b.callFeature(ctx, "Runs.GetStatus", []any{command.ID}, out)
	case ports.RunsList:
		return b.callFeature(ctx, "Runs.List", []any{command.Query}, out)
	case ports.RunsReplay:
		return b.callFeature(ctx, "Runs.Replay", []any{command.Request}, out)
	case ports.RunsRestore:
		return b.callFeature(ctx, "Runs.Restore", []any{command.ID}, out)
	case ports.RuntimeInfo:
		return b.callFeature(ctx, "Runtime.Info", nil, out)
	case ports.SchedulesCreate:
		return b.callFeature(ctx, "Schedules.Create", []any{command.Name, command.Trigger}, out)
	case ports.SchedulesDelete:
		return b.callFeature(ctx, "Schedules.Delete", []any{command.ID}, out)
	case ports.SchedulesGet:
		return b.callFeature(ctx, "Schedules.Get", []any{command.ID}, out)
	case ports.SchedulesList:
		return b.callFeature(ctx, "Schedules.List", []any{command.Query}, out)
	case ports.TenantGet:
		return b.callFeature(ctx, "Tenant.Get", nil, out)
	case ports.WebhooksCreate:
		return b.callFeature(ctx, "Webhooks.Create", []any{command.Request}, out)
	case ports.WebhooksDelete:
		return b.callFeature(ctx, "Webhooks.Delete", []any{command.ID}, out)
	case ports.WebhooksGet:
		return b.callFeature(ctx, "Webhooks.Get", []any{command.ID}, out)
	case ports.WebhooksList:
		return b.callFeature(ctx, "Webhooks.List", []any{command.Query}, out)
	case ports.WebhooksUpdate:
		return b.callFeature(ctx, "Webhooks.Update", []any{command.ID, command.Request}, out)
	case ports.WorkersGet:
		return b.callFeature(ctx, "Workers.Get", []any{command.ID}, out)
	case ports.WorkersList:
		return b.callFeature(ctx, "Workers.List", nil, out)
	case ports.WorkersPause:
		return b.callFeature(ctx, "Workers.Pause", []any{command.ID}, out)
	case ports.WorkersUnpause:
		return b.callFeature(ctx, "Workers.Unpause", []any{command.ID}, out)
	case ports.WorkflowsDelete:
		return b.callFeature(ctx, "Workflows.Delete", []any{command.ID}, out)
	case ports.WorkflowsGet:
		return b.callFeature(ctx, "Workflows.Get", []any{command.ID}, out)
	case ports.WorkflowsList:
		return b.callFeature(ctx, "Workflows.List", []any{command.Query}, out)
	default:
		return fmt.Errorf("wego: unsupported management request %T", request)
	}
}
