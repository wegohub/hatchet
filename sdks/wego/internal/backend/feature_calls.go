package backend

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	sdkfeatures "github.com/hatchet-dev/hatchet/sdks/go/features"
)

// featureCall 是已经绑定强类型 SDK 方法的私有调用，不通过反射寻找名称或猜测签名
type featureCall func(context.Context, []any) (any, error)

// featureCall 在编译期校验每个方法与参数类型，未知操作返回 nil
func (b *Backend) featureCall(operation string) featureCall {
	switch operation {
	case "Runs.Get":
		return feature1[string](b.features.Runs().Get)
	case "Runs.GetStatus":
		return feature1[string](b.features.Runs().GetStatus)
	case "Runs.GetDetails":
		return feature1[uuid.UUID](b.features.Runs().GetDetails)
	case "Runs.List":
		return feature1[rest.V1WorkflowRunListParams](b.features.Runs().List)
	case "Runs.Replay":
		return feature1[rest.V1ReplayTaskRequest](b.features.Runs().Replay)
	case "Runs.Cancel":
		return feature1[rest.V1CancelTaskRequest](b.features.Runs().Cancel)
	case "Runs.Restore":
		return feature1[string](b.features.Runs().Restore)
	case "Crons.Create":
		return feature2[string, sdkfeatures.CreateCronTrigger](b.features.Crons().Create)
	case "Crons.Get":
		return feature1[string](b.features.Crons().Get)
	case "Crons.List":
		return feature1[rest.CronWorkflowListParams](b.features.Crons().List)
	case "Crons.Delete":
		return featureError1[string](b.features.Crons().Delete)
	case "Schedules.Create":
		return feature2[string, sdkfeatures.CreateScheduledRunTrigger](b.features.Schedules().Create)
	case "Schedules.Get":
		return feature1[string](b.features.Schedules().Get)
	case "Schedules.List":
		return feature1[rest.WorkflowScheduledListParams](b.features.Schedules().List)
	case "Schedules.Delete":
		return featureError1[string](b.features.Schedules().Delete)
	case "Workflows.Get":
		return feature1[string](b.features.Workflows().Get)
	case "Workflows.List":
		return feature1[*rest.WorkflowListParams](b.features.Workflows().List)
	case "Workflows.Delete":
		return feature1[string](b.features.Workflows().Delete)
	case "Workers.Get":
		return feature1[string](b.features.Workers().Get)
	case "Workers.List":
		return feature0(b.features.Workers().List)
	case "Workers.Pause":
		return feature1[string](b.features.Workers().Pause)
	case "Workers.Unpause":
		return feature1[string](b.features.Workers().Unpause)
	case "Filters.Get":
		return feature1[string](b.features.Filters().Get)
	case "Filters.List":
		return feature1[*rest.V1FilterListParams](b.features.Filters().List)
	case "Filters.Create":
		return feature1[rest.V1CreateFilterRequest](b.features.Filters().Create)
	case "Filters.Update":
		return feature2[string, rest.V1FilterUpdateJSONRequestBody](b.features.Filters().Update)
	case "Filters.Delete":
		return feature1[string](b.features.Filters().Delete)
	case "Webhooks.Get":
		return feature1[string](b.features.Webhooks().Get)
	case "Webhooks.List":
		return feature1[rest.V1WebhookListParams](b.features.Webhooks().List)
	case "Webhooks.Update":
		return feature2[string, sdkfeatures.UpdateWebhookOpts](b.features.Webhooks().Update)
	case "Webhooks.Delete":
		return featureError1[string](b.features.Webhooks().Delete)
	case "Metrics.GetWorkflowMetrics":
		return feature2[string, *rest.WorkflowGetMetricsParams](b.features.Metrics().GetWorkflowMetrics)
	case "Metrics.GetQueueMetrics":
		return feature1[*rest.TenantGetQueueMetricsParams](b.features.Metrics().GetQueueMetrics)
	case "Metrics.GetTaskQueueMetrics":
		return feature0(b.features.Metrics().GetTaskQueueMetrics)
	case "Logs.List":
		return feature2[uuid.UUID, *rest.V1LogLineListParams](b.features.Logs().List)
	case "RateLimits.List":
		return feature1[*rest.RateLimitListParams](b.features.RateLimits().List)
	case "CEL.Debug":
		return feature4[string, map[string]any, *map[string]any, *map[string]any](b.features.CEL().Debug)
	case "Tenant.Get":
		return feature0(sdkfeatures.NewTenantCliet(b.raw.API(), b.config.TenantID).Get)
	}
	return nil
}

// feature0 转换 0 个 REST 参数并调用明确类型的方法；转换失败时不发送请求
func feature0[R any](fn func(context.Context) (R, error)) featureCall {
	return func(ctx context.Context, args []any) (any, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("wego: expected 0 feature arguments")
		}
		return fn(ctx)
	}
}

// feature1 转换 1 个 REST 参数并调用明确类型的方法；转换失败时不发送请求
func feature1[A any, R any](fn func(context.Context, A) (R, error)) featureCall {
	return func(ctx context.Context, args []any) (any, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("wego: expected 1 feature arguments")
		}
		// v0 是第 1 个明确类型参数，不影响其他参数或调用方输入
		var v0 A
		// err 在实际 API 调用前报告投影失败
		if err := convert(args[0], &v0); err != nil {
			return nil, err
		}
		return fn(ctx, v0)
	}
}

// feature2 转换 2 个 REST 参数并调用明确类型的方法；转换失败时不发送请求
func feature2[A any, B any, R any](fn func(context.Context, A, B) (R, error)) featureCall {
	return func(ctx context.Context, args []any) (any, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("wego: expected 2 feature arguments")
		}
		// v0 是第 1 个明确类型参数，不影响其他参数或调用方输入
		var v0 A
		// err 在实际 API 调用前报告投影失败
		if err := convert(args[0], &v0); err != nil {
			return nil, err
		}
		// v1 是第 2 个明确类型参数，不影响其他参数或调用方输入
		var v1 B
		// err 在实际 API 调用前报告投影失败
		if err := convert(args[1], &v1); err != nil {
			return nil, err
		}
		return fn(ctx, v0, v1)
	}
}

// feature4 转换 4 个 REST 参数并调用明确类型的方法；转换失败时不发送请求
func feature4[A any, B any, C any, D any, R any](fn func(context.Context, A, B, C, D) (R, error)) featureCall {
	return func(ctx context.Context, args []any) (any, error) {
		if len(args) != 4 {
			return nil, fmt.Errorf("wego: expected 4 feature arguments")
		}
		// v0 是第 1 个明确类型参数，不影响其他参数或调用方输入
		var v0 A
		// err 在实际 API 调用前报告投影失败
		if err := convert(args[0], &v0); err != nil {
			return nil, err
		}
		// v1 是第 2 个明确类型参数，不影响其他参数或调用方输入
		var v1 B
		// err 在实际 API 调用前报告投影失败
		if err := convert(args[1], &v1); err != nil {
			return nil, err
		}
		// v2 是第 3 个明确类型参数，不影响其他参数或调用方输入
		var v2 C
		// err 在实际 API 调用前报告投影失败
		if err := convert(args[2], &v2); err != nil {
			return nil, err
		}
		// v3 是第 4 个明确类型参数，不影响其他参数或调用方输入
		var v3 D
		// err 在实际 API 调用前报告投影失败
		if err := convert(args[3], &v3); err != nil {
			return nil, err
		}
		return fn(ctx, v0, v1, v2, v3)
	}
}

// featureError1 适配只返回 error 的删除方法，保持与有响应方法相同的参数校验
func featureError1[A any](fn func(context.Context, A) error) featureCall {
	return feature1(func(ctx context.Context, value A) (any, error) { return nil, fn(ctx, value) })
}
