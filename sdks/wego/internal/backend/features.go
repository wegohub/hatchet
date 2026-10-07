package backend

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"strings"

	adminpb "github.com/hatchet-dev/hatchet/internal/services/admin/contracts"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	clientconfig "github.com/hatchet-dev/hatchet/pkg/config/client"
	sdk "github.com/hatchet-dev/hatchet/sdks/go"
	sdkfeatures "github.com/hatchet-dev/hatchet/sdks/go/features"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// callFeature 适配本包生成的管理操作，显式转换官方 DTO，复制动态结果并规范化错误。
func (b *Backend) callFeature(ctx context.Context, operation string, args []any, out any) (err error) {
	defer func() {
		// recovered 捕获当前执行的 panic，边界应返回可诊断错误并继续资源清理。
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("wego: %s argument conversion: %v", operation, recovered)
		}
	}()

	// parts 解析结构化名称或参数，后续分派只接受明确注册的操作。
	parts := strings.Split(operation, ".")
	if len(parts) != 2 {
		return fmt.Errorf("wego: invalid feature operation")
	}
	if operation == "Runtime.Info" {
		// version 是实际引擎版本，预检据此验证 durable 驱逐等必需能力。
		version, e := b.raw.Dispatcher().GetVersion(ctx)
		if e != nil {
			return Normalize(e)
		}

		// supports, e 根据服务版本确认驱逐能力，必需能力缺失时直接验收失败。
		supports, e := sdk.SupportsDurableEviction(version)
		if e != nil {
			return Normalize(e)
		}

		return convert(model.InstanceInfo{
			Version:         version,
			TenantID:        b.config.TenantID,
			APIURL:          b.config.ServerURL,
			GRPCAddress:     b.config.Address,
			TLS:             b.config.TLS != nil,
			DurableEviction: supports,
		}, out)
	}
	// 检查 operation == "RateLimits.Upsert"；不满足协议或配置约束时返回 Unimplemented（wego: cancellable rate limit administration unavailable）。
	if operation == "RateLimits.Upsert" {
		// options, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值。
		options, ok := args[0].(model.CreateRatelimitOpts)
		// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值。
		if !ok {
			return fmt.Errorf("wego: rate limit options required")
		}

		// duration, ok 取得 adminpb.RateLimitDuration_value[strings.ToUpper 的结果，确认成功后才进入下一处理阶段。
		duration, ok := adminpb.RateLimitDuration_value[strings.ToUpper(string(options.Duration))]
		if !ok {
			return fmt.Errorf("wego: invalid rate limit duration")
		}
		// value 当前协议字段的独立值，指针不能借用调用方可变容器。
		value := adminpb.RateLimitDuration(duration)
		// _, err 取得 b.admin.PutRateLimit 的结果，确认成功后才进入下一处理阶段。
		_, err := b.admin.PutRateLimit(b.auth(ctx), &adminpb.PutRateLimitRequest{Key: options.Key, Limit: int32(options.Limit), Duration: value})
		return Normalize(err)
	}
	if operation == "Crons.Delete" || operation == "Schedules.Delete" || operation == "Webhooks.Delete" {
		// name 取得 args[0]. 的结果，确认成功后才进入下一处理阶段。
		name := args[0].(string)
		// tenant 当前实例的授权身份，只限定管理查询范围，不写入测试名称。
		tenant := uuid.MustParse(b.config.TenantID)
		// code 真实 HTTP 状态，200～299 均代表成功删除。
		var code int
		switch operation {
		case "Webhooks.Delete":
			// response 协议或 REST 响应，先检查错误和存在性再读取内容。
			response, e := b.raw.API().V1WebhookDeleteWithResponse(ctx, tenant, name)
			if e != nil {
				return Normalize(e)
			}
			code = response.StatusCode()
		case "Crons.Delete":
			// id 真实运行或成员身份，例如 run-1，不能使用显示名称代替。
			id, e := uuid.Parse(name)
			if e != nil {
				return e
			}
			// response 协议或 REST 响应，先检查错误和存在性再读取内容。
			response, e := b.raw.API().WorkflowCronDeleteWithResponse(ctx, tenant, id)
			if e != nil {
				return Normalize(e)
			}
			code = response.StatusCode()
		case "Schedules.Delete":
			// id 真实运行或成员身份，例如 run-1，不能使用显示名称代替。
			id, e := uuid.Parse(name)
			if e != nil {
				return e
			}
			// response 协议或 REST 响应，先检查错误和存在性再读取内容。
			response, e := b.raw.API().WorkflowScheduledDeleteWithResponse(ctx, tenant, id)
			if e != nil {
				return Normalize(e)
			}
			code = response.StatusCode()
		}
		if code < 200 || code >= 300 {
			return fmt.Errorf("wego: deletion HTTP status %d", code)
		}
		return nil
	}
	if operation == "Webhooks.Create" {
		// v, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值。
		v, ok := args[0].(model.CreateWebhookOpts)
		// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值。
		if !ok {
			return fmt.Errorf("wego: webhook options required")
		}

		// opts 是 backend 私有 webhook DTO，自有认证配置随后显式转换。
		opts := sdkfeatures.CreateWebhookOpts{
			Name:                         v.Name,
			SourceName:                   rest.V1WebhookSourceName(v.SourceName),
			EventKeyExpression:           v.EventKeyExpression,
			ScopeExpression:              v.ScopeExpression,
			StaticPayload:                v.StaticPayload,
			ReturnEventAsResponsePayload: v.ReturnEventAsResponsePayload,
		}
		// auth 保存类型分派后的准确值，各分支只按该类型读取字段或调用方法。
		switch auth := v.Auth.(type) {
		case model.BasicAuth:
			opts.Auth = sdkfeatures.BasicAuth{Username: auth.Username, Password: auth.Password}
		case model.APIKeyAuth:
			opts.Auth = sdkfeatures.APIKeyAuth{HeaderName: auth.HeaderName, APIKey: auth.APIKey}
		case model.HMACAuth:
			opts.Auth = sdkfeatures.HMACAuth{
				SigningSecret:       auth.SigningSecret,
				SignatureHeaderName: auth.SignatureHeaderName,
				Algorithm:           rest.V1WebhookHMACAlgorithm(auth.Algorithm),
				Encoding:            rest.V1WebhookHMACEncoding(auth.Encoding),
			}
		case model.SvixAuth:
			opts.Auth = sdkfeatures.SvixAuth{SigningSecret: auth.SigningSecret}
		default:
			return fmt.Errorf("wego: webhook authentication is required")
		}
		// create 的绑定在锁内完成，网络调用不阻塞其他管理客户端初始化。
		b.featureMu.Lock()
		// create 取得本实例 webhook 管理入口，惰性初始化由 featureMu 保护。
		create := b.features.Webhooks().Create
		b.featureMu.Unlock()
		// result 来自明确绑定的 webhook 创建 API，失败时不返回未注册的资源。
		result, e := create(ctx, opts)
		if e != nil {
			return Normalize(e)
		}
		if out != nil {
			return convert(result, out)
		}

		return nil
	}
	if operation == "Crons.Create" || operation == "Workflows.Get" || operation == "Workflows.Delete" || operation == "Metrics.GetWorkflowMetrics" {
		args = append([]any(nil), args...)
		args[0] = clientconfig.ApplyNamespace(args[0].(string), &b.config.Namespace)
	}
	if parts[0] == "Events" && parts[1] == "Push" {
		// options 明确列出本段注册、传输或清理选项，应用顺序决定重复字段的覆盖关系。
		options := []v0.PushOpFunc{v0.WithEventMetadata(traceMetadata(ctx, nil))}
		if len(args) > 2 {
			// scope 可选事件隔离范围，nil 表示未额外设置。
			var scope *string
			_ = convert(args[2], &scope)
			options = append(options, v0.WithFilterScope(scope))
		}
		return Normalize(b.raw.Event().Push(ctx, args[0].(string), args[1], options...))
	}

	// call 在锁内取得惰性初始化的强类型方法，网络请求在锁外执行。
	b.featureMu.Lock()
	// call 绑定编译期校验的 SDK 方法，不用反射猜测参数类型。
	call := b.featureCall(operation)
	b.featureMu.Unlock()
	if call == nil {
		return status.Error(codes.Unimplemented, "wego: unknown operation "+operation)
	}
	// result 是后端内部类型；通过结果投影复制为 wego 自有资源。
	result, err := call(ctx, args)
	if err != nil {
		return Normalize(err)
	}
	if out != nil {
		if operation == "Workflows.Delete" {
			return convert(map[string]any{"deleted": true}, out)
		}
		if result != nil {
			return convert(result, out)
		}
	}
	return nil
}
