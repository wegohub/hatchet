package backend

import (
	"testing"
	"time"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/option"
)

// TestIdempotencyProtocolMapping 验证不启用、缺省 TTL、显式 TTL 与 STATUS 的官方字段映射。
func TestIdempotencyProtocolMapping(t *testing.T) {
	request, err := workflowDefinition("fixture", spec.Task{}, "")
	if err != nil || request.Idempotency != nil {
		t.Fatal("unset idempotency must remain absent", request, err)
	}
	for _, method := range []model.IdempotencyMethod{"", model.IdempotencyMethodTTL, model.IdempotencyMethodStatus} {
		t.Run("method="+string(method), func(t *testing.T) {
			// value 使用毫秒边界，避免转换后的 TTL 与显式配置不一致。
			value := model.Idempotency{Expression: "input.routing.operation_id", Method: method, TTL: 1500 * time.Millisecond}
			policy := spec.Task{Idempotency: option.Some(value)}
			request, err := workflowDefinition("fixture", policy, "")
			if err != nil {
				t.Fatal(err)
			}
			want := v1.IdempotencyMethod_TTL
			if method == model.IdempotencyMethodStatus {
				want = v1.IdempotencyMethod_STATUS
			}
			actual := request.Idempotency
			if actual == nil || actual.Expression != value.Expression || actual.TtlMs != 1500 || actual.Method == nil || *actual.Method != want {
				t.Fatal("idempotency mapping changed", actual)
			}
			// 后续修改策略快照不改变已准备的协议请求。
			policy.Idempotency.Value.Expression = "input.routing.changed"
			policy.Idempotency.Value.Method = model.IdempotencyMethodStatus
			policy.Idempotency.Value.TTL = time.Hour
			if actual.Expression != value.Expression || actual.TtlMs != 1500 || *actual.Method != want {
				t.Fatal("protocol aliases mutable configuration", actual)
			}
		})
	}
}

// TestIdempotencyValidation 在注册前拒绝无效配置，包括转换为毫秒后会变成 0 的时长。
func TestIdempotencyValidation(t *testing.T) {
	for name, value := range map[string]model.Idempotency{
		"empty":          {},
		"missing_key":    {TTL: time.Second},
		"whitespace_key": {Expression: " \t\n", TTL: time.Second},
		"unknown_method": {Expression: "input.routing.id", Method: "invalid", TTL: time.Second},
		"zero_ttl":       {Expression: "input.routing.id"},
		"negative_ttl":   {Expression: "input.routing.id", TTL: -time.Second},
		"submillisecond": {Expression: "input.routing.id", TTL: 500 * time.Microsecond},
	} {
		t.Run(name, func(t *testing.T) {
			policy := spec.Task{Idempotency: option.Some(value)}
			if policy.Validate() == nil {
				t.Fatal("invalid idempotency accepted", value)
			}
			if _, err := workflowDefinition("fixture", policy, ""); err == nil {
				t.Fatal("invalid idempotency reached workflow request", value)
			}
		})
	}
	// 1 毫秒是服务端允许的最小值，两种模式必须一致接受。
	for _, method := range []model.IdempotencyMethod{model.IdempotencyMethodTTL, model.IdempotencyMethodStatus} {
		policy := spec.Task{Idempotency: option.Some(model.Idempotency{Expression: "input.routing.id", Method: method, TTL: time.Millisecond})}
		request, err := workflowDefinition("fixture", policy, "")
		if err != nil || request.Idempotency.TtlMs != 1 {
			t.Fatal("minimum TTL rejected", request, err)
		}
	}
}
