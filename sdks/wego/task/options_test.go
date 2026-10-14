package task

import (
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestIdempotencyOptionSnapshot 验证配置是独立值、选项可复用，最后一次配置完整覆盖前一次。
func TestIdempotencyOptionSnapshot(t *testing.T) {
	value := model.Idempotency{Expression: "input.routing.id", Method: model.IdempotencyMethodStatus, TTL: time.Minute}
	configure := WithIdempotency(value)
	// 调用方修改原结构体，不影响已经构造的选项。
	value.Expression, value.TTL = "input.routing.changed", time.Hour
	first, second := spec.Task{}, spec.Task{}
	configure(&first)
	configure(&second)
	first.Idempotency.Value.TTL = time.Second
	if !second.Idempotency.Set || second.Idempotency.Value.Expression != "input.routing.id" || second.Idempotency.Value.TTL != time.Minute {
		t.Fatal("reused option aliases caller or another task", second.Idempotency)
	}
	// 缺省 Method 使用 TTL，不能继承前一份配置的 STATUS 或一分钟期限。
	next := model.Idempotency{Expression: "input.routing.operation_id", TTL: 2 * time.Minute}
	WithIdempotency(next)(&second)
	if second.Idempotency.Value != next || second.Validate() != nil {
		t.Fatal("option did not replace complete policy", second.Idempotency)
	}
}

// TestIdempotencyPolicyMerge 验证方法继承完整默认策略，显式覆盖不拼接字段，显式空配置报错。
func TestIdempotencyPolicyMerge(t *testing.T) {
	base := spec.Task{}
	WithIdempotency(model.Idempotency{Expression: "input.routing.id", Method: model.IdempotencyMethodStatus, TTL: time.Minute})(&base)
	inherited := spec.Merge(base, spec.Task{})
	if inherited.Idempotency != base.Idempotency {
		t.Fatal("unset override lost inherited policy", inherited)
	}
	override := spec.Task{}
	WithIdempotency(model.Idempotency{Expression: "input.routing.operation_id", TTL: time.Hour})(&override)
	merged := spec.Merge(base, override)
	if merged.Idempotency != override.Idempotency || base.Idempotency.Value.Method != model.IdempotencyMethodStatus {
		t.Fatal("merge mixed or mutated policies", merged, base)
	}
	// 空配置不能被当作没有覆盖，否则会静默沿用错误的默认去重键。
	WithIdempotency(model.Idempotency{})(&override)
	invalid := spec.Merge(base, override)
	if !invalid.Idempotency.Set || invalid.Validate() == nil {
		t.Fatal("explicit empty policy was ignored", invalid)
	}
}
