package features

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// triggerBackend 记录触发器创建、事件发布与目标查询；其他后端能力不得被本场景调用。
type triggerBackend struct {
	// Backend 嵌入未使用的能力，避免为无关执行路径编写空实现。
	ports.Backend
	// calls 保存实际提交次数，非法配置必须为零。
	calls int
	// validations 保存目标查询次数，非法优先级也不能触发远程查询。
	validations int
	// priority 保存触发命令中的原始值。
	priority int32
	// scope 保留后端协议的字段存在性；公开调用者只传字符串值。
	scope *string
}

// ValidateTarget 记录实际绑定查询；本测试使用已注册的结构化任务名。
func (b *triggerBackend) ValidateTarget(context.Context, string, string, string, ...string) error {
	b.validations++
	return nil
}

// Feature 捕获触发器或事件命令；事件没有返回资源，创建命令返回普通 JSON 资源。
func (b *triggerBackend) Feature(_ context.Context, request ports.FeatureRequest, out any) error {
	b.calls++
	switch command := request.(type) {
	case ports.CronsCreate:
		b.priority = command.Trigger.Priority
	case ports.SchedulesCreate:
		b.priority = command.Trigger.Priority
	case ports.EventsPush:
		b.scope = command.Scope
		return nil
	default:
		return fmt.Errorf("unexpected trigger command %T", request)
	}
	*(out.(*model.Resource)) = model.Resource{"id": "fixture"}
	return nil
}

// TestEventScopeValue 省略、空值和具体范围保持不同的后端字段存在性，并隔离调用方切片。
func TestEventScopeValue(t *testing.T) {
	for _, scopes := range [][]string{nil, {""}, {"filter-scope"}} {
		backend := &triggerBackend{}
		client := New(backend, spec.Defaults(), nil).Events
		if err := client.Push(context.Background(), "created", map[string]any{}, scopes...); err != nil {
			t.Fatal(err)
		}
		if len(scopes) == 0 {
			if backend.scope != nil {
				t.Fatal("omitted scope was sent")
			}
			continue
		}
		want := scopes[0]
		scopes[0] = "changed"
		if backend.scope == nil || *backend.scope != want {
			t.Fatal("scope lost presence or shares caller storage", backend.scope)
		}
	}
}

// TestTriggerPriority 校验全部合法值与 int32 边界；非法值不得查询绑定、执行 codec 或提交。
func TestTriggerPriority(t *testing.T) {
	for _, kind := range []string{"cron", "schedule"} {
		for _, priority := range []int32{math.MinInt32, -1, 0, 1, 2, 3, 4, math.MaxInt32} {
			t.Run(fmt.Sprintf("%s/%d", kind, priority), func(t *testing.T) {
				backend := &triggerBackend{}
				encoded := 0
				base := base{backend: backend, encode: func(_ context.Context, name string, input any) (string, any, error) {
					encoded++
					return name, input, nil
				}}
				// resource 保存成功创建结果；参数非法时必须为空。
				var resource model.Resource
				// err 区分参数拒绝和正常创建，不能仅凭后端调用次数判断成功。
				var err error
				if kind == "cron" {
					resource, err = (&CronsClient{base}).Create(context.Background(), "fixture", model.CreateCronTrigger{Priority: priority})
				} else {
					resource, err = (&SchedulesClient{base}).Create(context.Background(), "fixture", model.CreateScheduledRunTrigger{Priority: priority})
				}
				if priority < 0 || priority > 3 {
					if err == nil || resource != nil || backend.calls != 0 || backend.validations != 0 || encoded != 0 {
						t.Fatal("invalid priority reached external work", err, backend, encoded)
					}
					return
				}
				if err != nil || resource.ID() != "fixture" || backend.calls != 1 || backend.validations != 1 || encoded != 1 || backend.priority != priority {
					t.Fatal("valid priority changed or was rejected", err, backend, encoded)
				}
			})
		}
	}
}

// TestTriggerPriorityJSONPresence 验证普通 JSON 的 omitempty：0 不输出 priority，1–3 按原值输出。
func TestTriggerPriorityJSONPresence(t *testing.T) {
	for priority := int32(0); priority <= 3; priority++ {
		for _, trigger := range []any{
			model.CreateCronTrigger{Priority: priority},
			model.CreateScheduledRunTrigger{Priority: priority, TriggerAt: time.Unix(0, 0)},
		} {
			data, err := json.Marshal(trigger)
			if err != nil {
				t.Fatal(err)
			}
			// object 保留字段是否存在，避免把缺少 priority 与显式 0 混为一谈。
			var object map[string]json.RawMessage
			if err := json.Unmarshal(data, &object); err != nil {
				t.Fatal(err)
			}
			actual, present := object["priority"]
			if present != (priority != 0) || (present && string(actual) != fmt.Sprint(priority)) {
				t.Fatalf("priority presence mismatch: %s", data)
			}
		}
	}
}
