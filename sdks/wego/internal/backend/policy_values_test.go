package backend

import (
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestRateLimitExpressionPresence 验证空表达式不发送，非空表达式原样保留，包括零消耗。
func TestRateLimitExpressionPresence(t *testing.T) {
	for _, expression := range []string{"", "0", "input.routing.cost"} {
		request, err := taskDefinition("fixture", ports.Definition{Name: "task", Policy: spec.Task{
			RateLimits: []model.RateLimit{{Key: "limit", KeyExpr: expression, UnitsExpr: expression, LimitValueExpr: expression}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		actual := request.RateLimits[0]
		for _, field := range []*string{actual.KeyExpr, actual.UnitsExpr, actual.LimitValuesExpr} {
			if expression == "" {
				if field != nil {
					t.Fatal("empty expression must remain absent", field)
				}
			} else if field == nil || *field != expression {
				t.Fatal("explicit expression changed", field, expression)
			}
		}
	}
}

// TestRateLimitUnitsPresence 保留 nil 和显式 0 的区别，避免零消耗被误写成默认消耗。
func TestRateLimitUnitsPresence(t *testing.T) {
	zero, positive := 0, 2
	for _, units := range []*int{nil, &zero, &positive} {
		request, err := taskDefinition("fixture", ports.Definition{Name: "task", Policy: spec.Task{
			RateLimits: []model.RateLimit{{Key: "limit", Units: units}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		actual := request.RateLimits[0].Units
		if units == nil {
			if actual != nil {
				t.Fatal("unspecified units must remain absent")
			}
		} else if actual == nil || int(*actual) != *units {
			t.Fatal("explicit units changed", actual, units)
		}
	}
}

// TestBatchValuePresence 验证零值保留三个官方默认，显式参数转换为独立协议字段。
func TestBatchValuePresence(t *testing.T) {
	for _, config := range []model.BatchConfig{
		{MaxSize: 2},
		{MaxSize: 2, MaxInterval: 250 * time.Millisecond, GroupKey: "input.routing.group", GroupMaxRuns: 3},
	} {
		request, err := taskDefinition("fixture", ports.Definition{Name: "task", Policy: spec.Task{Batch: &config}})
		if err != nil {
			t.Fatal(err)
		}
		actual := request.Batch
		if config.MaxInterval == 0 {
			if actual.BatchMaxIntervalMs != nil || actual.BatchGroupKey != nil || actual.BatchGroupMaxRuns != nil {
				t.Fatal("batch defaults were overridden", actual)
			}
		} else {
			if actual.BatchMaxIntervalMs == nil || *actual.BatchMaxIntervalMs != 250 ||
				actual.BatchGroupKey == nil || *actual.BatchGroupKey != config.GroupKey ||
				actual.BatchGroupMaxRuns == nil || *actual.BatchGroupMaxRuns != 3 {
				t.Fatal("explicit batch configuration changed", actual)
			}
			config.GroupKey, config.GroupMaxRuns = "changed", 9
			if *actual.BatchGroupKey != "input.routing.group" || *actual.BatchGroupMaxRuns != 3 {
				t.Fatal("batch protocol fields alias caller configuration")
			}
		}
	}
}

// TestBatchValueBounds 在毫秒与 int32 转换前拒绝负数、过小窗口和溢出配置。
func TestBatchValueBounds(t *testing.T) {
	for _, config := range []model.BatchConfig{
		{MaxSize: 0}, {MaxSize: 100001},
		{MaxSize: 1, MaxInterval: -time.Second},
		{MaxSize: 1, MaxInterval: time.Microsecond},
		{MaxSize: 1, MaxInterval: 24*time.Hour + time.Millisecond},
		{MaxSize: 1, GroupMaxRuns: -1}, {MaxSize: 1, GroupMaxRuns: 10001},
	} {
		policy := spec.Task{Batch: &config}
		if policy.Validate() == nil {
			t.Fatal("invalid batch accepted", config)
		}
		if _, err := taskDefinition("fixture", ports.Definition{Name: "task", Policy: policy}); err == nil {
			t.Fatal("invalid batch encoded", config)
		}
	}
	// 官方范围的上下界必须保留，不因值类型的零值判断而丢失。
	for _, interval := range []time.Duration{time.Millisecond, 24 * time.Hour} {
		policy := spec.Task{Batch: &model.BatchConfig{MaxSize: 100000, MaxInterval: interval, GroupMaxRuns: 10000}}
		if _, err := taskDefinition("fixture", ports.Definition{Name: "task", Policy: policy}); err != nil {
			t.Fatal("valid batch boundary rejected", err)
		}
	}
}
