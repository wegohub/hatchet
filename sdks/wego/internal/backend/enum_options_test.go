package backend

import (
	"math"
	"strings"
	"testing"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestConcurrencyMaxRunsValue 验证零值保留服务端默认，正数映射及负数拒绝。
func TestConcurrencyMaxRunsValue(t *testing.T) {
	for _, maximum := range []int32{0, 1, 2, math.MaxInt32, -1} {
		policy := spec.Task{Concurrency: []model.Concurrency{{MaxRuns: maximum}}}
		request, err := workflowDefinition("fixture", policy, "")
		if maximum < 0 {
			if err == nil || policy.Validate() == nil {
				t.Fatal("negative static concurrency accepted")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		actual := request.ConcurrencyArr[0].MaxRuns
		if maximum == 0 {
			if actual != nil {
				t.Fatal("unspecified maximum must remain absent", actual)
			}
		} else if actual == nil || *actual != maximum {
			t.Fatal("explicit maximum changed", actual, maximum)
		}
		// 转换后的协议字段不借用调用方的结构体字段。
		policy.Concurrency[0].MaxRuns = 3
		if actual != nil && *actual != maximum {
			t.Fatal("protocol maximum aliases caller configuration")
		}
	}
	// 动态表达式允许返回 0；静态字段未指定时不能主动填入 1 或改写表达式。
	expression := "0"
	request, err := workflowDefinition("fixture", spec.Task{Concurrency: []model.Concurrency{{MaxRunsExpression: expression}}}, "")
	if err != nil {
		t.Fatal(err)
	}
	actual := request.ConcurrencyArr[0]
	if actual.MaxRuns != nil || actual.MaxRunsExpression == nil || *actual.MaxRunsExpression != expression {
		t.Fatal("dynamic limit changed", actual)
	}
}

// TestEnumDefinitionPresence 验证空值保留协议缺省，显式常量映射到对应官方枚举。
func TestEnumDefinitionPresence(t *testing.T) {
	// 空策略不能被客户端固化为某个默认值，默认规则由实际服务端解释。
	for _, strategy := range []model.ConcurrencyLimitStrategy{
		"", model.CancelInProgress, model.CancelNewest, model.GroupRoundRobin,
		model.DropNewest, model.QueueNewest, model.CancelQueuedExceptNewest, model.CancelQueuedExceptOldest,
	} {
		t.Run("concurrency/"+string(strategy), func(t *testing.T) {
			request, err := workflowDefinition("fixture", spec.Task{Concurrency: []model.Concurrency{{LimitStrategy: strategy}}}, "")
			if err != nil {
				t.Fatal(err)
			}
			actual := request.ConcurrencyArr[0].LimitStrategy
			if strategy == "" {
				if actual != nil {
					t.Fatal("empty strategy must remain absent", actual)
				}
			} else if actual == nil || actual.String() != string(strategy) {
				t.Fatal("strategy changed in protocol conversion", actual, strategy)
			}
		})
	}
	// 空周期不覆盖静态资源；例如 Minute 则明确发送 MINUTE。
	for _, duration := range []model.RateLimitDuration{"", model.Second, model.Minute, model.Hour, model.Day, model.Week, model.Month, model.Year} {
		t.Run("duration/"+string(duration), func(t *testing.T) {
			request, err := taskDefinition("fixture", ports.Definition{Name: "task", Policy: spec.Task{RateLimits: []model.RateLimit{{Key: "fixture", Duration: duration}}}})
			if err != nil {
				t.Fatal(err)
			}
			actual := request.RateLimits[0].Duration
			if duration == "" {
				if actual != nil {
					t.Fatal("empty duration must remain absent", actual)
				}
			} else if actual == nil || actual.String() != strings.ToUpper(string(duration)) {
				t.Fatal("duration changed in protocol conversion", actual, duration)
			}
		})
	}
	if _, err := workflowDefinition("fixture", spec.Task{Concurrency: []model.Concurrency{{LimitStrategy: "invalid"}}}, ""); err == nil {
		t.Fatal("invalid strategy accepted")
	}
	if _, err := taskDefinition("fixture", ports.Definition{Name: "task", Policy: spec.Task{RateLimits: []model.RateLimit{{Duration: "invalid"}}}}); err == nil {
		t.Fatal("invalid duration accepted")
	}
}

// TestEnumComparatorProtocol 验证零值及全部比较常量都显式发送，未知数值在提交前失败。
func TestEnumComparatorProtocol(t *testing.T) {
	for _, comparator := range []model.WorkerLabelComparator{
		model.LabelEqual, model.LabelNotEqual, model.LabelGreater, model.LabelGreaterEqual, model.LabelLess, model.LabelLessEqual,
	} {
		options, err := runOptions(model.RunOptions{DesiredWorkerLabels: map[string]*model.DesiredWorkerLabel{
			"capacity": {Value: int32(8), Comparator: comparator},
		}})
		if err != nil {
			t.Fatal(err)
		}
		request := &v1.TriggerWorkflowRequest{}
		for _, option := range options {
			if err := option(request); err != nil {
				t.Fatal(err)
			}
		}
		actual := request.DesiredWorkerLabels["capacity"]
		if actual.Comparator == nil || int32(*actual.Comparator) != int32(comparator) {
			t.Fatal("comparator must have explicit protocol presence", actual, comparator)
		}
	}
	for _, invalid := range []model.WorkerLabelComparator{-1, 6} {
		if _, err := runOptions(model.RunOptions{DesiredWorkerLabels: map[string]*model.DesiredWorkerLabel{"capacity": {Value: int32(8), Comparator: invalid}}}); err == nil {
			t.Fatal("invalid comparator accepted", invalid)
		}
	}
}
