package spec

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestComparatorValueSnapshot 验证标签结构的值枚举随快照冻结，派生配置不能互相改写。
func TestComparatorValueSnapshot(t *testing.T) {
	input := model.RunOptions{DesiredWorkerLabels: map[string]*model.DesiredWorkerLabel{"capacity": {Value: int32(8), Comparator: model.LabelGreater}}}
	frozen := CloneRunOptions(input)
	input.DesiredWorkerLabels["capacity"].Comparator = model.LabelLess
	if frozen.DesiredWorkerLabels["capacity"].Comparator != model.LabelGreater {
		t.Fatal("caller mutation changed the snapshot")
	}
	derived := CloneRunOptions(frozen)
	derived.DesiredWorkerLabels["capacity"].Comparator = model.LabelEqual
	if frozen.DesiredWorkerLabels["capacity"].Comparator != model.LabelGreater {
		t.Fatal("derived mutation changed the parent")
	}
	for _, comparator := range []model.WorkerLabelComparator{model.LabelEqual, model.LabelNotEqual, model.LabelGreater, model.LabelGreaterEqual, model.LabelLess, model.LabelLessEqual, -1, 6} {
		_, err := ApplyRunOptions(func(o *model.RunOptions) {
			o.DesiredWorkerLabels = map[string]*model.DesiredWorkerLabel{"capacity": {Value: int32(8), Comparator: comparator}}
		})
		if (err != nil) != (comparator < model.LabelEqual || comparator > model.LabelLessEqual) {
			t.Fatal("unexpected comparator validation", comparator, err)
		}
	}
}

// TestRunOptionsSnapshotAndValidation 验证调用选项在构造时冻结，非法参数不会进入 codec I/O。
func TestRunOptionsSnapshotAndValidation(t *testing.T) {
	values := map[string]any{"group": "one", "policy": map[string]any{"cost": 1}}
	option := WithRouting(values)
	values["group"] = "mutated"
	values["policy"].(map[string]any)["cost"] = 9
	options, err := ApplyRunOptions(option, WithRouting(map[string]any{"group": "two"}))
	if err != nil {
		t.Fatal(err)
	}
	if options.Routing["group"] != "two" || options.Routing["policy"].(map[string]any)["cost"] != json.Number("1") {
		t.Fatal(options.Routing)
	}
	options.Routing["policy"].(map[string]any)["cost"] = json.Number("7")
	fresh, err := ApplyRunOptions(option)
	if err != nil || fresh.Routing["policy"].(map[string]any)["cost"] != json.Number("1") {
		t.Fatal(fresh, err)
	}
	for _, bad := range []any{math.NaN(), math.Inf(1), int64(9007199254740993), func() {}} {
		if _, err := ApplyRunOptions(WithRouting(map[string]any{"cost": bad})); err == nil {
			t.Fatal("accepted invalid routing", bad)
		}
	}
	for _, priority := range []int{-1, 0, 4, math.MaxInt} {
		if _, err := ApplyRunOptions(func(o *model.RunOptions) { o.Priority = &priority }); err == nil {
			t.Fatal("accepted priority", priority)
		}
	}
	if _, err := ApplyRunOptions(func(o *model.RunOptions) {
		o.DesiredWorkerLabels = map[string]*model.DesiredWorkerLabel{"region": {Value: map[string]any{"zone": "one"}}}
	}); err == nil {
		t.Fatal("container is not a protocol label value")
	}
}

// TestRoutingSurvivesEngineNumberNormalization 证明指数及小数形式经过服务端 JSON map 后仍是相同快照。
func TestRoutingSurvivesEngineNumberNormalization(t *testing.T) {
	input := map[string]any{"count": json.Number("1e3"), "cost": []any{json.Number("1.00"), json.Number("0.100")}}
	snapshot, err := SnapshotRouting(input)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	// engineView 模拟引擎解码业务外层 JSON 为 float64，业务字节本身仍为 base64。
	var engineView map[string]any
	if err := json.Unmarshal(encoded, &engineView); err != nil {
		t.Fatal(err)
	}
	restored, err := SnapshotRouting(engineView)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := json.Marshal(restored)
	if err != nil || string(stored) != string(encoded) || snapshot["count"] != json.Number("1000") {
		t.Fatal("routing changed after engine normalization", string(encoded), string(stored), err)
	}
	if _, err := SnapshotRouting(map[string]any{"cost": json.Number("1e999999999")}); err == nil {
		t.Fatal("unbounded exponent accepted")
	}
}
