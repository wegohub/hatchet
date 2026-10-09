package runtime

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// TestRoutingSnapshotsAndOverrides 默认与调用都保存独立快照，嵌套对象按顶层键整体覆盖
func TestRoutingSnapshotsAndOverrides(t *testing.T) {
	values := map[string]any{"group": "default", "policy": map[string]any{"cost": 1, "limit": 2}, "large": int64(9007199254740993)}
	option := WithRoutingDefaults("/fixture/Call", values)
	values["group"] = "mutated"
	values["policy"].(map[string]any)["cost"] = 100
	config := spec.Defaults()
	option(&config)
	if config.Namespace != "" {
		t.Fatal("namespace must default to empty")
	}
	call := map[string]any{"policy": map[string]any{"cost": 3}}
	ctx := callctx.WithRouting(context.Background(), call)
	call["policy"].(map[string]any)["cost"] = 9
	override, err := callctx.Routing(ctx)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := config.Routing("/fixture/Call", override)
	if err != nil {
		t.Fatal(err)
	}
	policy := merged["policy"].(map[string]any)
	if merged["group"] != "default" || policy["cost"] != json.Number("3") || policy["limit"] != nil || merged["large"] != json.Number("9007199254740993") {
		t.Fatalf("routing: %#v", merged)
	}
	// Clone 中的嵌套值也归局部实例所有
	local := config.Clone()
	local.RoutingDefaults["/fixture/Call"]["policy"].(map[string]any)["cost"] = 7
	if config.RoutingDefaults["/fixture/Call"]["policy"].(map[string]any)["cost"] != json.Number("1") {
		t.Fatal("nested default ownership leaked")
	}
}

// TestInvalidRoutingRejectedBeforeUse 无效 JSON 不被当作空 map，默认和调用入口均保留错误
func TestInvalidRoutingRejectedBeforeUse(t *testing.T) {
	for _, value := range []any{math.NaN(), func() {}, make(chan int)} {
		config := spec.Defaults()
		WithRoutingDefaults("/fixture/Call", map[string]any{"bad": value})(&config)
		if config.Validate() == nil {
			t.Fatal("invalid default accepted")
		}
		ctx := callctx.WithRouting(context.Background(), map[string]any{"bad": value})
		if _, err := callctx.Routing(ctx); err == nil {
			t.Fatal("invalid call accepted")
		}
	}
}
