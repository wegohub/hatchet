package stream

import (
	"encoding/json"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestInputIdentitySnapshots 验证 JSON 空白/对象顺序不改变身份，整数精度、请求顺序及 routing 仍能区分
func TestInputIdentitySnapshots(t *testing.T) {
	digest := func(message string, routing map[string]any) string {
		t.Helper()
		value, err := wire.EventDigest(json.RawMessage(message), routing)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	first := digest(`{"id":"9007199254740993","v":1,"name":"x"}`, map[string]any{"cost": 1})
	if first != digest(" {\"name\": \"x\", \"v\":1, \"id\":\"9007199254740993\"} ", map[string]any{"cost": 1}) {
		t.Fatal("JSON whitespace/object ordering changed identity")
	}
	if first == digest(`{"id":"9007199254740992","v":1,"name":"x"}`, map[string]any{"cost": 1}) || first == digest(`{"id":"9007199254740993","v":1,"name":"x"}`, map[string]any{"cost": 2}) {
		t.Fatal("distinct call identities collapsed")
	}
	if IdempotencyKey("a", "/fixture/Call", "key") == IdempotencyKey("b", "/fixture/Call", "key") || IdempotencyKey("a", "/fixture/Call", "key") == IdempotencyKey("a", "/fixture/Other", "key") {
		t.Fatal("tenant-shared keys lack method/namespace isolation")
	}
	input, err := json.Marshal(map[string]any{"_wego": map[string]any{"version": 6, "response": "wego", "input_digest": first}, "value": map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyRunIdentity(input, first); err != nil {
		t.Fatal(err)
	}
	if err := VerifyRunIdentity(input, "different-digest"); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	if _, err := wire.EventDigest(json.RawMessage(`{} {}`), nil); err == nil {
		t.Fatal(err)
	}
}
