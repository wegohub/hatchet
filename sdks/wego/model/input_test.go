package model

import (
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestInputMarshal 公开描述总有标记；无需 Runtime 就能生成 Console 可提交的 ProtoJSON。
func TestInputMarshal(t *testing.T) {
	input := Input{Value: wrapperspb.Int64(9007199254740993), Routing: map[string]any{"group": "one"}}
	for _, value := range []any{input, &input} {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		// fields 检查实际 JSON 契约，不依赖结构体字段顺序。
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			t.Fatal(err)
		}
		if string(fields["value"]) != `"9007199254740993"` || string(fields["_wego"]) != `{"version":6}` {
			t.Fatal(string(data))
		}
	}
	// value:null 是完整的显式输入；大整数普通 JSON 自动使用无 codec 的 base64。
	data, err := json.Marshal(Input{Value: nil})
	if err != nil || string(data) != `{"_wego":{"version":6},"value":null}` {
		t.Fatal(string(data), err)
	}
	data, err = json.Marshal(Input{Value: json.Number("9007199254740993")})
	if err != nil {
		t.Fatal(err)
	}
	// fields 检查实际 JSON 契约，不依赖结构体字段顺序。
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["_wego"]) != `{"encoding":"base64","version":6}` {
		t.Fatal(string(data))
	}
	if _, err := json.Marshal(Input{Routing: map[string]any{"cost": int64(9007199254740993)}}); err == nil {
		t.Fatal("unsafe routing accepted")
	}
}
