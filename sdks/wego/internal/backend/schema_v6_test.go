package backend

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// TestProtocolV6InputSchema 编译真实注册 schema；例如 client/bidi 的 gzip 输入必须接受 base64 字符串。
func TestProtocolV6InputSchema(t *testing.T) {
	for _, shape := range []string{"unary", "client", "server", "bidi"} {
		t.Run(shape, func(t *testing.T) {
			encoded, err := json.Marshal(inputSchema(shape))
			if err != nil {
				t.Fatal(err)
			}
			document, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
			if err != nil {
				t.Fatal(err)
			}
			compiler := jsonschema.NewCompiler()
			if err := compiler.AddResource("urn:wego:input", document); err != nil {
				t.Fatal(err)
			}
			schema, err := compiler.Compile("urn:wego:input")
			if err != nil {
				t.Fatal(err)
			}
			// plain 对应注册流方向；编码表示的形状与解码后的业务形状不能混为一谈。
			plain := `{}`
			if shape == "client" || shape == "bidi" {
				plain = `[]`
			}
			cases := []struct {
				// input 是 Console 实际传递的外层 JSON 对象。
				input string
				// valid 仅检查协议 schema，不以此替代 codec 内容校验。
				valid bool
			}{
				{`{"message":"business","value":1,"routing":{}}`, shape == "unary"},
				{`{"_wego":{"version":6,"idempotency_key":"key"},"value":` + plain + `}`, true},
				{`{"_wego":{"version":6,"idempotency_key":"key","encoding":"base64","codec_ids":["gzip.v1"]},"value":"eA=="}`, true},
				{`{"_wego":{"version":6,"idempotency_key":"key","encoding":"base64"},"value":[]}`, false},
				{`{"_wego":{"version":6,"idempotency_key":"key","codec_ids":["gzip.v1"]},"value":` + plain + `}`, false},
				{`{"_wego":{"version":5,"idempotency_key":"key"},"value":` + plain + `}`, false},
				{`{"_wego":{"version":6},"value":` + plain + `}`, shape == "unary"},
				{`{"_wego":null,"value":` + plain + `}`, false},
				{`{"_wego":{"version":6,"idempotency_key":"key","encoding":"unknown"},"value":` + plain + `}`, false},
				{`{"_wego":{"version":6,"idempotency_key":"key","response":"unknown"},"value":` + plain + `}`, false},
				{`{"_wego":{"version":6,"idempotency_key":"key"}}`, false},
			}
			for _, test := range cases {
				value, err := jsonschema.UnmarshalJSON(bytes.NewBufferString(test.input))
				if err != nil {
					t.Fatal(err)
				}
				err = schema.Validate(value)
				if (err == nil) != test.valid {
					t.Fatalf("schema disagrees: input=%s expected_valid=%v error=%v", test.input, test.valid, err)
				}
			}
		})
	}
}
