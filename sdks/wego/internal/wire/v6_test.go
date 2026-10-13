package wire

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/hatchet-dev/hatchet/sdks/wego/examples/codec/payload"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// TestV6Representations 无标记字段始终属于业务；显式 null 不等于缺少 value。
func TestV6Representations(t *testing.T) {
	config := spec.Defaults()
	for _, raw := range []string{`{"value":7,"routing":{"group":"business"}}`, `{"_wego":{"version":6},"value":null}`, `{"_wego":{"version":6},"value":{"_wego":"business"}}`} {
		packet, err := AsEnvelope(raw)
		if err != nil {
			t.Fatal(err)
		}
		opened, err := Open(context.Background(), "", packet, config, 1<<20, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		if packet.Native && string(opened.Payload) != raw {
			t.Fatal("native business object was unpacked")
		}
		if packet.Response == "wego" {
			t.Fatal("native response was not default")
		}
	}
	for _, raw := range []string{`{"_wego":null,"value":1}`, `{"_wego":{"version":5},"value":1}`, `{"_wego":{"version":6}}`, `{"_wego":{"version":6,"response":"other"},"value":1}`, `{"_wego":{"version":6,"codec_ids":["unknown"]},"value":1}`, `{"_wego":{"version":6},"value":1,"routing":null}`} {
		if _, err := AsEnvelope(raw); err == nil {
			t.Fatal("malformed protocol accepted", raw)
		}
	}
	oversized := `{"_wego":{"version":6,"extension":"` + strings.Repeat("x", 64<<10) + `"},"value":1}`
	if _, err := AsEnvelope(oversized); err == nil {
		t.Fatal("oversized header accepted")
	}
}

// TestV6ValueEncoding 明文、精度保真和输出契约分别处理；Worker 配置不强迫 native 输出使用 gzip。
func TestV6ValueEncoding(t *testing.T) {
	ctx := context.Background()
	config := spec.Defaults()
	for _, raw := range []string{`{"n":7}`, `{"n":9007199254740993}`, `{"n":0.123456789012345678901}`, `{"n":1e-400}`} {
		packet, err := Seal(ctx, NewEnvelope(ctx, "", "", json.RawMessage(raw), nil), config, 1<<20, 2<<20, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(packet)
		if err != nil {
			t.Fatal(err)
		}
		stored, err := AsEnvelope(data)
		if err != nil {
			t.Fatal(err)
		}
		opened, err := Open(ctx, "", stored, config, 1<<20, 4<<20)
		if err != nil || string(opened.Payload) != raw {
			t.Fatal(string(opened.Payload), err)
		}
		if (raw != `{"n":7}`) != (packet.Encoding == "base64") {
			t.Fatal("incorrect precision carrier", string(data))
		}
		if strings.Contains(string(data), `"method"`) || strings.Contains(string(data), `"scope"`) || strings.Contains(string(data), `"input_digest"`) {
			t.Fatal("redundant input fields", string(data))
		}
	}
	compressed, codecErr := payload.NewGzip(payload.WithThreshold(0))
	if codecErr != nil {
		t.Fatal(codecErr)
	}
	config.Middleware = []middleware.Option{middleware.WithPayload(compressed)}
	// 本场景强制编码小消息，以验证 native 和 wego 响应契约区别。

	out, err := Output(WithResponse(ctx, false), NewEnvelope(ctx, "", "", json.RawMessage(`{"ok":true}`), nil), config, 1024, 2048, 4096)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(out)
	if err != nil || string(data) != `{"ok":true}` {
		t.Fatal(string(data), err)
	}
	out, err = Output(WithResponse(ctx, true), NewEnvelope(ctx, "", "", json.RawMessage(`{"ok":true}`), nil), config, 1024, 2048, 4096)
	if err != nil || out.Encoding != "base64" || len(out.CodecIDs) != 1 {
		t.Fatal(out, err)
	}
}

// TestV6MetadataContract 验证可读头、任意二进制值和纳秒截止时间往返，畸形头不能改变原对象。
func TestV6MetadataContract(t *testing.T) {
	ctx := context.Background()
	packet := NewEnvelope(ctx, "", "", json.RawMessage(`{}`), nil)
	packet.Metadata = map[string][]string{"plain": {"hello"}, "token-bin": {string([]byte{0, 255, 254})}}
	packet.Headers = map[string][]string{"header-bin": {string([]byte{128})}}
	packet.Trailers = map[string][]string{"status": {"done"}}
	packet.Trace = map[string]string{"traceparent": "fixture"}
	packet.Deadline = 1790000000123456789
	packet, err := Seal(ctx, packet, spec.Defaults(), 1024, 2048, 4096)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(packet)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := AsEnvelope(data)
	if err != nil || decoded.Deadline != packet.Deadline || decoded.Metadata["token-bin"][0] != packet.Metadata["token-bin"][0] || decoded.Headers["header-bin"][0] != packet.Headers["header-bin"][0] {
		t.Fatal(decoded, err)
	}
	for _, raw := range []string{
		`{"_wego":{"version":6,"deadline":"bad"},"value":{}}`,
		`{"_wego":{"version":6,"deadline":"9999-01-01T00:00:00Z"},"value":{}}`,
		`{"_wego":{"version":6,"metadata":{"a-bin":["!"]}},"value":{}}`,
		`{"_wego":{"version":6,"headers":{"a-bin":["!"]}},"value":{}}`,
		`{"_wego":{"version":6,"trailers":{"a-bin":["!"]}},"value":{}}`,
		`{"_wego":{"version":6,"stream_mode":"other"},"value":{}}`,
	} {
		if err := json.Unmarshal([]byte(raw), &decoded); err == nil {
			t.Fatal("invalid header accepted")
		}
		if decoded.Deadline != packet.Deadline {
			t.Fatal("failed parse partially changed envelope")
		}
	}
	packet.Metadata["plain"] = []string{string([]byte{255})}
	if _, err := json.Marshal(packet); err == nil {
		t.Fatal("non-UTF8 ordinary metadata accepted")
	}
}

// TestV6ValueFailureBoundaries 验证损坏编码、解码预算和原生输出取消不被当作成功结果。
func TestV6ValueFailureBoundaries(t *testing.T) {
	ctx := context.Background()
	config := spec.Defaults()
	for _, raw := range []string{
		`{"_wego":{"version":6,"encoding":"base64"},"value":"!"}`,
		`{"_wego":{"version":6,"encoding":"base64"},"value":"bm90LWpzb24="}`,
	} {
		packet, err := AsEnvelope(raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Open(ctx, "", packet, config, 1024, 4096); err == nil {
			t.Fatal("corrupt value accepted")
		}
	}
	packet, err := AsEnvelope(`{"_wego":{"version":6,"encoding":"base64"},"value":"eyJsb25nIjoidGV4dCJ9"}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, "", packet, config, 2, 4096); err == nil {
		t.Fatal("base64 exceeded decode budget")
	}
	for _, raw := range []string{`not-json`, `{"long":"text"}`} {
		if _, err := Output(WithResponse(ctx, false), NewEnvelope(ctx, "", "", json.RawMessage(raw), nil), config, 2, 2048, 4096); err == nil {
			t.Fatal("invalid native output accepted")
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Output(WithResponse(cancelled, false), NewEnvelope(ctx, "", "", json.RawMessage(`null`), nil), config, 1024, 2048, 4096); err == nil {
		t.Fatal("cancelled native output accepted")
	}
	packet, err = Seal(ctx, NewEnvelope(ctx, "", "", json.RawMessage(`null`), nil), config, 1024, 2048, 4096)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := Open(ctx, "", packet, config, 1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := json.Marshal(opened); err == nil {
		t.Fatal("decoded packet re-emitted stored codec metadata")
	}
	if err := DecodeValue(ctx, "", packet, nil, config, 1024, 4096); err == nil {
		t.Fatal("nil decoding target accepted")
	}
}

// TestV6TaskStreamUsesCodec 验证原生任务实时业务字节的 wego 出口，任意二进制不被 JSON 或 UTF-8 改写。
func TestV6TaskStreamUsesCodec(t *testing.T) {
	ctx := context.Background()
	config := spec.Defaults()
	compressed, codecErr := payload.NewGzip(payload.WithThreshold(0))
	if codecErr != nil {
		t.Fatal(codecErr)
	}
	config.Middleware = []middleware.Option{middleware.WithPayload(compressed)}
	// 本场景强制编码小消息，以验证 native 和 wego 响应契约区别。

	data := []byte{255, 0, 254}
	encoded, err := EncodeNativeStream(ctx, config, data)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeNativeStream(ctx, config, string(encoded))
	if err != nil || string(decoded) != string(data) {
		t.Fatal(decoded, err)
	}
	if _, err := DecodeNativeStream(ctx, config, "!"); err == nil {
		t.Fatal("invalid realtime frame accepted")
	}
	codec, err := NewFrameCodec(config, 1<<20, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	control, err := codec.Encode(ctx, "", &LogFrame{Version: LogVersion, Kind: "CLAIM"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeNativeStream(ctx, config, base64.StdEncoding.EncodeToString(control)); err == nil {
		t.Fatal("RPC control frame exposed as ordinary business event")
	}
	config.PayloadLimits.MaxMessageBytes = 2
	if _, err := EncodeNativeStream(ctx, config, data); err == nil {
		t.Fatal("oversized realtime business data accepted")
	}
}
