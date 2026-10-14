package backend

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware/payload"
)

// TestStaticInputReplacesUnreadableHistory 验证旧封包损坏时使用已校验的当前封包，其他协议故障仍会阻止注册。
func TestStaticInputReplacesUnreadableHistory(t *testing.T) {
	ctx := context.Background()
	cipher, err := payload.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	config := spec.Defaults()
	config.Middleware = []middleware.Option{middleware.WithPayload(cipher)}
	b := &Backend{config: config}
	old, err := wire.EncodeValue(ctx, config, map[string]any{"value": 7}, nil)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := b.staticDigest(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	current, err := wire.EncodeValue(ctx, config, map[string]any{"value": 7}, nil)
	if err != nil {
		t.Fatal(err)
	}
	currentJSON, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}

	// 旧密文保持合法 base64，但认证失败；模拟持久封包指向已丢失的业务字节。
	var encoded string
	if err := json.Unmarshal(old.Payload, &encoded); err != nil {
		t.Fatal(err)
	}
	broken, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	broken[len(broken)-1] ^= 1
	old.Payload, err = json.Marshal(base64.StdEncoding.EncodeToString(broken))
	if err != nil {
		t.Fatal(err)
	}
	previous := map[string]staticInput{"filter:0": {Digest: digest, Packet: old}}
	prepared := map[string]staticInput{}
	got, err := b.prepareStaticInput(ctx, "filter:0", string(currentJSON), previous, prepared)
	if err != nil || got != string(currentJSON) || prepared["filter:0"].Packet.Payload == nil {
		t.Fatalf("当前封包未替代损坏的旧封包: got=%q err=%v", got, err)
	}

	// 缺少解码组件不属于可忽略的数据损坏，不能在配置错误时静默创建新版本。
	old.CodecIDs = []string{"unregistered.v1"}
	previous["filter:0"] = staticInput{Digest: digest, Packet: old}
	_, err = b.prepareStaticInput(ctx, "filter:0", string(currentJSON), previous, map[string]staticInput{})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("未知历史 codec 应保留配置错误: %v", err)
	}
}

// TestStaticInputReusesRandomCiphertext 验证相同逻辑输入复用已注册密文，变更业务字段则创建新封包。
func TestStaticInputReusesRandomCiphertext(t *testing.T) {
	ctx := context.Background()
	cipher, err := payload.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	config := spec.Defaults()
	config.TenantID = "00000000-0000-4000-8000-000000000001"
	config.Middleware = []middleware.Option{middleware.WithPayload(cipher)}
	backend := &Backend{config: config}
	old, err := wire.EncodeValue(ctx, config, map[string]any{"value": 7}, nil)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := backend.staticDigest(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	schema := map[string]any{"x-wego-static-inputs": map[string]staticInput{"cron": {Digest: digest, Packet: old}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("name") != "" {
			_ = json.NewEncoder(w).Encode(map[string]any{"rows": []any{map[string]any{"name": "fixture", "metadata": map[string]any{"id": "00000000-0000-4000-8000-000000000002"}}}})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"id": "00000000-0000-4000-8000-000000000003"}, "inputJsonSchema": schema})
		}
	}))
	defer server.Close()
	api, err := rest.NewClientWithResponses(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	backend.raw = &rereviewAPIClient{api: api}
	stored, _ := json.Marshal(old)
	for _, value := range []int{7, 8} {
		current, err := wire.EncodeValue(ctx, config, map[string]any{"value": value}, nil)
		if err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(current)
		if string(encoded) == string(stored) {
			t.Fatal("AES fixture did not generate independent ciphertext")
		}
		input := string(encoded)
		request := &v1.CreateWorkflowVersionRequest{Name: "fixture", CronInput: &input, InputJsonSchema: []byte(`{"type":"object"}`)}
		if err := backend.reuseStaticInputs(ctx, request); err != nil {
			t.Fatal(err)
		}
		if (value == 7) != (*request.CronInput == string(stored)) {
			t.Fatal("static reuse does not match logical input", value)
		}
	}
}

// TestStaticDigestUsesStoredCodecOrder 接收注册表的顺序和额外能力不改变持久封包的逻辑摘要。
func TestStaticDigestUsesStoredCodecOrder(t *testing.T) {
	cipher, err := payload.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	config := spec.Defaults()
	config.Middleware = []middleware.Option{middleware.WithPayload(&payload.Gzip{}), middleware.WithPayload(cipher)}
	packet, err := wire.EncodeValue(context.Background(), config, map[string]any{"value": 7}, nil)
	if err != nil {
		t.Fatal(err)
	}
	backend := &Backend{config: config}
	before, err := backend.staticDigest(context.Background(), packet)
	if err != nil {
		t.Fatal(err)
	}
	backend.config.Middleware = []middleware.Option{middleware.WithPayload(cipher), middleware.WithPayload(&payload.Gzip{})}
	backend.config.EncodingCodecsSet = true
	after, err := backend.staticDigest(context.Background(), packet)
	if err != nil || before != after {
		t.Fatal("registry changed static digest", before, after, err)
	}
}
