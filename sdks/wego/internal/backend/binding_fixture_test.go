package backend

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/hatchet-dev/hatchet/pkg/client/rest"
)

// bindingAPI 模拟按名称及当前版本读取持久定义；不通过绕过生产校验来测试提交协议。
func bindingAPI(t *testing.T) string {
	t.Helper()
	// current 保存本次名称查询，fixture 的调用按查询后读版本串行进行。
	var current string
	// mu 保护受控的版本查询名称。
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		defer mu.Unlock()
		if name := r.URL.Query().Get("name"); name != "" {
			current = name
			_ = json.NewEncoder(w).Encode(map[string]any{"rows": []any{map[string]any{"metadata": map[string]any{"id": "00000000-0000-4000-8000-000000000002"}, "name": name}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"id": "00000000-0000-4000-8000-000000000003"}, "inputJsonSchema": map[string]any{"x-wego-binding": map[string]any{"version": 6, "kind": "task", "namespace": "", "workflow": current}}})
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// bindFixtureTarget 配置真实 REST 客户端，其他受控协议出口保持独立。
func bindFixtureTarget(t *testing.T, b *Backend) {
	t.Helper()
	api, err := rest.NewClientWithResponses(bindingAPI(t))
	if err != nil {
		t.Fatal(err)
	}
	b.config.TenantID = "00000000-0000-4000-8000-000000000001"
	b.raw = &rereviewAPIClient{api: api}
	b.features = newFeatureClients(b.raw)
}
