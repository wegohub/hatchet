//go:build e2e

package backend

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeP0Report 保存实际运行证据，不记录 token、数据库连接字符串或业务完整载荷
func writeP0Report(t *testing.T, b *Backend, name string, data map[string]any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	version, err := b.raw.Dispatcher().GetVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimPrefix(version, "v") != "0.110.5" {
		t.Fatalf("P0 release mismatch: %s", version)
	}
	manifestData, err := os.ReadFile("../../examples/acceptance.json")
	if err != nil {
		t.Fatal(err)
	}
	// manifest 是实际构建版本和公开协议，不将 P0 协议误记为已切换的公开 API
	var manifest struct {
		// Version 对应生成清单的 SDK 版本
		Version string `json:"sdk_version"`
		// Protocol 是当前公开 RPC 的协议版本
		Protocol uint32 `json:"protocol_version"`
	}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	data["sdk_version"], data["public_protocol_version"], data["probe_protocol_version"] = manifest.Version, manifest.Protocol, uint32(4)
	data["server_version"], data["recorded_at"], data["command"] = version, time.Now().UTC().Format(time.RFC3339), os.Getenv("WEGO_TEST_COMMAND")
	data["mq"], data["source_boundary"] = "PostgreSQL", "only sdks/wego; unmodified official release"
	encoded, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	dir := "../../.test-results"
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".json"), append(encoded, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
