//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego"
)

// writeStreamEvidence 保存本轮真实断言与身份；调用方只在清理完成后写 PASSED，不写凭证或业务 payload
func writeStreamEvidence(t *testing.T, name string, evidence map[string]any) {
	t.Helper()
	evidence["status"], evidence["sdk_version"], evidence["command"], evidence["mq"] = "PASSED", wego.Version, os.Getenv("WEGO_TEST_COMMAND"), "postgresql"
	evidence["protocol_version"] = 4
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("..", "..", ".test-results", fmt.Sprintf("%s-%d.json", name, time.Now().UnixNano()))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	t.Log("evidence:", path)
}
