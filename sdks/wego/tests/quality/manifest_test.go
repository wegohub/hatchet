package quality

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
)

// TestManifestHasExecutableCoverage 检查每个官方源文件、构造片段和扩展测试都有真实可执行入口。
func TestManifestHasExecutableCoverage(t *testing.T) {
	// data, err 接收 os.ReadFile 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	data, err := os.ReadFile(filepath.Join(root(t), "sdks/wego/examples/acceptance.json"))
	if err != nil {
		t.Fatal(err)
	}
	// m 验收清单的解码目标，验证每个源文件都有可运行示例与测试。
	var m struct {
		// Sources 官方源文件与验收条目的映射集合。
		Sources []struct {
			// Source, Example 官方源文件路径和可运行 wego 示例路径。
			Source, Example string
			// Scenarios, Tests 源文件关联的场景与测试入口。
			Scenarios, Tests []string
			// Fragments 该文件中所有 standalone 构造片段的清单。
			Fragments []json.RawMessage
		}
		// ExtraTests 扩展验收组到测试入口的映射。
		ExtraTests map[string]string `json:"extra_tests"`
	}
	if err = json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Sources) != 28 {
		t.Fatalf("official source inventory: %d", len(m.Sources))
	}
	// 逐项处理 m.Sources，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, source := range m.Sources {
		if len(source.Fragments) == 0 || len(source.Tests) == 0 {
			t.Error("source has no fragments or assertions:", source.Source)
		}
		// 逐项处理 []string{source.Source, source.Example}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
		for _, path := range []string{source.Source, source.Example} {
			if _, err = os.Stat(filepath.Join(root(t), path)); err != nil {
				t.Error(err)
			}
		}
		// 逐项处理 source.Scenarios，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
		for _, name := range source.Scenarios {
			if name != "embedded" && scenarios.Registry[name] == nil {
				t.Error("required scenario has no executable implementation:", name)
			}
		}
	}
	// 逐项处理 m.ExtraTests，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for name, test := range m.ExtraTests {
		if test == "" {
			t.Error("extra scenario has no test:", name)
		}
	}
}
