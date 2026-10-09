package quality

import (
	"os/exec"
	"testing"
)

// TestFragmentEvidenceCannotBeInferred 执行缺失、失败、Skip 及错误归组的报告组装回归
func TestFragmentEvidenceCannotBeInferred(t *testing.T) {
	// command 的 Python 测试不依赖引擎，确保删除一项断言后报告必然拒绝完成
	command := exec.Command("python3", "-m", "unittest", "discover", "-s", "sdks/wego/scripts", "-p", "test_*.py", "-v")
	command.Dir = root(t)
	// output 包含每种拒绝路径的断言，失败时保留完整诊断
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("fragment evidence regression: %v\n%s", err, output)
	}
}
