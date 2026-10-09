package quality

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestGenerationAndAcceptanceManifestAreReproducible 使用固定 protoc 工具重新生成协议与验收清单，逐字节比较仓库中的输出
func TestGenerationAndAcceptanceManifestAreReproducible(t *testing.T) {
	// base 仓库根目录，协议和清单复现检查从此定位输入文件
	base := root(t)
	// paths 本步骤需要遍历的名称或路径列表，注册、查询和清理使用同一集合以保持对应
	paths := []string{
		"sdks/wego/internal/wire/stream.pb.go",
		"sdks/wego/examples/proto/demo.pb.go",
		"sdks/wego/examples/proto/demo_grpc.pb.go",
	}
	// before 生成前按路径保存的字节快照，重新生成后逐文件比较，防止手改生成文件
	before := map[string][]byte{}
	// 逐项处理 paths，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, path := range paths {
		// data, err 接收 os.ReadFile 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		data, err := os.ReadFile(filepath.Join(base, path))
		if err != nil {
			t.Fatal(err)
		}
		before[path] = data
	}
	// generated 创建测试独占的生成输出目录，测试结束后自动清理
	generated := t.TempDir()
	// command 创建可审计的外部检查命令，失败时保留退出码和完整诊断
	command := exec.Command("sh", "sdks/wego/scripts/generate.sh")
	command.Dir = base
	command.Env = append(os.Environ(), "WEGO_GENERATE_OUTPUT="+generated)
	// data, err 接收 command.CombinedOutput 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
	if data, err := command.CombinedOutput(); err != nil {
		t.Fatalf("pinned generation: %v\n%s", err, data)
	}
	// 逐项处理 paths，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, path := range paths {
		// data, err 接收 os.ReadFile 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		data, err := os.ReadFile(filepath.Join(generated, path))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before[path], data) {
			t.Fatalf("generated file is stale: %s", path)
		}
	}
	command = exec.Command("go", "run", "./sdks/wego/scripts/manifest", "-check")
	command.Dir = base
	// 子进程使用 SDK 自有工作区，根模块保持原样
	command.Env = append(os.Environ(), "GOWORK="+filepath.Join(base, "sdks", "wego", "go.work"))
	// data, err 接收 command.CombinedOutput 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
	if data, err := command.CombinedOutput(); err != nil {
		t.Fatalf("manifest: %v\n%s", err, data)
	}
}
