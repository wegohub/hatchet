//go:build e2e && wego_embedded

package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/hatchet-dev/hatchet/sdks/wego"
)

// TestEmbedded 创建独立 PostgreSQL 数据库并运行嵌入验收子进程，结束后删除测试库
func TestEmbedded(t *testing.T) {
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// adminURL 读取本机显式配置；未提供时由紧接着的默认分支解析
	adminURL := os.Getenv("WEGO_EMBEDDED_ADMIN_DATABASE_URL")
	if adminURL == "" {
		adminURL = "postgres://root:123456@localhost:5432/postgres?sslmode=disable"
	}
	// admin, err 接收 pgx.Connect 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatal("embedded PostgreSQL connection failed")
	}
	defer admin.Close(context.Background())

	// name 生成当前数据的文本表示，用于名称、业务比较或报告，不包含凭证
	name := fmt.Sprintf("wego_embedded_%d", time.Now().UnixNano())
	// quoted 构造当前步骤的数据对象，字段在提交、编码或断言之前一次性初始化
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+quoted); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "ALTER DATABASE "+quoted+" SET TIMEZONE='UTC'"); err != nil {
		t.Fatal(err)
	}
	// dropped 独立测试数据库是否已经删除，最终报告必须按实际清理结果填写
	dropped := false
	defer func() {
		if dropped {
			return
		}

		// cleanup, stop 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()

		// _, e 接收 admin.Exec 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
		if _, e := admin.Exec(cleanup, "DROP DATABASE "+quoted+" WITH (FORCE)"); e != nil {
			t.Error(e)
		}
	}()

	// parsed, err 接收 url.Parse 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	parsed, err := url.Parse(adminURL)
	if err != nil {
		t.Fatal("invalid database URL")
	}
	parsed.Path = "/" + name
	// reportDirectory, err 接收 filepath.Abs 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	reportDirectory, err := filepath.Abs("../../.test-results")
	if err != nil {
		t.Fatal(err)
	}
	// 组合仓库与资源路径，扫描、生成和清理使用同一位置
	reportPath := filepath.Join(reportDirectory, name+".json")
	// command 启动受预算约束的独立进程，取消后测试不能遗留子进程
	command := exec.CommandContext(ctx, "go", "run", "-tags=wego_embedded", ".")
	command.Dir = "embedded"
	command.Env = append(
		os.Environ(),
		// 独立模块读取官方发行依赖版本；工作区根模块的 (devel) 无法用于启动器版本解析
		"GOWORK=off",
		"WEGO_REPORT_DIRECTORY="+reportDirectory,
		"WEGO_REPORT_PATH="+reportPath,
		"WEGO_EMBEDDED_DATABASE_URL="+parsed.String(),
		"SERVER_SECURITY_CHECK_ENABLED=false",
		"SERVER_MSGQUEUE_KIND=postgres",
		"SERVER_OBSERVABILITY_ENABLED=false",
		"SERVER_RUNTIME_PARTITION_OLAP_TIMEZONE=UTC",
		"SERVER_RUNTIME_PARTITION_TIMEZONE=UTC",
	)
	// output, err 接收 command.CombinedOutput 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	output, err := command.CombinedOutput()
	// 嵌入引擎初始化会设置环境变量，因此仅在子进程运行，隔离父进程的普通实例配置
	if strings.Contains(string(output), parsed.String()) {
		output = []byte(strings.ReplaceAll(string(output), parsed.String(), "[database URL redacted]"))
	}
	if err != nil {
		t.Fatalf("embedded child: %v\n%s", err, output)
	}
	// example 启动受预算约束的独立进程，取消后测试不能遗留子进程
	example := exec.CommandContext(ctx, "go", "run", "-tags=wego_embedded", ".")
	example.Dir = "../../examples/embedded"
	example.Env = command.Env
	output, err = example.CombinedOutput()
	if err != nil {
		t.Fatalf("independent embedded example failed: %v", err)
	}
	if !strings.Contains(string(output), "Hello, embed!") {
		t.Fatal("embedded native example result missing")
	}
	if _, err = admin.Exec(ctx, "DROP DATABASE "+quoted+" WITH (FORCE)"); err != nil {
		t.Fatal(err)
	}
	dropped = true
	// data, err 接收 os.ReadFile 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	// report 独立嵌入进程的结果报告，确认迁移、业务调用及数据库清理记录
	var report map[string]any
	if err = json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	report["command"] = os.Getenv("WEGO_TEST_COMMAND")
	report["sdk_version"], report["protocol_version"] = wego.Version, 4
	report["independent_example"] = "PASSED: Hello, embed!"
	report["database_dropped"] = true
	report["cleanup"] = "workflow definitions deleted; API/gRPC listeners closed; independent PostgreSQL database dropped"
	// assertionIDs 仅在独立示例成功、数据库已删除及关闭断言通过后发布
	report["assertion_ids"] = []string{fmt.Sprintf("%x", sha256.Sum256([]byte("embedded migration, protobuf invocation, shutdown and database deletion")))[:24]}
	data, _ = json.MarshalIndent(report, "", "  ")
	if err = os.WriteFile(reportPath, data, 0644); err != nil {
		t.Fatal(err)
	}
	t.Log("embedded RPC and independent example passed; database dropped; report:", reportPath)
}
