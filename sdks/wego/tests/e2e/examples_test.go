//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// preflight 在真实验收前检查 API、gRPC、实例版本及必需能力，失败不静默跳过
func preflight(t *testing.T) {
	t.Helper()
	if os.Getenv("HATCHET_CLIENT_TOKEN") == "" {
		t.Fatal("HATCHET_CLIENT_TOKEN is required; real acceptance cannot be skipped")
	}
	// api 读取本机显式配置；未提供时由紧接着的默认分支解析
	api := os.Getenv("WEGO_API_URL")
	if api == "" {
		api = "http://localhost:8080"
	}
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// req, _ 将请求绑定到调用预算，取消后 HTTP I/O 必须结束
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, api+"/api/live", nil)
	// response, err 接收 http.DefaultClient.Do 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("API health: %d", response.StatusCode)
	}
	// address 读取本机显式配置；未提供时由紧接着的默认分支解析
	address := os.Getenv("WEGO_GRPC_ADDRESS")
	if address == "" {
		address = "localhost:7077"
	}
	// connection, err 接收 net.DialTimeout 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	connection, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	connection.Close()
}

// TestExamples 遍历 manifest 的可执行场景，逐条保存运行身份和断言，缺失能力不得 Skip
func TestExamples(t *testing.T) {
	preflight(t)
	// manifestData, err 接收 os.ReadFile 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	manifestData, err := os.ReadFile(filepath.Join("..", "..", "examples", "acceptance.json"))
	if err != nil {
		t.Fatal(err)
	}
	// manifest 验收清单解码目标，核对官方片段和场景入口映射
	var manifest struct {
		// Sources 官方源文件与验收条目的映射集合
		Sources []struct {
			// Source 官方源文件路径
			Source string `json:"source"`
			// Scenarios 对应的可执行 wego 验收场景名称集合
			Scenarios []string `json:"scenarios"`
			// Fragments 该文件中所有 standalone 构造片段的清单
			Fragments []json.RawMessage `json:"fragments"`
		}
		// ExtraTests 扩展验收组到测试入口的映射
		ExtraTests map[string]string `json:"extra_tests"`
	}
	if err = json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Sources) != 28 {
		t.Fatal("manifest must inventory exactly 28 official sources")
	}
	// 逐项处理 manifest.Sources，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, source := range manifest.Sources {
		// 逐项处理 source.Scenarios，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for _, scenario := range source.Scenarios {
			if scenario != "embedded" && scenarios.Registry[scenario] == nil {
				t.Fatalf("required scenario missing: %s for %s", scenario, source.Source)
			}
		}
	}
	// conn, err 接收 client.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	conn, err := client.New(client.WithRuntime(scenarios.Runtime("wego_preflight_")...))
	if err != nil {
		t.Fatal(err)
	}
	// info, err 接收 conn.Info 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	info, err := conn.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !info.DurableEviction {
		t.Fatal("instance does not support required durable eviction capability")
	}
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	// report 并发安全的验收记录收集器，成功断言与清理结果逐条写入，不保存凭证
	report := &scenarios.Report{}
	// results 场景名称到实际验收状态的映射；仅业务断言和清理通过后写入 PASSED
	results := map[string]string{}
	// names 本步骤需要遍历的名称或路径列表，注册、查询和清理使用同一集合以保持对应
	names := []string{}
	// 逐项处理 scenarios.Registry，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for name := range scenarios.Registry {
		names = append(names, name)
	}
	sort.Strings(names)
	// 逐项处理 names，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			// budget 此 Worker 注销时使用的明确预算，不能用未受限的默认上下文等待
			budget := 3 * time.Minute
			if name == "temporal" {
				budget = 6 * time.Minute
			}
			// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()

			// start 读取当前已确认断言数，后续只为新增记录关联测试场景
			start := report.Count()
			// err 接收 scenarios.Run 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
			err := scenarios.Run(ctx, name, report)
			report.AttributeSince(start, name)
			if err != nil {
				results[name] = "FAILED: " + err.Error()
				t.Fatal(err)
			}
			results[name] = "PASSED"
		})
	}
	// path 读取本机显式配置；未提供时由紧接着的默认分支解析
	path := os.Getenv("WEGO_REPORT_PATH")
	if path == "" {
		path = filepath.Join("..", "..", ".test-results", fmt.Sprintf("acceptance-%d.json", time.Now().UnixNano()))
	}
	// err 接收 os.MkdirAll 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	// sourceResults 官方源文件到对应场景结果的映射，所有 standalone 构造片段都须有执行证据
	sourceResults := map[string]any{}
	// 逐项处理 manifest.Sources，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, source := range manifest.Sources {
		// state 当前步骤使用的字符串值 "PASSED"，用于路由、请求或断言
		state := "PASSED"
		// 逐项处理 source.Scenarios，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for _, scenario := range source.Scenarios {
			if strings.HasPrefix(results[scenario], "FAILED") {
				state = "FAILED"
				break
			}
			if results[scenario] != "PASSED" {
				state = "NOT_RUN"
			}
		}
		sourceResults[source.Source] = map[string]any{"status": state, "scenarios": source.Scenarios, "fragments": source.Fragments}
	}
	// data, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷
	data, _ := json.MarshalIndent(
		map[string]any{
			"server_version":   info.Version,
			"sdk_version":      wego.Version,
			"protocol_version": wire.Version,
			"command":          os.Getenv("WEGO_TEST_COMMAND"),
			"test_binary_args": os.Args,
			"generation_tools": map[string]string{"protoc": "29.6", "protoc-gen-go": "1.36.12", "protoc-gen-go-grpc": "1.5.1"},
			"mq":               "postgresql",
			"records":          report.Records,
			"results":          results,
			"sources":          sourceResults,
			"extra_tests":      manifest.ExtraTests,
			"history":          "run history retained under unique test namespaces",
		},
		"",
		"  ",
	)
	// err 接收 os.WriteFile 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("acceptance report: %s", path)
}
