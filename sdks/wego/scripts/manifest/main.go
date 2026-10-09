// manifest 枚举官方示例中的每个 standalone 定义，并校验验收映射是否完整
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// fragment 官方源文件内一个 standalone 构造片段的位置与任务名称
type fragment struct {
	// ID 不依赖行号，源路径、函数和任务名共同标识同一业务片段
	ID string `json:"id"`
	// RequiredAssertions 是逐片段显式关联的实际执行断言，不能由场景成功推导
	RequiredAssertions []assertionRef `json:"required_assertions"`
	// Function 业务 handler 或验收片段的函数名
	Function string `json:"function"`
	// Constructor 官方示例片段调用的任务构造函数
	Constructor string `json:"constructor"`
	// Task 验收片段中的任务名称
	Task string `json:"task"`
	// Line 官方源文件中的声明行号，便于定位验收范围
	Line int `json:"line"`
}

// entry 一个官方源文件到 wego 示例、场景及测试的验收映射
type entry struct {
	// Source 官方源文件路径
	Source string `json:"source"`
	// Fragments 该文件中所有 standalone 构造片段的清单
	Fragments []fragment `json:"fragments"`
	// Scenarios 对应的可执行 wego 验收场景名称集合
	Scenarios []string `json:"scenarios"`
	// Example 可以独立运行的示例路径
	Example string `json:"example"`
	// Tests 覆盖此条目行为的测试入口
	Tests []string `json:"tests"`
	// Command 对应 JSON 字段 command，用于将受控请求或验收数据解码到此结构
	Command string `json:"command"`
}

// manifest 完整验收清单，关联 SDK 版本、协议版本及所有官方片段
type manifest struct {
	// Version SDK、服务或协议版本；各自用于报告或兼容性校验
	Version string `json:"sdk_version"`
	// Protocol wego wire 协议版本，与实际编码器保持一致
	Protocol int `json:"protocol_version"`
	// Sources 官方源文件与验收条目的映射集合
	Sources []entry `json:"sources"`
	// ExtraScenarios 不属于官方 standalone 片段的扩展场景，如三种流
	ExtraScenarios []string `json:"extra_scenarios"`
	// ExtraTests 扩展验收组到测试入口的映射
	ExtraTests map[string]string `json:"extra_tests"`
	// Assertions 描述测试预期保证的断言
	Assertions string `json:"assertions"`
	// Parameters 运行命令所需的公开配置，不包含凭证
	Parameters map[string]string `json:"parameters"`
}

// scenarios 保存此作用域的初始配置或查找表，后续调用使用相同值保持注册与执行一致
var scenarios = map[string][]string{
	"simple/main.go":                        {"simple", "child-workflows"},
	"retries/main.go":                       {"retries"},
	"concurrency/main.go":                   {"concurrency"},
	"rate-limiting/main.go":                 {"rate-limiting"},
	"slot-cost/main.go":                     {"slot-cost"},
	"runtime-affinity/main.go":              {"runtime-affinity"},
	"sticky-workers/main.go":                {"sticky-workers"},
	"child-workflows/main.go":               {"child-workflows"},
	"cron/main.go":                          {"cron"},
	"events/main.go":                        {"events"},
	"on-event/main.go":                      {"on-event"},
	"webhooks/main.go":                      {"webhooks"},
	"idempotency/worker.go":                 {"idempotency"},
	"logs_test/main.go":                     {"logs"},
	"panic-handler/main.go":                 {"panic-handler"},
	"opentelemetry_instrumentation/main.go": {"observability"},
	"streaming/shared/task.go":              {"streaming"},
	"embedded/main.go":                      {"embedded"},
	"stubs/stub-workflow.go":                {"stubs"},
	"sdk-migration/main.go":                 {"sdk-migration"},
	"sdk-migration/v1.go":                   {"sdk-migration-v1"},
	"migration-guides/mergent.go":           {"mergent"},
	"migration-guides/temporal.go": {
		"temporal",
		"concurrency",
		"rate-limiting",
		"logs",
		"cron",
	},
	"durable/sleep/main.go":            {"durable-sleep"},
	"durable/event/main.go":            {"durable-event"},
	"durable/eviction/main.go":         {"eviction"},
	"durable/eviction/trigger/main.go": {"eviction"},
	"batch_assign/main.go":             {"batch"},
}

// build 扫描官方 standalone 构造片段并生成可执行验收映射，保留源文件行号
func build(repo string) (manifest, error) {
	// coverage 由维护者逐片段定义，不能自动把文件级通过复制给每个构造片段
	coverage := map[string][]assertionRef{}
	// data 是显式断言契约；读取或解析失败必须阻止生成
	data, err := os.ReadFile(filepath.Join(repo, "sdks/wego/examples/fragment_assertions.json"))
	if err != nil {
		return manifest{}, err
	}
	if err = json.Unmarshal(data, &coverage); err != nil {
		return manifest{}, err
	}
	// m 构造当前步骤的数据对象，字段在提交、编码或断言之前一次性初始化
	m := manifest{
		Version:  wego.Version,
		Protocol: wire.Version,
		ExtraScenarios: []string{
			"grpc-streams",
			"stream-faults",
			"shutdown",
			"middleware",
			"durable-restart",
			"batch-shutdown",
		},
		Assertions: "Scenario implementations assert execution results, policies, identities and cleanup; evidence is recorded in .test-results/acceptance-*.json.",
		Parameters: map[string]string{
			"api":       "http://localhost:8080",
			"grpc":      "localhost:7077",
			"tls":       "plaintext",
			"mq":        "postgresql",
			"token":     "HATCHET_CLIENT_TOKEN or WEGO_TOKEN_FILE; never recorded",
			"namespace": "unique per scenario",
			"history":   "retained; workflow/trigger resources deleted",
			"embedded":  "independent process, database, ports; e2e,wego_embedded tags",
		},
	}
	m.ExtraTests = map[string]string{
		"disabled-methods":       "TestDisabledWorkerMethods",
		"dual-entry":             "TestExamples/dual-entry",
		"logging-shutdown":       "TestLoggingShutdown",
		"grpc-streams":           "TestExamples/grpc-streams",
		"stream-faults":          "TestStreamFaults",
		"stream-recovery":        "TestReliableStreamRecovery",
		"stream-restart":         "TestStreamWorkerRestart",
		"worker-events":          "TestWorkerEvents",
		"stream-submission":      "TestStreamAmbiguousSubmissionCancellation",
		"stream-realtime":        "TestRealtimeStreams",
		"result-cancel":          "TestResultCancellationCancelsSharedRun",
		"shutdown":               "TestExamples/shutdown",
		"middleware":             "TestExamples/middleware",
		"minio-codec":            "TestMinIOCodec",
		"durable-restart":        "TestDurableRestart",
		"durable-memo-recovery":  "TestDurablePendingMemoRecovery",
		"stream-cost":            "TestReviewStreamingCost",
		"stream-cost-concurrent": "TestReviewConcurrentStreamingCost",
		"batch-shutdown":         "TestBatchShutdownBudget",
	}
	// paths 本步骤需要遍历的名称或路径列表，注册、查询和清理使用同一集合以保持对应
	paths := []string{}
	// 逐项处理 scenarios，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for path := range scenarios {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	// 逐项处理 paths，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, path := range paths {
		// fset 记录官方构造片段的源码位置，稳定身份不依赖可能改变的行号
		fset := token.NewFileSet()
		// file, err 接收 parser.ParseFile 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		file, err := parser.ParseFile(fset, filepath.Join(repo, "sdks/go/examples", path), nil, 0)
		if err != nil {
			return m, err
		}

		// example 当前官方源文件对应的可运行示例路径，按场景别名转换后写入 manifest
		example := path
		// 按协议、输入类型或状态分派处理；每个分支只应用与该类型匹配的转换和状态更新
		switch path {
		case "sdk-migration/v1.go":
			example = "sdk-migration/v1/main.go"
		case "migration-guides/mergent.go":
			example = "migration-guides/mergent/main.go"
		case "migration-guides/temporal.go":
			example = "migration-guides/temporal/main.go"
		case "streaming/shared/task.go":
			example = "streaming/main.go"
		}
		// e 构造当前步骤的数据对象，字段在提交、编码或断言之前一次性初始化
		e := entry{
			Source:    "sdks/go/examples/" + path,
			Scenarios: scenarios[path],
			Example:   "sdks/wego/examples/" + example,
		}
		// 逐项处理 file.Decls，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for _, decl := range file.Decls {
			// fn, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值
			fn, ok := decl.(*ast.FuncDecl)
			// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值
			if !ok {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				// call, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值
				call, ok := node.(*ast.CallExpr)
				// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值
				if !ok {
					return true
				}

				// sel, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !strings.HasPrefix(sel.Sel.Name, "NewStandalone") {
					return true
				}

				// name 注册或查询时使用的名称；必须与提交任务的名称对应
				name := "dynamic"
				if len(call.Args) > 0 {
					// value 是构造函数的字面量名称；动态表达式另用源码位置标识，不能猜测任务名
					if value, ok := call.Args[0].(*ast.BasicLit); ok {
						name = strings.Trim(value.Value, "\"")
					}
				}
				// id 保持源码移动行号后仍可关联相同片段
				id := e.Source + "::" + fn.Name.Name + "::" + name
				e.Fragments = append(e.Fragments, fragment{
					ID: id, RequiredAssertions: coverage[id],
					Function:    fn.Name.Name,
					Constructor: sel.Sel.Name,
					Task:        name,
					Line:        fset.Position(call.Pos()).Line,
				})
				return true
			})
		}
		if len(e.Fragments) == 0 {
			return m, fmt.Errorf("source contains no standalone declarations: %s", path)
		}

		// 逐项处理 e.Scenarios，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for _, scenario := range e.Scenarios {
			e.Tests = append(e.Tests, "TestExamples/"+scenario)
		}
		e.Command = "sdks/wego/scripts/local-test.py go test -tags=e2e ./sdks/wego/tests/e2e/... -run 'TestExamples/(" + strings.Join(e.Scenarios, "|") + ")$' -v -timeout 10m"
		// 嵌入引擎使用独立进程验收入口，设置对应 build tag，避免影响普通测试环境
		if path == "embedded/main.go" {
			e.Tests = []string{"TestEmbedded"}
			e.Command = "sdks/wego/scripts/local-test.py go test -tags=e2e,wego_embedded ./sdks/wego/tests/e2e/... -run TestEmbedded -v -timeout 10m"
		}
		// fragment 的每项必需断言必须显式声明，新增示例不能静默借用整体场景状态
		for _, fragment := range e.Fragments {
			if len(fragment.RequiredAssertions) == 0 {
				return m, fmt.Errorf("missing fragment assertions: %s", fragment.ID)
			}
		}
		m.Sources = append(m.Sources, e)
	}
	return m, nil
}

// main 组织此示例或测试进程的初始化、执行和清理；错误以日志或非零退出码报告，不把启动成功代替业务成功
func main() {
	// repo 是输入仓库根目录，官方示例与显式断言契约从同一根目录读取
	repo := flag.String("repo", ".", "repository root")
	// out 是显式 manifest 输出路径；生成器不会修改官方示例源文件
	out := flag.String("out", "sdks/wego/examples/acceptance.json", "manifest path")
	// check 为 true 时只比较可复现输出，不覆盖待核对的 manifest
	check := flag.Bool("check", false, "verify reproducibility")
	flag.Parse()
	// m, err 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值
	m, err := build(*repo)
	if err != nil {
		panic(err)
	}
	// data, err 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		panic(err)
	}
	data = append(data, '\n')
	// 组合仓库与资源路径，扫描、生成和清理使用同一位置
	path := filepath.Join(*repo, *out)
	// 检查模式只比较现有 manifest，不写文件；生成值与现有值不同时明确失败
	if *check {
		// existing, err 接收 os.ReadFile 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		existing, err := os.ReadFile(path)
		if err != nil {
			panic(err)
		}
		// 生成结果与现有数据不同，拒绝把陈旧的协议或 manifest 视为可复现
		if !bytes.Equal(existing, data) {
			panic("acceptance manifest is stale")
		}
		return
	}
	if err = os.WriteFile(path, data, 0644); err != nil {
		panic(err)
	}
}

// assertionRef 指向实际断言身份和执行场景；Assertion 为便于人工复核的文字契约
type assertionRef struct {
	// Scenario 必须与运行器实际收集的 test_scenario 一致
	Scenario string `json:"scenario"`
	// ID 是断言文字的 SHA-256 前 24 位，文字变化需显式更新映射
	ID string `json:"id"`
	// Assertion 描述必须实际验证的数据或行为
	Assertion string `json:"assertion"`
}
