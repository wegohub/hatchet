package log_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/logging"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	wlog "github.com/hatchet-dev/hatchet/sdks/wego/log"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// lockedBuffer 允许并发日志及断言读取，避免测试输出容器自身产生 race。
type lockedBuffer struct {
	// mu 保护 buffer 的读写。
	mu sync.Mutex
	// buffer 保存 JSON 行，用于检查去重后的实际输出。
	buffer bytes.Buffer
}

// Write 满足 io.Writer，每次输出按一条记录串行写入。
func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

// rows 解码已经完成的记录，调用方须先等待本组日志结束。
func (b *lockedBuffer) rows(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	// rows 收集完成的 JSON 日志行，用于验证真实输出。
	var rows []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(b.buffer.String()), "\n") {
		if line == "" {
			continue
		}
		// row 是当前 JSON 行的独立解码目标。
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}

// configure 只替换 wego 输出并恢复它，检查标准 slog.Default 从未被 SDK 改写。
func configure(t *testing.T, handler slog.Handler) {
	t.Helper()
	old, standard := logging.Output(), slog.Default()
	c := spec.Defaults()
	runtime.WithLogger(slog.New(handler))(&c)
	t.Cleanup(func() { logging.SetOutput(old) })
	if slog.Default() != standard {
		t.Fatal("SDK changed slog.Default")
	}
}

// TestContextAttributes 验证后写覆盖、父子隔离、空属性及无名组。
func TestContextAttributes(t *testing.T) {
	b := &lockedBuffer{}
	configure(t, slog.NewJSONHandler(b, nil))
	parent := wlog.With(context.Background(), slog.String("group", "a"), slog.Int("count", 10))
	child := wlog.With(parent, slog.String("group", "b"), slog.Int("count", 20))
	if wlog.With(parent) != parent {
		t.Fatal("empty With changed context")
	}
	wlog.Info(child, "child", "count", 30, slog.Group("", slog.String("inline", "yes")), slog.Attr{})
	wlog.Info(parent, "parent")
	rows := b.rows(t)
	if len(rows) != 2 || rows[0]["group"] != "b" || rows[0]["count"] != float64(30) || rows[0]["inline"] != "yes" || rows[1]["count"] != float64(10) {
		t.Fatal(rows)
	}
}

// TestGroupedHandler 验证根 id 与 request.id 独立；同层具名组整体覆盖。
func TestGroupedHandler(t *testing.T) {
	b := &lockedBuffer{}
	base := slog.New(wlog.Handler(wlog.Handler(slog.NewJSONHandler(b, nil))))
	parent := base.With("id", "root", "count", 1)
	grouped := parent.WithGroup("request").With("id", "fixed", "count", 2)
	ctx := wlog.With(context.Background(), slog.String("id", "context"), slog.Int("count", 3))
	grouped.InfoContext(ctx, "grouped", "count", 4)
	parent.InfoContext(ctx, "root")
	base.Info("replace", slog.Group("data", slog.Int("a", 1)), slog.Group("data", slog.Int("b", 2)))
	rows := b.rows(t)
	request := rows[0]["request"].(map[string]any)
	if rows[0]["id"] != "root" || request["id"] != "context" || request["count"] != float64(4) || rows[1]["id"] != "context" {
		t.Fatal(rows)
	}
	data := rows[2]["data"].(map[string]any)
	if len(data) != 1 || data["b"] != float64(2) {
		t.Fatal(rows)
	}
}

// countedValue 确认禁用级别没有调用业务 LogValuer。
type countedValue struct {
	// calls 保存实际解析次数，支持并发断言。
	calls atomic.Int32
}

// LogValue 返回可检查的字段，同时记录解析次数。
func (v *countedValue) LogValue() slog.Value { v.calls.Add(1); return slog.StringValue("resolved") }

// TestLevelsSourceAndBoundLogger 验证动态级别、实际调用位置和标准派生日志器。
func TestLevelsSourceAndBoundLogger(t *testing.T) {
	b := &lockedBuffer{}
	// level 可动态修改，过滤与输出使用当前值。
	var level slog.LevelVar
	configure(t, slog.NewJSONHandler(b, &slog.HandlerOptions{Level: &level, AddSource: true}))
	v := &countedValue{}
	ctx := wlog.With(context.Background(), slog.Any("value", v))
	wlog.Debug(ctx, "disabled")
	if v.calls.Load() != 0 || wlog.Enabled(ctx, slog.LevelDebug) {
		t.Fatal("disabled log resolved attributes")
	}
	level.Set(slog.LevelDebug)
	_, _, line, _ := goruntime.Caller(0)
	wlog.Debug(ctx, "enabled")
	wlog.FromContext(ctx).With("component", "decoder").WithGroup("request").Info("bound", "id", 3)
	rows := b.rows(t)
	if rows[0]["source"].(map[string]any)["line"] != float64(line+1) || rows[0]["value"] != "resolved" {
		t.Fatal(rows)
	}
	if rows[1]["request"].(map[string]any)["value"] != "resolved" || rows[1]["component"] != "decoder" {
		t.Fatal(rows)
	}
}

// execution 只提供本场景使用的身份，其他任务能力不得被日志代码调用。
type execution struct {
	// Execution 嵌入未使用的能力，测试只覆盖 Info。
	ports.Execution
}

// Info 返回固定标量身份，不保存真实租户或业务名称。
func (*execution) Info() model.TaskInfo {
	return model.TaskInfo{RunID: "run-1", TaskRunID: "task-1", WorkerID: "worker-1", WorkerKey: "key-1", RetryCount: 2}
}

// failingHandler 提供明确本地错误，验证远程上报仍执行且两个错误都保留。
type failingHandler struct {
	// Handler 委托级别和派生方法，只有 Handle 注入失败。
	slog.Handler
	// err 本地写入失败的独立标记。
	err error
}

// Handle 返回本地故障，不执行额外的日志递归。
func (h *failingHandler) Handle(context.Context, slog.Record) error { return h.err }

// TestReportingAndIdentity 验证 R 系列打印、能力错误、取消打印、保护字段及远程结构化快照。
func TestReportingAndIdentity(t *testing.T) {
	b := &lockedBuffer{}
	configure(t, slog.NewJSONHandler(b, nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	wlog.Error(ctx, "canceled local")
	if err := wlog.RInfo(ctx, "ordinary"); !errors.Is(err, model.ErrTaskContext) {
		t.Fatal(err)
	}
	exec := &execution{}
	state := &callctx.State{Execution: exec}
	task := logging.WithTask(callctx.Bind(context.Background(), state), exec.Info())
	if err := wlog.RInfo(task, "disabled reporting"); !errors.Is(err, model.ErrLogReportDisabled) {
		t.Fatal(err)
	}
	// reported 保存已交给上报回调的记录快照。
	var reported slog.Record
	// reports 记录上报次数，禁用级别不能增加它。
	var reports int
	state.Report = func(ctx context.Context, record slog.Record) error { reports++; reported = record.Clone(); return nil }
	if err := wlog.RDebug(task, "filtered", slog.Any("unsupported", make(chan int))); err != nil || reports != 0 {
		t.Fatal(err, reports)
	}
	span := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}})
	task = trace.ContextWithSpanContext(wlog.With(task, slog.Int("count", 10)), span)
	if err := wlog.RInfo(task, "reported", "count", 20, "run_id", "spoof"); err != nil {
		t.Fatal(err)
	}
	metadata, err := logging.Metadata(reported)
	if err != nil {
		t.Fatal(err)
	}
	// attrs 按固定字段、context、调用字段依次合并，返回独立记录。
	var attrs map[string]any
	if err := json.Unmarshal(metadata, &attrs); err != nil {
		t.Fatal(err)
	}
	rows := b.rows(t)
	last := rows[len(rows)-1]
	if reports != 1 || attrs["count"] != float64(20) || attrs["run_id"] != "run-1" || attrs["trace_id"] != span.TraceID().String() || last["time"] != reported.Time.Format("2006-01-02T15:04:05.999999999Z07:00") {
		t.Fatal(rows, attrs)
	}
	local, remote := errors.New("local failure"), errors.New("remote failure")
	configure(t, &failingHandler{Handler: slog.NewJSONHandler(b, nil), err: local})
	state.Report = func(context.Context, slog.Record) error { reports++; return remote }
	err = wlog.RError(task, "both fail")
	if !errors.Is(err, local) || !errors.Is(err, remote) || reports != 2 {
		t.Fatal(err, reports)
	}
}

// TestConcurrentReplacement 每条日志固定一个出口，并发更换与写入不修改标准默认实例。
func TestConcurrentReplacement(t *testing.T) {
	b := &lockedBuffer{}
	configure(t, slog.NewJSONHandler(b, nil))
	// wg 确认本组自有协程全部退出，不能把启动当作完成。
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := range 50 {
				config := spec.Defaults()
				runtime.WithLogger(slog.New(slog.NewJSONHandler(b, nil)))(&config)
				wlog.Info(context.Background(), "concurrent", "n", i)
			}
		})
	}
	wg.Wait()
	if got := len(b.rows(t)); got != 400 {
		t.Fatal(got)
	}
}

// TestDefaultFallback 在独立进程验证未配置的普通日志使用标准默认输出。
func TestDefaultFallback(t *testing.T) {
	if os.Getenv("WEGO_LOG_DEFAULT_CHILD") == "1" {
		logging.SetOutput(nil)
		wlog.Info(context.Background(), "default-fallback")
		return
	}
	command := exec.Command(os.Args[0], "-test.run=^TestDefaultFallback$")
	command.Env = append(os.Environ(), "WEGO_LOG_DEFAULT_CHILD=1")
	data, err := command.CombinedOutput()
	if err != nil || !bytes.Contains(data, []byte("default-fallback")) {
		t.Fatal(fmt.Sprint(err), string(data))
	}
}
