//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestDurableRestart 重启独立 Worker 进程，验证 durable 重放复用子 RunID 和已记录结果
func TestDurableRestart(t *testing.T) {
	testDurableRestart(t, false)
}

// testDurableRestart 共用真实进程恢复流程；pending 为 true 时证明空载荷补算和再次恢复的持久复用
func testDurableRestart(t *testing.T, pending bool) {
	t.Helper()
	preflight(t)
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	// proxy 仅在 pending 分支丢弃完成帧，普通重启用例不增加故障代理
	var proxy *memoFaultProxy
	// proxyAddress 传给 Worker 子进程，父进程 Conn 保持真实引擎连接
	var proxyAddress string
	if pending {
		proxy, proxyAddress = startMemoFaultProxy(t)
	}

	// namespace 生成当前数据的文本表示，用于名称、业务比较或报告，不包含凭证
	namespace := fmt.Sprintf("wego_restart_%d_", time.Now().UnixNano())
	// ready 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	ready := make(chan struct{}, 4)
	// snapshots 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	snapshots := make(chan *pb.Reply, 16)
	// fixture 构造共享注册表的 Server，业务只在 Serve 启动后接收调用
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 按协议、输入类型或状态分派处理；每个分支只应用与该类型匹配的转换和状态更新
		switch r.URL.Path {
		case "/ready":
			ready <- struct{}{}
		case "/snapshot":
			// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
			var out pb.Reply
			// e 保存 JSON 解析结果；解码失败不使用目标的零值作为有效业务数据
			if e := json.NewDecoder(r.Body).Decode(&out); e != nil {
				http.Error(w, e.Error(), 400)
				return
			}
			snapshots <- &out
		default:
			http.NotFound(w, r)
		}
	}))
	defer fixture.Close()

	// 组合仓库与资源路径，扫描、生成和清理使用同一位置
	binary := filepath.Join(t.TempDir(), "durable-worker")
	// build 启动受预算约束的独立进程，取消后测试不能遗留子进程
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./fixtures/durable-worker")
	// data, e 接收 build.CombinedOutput 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
	if data, e := build.CombinedOutput(); e != nil {
		t.Fatalf("build fixture: %v\n%s", e, data)
	}
	// process 当前独立 Worker 子进程，重启前终止并确认退出，避免两个进程混用同一 fixture
	var process *exec.Cmd
	// start 启动独立 Worker 进程并检查真实消费就绪，重启不能靠固定等待猜测成功
	start := func() {
		process = exec.CommandContext(ctx, binary)
		process.Env = append(os.Environ(), "WEGO_RESTART_NAMESPACE="+namespace, "WEGO_RESTART_FIXTURE_URL="+fixture.URL, "WEGO_RESTART_PROXY_ADDRESS="+proxyAddress)
		// logFile, e 创建仅用于此测试的进程输出文件，不写入凭证
		logFile, e := os.CreateTemp(t.TempDir(), "worker-*.log")
		if e != nil {
			t.Fatal(e)
		}
		process.Stdout, process.Stderr = logFile, logFile
		if e = process.Start(); e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() {
			logFile.Close()
		})
		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("worker readiness:", ctx.Err())
		}
	}
	// stop 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据
	stop := func() {
		if process != nil && process.Process != nil {
			_ = process.Process.Kill()
			_ = process.Wait()
			process = nil
		}
	}
	defer stop()

	start()
	// conn, err 接收 client.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	conn, err := client.New(client.WithRuntime(scenarios.Runtime(namespace)...))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// cleaned 是否已完成本用例的资源清理，避免正常路径与 defer 重复删除同一资源
	cleaned := false
	// cleanupResources 在独立 Worker 重启后删除本轮任务定义，运行历史按 namespace 保留
	cleanupResources := func() error {
		// cleanup, stop 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()

		// errs 关闭或多项验收中的错误集合，结束时合并报告而不丢失后续清理失败
		var errs []error
		// 逐项处理 pb.UnaryGreeter_ServiceDesc.Methods，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for _, d := range pb.UnaryGreeter_ServiceDesc.Methods {
			// _, e 操作实际工作流定义，删除成功会使共享名称缓存失效
			_, e := conn.Workflows().Delete(cleanup, "/wego.example.v1.UnaryGreeter/"+d.MethodName)
			errs = append(errs, e)
		}
		return errors.Join(errs...)
	}
	defer func() {
		if !cleaned {
			// e 接收 cleanupResources 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
			if e := cleanupResources(); e != nil {
				t.Error(e)
			}
		}
	}()

	// ref, err 接收 conn.RunNoWait 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	ref, err := conn.RunNoWait(ctx, pb.UnaryGreeter_WaitHello_FullMethodName, &pb.Request{Message: "durable restart"})
	if err != nil {
		t.Fatal(err)
	}
	// first, resumed 驱逐或重启前后的业务输出，比较子 RunID 和已记录结果以验证 durable 重放
	var first, resumed *pb.Reply
	// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
	select {
	case first = <-snapshots:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if first.RunId != ref.RunID || first.ChildRunId == "" || first.ProcessedAt == "" {
		t.Fatalf("first snapshot: %v", first)
	}
	if pending {
		select {
		case <-proxy.dropped:
		case <-ctx.Done():
			t.Fatal("memo fault was not injected:", ctx.Err())
		}
	}
	// expectedTime 为已完成 memo 的值；pending 恢复可以补算新值，后续重启则必须复用它
	expectedTime := first.ProcessedAt
	stop()
	start()
	// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
	select {
	case resumed = <-snapshots:
	case <-ctx.Done():
		t.Fatal("durable did not resume after owner exit:", ctx.Err())
	}
	if resumed.RunId != first.RunId || resumed.ChildRunId != first.ChildRunId || resumed.WorkerId == first.WorkerId || resumed.InvocationCount < 2 {
		t.Fatalf("restart did not reuse memo/child/waits: first=%v resumed=%v", first, resumed)
	}
	if !pending && resumed.ProcessedAt != expectedTime {
		t.Fatalf("completed memo changed after restart: %v %v", first, resumed)
	}
	if pending {
		// 恢复必须实际观察到引擎已存在且无 payload 的 ACK，而非只重跑已完成的 memo
		select {
		case <-proxy.pending:
		case <-ctx.Done():
			t.Fatal("engine did not replay a pending memo:", ctx.Err())
		}
		if resumed.ProcessedAt == first.ProcessedAt || resumed.ProcessedAt == "" {
			t.Fatal("pending memo was not recomputed")
		}
		expectedTime = resumed.ProcessedAt
		stop()
		start()
		// persisted 来自第三个 Worker；必须证明补算值已经进入引擎持久账本
		var persisted *pb.Reply
		select {
		case persisted = <-snapshots:
		case <-ctx.Done():
			t.Fatal("completed memo did not resume:", ctx.Err())
		}
		if persisted.ProcessedAt != expectedTime || persisted.RunId != first.RunId || persisted.ChildRunId != first.ChildRunId || persisted.InvocationCount < 3 || persisted.WorkerId == resumed.WorkerId {
			t.Fatalf("recomputed memo was not persisted: second=%v third=%v", resumed, persisted)
		}
		resumed = persisted
	}
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		if err = conn.Events().Push(ctx, "restart:finish", map[string]string{"run_id": ref.RunID}); err != nil {
			t.Fatal(err)
		}
		// state, e 通过明确 RunID 查询或管理执行记录，结果必须来自实际引擎
		state, e := conn.Runs().GetStatus(ctx, ref.RunID)
		if e == nil && state == model.Completed {
			break
		}
		if e == nil && (state == model.Failed || state == model.Cancelled) {
			t.Fatalf("restart run terminated: %s", state)
		}
		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	// result, err 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据
	result, err := ref.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
	var out pb.Reply
	if err = result.Into(&out); err != nil {
		t.Fatal(err)
	}
	if out.RunId != first.RunId || out.ChildRunId != first.ChildRunId || out.ProcessedAt != expectedTime {
		t.Fatalf("final restart result: %v", &out)
	}
	stop()
	// info, err 接收 conn.Info 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	info, err := conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = cleanupResources(); err != nil {
		t.Fatal(err)
	}
	cleaned = true
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	// 组合仓库与资源路径，扫描、生成和清理使用同一位置
	prefix := "durable-restart"
	if pending {
		prefix = "durable-memo-recovery"
	}
	// path 区分普通重启与真实 pending 故障恢复证据，不能互相作为替代
	path := filepath.Join("..", "..", ".test-results", fmt.Sprintf("%s-%d.json", prefix, time.Now().UnixNano()))
	// data, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷
	data, _ := json.MarshalIndent(
		map[string]any{
			"status":                "PASSED",
			"sdk_version":           wego.Version,
			"protocol_version":      4,
			"pending_memo_injected": pending,
			"recomputed_time":       expectedTime,
			"command":               os.Getenv("WEGO_TEST_COMMAND"),
			"server_version":        info.Version,
			"mq":                    "postgresql",
			"namespace":             namespace,
			"run_id":                out.RunId,
			"child_run_id":          out.ChildRunId,
			"before_worker_id":      first.WorkerId,
			"after_invocation":      resumed.InvocationCount,
			"after_worker_id":       out.WorkerId,
			"assertions": []string{
				"worker process exited and restarted",
				"same parent and child RunID",
				"completed memo reused; pending memo recomputed then persisted when injected",
				"wait sequence resumed",
				"completed correlated event",
				"engine redispatch preserves durable state",
			},
			"cleanup": "worker processes stopped; workflow definitions deleted; history retained",
		},
		"",
		"  ",
	)
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	t.Log("restart report:", path)
}
