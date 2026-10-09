//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestStreamWorkerRestart 杀死已经确认两条输出的实际 Worker，验证引擎重投和新进程 checkpoint 恢复
func TestStreamWorkerRestart(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	namespace := fmt.Sprintf("stream_restart_%d", time.Now().UnixNano())
	ready, started, resumed := make(chan model.TaskInfo, 2), make(chan model.TaskInfo, 1), make(chan model.TaskInfo, 1)
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// info 来自实际执行或就绪屏障，不能按输入推测任务身份
		var info model.TaskInfo
		if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
			http.Error(w, "invalid fixture identity", 400)
			return
		}
		// target 按通知类型路由，不混用启动和接管屏障
		var target chan model.TaskInfo
		switch r.URL.Path {
		case "/ready":
			target = ready
		case "/started":
			target = started
		case "/resumed":
			target = resumed
		default:
			http.NotFound(w, r)
			return
		}
		select {
		case target <- info:
		case <-r.Context().Done():
		}
	}))
	defer fixture.Close()
	// binary 固定本轮 SDK 实现，两个进程执行同一注册定义和重试策略
	binary := filepath.Join(t.TempDir(), "stream-worker")
	build := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, "./fixtures/stream-worker")
	if data, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build fixture: %v\n%s", err, data)
	}
	// process 只指向当前测试子进程；重启前先终止并确认退出
	var process *exec.Cmd
	stop := func() {
		if process != nil {
			_ = process.Process.Kill()
			_ = process.Wait()
			process = nil
		}
	}
	defer stop()
	start := func() {
		process = exec.CommandContext(ctx, binary)
		process.Env = append(os.Environ(), "WEGO_STREAM_RESTART_NAMESPACE="+namespace, "WEGO_STREAM_RESTART_FIXTURE="+fixture.URL)
		logFile, err := os.CreateTemp(t.TempDir(), "worker-*.log")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { logFile.Close() })
		process.Stdout, process.Stderr = logFile, logFile
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("worker readiness:", ctx.Err())
		}
	}
	conn, err := client.New(client.WithRuntime(scenarios.Runtime(namespace)...))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	start()
	watch, err := pb.NewGreeterClient(conn).WatchHellos(client.WithIdempotencyKey(ctx, "restart-operation"), &pb.Request{Count: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 2; i++ {
		reply, err := watch.Recv()
		if err != nil || reply.Count != i {
			t.Fatal(reply, err)
		}
	}
	// before、after 保存不同进程的真实身份，用于核对同一逻辑运行
	var before, after model.TaskInfo
	select {
	case before = <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	checkpoint, err := client.StreamCheckpoint(watch.Context())
	if err != nil || checkpoint.OutputSeq != 2 {
		t.Fatal(checkpoint, err)
	}
	stop()
	start()
	for i := int32(2); i < 4; i++ {
		reply, err := watch.Recv()
		if err != nil || reply.Count != i || reply.RunId != before.RunID {
			t.Fatal(reply, err)
		}
	}
	if _, err := watch.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	select {
	case after = <-resumed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if before.RunID != after.RunID || before.TaskRunID != after.TaskRunID || before.WorkerKey == after.WorkerKey || after.RetryCount != 1 {
		t.Fatalf("invalid recovery: before=%+v after=%+v", before, after)
	}
	// 已知 RunID 的恢复只消费剩余输出，不再次启动 handler，也不依赖幂等键 TTL
	raw, err := conn.ResumeStream(ctx, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(2); i < 4; i++ {
		// reply 是断点之后的一条输出，序号必须从 2 接续
		var reply pb.Reply
		if err := raw.RecvMsg(&reply); err != nil || reply.Count != i {
			t.Fatal(&reply, err)
		}
	}
	if err := raw.RecvMsg(&pb.Reply{}); err != io.EOF {
		t.Fatal(err)
	}
	stop()
	for _, method := range pb.Greeter_ServiceDesc.Methods {
		if _, err := conn.Workflows().Delete(ctx, "/wego.example.v1.Greeter/"+method.MethodName); err != nil {
			t.Fatal(err)
		}
	}
	for _, method := range pb.Greeter_ServiceDesc.Streams {
		if _, err := conn.Workflows().Delete(ctx, "/wego.example.v1.Greeter/"+method.StreamName); err != nil {
			t.Fatal(err)
		}
	}
	info, err := conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writeStreamEvidence(t, "stream-restart", map[string]any{"server_version": info.Version, "namespace": namespace, "run_id": before.RunID, "task_id": before.TaskRunID, "before_worker_key": before.WorkerKey, "after_worker_key": after.WorkerKey, "epoch": after.RetryCount, "assertions": []string{"SIGKILL after two durable outputs", "same run/task IDs after engine retry", "new process gets a new worker key", "handler restored two historical outputs", "continuous four outputs and authoritative EOF", "client checkpoint replays only remaining outputs"}, "cleanup": "processes stopped; definitions deleted; run and topic history retained"})
}
