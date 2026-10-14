//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/backend"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware/payload"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// nativeTaskTrigger 通过官方 REST 提交普通定义，目标类型来自实际注册而不是 payload 外形。
func nativeTaskTrigger(t *testing.T, ctx context.Context, observer *backend.Backend, name string, input any) string {
	t.Helper()
	config := observer.Config()
	data := consoleRequest(t, ctx, config, "/api/v1/stable/tenants/"+config.TenantID+"/workflow-runs/trigger", map[string]any{"workflowName": binding.NativeName(config.Namespace, name), "input": input, "return_only_id": true})
	// response 只读取官方生成的运行身份，不从本地输入合成。
	var response struct {
		// Run 是返回的持久运行资源。
		Run struct {
			// Metadata 包含实际 RunID。
			Metadata struct {
				// ID 供结果与终态查询使用。
				ID string `json:"id"`
			} `json:"metadata"`
		} `json:"run"`
	}
	if err := json.Unmarshal(data, &response); err != nil || response.Run.Metadata.ID == "" {
		t.Fatal(string(data), err)
	}
	return response.Run.Metadata.ID
}

// nativeBusinessResult 断言存储结果为原生业务值；已有 Worker codec 不能强迫输出包裹 _wego。
func nativeBusinessResult(t *testing.T, ctx context.Context, observer *backend.Backend, runID, taskName string, target any) {
	t.Helper()
	_, ref, err := observer.LookupRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := ref.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	value, present := result.Outputs[taskName]
	if !present {
		t.Fatal("missing native task output", taskName)
	}
	packet, err := wire.AsEnvelope(value)
	if err != nil || !packet.Native {
		t.Fatal("native output was protocol-encoded", err)
	}
	config := observer.Config()
	if err := wire.DecodeValue(ctx, "", packet, target, config, 1<<20, 4<<20); err != nil {
		t.Fatal(err)
	}
}

// TestProtocolV6NativeBusinessPaths 验证普通任务、protobuf、DAG、混合 Batch、durable 事件和官方 Webhook 的原生响应。
func TestProtocolV6NativeBusinessPaths(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	namespace := "native_paths_" + uuid.NewString() + "_"
	options := append(scenarios.Runtime(namespace), runtime.WithMiddleware(middleware.WithPayload(&payload.Gzip{})))
	conn, err := client.New(client.WithRuntime(options...))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()
	config := spec.Defaults()
	for _, option := range options {
		option(&config)
	}
	observer, err := backend.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := observer.Close(); err != nil {
			t.Error(err)
		}
	}()
	called := make(chan string, 4)
	echo := conn.NewStandaloneTask("echo", func(ctx context.Context, input map[string]any) (map[string]any, error) {
		if input["message"] == "webhook" {
			info, _ := task.Info(ctx)
			called <- info.RunID
		}
		return input, nil
	}, task.WithEvents("plain"))
	protobuf := conn.NewStandaloneTask("protobuf", func(_ context.Context, input *pb.Request) (*pb.Reply, error) {
		return &pb.Reply{Message: input.Message, Count: input.Count}, nil
	})
	workflow := conn.NewWorkflow("dag")
	parent := workflow.NewTask("parent", func(_ context.Context, input map[string]any) (map[string]any, error) { return input, nil })
	workflow.NewTask("child", func(ctx context.Context, _ map[string]any) (map[string]any, error) {
		// output 按父结果自身格式还原；native 调用不能要求父输出带协议标记。
		var output map[string]any
		if err := task.ParentOutput(ctx, "parent", &output); err != nil {
			return nil, err
		}
		return output, nil
	}).WithParents(parent)
	interval := 500 * time.Millisecond
	batchSizes := make(chan int, 2)
	batch := conn.NewStandaloneBatchTask("batch", func(_ context.Context, members map[string]*pb.Request) (map[string]*pb.Reply, error) {
		batchSizes <- len(members)
		output := make(map[string]*pb.Reply, len(members))
		for id, input := range members {
			output[id] = &pb.Reply{Message: input.Message, Count: input.Count}
		}
		return output, nil
	}, model.BatchConfig{MaxSize: 2, MaxInterval: interval})
	failureSizes := make(chan int, 2)
	failingBatch := conn.NewStandaloneBatchTask("batch-failure", func(_ context.Context, members map[string]*pb.Request) (map[string]*pb.Reply, error) {
		failureSizes <- len(members)
		return nil, status.Error(codes.Aborted, "batch rejected")
	}, model.BatchConfig{MaxSize: 2, MaxInterval: interval})
	started := make(chan struct{}, 1)
	durable := conn.NewStandaloneDurableTask("wait", func(ctx context.Context, _ map[string]any) (*pb.Reply, error) {
		started <- struct{}{}
		input := new(pb.Request)
		if err := task.WaitForEventInto(ctx, "resume", "true", input); err != nil {
			return nil, err
		}
		return &pb.Reply{Message: input.Message, Count: input.Count}, nil
	})
	worker, err := conn.NewWorker("", client.WithWorkflows(echo, protobuf, workflow, batch, failingBatch, durable))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	if err := worker.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	// cleanupOnce 使成功路径先完成清理再写证据，失败路径也只释放一次。
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			budget, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if err := worker.Shutdown(budget); err != nil {
				t.Error(err)
			}
			for _, name := range []string{"echo", "protobuf", "dag", "batch", "batch-failure", "wait"} {
				if _, err := conn.Workflows().Delete(budget, name); err != nil {
					t.Error(err)
				}
			}
		})
	}
	defer cleanup()
	runIDs := []string{}
	for _, name := range []string{"echo", "dag"} {
		runID := nativeTaskTrigger(t, ctx, observer, name, map[string]any{"value": 7, "routing": map[string]any{"message": "business"}})
		taskName := name
		if name == "dag" {
			taskName = "child"
		}
		// result 的同名字段属于业务数据，不能被输入识别拆掉。
		var result map[string]any
		nativeBusinessResult(t, ctx, observer, runID, taskName, &result)
		if result["value"] != json.Number("7") || result["routing"].(map[string]any)["message"] != "business" {
			t.Fatal(result)
		}
		runIDs = append(runIDs, runID)
	}
	protoRun := nativeTaskTrigger(t, ctx, observer, "protobuf", map[string]any{"message": "plain", "count": 9})
	reply := new(pb.Reply)
	nativeBusinessResult(t, ctx, observer, protoRun, "protobuf", reply)
	if reply.Message != "plain" || reply.Count != 9 {
		t.Fatal(reply)
	}
	runIDs = append(runIDs, protoRun)
	ref, err := batch.RunNoWait(ctx, &pb.Request{Message: "wego", Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	nativeBatch := nativeTaskTrigger(t, ctx, observer, "batch", map[string]any{"message": "native", "count": 2})
	result, err := ref.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := result.IntoContext(ctx, reply); err != nil || reply.Message != "wego" {
		t.Fatal(reply, err)
	}
	nativeBusinessResult(t, ctx, observer, nativeBatch, "batch", reply)
	if reply.Message != "native" || reply.Count != 2 {
		t.Fatal(reply)
	}
	select {
	case size := <-batchSizes:
		if size != 2 {
			t.Fatal("mixed contracts did not share a batch", size)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	runIDs = append(runIDs, ref.RunID, nativeBatch)
	// 整批失败也必须按成员契约上报：SDK 保留 status，原生运行只存普通错误文本。
	failedRef, err := failingBatch.RunNoWait(ctx, &pb.Request{Message: "wego"})
	if err != nil {
		t.Fatal(err)
	}
	nativeFailed := nativeTaskTrigger(t, ctx, observer, "batch-failure", map[string]any{"message": "native"})
	if _, err := failedRef.Result(ctx); status.Code(err) != codes.Aborted {
		t.Fatal("wego batch lost status", err)
	}
	_, nativeRef, err := observer.LookupRun(ctx, nativeFailed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nativeRef.Wait(ctx); err == nil || status.Code(err) != codes.Unknown || !strings.Contains(err.Error(), "batch rejected") {
		t.Fatal("native batch error contract changed", err)
	}
	select {
	case size := <-failureSizes:
		if size != 2 {
			t.Fatal("failure contracts did not share a batch", size)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	runIDs = append(runIDs, failedRef.RunID, nativeFailed)
	waitRun := nativeTaskTrigger(t, ctx, observer, "wait", map[string]any{})
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	completed := make(chan error, 1)
	go func() {
		_, ref, err := observer.LookupRun(ctx, waitRun)
		if err == nil {
			_, err = ref.Wait(ctx)
		}
		completed <- err
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for finished := false; !finished; {
		if err := observer.Feature(ctx, ports.EventsPush{Key: "resume", Input: map[string]any{"message": "native-event", "count": 3}}, nil); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-completed:
			if err != nil {
				t.Fatal(err)
			}
			finished = true
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	nativeBusinessResult(t, ctx, observer, waitRun, "wait", reply)
	if reply.Message != "native-event" {
		t.Fatal(reply)
	}
	runIDs = append(runIDs, waitRun)
	hookName := namespace + "hook"
	_, err = conn.Webhooks().Create(ctx, model.CreateWebhookOpts{Name: hookName, SourceName: model.Generic, EventKeyExpression: strconv.Quote(namespace + "plain"), Auth: model.APIKeyAuth{HeaderName: "X-Api-Key", APIKey: "fixture-secret"}})
	if err != nil {
		t.Fatal(err)
	}
	// hookDeleted 防止成功路径确认删除后再发起重复管理请求。
	hookDeleted := false
	defer func() {
		if hookDeleted {
			return
		}
		if err := conn.Webhooks().Delete(context.Background(), hookName); err != nil {
			t.Error(err)
		}
	}()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, config.ServerURL+"/api/v1/stable/tenants/"+observer.Config().TenantID+"/webhooks/"+hookName, bytes.NewBufferString(`{"message":"webhook"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Api-Key", "fixture-secret")
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	response.Body.Close()
	if readErr != nil || response.StatusCode >= 300 {
		t.Fatalf("native webhook status=%d body=%s error=%v", response.StatusCode, body, readErr)
	}
	select {
	case runID := <-called:
		// webhookResult 证明官方 HTTP 投递直接触发业务，没有经过 SDK 事件编码分支。
		var webhookResult map[string]any
		nativeBusinessResult(t, ctx, observer, runID, "echo", &webhookResult)
		if webhookResult["message"] != "webhook" {
			t.Fatal(webhookResult)
		}
		runIDs = append(runIDs, runID)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	info, err := conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Webhooks().Delete(ctx, hookName); err != nil {
		t.Fatal(err)
	}
	hookDeleted = true
	cleanup()
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := observer.Close(); err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		return
	}
	writeStreamEvidence(t, "v6-native-paths", map[string]any{"namespace": namespace, "server_version": info.Version, "run_ids": runIDs, "assertions": []string{"native JSON and protobuf", "DAG parent native output", "mixed batch response contracts", "mixed batch failure contracts", "durable native event decoding", "official native webhook invokes handler"}, "cleanup": "scoped workflows and webhook deleted; worker and connections closed; run history retained"})
}
