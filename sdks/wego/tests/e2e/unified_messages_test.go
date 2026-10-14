//go:build e2e

package e2e

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware/payload"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// TestUnifiedMessages 验证真实普通任务、protobuf 任务、DAG、Batch 与 durable 事件全部执行同一 codec。
func TestUnifiedMessages(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	key, err := hex.DecodeString(os.Getenv("WEGO_CODEC_AES_KEY"))
	if err != nil || len(key) != 32 {
		t.Fatal("WEGO_CODEC_AES_KEY must be configured")
	}
	encrypted, err := payload.NewAESGCM(key)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := payload.NewS3(ctx, payload.ConfigFromEnv(), "unified-"+uuid.NewString()+"/")
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	for _, scenario := range []struct {
		// name 表示是否使用 codec，不包含运行环境的业务命名。
		name string
		// chain 在所有业务路径共用，下载与解密不能只出现在 RPC 入口。
		chain []middleware.Option
	}{{"bytes", nil}, {"gzip-aes-s3", []middleware.Option{middleware.WithPayload(&payload.Gzip{}), middleware.WithPayload(encrypted), middleware.WithPayload(objects)}}} {
		t.Run(scenario.name, func(t *testing.T) {
			namespace := "unified_" + uuid.NewString() + "_"
			options := append(scenarios.Runtime(namespace), runtime.WithMiddleware(scenario.chain...), runtime.WithRoutingDefaults("echo", map[string]any{"group": "one", "cost": json.Number("1e3")}), runtime.WithEventRoutingDefaults("resume", map[string]any{"group": "one"}))
			conn, err := client.New(client.WithRuntime(options...))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := conn.Close(); err != nil {
					t.Error(err)
				}
			}()
			// filtered 只记录真实事件执行；直接 Run 不带 FilterPayload。
			filtered := make(chan string, 1)
			echo := conn.NewStandaloneTask("echo", func(ctx context.Context, input map[string]any) (map[string]any, error) {
				info, _ := task.Info(ctx)
				if input["event"] == true {
					select {
					case filtered <- info.RunID:
					default:
					}
					if info.FilterPayload["source"] != "fixture" || input["integer"] != json.Number("9007199254740993") {
						return nil, fmt.Errorf("event or filter payload was not decoded")
					}
				}
				return input, nil
			}, task.WithEvents("created"),
				task.WithCron("0 0 1 1 *"), task.WithCronInput(model.Input{Value: map[string]any{"integer": 7}}),
				task.WithDefaultFilters(model.DefaultFilter{Expression: "payload.routing.channel == 'fixture'", Scope: "fixture", Payload: model.Input{Value: map[string]any{"source": "fixture"}, Routing: map[string]any{"channel": "fixture"}}}))
			releaseAsync := make(chan struct{})
			async := conn.NewStandaloneTask("async", func(ctx context.Context, input map[string]any) (map[string]any, error) {
				select {
				case <-releaseAsync:
					return input, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			})
			stopOutput := make(chan struct{})
			nativeStream := conn.NewStandaloneTask("native-stream", func(ctx context.Context, _ map[string]any) (map[string]any, error) {
				ticker := time.NewTicker(50 * time.Millisecond)
				defer ticker.Stop()
				for {
					if err := task.StreamEvent(ctx, []byte{0xff, 0, 0xfe}); err != nil {
						return nil, err
					}
					select {
					case <-stopOutput:
						return map[string]any{"completed": true}, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-ticker.C:
					}
				}
			})
			nativeProto := conn.NewStandaloneTask("native-proto", func(ctx context.Context, input *pb.Request) (*pb.Reply, error) {
				return &pb.Reply{Message: input.Message, Count: input.Count}, nil
			})
			started := make(chan struct{}, 1)
			durable := conn.NewStandaloneDurableTask("wait", func(ctx context.Context, input map[string]any) (map[string]any, error) {
				started <- struct{}{}
				result := new(pb.Request)
				if err := task.WaitForEventInto(ctx, "resume", "input.routing.group == 'one'", result); err != nil {
					return nil, err
				}
				return map[string]any{"message": result.Message, "count": result.Count}, nil
			})
			workflow := conn.NewWorkflow("dag")
			parent := workflow.NewTask("parent", func(context.Context, map[string]any) (map[string]any, error) {
				return map[string]any{"integer": int64(9007199254740993)}, nil
			})
			workflow.NewTask("child", func(ctx context.Context, _ map[string]any) (map[string]any, error) {
				// restored 保留 codec 内的大整数，ParentOutput 不经过 float64 映射。
				var restored map[string]any
				if err := task.ParentOutput(ctx, parent.GetName(), &restored); err != nil {
					return nil, err
				}
				return restored, nil
			}).WithParents(parent)
			interval := 100 * time.Millisecond
			batch := conn.NewStandaloneBatchTask("batch", func(ctx context.Context, inputs map[string]*pb.Request) (map[string]*pb.Reply, error) {
				output := make(map[string]*pb.Reply, len(inputs))
				for id, input := range inputs {
					output[id] = &pb.Reply{Message: input.Message, Count: input.Count}
				}
				return output, nil
			}, model.BatchConfig{MaxSize: 2, MaxInterval: interval})
			worker, err := conn.NewWorker("", client.WithWorkflows(echo, async, nativeStream, nativeProto, durable, workflow, batch))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := worker.Start(); err != nil {
				t.Fatal(err)
			}
			if err := worker.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			defer func() {
				budget, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				if err := worker.Shutdown(budget); err != nil {
					t.Error(err)
				}
			}()
			// 顺序副本注册不得因随机密文改变 workflow 版本。
			before, err := unifiedVersion(ctx, conn, echo.GetName())
			if err != nil {
				t.Fatal(err)
			}
			replica, err := conn.NewWorker("", client.WithWorkflows(echo))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := replica.Start(); err != nil {
				t.Fatal(err)
			}
			if err := replica.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			after, err := unifiedVersion(ctx, conn, echo.GetName())
			if err != nil {
				t.Fatal(err)
			}
			if before != after {
				t.Fatal("replica changed static-input definition version")
			}
			if err := replica.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			runIDs := []string{}
			// 提交 deadline 到期后，异步业务仍等待独立释放，不继承该绝对时间。
			submitCtx, stopSubmit := context.WithTimeout(ctx, 2*time.Second)
			asyncRef, err := async.RunNoWait(submitCtx, map[string]any{"value": 7})
			if err != nil {
				stopSubmit()
				t.Fatal(err)
			}
			<-submitCtx.Done()
			stopSubmit()
			close(releaseAsync)
			if _, err := asyncRef.Result(ctx); err != nil {
				t.Fatal("async execution inherited submission deadline", err)
			}
			runIDs = append(runIDs, asyncRef.RunID)
			streamRef, err := nativeStream.RunNoWait(ctx, map[string]any{})
			if err != nil {
				t.Fatal(err)
			}
			consume, stopConsume := context.WithCancel(ctx)
			messages, streamErrors := conn.Runs().SubscribeToStream(consume, streamRef.RunID)
			select {
			case message := <-messages:
				if message != string([]byte{0xff, 0, 0xfe}) {
					t.Fatal("native stream bytes changed")
				}
			case err := <-streamErrors:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			close(stopOutput)
			stopConsume()
			// 等到订阅真实退出，保证关闭流程不遗留 codec 下载。
			for range messages {
			}
			for range streamErrors {
			}
			if _, err := streamRef.Result(ctx); err != nil {
				t.Fatal(err)
			}
			runIDs = append(runIDs, streamRef.RunID)
			// 事件输入及 Filter payload 必须经过相同 codec，调度仍可读取明文 routing。
			filterScope := "fixture"
			if err := conn.Events().Push(ctx, "created", map[string]any{"event": true, "integer": int64(9007199254740993)}, filterScope); err != nil {
				t.Fatal(err)
			}
			// filteredRun 来自实际事件 handler，后续检查该运行的权威终态。
			var filteredRun string
			select {
			case filteredRun = <-filtered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err := unifiedWaitCompleted(ctx, conn, filteredRun); err != nil {
				t.Fatal(err)
			}
			runIDs = append(runIDs, filteredRun)
			result, err := echo.Run(ctx, map[string]any{"integer": int64(9007199254740993), "null": nil})
			if err != nil {
				t.Fatal(err)
			}
			// output 是业务结果；Raw 仍须保留存储封包，不能自动执行对象下载。
			var output map[string]any
			if err := result.IntoContext(ctx, &output); err != nil || output["integer"] != json.Number("9007199254740993") || output["null"] != nil {
				t.Fatal(output, err)
			}
			runIDs = append(runIDs, result.RunID)
			result, err = nativeProto.Run(ctx, &pb.Request{Message: "native", Count: 7})
			if err != nil {
				t.Fatal(err)
			}
			reply := new(pb.Reply)
			if err := result.IntoContext(ctx, reply); err != nil || reply.Message != "native" || reply.Count != 7 {
				t.Fatal(reply, err)
			}
			runIDs = append(runIDs, result.RunID)
			result, err = workflow.Run(ctx, map[string]any{})
			if err != nil {
				t.Fatal(err)
			}
			if err := result.TaskOutput("child").IntoContext(ctx, &output); err != nil || output["integer"] != json.Number("9007199254740993") {
				t.Fatal(output, err)
			}
			runIDs = append(runIDs, result.RunID)
			refs, err := batch.RunMany(ctx, []client.RunManyInput{{Input: &pb.Request{Message: "first", Count: 1}}, {Input: &pb.Request{Message: "second", Count: 2}}})
			if err != nil || len(refs) != 2 {
				t.Fatal(refs, err)
			}
			for index, ref := range refs {
				result, err := ref.Result(ctx)
				if err != nil {
					t.Fatal(err)
				}
				reply := new(pb.Reply)
				if err := result.IntoContext(ctx, reply); err != nil || reply.Count != int32(index+1) {
					t.Fatal(reply, err)
				}
				runIDs = append(runIDs, ref.RunID)
			}
			ref, err := durable.RunNoWait(ctx, map[string]any{})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// 轮询发布规避业务 entered 与持久等待登记之间的间隙，直到结果观察者确认完成。
			completed := make(chan error, 1)
			go func() {
				result, err := ref.Result(ctx)
				if err == nil {
					// event 保存已解码的业务对象，用十进制 Number 验证数值保真。
					var event map[string]any
					err = result.IntoContext(ctx, &event)
					if err == nil && (event["message"] != "resumed" || event["count"] != json.Number("8")) {
						err = fmt.Errorf("event mismatch: %v", event)
					}
				}
				completed <- err
			}()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for finished := false; !finished; {
				if err := conn.Events().Push(ctx, "resume", &pb.Request{Message: "resumed", Count: 8}); err != nil {
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
			runIDs = append(runIDs, ref.RunID)
			info, err := conn.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := worker.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			// 删除本场景定义；输出和对象保留用于之后按 RunID 解码，不能提前删除 S3 内容。
			for _, name := range []string{echo.GetName(), async.GetName(), nativeStream.GetName(), nativeProto.GetName(), durable.GetName(), workflow.GetName(), batch.GetName()} {
				if _, err := conn.Workflows().Delete(ctx, name); err != nil {
					t.Error(err)
				}
			}
			if err := conn.Close(); err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			writeStreamEvidence(t, "unified-"+scenario.name, map[string]any{"namespace": namespace, "server_version": info.Version, "run_ids": runIDs, "assertions": []string{"JSON integer/null preserved", "native protobuf retains ordinary task identity", "DAG parent output codec", "protobuf batch member results", "durable protobuf event codec and routing", "random static codec replicas share version", "async deadline is submission-only", "native realtime binary codec roundtrip", "event and filter business codec with CEL routing"}, "cleanup": "worker and connection closed; definitions deleted; referenced S3 objects and run history retained"})
		})
	}
}

// unifiedVersion 读取当前版本真实身份，不用名称查询的摘要行推断注册版本数。
func unifiedVersion(ctx context.Context, conn *client.Conn, name string) (string, error) {
	workflow, err := conn.Workflows().Get(ctx, name)
	if err != nil {
		return "", err
	}
	info, err := conn.Info(ctx)
	if err != nil {
		return "", err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, info.APIURL+"/api/v1/workflows/"+workflow.ID()+"/versions", nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+os.Getenv("HATCHET_CLIENT_TOKEN"))
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("version lookup HTTP %d", response.StatusCode)
	}
	// resource 是官方响应的普通 JSON 视图，没有导入后端 DTO。
	var resource model.Resource
	if err := json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&resource); err != nil {
		return "", err
	}
	if resource.ID() == "" {
		return "", fmt.Errorf("version identity missing")
	}
	return resource.ID(), nil
}

// unifiedWaitCompleted 确认过滤任务的输出上报完成；进入 handler 不等于引擎成功。
func unifiedWaitCompleted(ctx context.Context, conn *client.Conn, runID string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := conn.Runs().GetStatus(ctx, runID)
		// 事件运行异步进入查询存储，只有尚未复制产生的 404 可以等待。
		if err != nil && !strings.Contains(err.Error(), "status 404") {
			return err
		}
		if state == model.Completed {
			return nil
		}
		if state == model.Failed || state == model.Cancelled {
			return fmt.Errorf("filtered task terminated: %s", state)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
