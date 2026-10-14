//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/backend"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware/payload"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// TestReadableFailureInConsole 验证真实引擎保存 JSON 错误对象，控制台与客户端读取同一失败。
func TestReadableFailureInConsole(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// 服务返回带详情的参数错误，例如缺少 id；任务必须失败而非返回成功载荷。
	service := &scenarios.Service{Say: func(context.Context, *pb.Request) (*pb.Reply, error) {
		detailed, err := status.New(codes.InvalidArgument, "missing id").WithDetails(&errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: "id", Description: "required"}},
		})
		if err != nil {
			return nil, err
		}
		return nil, detailed.Err()
	}}
	h, err := scenarios.Start(ctx, "readable_failure", &scenarios.Report{}, service, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	})
	ref, err := h.Conn.RunNoWait(ctx, pb.UnaryGreeter_SayHello_FullMethodName, &pb.Request{Message: "missing id"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ref.Result(ctx); status.Code(err) != codes.InvalidArgument || len(status.Convert(err).Details()) != 1 {
		t.Fatalf("客户端错误详情未恢复: %v", err)
	}
	// Console 的运行错误必须是对象 JSON，不能成为带引号的 base64 字符串。
	config := spec.Defaults()
	for _, option := range h.Runtime {
		option(&config)
	}
	check := time.NewTicker(200 * time.Millisecond)
	defer check.Stop()
	for ctx.Err() == nil {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, config.ServerURL+"/api/v1/stable/workflow-runs/"+ref.RunID, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+config.Token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		// details 读取 Output 页签使用的运行错误正文。
		var details struct {
			// Run 保存运行终态的错误正文。
			Run struct {
				// ErrorMessage 是控制台显示的原始错误字符串。
				ErrorMessage string `json:"errorMessage"`
			} `json:"run"`
		}
		decodeErr := json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&details)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || decodeErr != nil {
			t.Fatalf("读取 Console 运行详情失败: status=%d error=%v", response.StatusCode, decodeErr)
		}
		if details.Run.ErrorMessage != "" {
			// stored 必须是结构化 JSON 对象；带引号的序列化字符串不能通过此断言。
			var stored map[string]any
			if err := json.Unmarshal([]byte(details.Run.ErrorMessage), &stored); err != nil || stored["message"] != "missing id" || stored["code_name"] != "InvalidArgument" {
				t.Fatalf("控制台运行错误不可读: %q (%v)", details.Run.ErrorMessage, err)
			}
			return
		}
		select {
		case <-ctx.Done():
		case <-check.C:
		}
	}
	t.Fatal("控制台未收到运行错误:", ctx.Err())
}

// consoleRequest 使用 Console 的公开 REST 路径；Token 仅用于请求，不写日志和报告。
func consoleRequest(t *testing.T, ctx context.Context, config spec.Runtime, path string, body any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, config.ServerURL+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+config.Token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil || response.StatusCode >= 300 {
		t.Fatalf("Console request %s: status=%d body=%s error=%v", path, response.StatusCode, data, err)
	}
	return data
}

// nativeObserver 使用与 Worker 相同的配置，但执行入口仍通过官方 REST 提交裸值。
func nativeObserver(t *testing.T, h *scenarios.Harness) *backend.Backend {
	t.Helper()
	config := spec.Defaults()
	for _, option := range h.Runtime {
		option(&config)
	}
	observer, err := backend.New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := observer.Close(); err != nil {
			t.Error(err)
		}
	})
	return observer
}

// nativeTrigger 保存官方返回的 RunID，不能从业务输入或 handler 的本地记录伪造身份。
func nativeTrigger(t *testing.T, ctx context.Context, observer *backend.Backend, method string, input any) string {
	t.Helper()
	config := observer.Config()
	data := consoleRequest(t, ctx, config, "/api/v1/stable/tenants/"+config.TenantID+"/workflow-runs/trigger", map[string]any{"workflowName": binding.WorkflowName(config.Namespace, method), "input": input, "return_only_id": true})
	// response 只读取官方运行资源的身份；不把 HTTP 成功等价为业务成功。
	var response struct {
		// Run 是官方返回的资源，不是业务 result。
		Run struct {
			// Metadata 提供可查询的持久 RunID。
			Metadata struct {
				// ID 用于等待最终引擎结果。
				ID string `json:"id"`
			} `json:"metadata"`
		} `json:"run"`
	}
	if err := json.Unmarshal(data, &response); err != nil || uuid.Validate(response.Run.Metadata.ID) != nil {
		t.Fatal("invalid Console RunID", string(data), err)
	}
	return response.Run.Metadata.ID
}

// TestProtocolV6ConsoleUnaryScheduledCron 验证三种 Console 入口均无需 wego 客户端封包或输出 codec。
func TestProtocolV6ConsoleUnaryScheduledCron(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	service := &eventService{executions: make(chan eventExecution, 8)}
	h, err := scenarios.StartRegistered(ctx, "native_console", &scenarios.Report{}, func(r grpc.ServiceRegistrar) { pb.RegisterGreeterServer(r, service) }, streamNames(), []runtime.Option{runtime.WithMiddleware(middleware.WithPayload(&payload.Gzip{}))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	})
	observer := nativeObserver(t, h)
	config := observer.Config()
	method := pb.Greeter_SayHello_FullMethodName
	// runs 记录本轮真实执行身份，清理限定为当前 namespace。
	var runs []string
	assertReply := func(id string) {
		t.Helper()
		_, ref, err := observer.LookupRun(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		result, err := ref.Wait(ctx)
		if err != nil || len(result.Outputs) != 1 {
			t.Fatal("native completion", result, err)
		}
		for _, output := range result.Outputs {
			data, err := json.Marshal(output)
			if err != nil {
				t.Fatal(err)
			}
			// reply 由正式业务 ProtoJSON 解码，协议帧不得通过此检查。
			var reply pb.Reply
			if err := protojson.Unmarshal(data, &reply); err != nil || reply.Message != "native" || reply.RunId != id {
				t.Fatal("native result is not business ProtoJSON", string(data), err)
			}
		}
		runs = append(runs, id)
	}
	// immediate 对照裸对象与显式 Input；后者提供 routing，但 response 缺省仍为 native。
	for _, input := range []any{map[string]any{"message": "native"}, map[string]any{"_wego": map[string]any{"version": 6}, "value": map[string]any{"message": "native"}, "routing": map[string]any{"group": "one"}}} {
		id := nativeTrigger(t, ctx, observer, method, input)
		assertReply(id)
		select {
		case <-service.executions:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	name := url.PathEscape(binding.WorkflowName(config.Namespace, method))
	path := "/api/v1/tenants/" + config.TenantID + "/workflows/" + name
	consoleRequest(t, ctx, config, path+"/scheduled", map[string]any{"input": map[string]any{"message": "native"}, "triggerAt": time.Now().Add(time.Second).UTC()})
	select {
	case executed := <-service.executions:
		assertReply(executed.runID)
	case <-ctx.Done():
		t.Fatal("native Schedule did not execute", ctx.Err())
	}
	consoleRequest(t, ctx, config, path+"/crons", map[string]any{"input": map[string]any{"message": "native"}, "cronName": "native-minute", "cronExpression": "* * * * *"})
	select {
	case executed := <-service.executions:
		assertReply(executed.runID)
	case <-ctx.Done():
		t.Fatal("native Cron did not execute", ctx.Err())
	}
	info, err := h.Conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writeStreamEvidence(t, "v6-console", map[string]any{"server_version": info.Version, "namespace": h.Namespace, "run_ids": runs, "assertions": []string{"bare object and marked Input completed", "native response stayed plain despite Worker gzip", "Console Schedule and minute Cron executed"}, "cleanup": "workflow and triggers removed; run history retained"})
}

// TestProtocolV6NativeStreaming 原生三种流均不进入持久日志；Reliable 配置不能改变原生 PutStream 契约。
func TestProtocolV6NativeStreaming(t *testing.T) {
	preflight(t)
	for _, method := range []string{pb.Greeter_UploadHellos_FullMethodName, pb.Greeter_WatchHellos_FullMethodName, pb.Greeter_ChatHellos_FullMethodName} {
		for _, inputEncoding := range []string{"json", "gzip"} {
			t.Run(method+"/"+inputEncoding, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				service := &realtimeEventService{eventService: eventService{executions: make(chan eventExecution, 2)}, received: make(chan struct{})}
				h, err := scenarios.StartRegistered(ctx, "native_stream", &scenarios.Report{}, func(r grpc.ServiceRegistrar) { pb.RegisterGreeterServer(r, service) }, streamNames(), []runtime.Option{runtime.WithMiddleware(middleware.WithPayload(&payload.Gzip{}))})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := h.Close(); err != nil {
						t.Error(err)
					}
				})
				observer := nativeObserver(t, h)
				// value 使用注册形态选择单请求或请求数组。
				var value any = map[string]any{"count": 2}
				if method != pb.Greeter_WatchHellos_FullMethodName {
					value = []any{map[string]any{"count": 0}, map[string]any{"count": 1}}
				}
				// input 单独声明承载表示，响应契约与是否使用 gzip 无关。
				var input any = map[string]any{"_wego": map[string]any{"version": 6, "idempotency_key": uuid.NewString()}, "value": value}
				if inputEncoding == "gzip" {
					// 同一原生响应契约允许编码输入；输入 codec 不应导致输出自动采用 wego 协议。
					plain, err := json.Marshal(value)
					if err != nil {
						t.Fatal(err)
					}
					packet := wire.NewEnvelope(ctx, "", "", plain, nil)
					packet.Response = "native"
					packet.IdempotencyKey = uuid.NewString()
					input, err = wire.Seal(ctx, packet, observer.Config(), 1<<20, 2<<20, 4<<20)
					if err != nil {
						t.Fatal(err)
					}
				}
				id := nativeTrigger(t, ctx, observer, method, input)
				if method != pb.Greeter_UploadHellos_FullMethodName {
					// 重发直到订阅实际消费一条业务 chunk；实时路径没有历史回放或订阅就绪 ACK。
					consumed := false
					err = observer.Stream(ctx, id, func(raw string) error {
						// reply 由正式业务 ProtoJSON 解码，协议帧不得通过此检查。
						var reply pb.Reply
						if err := protojson.Unmarshal([]byte(raw), &reply); err != nil || reply.Count != 7 {
							t.Fatal("native chunk contains protocol or codec", raw, err)
						}
						if !consumed {
							close(service.received)
							consumed = true
						}
						return io.EOF
					})
					if err != io.EOF || !consumed {
						t.Fatal("native PutStream not observed", err)
					}
				}
				_, ref, err := observer.LookupRun(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				result, err := ref.Wait(ctx)
				if err != nil || len(result.Outputs) != 1 {
					t.Fatal("native stream completion", err)
				}
				for _, raw := range result.Outputs {
					data, err := json.Marshal(raw)
					if err != nil {
						t.Fatal(err)
					}
					if method == pb.Greeter_UploadHellos_FullMethodName {
						// reply 由正式业务 ProtoJSON 解码，协议帧不得通过此检查。
						var reply pb.Reply
						if err := protojson.Unmarshal(data, &reply); err != nil || reply.Count != 2 {
							t.Fatal(string(data), err)
						}
					} else {
						// summary 仅允许 sent_messages，不保留全部输出。
						var summary map[string]uint64
						if err := json.Unmarshal(data, &summary); err != nil || len(summary) != 1 || summary["sent_messages"] == 0 {
							t.Fatal("native result cached protocol frames", string(data), err)
						}
					}
				}
				packet, err := wire.AsEnvelope(input)
				if err != nil {
					t.Fatal(err)
				}
				assertNativeStreamCollision(t, ctx, h, method, packet.IdempotencyKey)
				info, err := h.Conn.Info(ctx)
				if err != nil {
					t.Fatal(err)
				}
				writeStreamEvidence(t, "v6-native-stream", map[string]any{"server_version": info.Version, "namespace": h.Namespace, "run_id": id, "method": method, "input_encoding": inputEncoding, "assertions": []string{"native business ProtoJSON", "stream input key accepted", "plaintext result or sent_messages summary", "wego cannot attach native same-key run"}, "cleanup": "isolated workflow deleted; run history retained"})
			})
		}
	}
}

// assertNativeStreamCollision 使用真实冲突运行，SDK 不能把原生响应契约当作可恢复的 wego 流。
func assertNativeStreamCollision(t *testing.T, ctx context.Context, h *scenarios.Harness, method, key string) {
	t.Helper()
	generated := pb.NewGreeterClient(h.Conn)
	options := client.WithRunOptions(client.WithStreamIdempotencyKey(key))
	// err 保存实际幂等冲突；不能把普通调用失败当作附着隔离成功。
	var err error
	switch method {
	case pb.Greeter_UploadHellos_FullMethodName:
		stream, startErr := generated.UploadHellos(ctx, options)
		if startErr != nil {
			t.Fatal(startErr)
		}
		if err := stream.Send(&pb.Request{Count: 1}); err != nil {
			t.Fatal(err)
		}
		_, err = stream.CloseAndRecv()
	case pb.Greeter_WatchHellos_FullMethodName:
		_, err = generated.WatchHellos(ctx, &pb.Request{Count: 2}, options)
	case pb.Greeter_ChatHellos_FullMethodName:
		stream, startErr := generated.ChatHellos(ctx, options)
		if startErr != nil {
			t.Fatal(startErr)
		}
		if err := stream.Send(&pb.Request{Count: 1}); err != nil {
			t.Fatal(err)
		}
		err = stream.CloseSend()
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatal("native run attached as wego stream", method, err)
	}
}
