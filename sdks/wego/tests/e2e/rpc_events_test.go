//go:build e2e

package e2e

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/codec/payload"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/backend"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// eventExecution 只记录真实 handler 的运行身份和方法，不把事件发布成功作为执行成功。
type eventExecution struct {
	// method 对应注册描述中的完整 RPC 名称。
	method string
	// runID 用于读取引擎最终结果，任务身份再由持久读模型核对。
	runID string
}

// eventService 使用标准 handler；通知通道只给验收观察运行，不参与调度或输出。
type eventService struct {
	// faultService 提供顺序严格的三种流业务实现。
	faultService
	// executions 保留本轮最多十个调用的身份，不阻塞业务输出。
	executions chan eventExecution
}

// record 在业务执行后记录身份，保证测试等待的是实际引擎投递。
func (s *eventService) record(ctx context.Context, method string) {
	info, _ := task.Info(ctx)
	s.executions <- eventExecution{method: method, runID: info.RunID}
}

// SayHello 回显事件输入，验证 unary 的默认事件和 codec 输出。
func (s *eventService) SayHello(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	defer s.record(ctx, pb.Greeter_SayHello_FullMethodName)
	return scenarios.Reply(ctx, in), nil
}

// UploadHellos 用标准 Recv 消费数组；空数组也必须得到 Count=0 的最终响应。
func (s *eventService) UploadHellos(st grpc.ClientStreamingServer[pb.Request, pb.Reply]) error {
	defer s.record(st.Context(), pb.Greeter_UploadHellos_FullMethodName)
	return s.faultService.UploadHellos(st)
}

// WatchHellos 发布有序 DATA，并保留正式 CLAIM 和结束帧。
func (s *eventService) WatchHellos(in *pb.Request, st grpc.ServerStreamingServer[pb.Reply]) error {
	if !in.Fail {
		defer s.record(st.Context(), pb.Greeter_WatchHellos_FullMethodName)
		return s.faultService.WatchHellos(in, st)
	}
	// 首次发送 0 后失败；重试读取事件作用域的历史，继续发送 1，不重复已有输出。
	history, err := task.Checkpoint(st.Context())
	if err != nil {
		return err
	}
	next := int32(0)
	for {
		previous := new(pb.Reply)
		if err := history.Next(previous); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if previous.Count != next {
			return status.Error(codes.DataLoss, "event history differs")
		}
		next++
	}
	info, _ := task.Info(st.Context())
	if info.RetryCount > 0 && next != 1 {
		return status.Error(codes.DataLoss, "event history missing")
	}
	for i := next; i < in.Count; i++ {
		if err := st.Send(scenarios.Reply(st.Context(), &pb.Request{Count: i})); err != nil {
			return err
		}
		if info.RetryCount == 0 {
			return status.Error(codes.Unavailable, "event retry after persisted output")
		}
	}
	s.record(st.Context(), pb.Greeter_WatchHellos_FullMethodName)
	return nil
}

// ChatHellos 在单任务中消费整批输入并逐条输出，不创建控制任务。
func (s *eventService) ChatHellos(st grpc.BidiStreamingServer[pb.Request, pb.Reply]) error {
	defer s.record(st.Context(), pb.Greeter_ChatHellos_FullMethodName)
	return s.faultService.ChatHellos(st)
}

// TestRPCEventsCodecIDs 验证四种默认事件、空输入和共享事件的双方法触发；两端注册和发送顺序分别不同。
func TestRPCEventsCodecIDs(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	key, err := hex.DecodeString(os.Getenv("WEGO_CODEC_AES_KEY"))
	if err != nil || len(key) != 32 {
		t.Fatal("AES test key must be configured")
	}
	aes, err := payload.NewAESGCM(key)
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := payload.NewGzip(payload.WithThreshold(0))
	if err != nil {
		t.Fatal(err)
	}
	objects, err := payload.NewS3(ctx, payload.ConfigFromEnv(), "event-codecs-"+uuid.NewString()+"/", payload.WithThreshold(0))
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	for _, ids := range [][]string{{payload.GzipCodecID}, {payload.AESGCMCodecID}, {payload.S3ObjectCodecID}, {payload.GzipCodecID, payload.AESGCMCodecID, payload.S3ObjectCodecID}, {}} {
		t.Run("chain="+joinCodecIDs(ids), func(t *testing.T) {
			service := &eventService{executions: make(chan eventExecution, 10)}
			options := []runtime.Option{runtime.WithMiddleware(middleware.WithPayload(objects), middleware.WithPayload(aes), middleware.WithPayload(compressed)), runtime.WithEncodingCodecs(ids...)}
			h, err := scenarios.StartRegistered(ctx, "rpc_events", &scenarios.Report{}, func(r grpc.ServiceRegistrar) { pb.RegisterGreeterServer(r, service) }, streamNames(), options,
				worker.WithTask(pb.Greeter_UploadHellos_FullMethodName, task.WithEvents("fanout", pb.Greeter_UploadHellos_FullMethodName, "fanout")),
				worker.WithTask(pb.Greeter_ChatHellos_FullMethodName, task.WithEvents("fanout")),
				worker.WithTask(pb.Greeter_WatchHellos_FullMethodName, task.WithRetries(1)))
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			defer func() {
				if !closed {
					if err := h.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			// 客户端注册顺序与 Worker 相反，发送链再反转；接收组件不能据此拒绝历史消息。
			sendIDs := append([]string(nil), ids...)
			for i, j := 0, len(sendIDs)-1; i < j; i, j = i+1, j-1 {
				sendIDs[i], sendIDs[j] = sendIDs[j], sendIDs[i]
			}
			receiver := spec.Defaults()
			for _, option := range append(scenarios.Runtime(h.Namespace), runtime.WithMiddleware(middleware.WithPayload(compressed), middleware.WithPayload(aes), middleware.WithPayload(objects)), runtime.WithEncodingCodecs(sendIDs...)) {
				option(&receiver)
			}
			observer, err := backend.New(receiver)
			if err != nil {
				t.Fatal(err)
			}
			defer observer.Close()
			connOptions := append(scenarios.Runtime(h.Namespace), runtime.WithMiddleware(middleware.WithPayload(compressed), middleware.WithPayload(aes), middleware.WithPayload(objects)), runtime.WithEncodingCodecs(sendIDs...))
			conn, err := client.New(client.WithRuntime(connOptions...))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			requests := []*pb.Request{{Count: 0}, {Count: 1}}
			// fanout 的同一次事件必须产生两个独立运行，不能被跨方法幂等键互相抑制。
			cases := []struct {
				// key 指定默认事件或共享事件，Value 始终由 SDK 生成协议封包。
				key string
				// value 是业务消息或 protobuf 数组，不是裸后端输入。
				value any
				// executions 和 count 分别约束运行数与业务输出数量。
				executions, count int
			}{{pb.Greeter_SayHello_FullMethodName, &pb.Request{Count: 2}, 1, 2}, {pb.Greeter_WatchHellos_FullMethodName, &pb.Request{Count: 2}, 1, 2}, {pb.Greeter_UploadHellos_FullMethodName, requests, 1, 2}, {pb.Greeter_ChatHellos_FullMethodName, requests, 1, 2}, {"fanout", requests, 2, 2}, {pb.Greeter_UploadHellos_FullMethodName, []*pb.Request{}, 1, 0}, {pb.Greeter_ChatHellos_FullMethodName, []*pb.Request{}, 1, 0}, {pb.Greeter_WatchHellos_FullMethodName, &pb.Request{Count: 2, Fail: true}, 1, 2}}
			// runs 只包含本轮已确认执行的身份，不记录凭证。
			var runs []string
			for _, test := range cases {
				if err := conn.Events().Push(ctx, test.key, model.Input{Value: test.value, Routing: map[string]any{"group": "fixture"}}); err != nil {
					t.Fatal(err)
				}
				for range test.executions {
					// execution 区分共享事件的两个方法，避免把发布成功当作运行成功。
					var execution eventExecution
					select {
					case execution = <-service.executions:
					case <-ctx.Done():
						t.Fatal("event handler did not execute", ctx.Err())
					}
					for _, previous := range runs {
						if previous == execution.runID {
							t.Fatal("event executions shared RunID", execution.runID)
						}
					}
					runs = append(runs, execution.runID)
					assertEventResult(t, ctx, observer, receiver, h.Namespace, execution, test.count)
				}
			}
			info, err := conn.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if err := observer.Close(); err != nil {
				t.Fatal(err)
			}
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			writeStreamEvidence(t, "rpc-events", map[string]any{"namespace": h.Namespace, "run_ids": runs, "encoding_ids": ids, "server_version": info.Version, "assertions": []string{"four default RPC events completed", "shared stream event created two distinct runs", "empty input arrays completed", "event retry restored checkpoint and continued output", "stored results and durable frames decoded with different registry order"}, "cleanup": "definitions deleted; instances closed; run history and referenced objects retained"})
		})
	}
}

// joinCodecIDs 为测试提供格式标识，空发送链的名称明确为 none。
func joinCodecIDs(ids []string) string {
	if len(ids) == 0 {
		return "none"
	}
	name := ids[0]
	for _, id := range ids[1:] {
		name += "+" + id
	}
	return name
}

// assertEventResult 等待引擎最终成功，再读取实际存储的响应或完整流帧；缺帧不能只凭 handler 返回通过。
func assertEventResult(t *testing.T, ctx context.Context, b *backend.Backend, config spec.Runtime, namespace string, execution eventExecution, count int) {
	t.Helper()
	taskID, ref, err := b.LookupRun(ctx, execution.runID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := ref.Wait(ctx)
	if err != nil {
		t.Fatal(execution.method, err)
	}
	if len(result.Outputs) != 1 {
		t.Fatal("missing standalone result", result.Outputs)
	}
	// final 取自引擎已接受的最终清单；结束帧本身不能证明整个运行成功。
	var final stream.Completion
	for _, raw := range result.Outputs {
		packet, err := wire.AsEnvelope(raw)
		if err != nil {
			t.Fatal(err)
		}
		packet, err = wire.Open(ctx, execution.method, packet, config, 1<<20, 4<<20)
		if err != nil {
			t.Fatal(err)
		}
		if execution.method == pb.Greeter_SayHello_FullMethodName || execution.method == pb.Greeter_UploadHellos_FullMethodName {
			// reply 必须来自真实持久业务输出，而非本地 handler 记录。
			var reply pb.Reply
			if err := protojson.Unmarshal(packet.Payload, &reply); err != nil || int(reply.Count) != count {
				t.Fatal("stored event reply", &reply, err)
			}
			return
		}
		if err := json.Unmarshal(packet.Payload, &final); err != nil {
			t.Fatal(err)
		}
	}
	codec, err := wire.NewFrameCodec(config, 1<<20, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	outputs := 0
	state := stream.State{TaskID: taskID, Method: execution.method, RunID: execution.runID}
	err = b.SubscribeDurable(ctx, ports.DurableSubscription{Namespace: namespace, Topic: stream.Topic(taskID)}, func(entry ports.DurableEntry) error {
		frame, err := codec.Decode(ctx, execution.method, entry.Payload)
		if err != nil {
			return err
		}
		if frame.Method != execution.method || frame.RunId != execution.runID {
			t.Fatal("wrong durable event identity")
		}
		if state.InputDigest == "" {
			state.InputDigest = frame.InputDigest
		}
		accepted, err := state.Apply(frame)
		if err != nil {
			return err
		}
		if accepted {
			// reply 必须来自真实持久业务输出，而非本地 handler 记录。
			var reply pb.Reply
			if err := protojson.Unmarshal(frame.Payload, &reply); err != nil {
				return err
			}
			if int(reply.Count) != outputs {
				t.Fatal("stream response order", &reply, outputs)
			}
			outputs++
		}
		if frame.Kind == "ATTEMPT_END" {
			// terminal 只用成功 attempt 结束扫描，之前失败 attempt 的历史仍参与恢复。
			var terminal statuspb.Status
			if err := proto.Unmarshal(frame.Status, &terminal); err != nil {
				return err
			}
			if terminal.Code == 0 {
				return io.EOF
			}
		}
		return nil
	})
	if !errors.Is(err, io.EOF) || outputs != count {
		t.Fatal("incomplete durable event output", outputs, count, err)
	}
	if err := stream.VerifyCompletion(state, uint64(outputs), final); err != nil {
		t.Fatal("event completion mismatch", err)
	}
}
