package rpc

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware/payload"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestStreamEventsExecute 使用真正的事件封包和 RPC 适配器，验证数组、空流与最终结果。
func TestStreamEventsExecute(t *testing.T) {
	for _, name := range []string{"UploadHellos", "WatchHellos", "ChatHellos"} {
		for _, empty := range []bool{false, true} {
			t.Run(name+"/empty="+map[bool]string{false: "false", true: "true"}[empty], func(t *testing.T) {
				s, fixture, cancel := fixtureStream(t, name)
				defer cancel()
				if !slices.Contains(fixture.definition.Policy.Events, s.method) {
					t.Fatal("RPC default event missing")
				}
				count := int32(2)
				if empty {
					count = 0
				}
				// value 的形状由 RPC 描述决定；客户端流使用数组。
				var value any = &pb.Request{Message: "event", Count: count}
				if name != "WatchHellos" {
					requests := []*pb.Request{}
					for range count {
						requests = append(requests, &pb.Request{Message: "event"})
					}
					value = requests
				}
				packet, err := wire.EncodeEvent(s.ctx, s.engine.Config, s.method, model.Input{Value: value, Routing: map[string]any{"group": "one"}})
				if err != nil {
					t.Fatal(err)
				}
				ref, err := fixture.Run(s.ctx, fixture.definition.Name, packet, model.RunOptions{})
				if err != nil {
					t.Fatal(err)
				}
				result, err := ref.Wait(s.ctx)
				if err != nil {
					t.Fatal(err)
				}
				if name == "UploadHellos" {
					for _, raw := range result.Outputs {
						envelope, err := wire.AsEnvelope(raw)
						if err != nil {
							t.Fatal(err)
						}
						// reply 校验经过结果 codec 还原后的业务响应。
						var reply pb.Reply
						if err := wire.DecodeValue(s.ctx, s.method, envelope, &reply, s.engine.Config, 4096, 8192); err != nil || reply.Count != count {
							t.Fatal("client stream event result", err, &reply)
						}
					}
					return
				}
				codec, err := wire.NewFrameCodec(s.engine.Config, 4096, 8192)
				if err != nil {
					t.Fatal(err)
				}
				// outputs 只计业务 DATA，控制帧不计入业务消息数。
				var outputs int32
				for _, entry := range fixture.entries {
					frame, err := codec.Decode(s.ctx, s.method, entry.Payload)
					if err != nil {
						t.Fatal(err)
					}
					if frame.Kind == "DATA" {
						outputs++
					}
				}
				if outputs != count {
					t.Fatal("event output count", outputs, count)
				}
			})
		}
	}
}

// TestInvalidEventInputFinishesAfterClaim 未知 codec 和错误请求都必须由获胜执行报告，而非门禁静默丢弃。
func TestInvalidEventInputFinishesAfterClaim(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		s, fixture, cancel := fixtureStream(t, "ChatHellos")
		config := spec.Defaults()
		if compressed {
			compressed, codecErr := payload.NewGzip(payload.WithThreshold(0))
			if codecErr != nil {
				t.Fatal(codecErr)
			}
			config.Middleware = []middleware.Option{middleware.WithPayload(compressed)}

		}
		// 对 bidi 发送对象而非数组；gzip 场景应更早在 codec 查找处明确失败。
		packet, err := wire.EncodeEvent(context.Background(), config, s.method, &pb.Request{Message: "invalid shape"})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		ref, err := fixture.Run(s.ctx, fixture.definition.Name, packet, model.RunOptions{})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		_, err = ref.Wait(s.ctx)
		want := codes.DataLoss
		if compressed {
			want = codes.FailedPrecondition
		}
		cancel()
		if status.Code(err) != want || len(fixture.entries) == 0 {
			t.Fatal("invalid event escaped owned failure", err)
		}
		// carrier 校验事件没有被追加已删除的方法字段，执行目标只来自注册绑定。
		var carrier map[string]any
		if err := json.Unmarshal(fixture.input, &carrier); err != nil || carrier["method"] != nil {
			t.Fatal("event envelope was rewritten as direct RPC", err)
		}
	}
}

// TestNativeStreamMissingSubmissionKey 原生三种流也必须提供非空提交键，缺键时不能启动 handler 或发布输出。
func TestNativeStreamMissingSubmissionKey(t *testing.T) {
	for _, name := range []string{"UploadHellos", "WatchHellos", "ChatHellos"} {
		for _, input := range []string{`{"_wego":{"version":6},"value":[]}`, `{"_wego":{"version":6,"idempotency_key":""},"value":[]}`, `{}`} {
			s, fixture, cancel := fixtureStream(t, name)
			function := fixture.definition.Function.(func(context.Context, any) (any, error))
			_, err := function(s.ctx, json.RawMessage(input))
			cancel()
			if status.Code(err) != codes.InvalidArgument || len(fixture.entries) != 0 {
				t.Fatal("missing key reached stream execution", name, err)
			}
		}
	}
}
