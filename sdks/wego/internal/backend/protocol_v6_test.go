package backend

import (
	"context"
	"encoding/json"
	"testing"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware/payload"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestNativeValueFieldsRemainBusiness 普通 JSON 任务的 value/routing 同名业务字段不能被拆掉。
func TestNativeValueFieldsRemainBusiness(t *testing.T) {
	config := spec.Defaults()
	config.Middleware = []middleware.Option{middleware.WithPayload(&payload.Gzip{})}
	handler := func(_ context.Context, input map[string]any) (any, error) { return input, nil }
	result, err := invokePayload(handler, context.Background(), json.RawMessage(`{"value":7,"routing":{"message":"business"}}`), config, "task", spec.Task{}, "task")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(result)
	if err != nil || string(raw) != `{"routing":{"message":"business"},"value":7}` {
		t.Fatal(string(raw), err)
	}
}

// TestMixedBatchResponseContracts 逐项和广播都按真实成员输入保存契约，不能把全批结果统一加密。
func TestMixedBatchResponseContracts(t *testing.T) {
	ctx := context.Background()
	config := spec.Defaults()
	config.Middleware = []middleware.Option{middleware.WithPayload(&payload.Gzip{})}
	marked, err := wire.Seal(ctx, wire.NewEnvelope(ctx, "", "", json.RawMessage(`{"message":"wego"}`), nil), config, 1024, 2048, 4096)
	if err != nil {
		t.Fatal(err)
	}
	inputs := map[string]any{"native": json.RawMessage(`{"message":"native"}`), "wego": marked}
	for _, broadcast := range []bool{false, true} {
		policy := spec.Task{Batch: &model.BatchConfig{BroadcastOutput: broadcast}}
		handler := func(_ context.Context, members map[string]*pb.Request) (any, error) {
			if broadcast {
				return &pb.Reply{Message: "shared"}, nil
			}
			results := make(map[string]*pb.Reply, len(members))
			for id, input := range members {
				results[id] = &pb.Reply{Message: input.Message}
			}
			return results, nil
		}
		result, err := invokePayload(handler, ctx, inputs, config, "task", policy, "task")
		if err != nil {
			t.Fatal(err)
		}
		outputs := result.(map[string]any)
		for id, raw := range outputs {
			stored, err := json.Marshal(raw)
			if err != nil {
				t.Fatal(err)
			}
			packet, err := wire.AsEnvelope(stored)
			if err != nil || packet.Native != (id == "native") {
				t.Fatal("mixed batch response mode", id, err)
			}
			expected := id
			if broadcast {
				expected = "shared"
			}
			reply := new(pb.Reply)
			if err := wire.DecodeValue(ctx, "", packet, reply, config, 1024, 4096); err != nil || reply.Message != expected {
				t.Fatal("member result", id, reply, err)
			}
		}
	}
}

// TestPreparedTargetIsolation 只有本实例、同地址、同形态和同类型的提交能复用目标校验凭据。
func TestPreparedTargetIsolation(t *testing.T) {
	owner := new(Backend)
	proof := targetProof{owner: owner, name: "/fixture.Service/Call", shape: "server", mode: "reliable", types: []string{"fixture.Request"}}
	ctx := context.WithValue(context.Background(), targetProofKey{}, proof)
	if err := owner.ValidateTarget(ctx, proof.name, proof.shape, proof.mode); err != nil {
		t.Fatal("validated submission repeated remote lookup", err)
	}
	if err := owner.ValidateTarget(ports.WithExpectedOutput(ctx, "fixture.OtherReply"), proof.name, proof.shape, proof.mode); err == nil {
		t.Fatal("changed response type reused proof")
	}
	for _, attempt := range []struct {
		// backend、name、shape、mode 和 types 分别改变一个校验维度。
		backend *Backend
		// name 是本次请求的逻辑地址。
		name string
		// shape 固定流方向。
		shape string
		// mode 固定恢复能力。
		mode string
		// types 不能用相同地址替换另一种 protobuf 请求。
		types []string
	}{
		{new(Backend), proof.name, proof.shape, proof.mode, nil},
		{owner, "/fixture.Service/Other", proof.shape, proof.mode, nil},
		{owner, proof.name, "bidi", proof.mode, nil},
		{owner, proof.name, proof.shape, "realtime", nil},
		{owner, proof.name, proof.shape, proof.mode, []string{"fixture.Other"}},
	} {
		if err := attempt.backend.ValidateTarget(ctx, attempt.name, attempt.shape, attempt.mode, attempt.types...); err == nil {
			t.Fatal("foreign target reused proof", attempt)
		}
	}
}

// TestStoredRunContract 固定运行版本的流方向、模式、请求与响应类型均需匹配，不能只检查方法名。
func TestStoredRunContract(t *testing.T) {
	method := "/fixture.Service/Call"
	registered := rpcBinding{Kind: "rpc", Version: wire.Version, Method: method, Workflow: binding.WorkflowName("", method), Action: binding.Action("", method), Shape: "server", Mode: "reliable", InputType: "fixture.Request", OutputType: "fixture.Reply"}
	contract := ports.RPCContract{Shape: registered.Shape, Mode: registered.Mode, InputType: registered.InputType, OutputType: registered.OutputType}
	if err := registered.validateRunTarget("", method, []ports.RPCContract{contract}); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ports.RPCContract){
		func(c *ports.RPCContract) { c.Shape = "bidi" },
		func(c *ports.RPCContract) { c.Mode = "realtime" },
		func(c *ports.RPCContract) { c.InputType = "fixture.OtherRequest" },
		func(c *ports.RPCContract) { c.OutputType = "fixture.OtherReply" },
	} {
		changed := contract
		mutate(&changed)
		if err := registered.validateRunTarget("", method, []ports.RPCContract{changed}); err == nil {
			t.Fatal("different stored contract accepted", changed)
		}
	}
}

// TestGeneratedResponseBinding 输入类型相同也不能调用响应类型不同的部署定义。
func TestGeneratedResponseBinding(t *testing.T) {
	method := "/fixture.Service/Call"
	stored := rpcBinding{Version: wire.Version, Kind: "rpc", Method: method, Workflow: binding.WorkflowName("", method), Action: binding.Action("", method), Shape: "unary", InputType: "fixture.Request", OutputType: "fixture.Reply"}
	if err := stored.validateTarget("", stored.Workflow, method, "unary", "", []string{stored.InputType}, stored.OutputType); err != nil {
		t.Fatal(err)
	}
	if err := stored.validateTarget("", stored.Workflow, method, "unary", "", []string{stored.InputType}, "fixture.OtherReply"); err == nil {
		t.Fatal("response binding mismatch accepted")
	}
}
