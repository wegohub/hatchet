//go:build e2e

package backend

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestRPCBindingsP1 验证真实引擎的大小写路由、多副本共享定义与实例身份
func TestRPCBindingsP1(t *testing.T) {
	b, config, ctx := p0Backend(t)
	// 两个方法只差大小写，不能依赖引擎小写 action 的原始显示名称
	methods := []string{"/fixture.Greeter/SayHello", "/fixture.Greeter/Sayhello"}
	definitions := make([]ports.Definition, 0, len(methods))
	for _, method := range methods {
		definitions = append(definitions, ports.Definition{
			Name: binding.Name(method), RPCMethod: method, Policy: spec.Task{},
			Function: func(ctx context.Context, value any) (any, error) {
				input, err := wire.AsEnvelope(value)
				if err != nil {
					return nil, err
				}
				request := new(pb.Request)
				if err := wire.Decode(ctx, method, input, request, nil, 1024); err != nil {
					return nil, err
				}
				state, ok := callctx.Get(ctx)
				if !ok || state.Execution.Info().WorkerKey == "" {
					t.Error("missing SDK worker key")
				}
				// 执行上下文与引擎注册标签必须一致，业务无需另行维护展示名称
				if ok && state.Execution.Info().WorkerLabels["worker_name"] != config.InstanceName {
					t.Error("execution worker name label differs from registered display name")
				}
				return wire.Encode(ctx, method, &pb.Reply{Message: method + ":" + request.Message}, nil, nil, 1024)
			},
		})
	}
	// 同名实例的展示名称可以相同；其 key、注册身份和关闭资源必须独立
	config.InstanceName = "replica-display"
	config.Labels = map[string]any{"region": "zone-a", "worker_name": "configured"}
	first, err := b.Worker(ctx, "", definitions, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		budget, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = first.Close(budget)
	})
	before := map[string]string{}
	// versions 记录第一副本的实际版本 ID；第二副本不能因实例身份创建新版本
	versions := map[string]string{}
	for _, method := range methods {
		workflow, err := b.features.Workflows().Get(ctx, binding.WorkflowName(config.Namespace, method))
		if err != nil {
			t.Fatal(err)
		}
		before[method] = workflow.Metadata.Id
		version, err := b.raw.API().WorkflowVersionGetWithResponse(ctx, uuid.MustParse(workflow.Metadata.Id), nil)
		if err != nil || version.JSON200 == nil {
			t.Fatalf("initial workflow version unavailable: %v", err)
		}
		versions[method] = version.JSON200.Metadata.Id
	}
	second, err := b.Worker(ctx, "", definitions, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := second.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		budget, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = second.Close(budget)
	})
	a, c := first.(ports.WorkerIdentity), second.(ports.WorkerIdentity)
	if a.InstanceName() != c.InstanceName() || a.WorkerKey() == c.WorkerKey() || first.ID() == second.ID() {
		t.Fatal("replica identities conflated")
	}
	// 读取引擎实际保存的两个副本，不能仅用本地配置推断控制台标签
	for _, current := range []ports.Worker{first, second} {
		registered, err := b.raw.API().WorkerGetWithResponse(ctx, uuid.MustParse(current.ID()))
		if err != nil || registered.JSON200 == nil || registered.JSON200.Labels == nil {
			t.Fatalf("registered worker labels unavailable: %v", err)
		}
		labels := map[string]string{}
		for _, label := range *registered.JSON200.Labels {
			if label.Value != nil {
				labels[label.Key] = *label.Value
			}
		}
		if labels["worker_name"] != registered.JSON200.Name || labels["worker_name"] != config.InstanceName || labels["region"] != "zone-a" {
			t.Fatalf("registered worker name/custom labels differ: %v", labels)
		}
	}
	runIDs := []string{}
	for _, method := range methods {
		workflow, err := b.features.Workflows().Get(ctx, binding.WorkflowName(config.Namespace, method))
		if err != nil || workflow.Metadata.Id != before[method] {
			t.Fatalf("workflow not shared: %v", err)
		}
		// Get 返回完整版本后再验证持久 schema，不把注册请求内存值当作证据
		id := uuid.MustParse(workflow.Metadata.Id)
		version, err := b.raw.API().WorkflowVersionGetWithResponse(ctx, id, nil)
		if err != nil || version.JSON200 == nil || version.JSON200.InputJsonSchema == nil {
			t.Fatalf("binding schema unavailable: %v", err)
		}
		if version.JSON200.Metadata.Id != versions[method] {
			t.Fatal("replica registration created a new workflow version")
		}
		stored := (*version.JSON200.InputJsonSchema)["x-wego-binding"].(map[string]any)
		if stored["method"] != method || stored["action"] != binding.Action(config.Namespace, method) {
			t.Fatalf("persisted binding lost: %#v", stored)
		}
		input, err := wire.Encode(ctx, method, &pb.Request{Message: "mixed CASE"}, map[string]any{"group": "one", "cost": 3}, nil, 1024)
		if err != nil {
			t.Fatal(err)
		}
		run, err := b.Run(ctx, method, input, model.RunOptions{})
		if err != nil {
			t.Fatal(err)
		}
		result, err := run.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		runIDs = append(runIDs, run.ID)
		if len(result.Outputs) != 1 {
			t.Fatal("standalone output shape changed")
		}
		for _, value := range result.Outputs {
			envelope, err := wire.AsEnvelope(value)
			if err != nil {
				t.Fatal(err)
			}
			out := new(pb.Reply)
			if err := wire.Decode(ctx, method, envelope, out, nil, 1024); err != nil || out.Message != method+":mixed CASE" {
				t.Fatalf("case route mismatch: %v %v", out, err)
			}
		}
		// 实际持久输入应为可读对象，不是生成结构体的普通 JSON 或二进制 base64
		data, err := b.RunInput(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		// persisted 来自真实运行详情，不从本地请求副本推断调度字段
		var persisted map[string]any
		if err := json.Unmarshal(data, &persisted); err != nil {
			t.Fatal(err)
		}
		if persisted["payload"].(map[string]any)["message"] != "mixed CASE" || persisted["routing"].(map[string]any)["cost"] != float64(3) {
			t.Fatal("JSON scheduling input lost")
		}
	}
	// 停止实例只注销消费者；持久 workflow 定义仍保留且未被自动暂停
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for _, method := range methods {
		workflow, err := b.features.Workflows().Get(ctx, binding.WorkflowName(config.Namespace, method))
		if err != nil || workflow.IsPaused != nil && *workflow.IsPaused {
			t.Fatalf("shutdown changed definition: %v", err)
		}
	}
	if name := instanceName(spec.Defaults(), "", a.WorkerKey()); !strings.HasSuffix(name, strings.ReplaceAll(a.WorkerKey(), "-", "")[:12]) || strings.HasPrefix(name, "wego-") {
		t.Fatalf("default instance name: %s", name)
	}
	writeP0Report(t, b, "v020-p1-bindings", map[string]any{"namespace": config.Namespace, "run_ids": runIDs, "workflow_ids": before, "workflow_version_ids": versions, "worker_keys": []string{a.WorkerKey(), c.WorkerKey()}, "checks": []string{"case_action_isolation", "replica_definition_sharing", "replica_version_sharing", "persisted_schema", "protojson_routing", "display_name_identity", "registered_worker_name_labels", "definitions_retained"}, "cleanup": "workers closed; definitions and run history retained"})
}
