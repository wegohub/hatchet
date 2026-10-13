package client

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// messageBackend 模拟服务端 JSON 往返，记录实际输入；不直接把本地 Envelope 作为已部署证据。
type messageBackend struct {
	// Backend 未覆盖的接口不参与本次受控消息往返。
	ports.Backend
	// config 发送和解码使用同一冻结配置，测试关注提交语义而非后端实现。
	config spec.Runtime
	// packets 是经过 JSON 存储转换后的输入，不引用调用方数据。
	packets []wire.Envelope
	// options 记录每项独立调度配置。
	options []model.RunOptions
}

// Close 测试后端没有外部资源，允许实例的完整关闭流程正常结束。
func (*messageBackend) Close() error { return nil }

// Run 还原提交值再按响应契约生成输出，验证客户端没有把明文或类型判断绕过通用封包。
func (b *messageBackend) Run(ctx context.Context, name string, input any, opts model.RunOptions) (ports.Run, error) {
	stored, err := json.Marshal(input)
	if err != nil {
		return ports.Run{}, err
	}
	packet, err := wire.AsEnvelope(stored)
	if err != nil {
		return ports.Run{}, err
	}
	b.packets = append(b.packets, packet)
	b.options = append(b.options, opts)
	opened, err := wire.Open(ctx, "", packet, b.config, 1<<20, 4<<20)
	if err != nil {
		return ports.Run{}, err
	}
	output, err := wire.Output(ctx, wire.NewEnvelope(ctx, "", "", opened.Payload, nil), b.config, 1<<20, 2<<20, 4<<20)
	if err != nil {
		return ports.Run{}, err
	}
	return ports.Run{ID: name, Wait: func(context.Context) (ports.Result, error) {
		return ports.Result{RunID: name, Outputs: map[string]any{"echo": output}}, nil
	}}, nil
}

// RunMany 每项调用共享准备好的输入，不在失败时猜测后端返回的 InputIndex。
func (b *messageBackend) RunMany(ctx context.Context, name string, inputs []model.RunManyInput) ([]ports.Run, error) {
	refs := make([]ports.Run, 0, len(inputs))
	for index, input := range inputs {
		ref, err := b.Run(ctx, name, input.Input, input.Options)
		if err != nil {
			return refs, err
		}
		ref.InputIndex = index
		refs = append(refs, ref)
	}
	return refs, nil
}

// messageConn 构造真实 Engine 生命周期，避免手工缺少 I/O 表导致测试漏掉关闭约束。
func messageConn(t *testing.T) (*Conn, *messageBackend) {
	t.Helper()
	config := spec.Defaults()
	config.Telemetry.Trace.DisableWorkerExporter = true
	backend := &messageBackend{config: config}
	instance, err := engine.NewWithBackend(config, backend)
	if err != nil {
		t.Fatal(err)
	}
	conn := FromEngine(instance, false)
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	return conn, backend
}

// TestV6SubmissionDeadlineAndPrecision 同步请求保留执行 deadline，异步不继承；大整数结果无损还原。
func TestV6SubmissionDeadlineAndPrecision(t *testing.T) {
	conn, backend := messageConn(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	routing := map[string]any{"group": "a"}
	result, err := conn.Run(ctx, "echo", model.Input{Value: map[string]any{"n": json.Number("9007199254740993")}, Routing: routing}, WithRunPriority(2))
	if err != nil {
		t.Fatal(err)
	}
	// value 检查业务值的精度与实际类型。
	var value map[string]any
	if err := result.Into(&value); err != nil || value["n"] != json.Number("9007199254740993") {
		t.Fatal(value, err)
	}
	routing["group"] = "changed"
	if backend.packets[0].Deadline == 0 || backend.packets[0].Encoding != "base64" || backend.packets[0].Routing["group"] != "a" {
		t.Fatal("sync input lost deadline, precision or routing snapshot")
	}
	ref, err := conn.RunNoWait(ctx, "echo", nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err = ref.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// output 用于区分 JSON null 与缺少结果。
	var output any
	if err := result.Into(&output); err != nil || output != nil {
		t.Fatal(output, err)
	}
	if backend.packets[1].Deadline != 0 {
		t.Fatal("async submission inherited execution deadline")
	}
}

// TestV6BulkLocalValidationAtomicity 后项非法时不提交前项；合法批次的索引和 routing 分别保留。
func TestV6BulkLocalValidationAtomicity(t *testing.T) {
	conn, backend := messageConn(t)
	invalid := []RunManyInput{{Input: map[string]any{"n": 1}}, {Input: make(chan int)}}
	if _, err := conn.RunMany(context.Background(), "echo", invalid); err == nil || len(backend.packets) != 0 {
		t.Fatal("invalid batch was partially submitted")
	}
	inputs := []RunManyInput{{Input: map[string]any{"n": 1}, Options: []RunOption{spec.WithRouting(map[string]any{"group": "a"})}}, {Input: map[string]any{"n": 2}}}
	refs, err := conn.RunMany(context.Background(), "echo", inputs)
	if err != nil || len(refs) != 2 {
		t.Fatal(refs, err)
	}
	for index, ref := range refs {
		if ref.InputIndex != index {
			t.Fatal("input index was lost")
		}
		result, err := ref.Result()
		if err != nil {
			t.Fatal(err)
		}
		// value 检查业务值的精度与实际类型。
		var value map[string]any
		if err := result.Into(&value); err != nil {
			t.Fatal(err)
		}
	}
	if backend.packets[0].Routing["group"] != "a" || len(backend.packets[1].Routing) != 0 {
		t.Fatal("bulk routing leaked across inputs")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := conn.Run(cancelled, "echo", nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
