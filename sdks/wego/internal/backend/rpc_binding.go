package backend

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// rpcSchema 持久保存显式绑定，跨实例注册的 checksum 不包含 worker_key 或时间
func rpcSchema(namespace, method, shape, mode string) ([]byte, error) {
	if shape == "" {
		shape = "unary"
	}
	schema := inputSchema(shape)
	schema["x-wego-binding"] = rpcBinding{Kind: "rpc", Version: wire.Version, Namespace: namespace, Method: method, Workflow: binding.WorkflowName(namespace, method), Action: binding.Action(namespace, method), Shape: shape, Mode: mode}
	return json.Marshal(schema)
}

// rpcBinding 是存储在 workflow version 的不可变协议约定，不包含实例随机身份
type rpcBinding struct {
	// Kind 表示注册资源类别，protobuf 输入不会将普通任务变为 RPC。
	Kind string `json:"kind"`
	// Version 区分升级前后的任务 envelope
	Version int `json:"version"`
	// Namespace、Method 固定逻辑调用范围
	Namespace string `json:"namespace"`
	// Method 保留 protobuf 大小写
	Method string `json:"method"`
	// Workflow 是供人阅读的共享定义名称
	Workflow string `json:"workflow"`
	// Action 是引擎的小写稳定路由键
	Action string `json:"action"`
	// Shape 固定请求是单条还是批次数组
	Shape string `json:"shape"`
	// Mode 固定持久或实时输出，unary 为空
	Mode string `json:"mode"`
	// InputType 是显式 protobuf 输入约束，空值表示 JSON 入口。
	InputType string `json:"input_type,omitempty"`
	// OutputType 固定返回 protobuf 类型，不从 payload 字段猜测任务身份。
	OutputType string `json:"output_type,omitempty"`
	// Tasks 按定义保存输入输出约束，DAG 的子任务不创建另一份 workflow。
	Tasks map[string]payloadBinding `json:"tasks,omitempty"`
}

// validateRPCBinding 从正式 API 读取当前版本；按 version ID 缓存约定，不将注册请求内存当作证据
func (b *Backend) validateRPCBinding(ctx context.Context, input ports.RPCInput) error {
	return b.ValidateTarget(ctx, input.RPCMethod(), input.RPCShape(), input.RPCMode())
}

// ValidateTarget 读取当前持久定义，按版本缓存解析，不将本地配置当作已部署证据。
func (b *Backend) ValidateTarget(ctx context.Context, name, shape, mode string, typeNames ...string) error {
	if proof, ok := ctx.Value(targetProofKey{}).(targetProof); ok && proof.matches(b, name, shape, mode, typeNames, ports.ExpectedOutput(ctx)) {
		return nil
	}
	if b.features == nil || b.raw == nil {
		return status.Error(codes.FailedPrecondition, "wego: deployed target validation unavailable")
	}
	budget, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	method, workflowName := "", name
	if strings.HasPrefix(name, "/") {
		if err := binding.ValidateName(b.config.Namespace, name); err != nil {
			return err
		}
		method, workflowName = name, binding.WorkflowName(b.config.Namespace, name)
	} else {
		workflowName = nativeWorkflowName(b.config.Namespace, name)
	}
	id, err := b.currentWorkflowID(budget, workflowName)
	if err != nil {
		return err
	}
	response, err := b.raw.API().WorkflowVersionGetWithResponse(budget, id, nil)
	if err != nil {
		return Normalize(err)
	}
	if err := responseStatus(response.StatusCode(), response.JSON200 != nil); err != nil {
		return err
	}
	if response.JSON200 == nil || response.JSON200.InputJsonSchema == nil {
		return status.Error(codes.FailedPrecondition, "wego: workflow has no wego protocol binding")
	}
	version := response.JSON200.Metadata.Id
	if uuid.Validate(version) != nil {
		return status.Error(codes.DataLoss, "wego: workflow version identity missing")
	}
	stored, err := b.cachedBinding(version, *response.JSON200.InputJsonSchema)
	if err != nil {
		return err
	}
	return stored.validateTarget(b.config.Namespace, workflowName, method, shape, mode, typeNames, ports.ExpectedOutput(ctx))
}

// cachedBinding 以不可变版本 ID 缓存解析结果；当前版本查询仍在每次提交前执行。
func (b *Backend) cachedBinding(version string, schema map[string]any) (rpcBinding, error) {
	b.featureMu.Lock()
	stored, found := b.rpcBindings[version]
	b.featureMu.Unlock()
	if !found {
		data, err := json.Marshal(schema["x-wego-binding"])
		if err != nil {
			return rpcBinding{}, status.Error(codes.DataLoss, "wego: malformed workflow binding")
		}
		if err := json.Unmarshal(data, &stored); err != nil {
			return rpcBinding{}, status.Error(codes.DataLoss, "wego: malformed workflow binding")
		}
		// 缓存只减少重复解析，当前版本仍每次读取；删除或升级不会使用过期名称缓存
		b.featureMu.Lock()
		if b.rpcBindings == nil || len(b.rpcBindings) >= 1024 {
			b.rpcBindings = map[string]rpcBinding{}
		}
		b.rpcBindings[version] = stored
		b.featureMu.Unlock()
	}
	return stored, nil
}

// validateTarget 对照持久绑定检查地址、形态与类型；业务 JSON 的字段不参与任务分类。
func (stored *rpcBinding) validateTarget(namespace, workflowName, method, shape, mode string, typeNames []string, outputType string) error {
	if stored.Version != wire.Version || stored.Namespace != namespace || stored.Workflow != workflowName {
		return status.Error(codes.FailedPrecondition, "wego: registered workflow protocol or namespace mismatch")
	}
	if method != "" {
		if stored.Kind != "rpc" || stored.Method != method || stored.Action != binding.Action(namespace, method) || stored.Shape != shape || stored.Mode != mode {
			return status.Error(codes.FailedPrecondition, "wego: registered RPC shape or stream mode mismatch")
		}
	} else if !nativeBindingKind(stored.Kind) {
		return status.Error(codes.FailedPrecondition, "wego: target is not a native wego definition")
	}
	if err := stored.validateResponseType(outputType); err != nil {
		return err
	}
	for _, actual := range typeNames {
		if stored.InputType != "" && actual != stored.InputType {
			return status.Error(codes.InvalidArgument, "wego: input protobuf type differs from registered binding")
		}
	}
	return nil
}

// validateResponseType 生成桩的响应约束与输入约束分别检查；普通动态结果可不预先指定类型。
func (stored *rpcBinding) validateResponseType(expected string) error {
	if expected != "" && stored.OutputType != expected {
		return status.Error(codes.FailedPrecondition, "wego: response protobuf type differs from registered binding")
	}
	return nil
}

// nativeBindingKind 明确列出普通任务类别，不允许 RPC 注册绑定被普通名称调用绕过。
func nativeBindingKind(kind string) bool {
	switch kind {
	case "task", "durable", "batch", "workflow":
		return true
	}
	return false
}

// instanceName 默认与当前随机 key 对应；显式展示名称允许重复，不参与执行所有权
func instanceName(config spec.Runtime, requested, key string) string {
	if config.InstanceName != "" {
		return config.InstanceName
	}
	if requested != "" {
		return requested
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "localhost"
	}
	short := strings.ReplaceAll(key, "-", "")[:12]
	name := hostname + "-" + short
	if config.Namespace != "" {
		name = config.Namespace + "-" + name
	}
	return name
}

// WorkerKey 返回本次 SDK 实例的逻辑身份，同名 Worker 不共享它
func (w *worker) WorkerKey() string { return w.key }

// InstanceName 返回完整展示名称，不能用它定向取消执行
func (w *worker) InstanceName() string { return w.name }

// workerKey 从本实例连接的注册表恢复逻辑身份；没有注册的网络 handler 返回空值
func (b *Backend) workerKey(workerID string) string {
	if b == nil || b.observer == nil {
		return ""
	}
	b.observer.mu.Lock()
	defer b.observer.mu.Unlock()
	for key, id := range b.observer.ids {
		if id == workerID {
			return key
		}
	}
	return ""
}

// nativeWorkflowName 与注册路径共用大小写和 namespace 规则。
func nativeWorkflowName(namespace, name string) string {
	return binding.NativeName(namespace, name)
}

// nativeSchema 持久记录普通任务种类，输入字段不参与类型识别。
func nativeSchema(namespace, name, kind string) ([]byte, error) {
	schema := inputSchema("unary")
	schema["x-wego-binding"] = rpcBinding{Version: wire.Version, Namespace: namespace, Workflow: nativeWorkflowName(namespace, name), Kind: kind}
	return json.Marshal(schema)
}

// payloadBinding 保存公开定义的类型约束；不包含 handler、实例或可变配置对象。
type payloadBinding struct {
	// Input 是 handler 的 protobuf 请求类型，JSON handler 为空。
	Input string `json:"input_proto,omitempty"`
	// Output 是 handler 的 protobuf 响应类型，JSON handler 为空。
	Output string `json:"output_proto,omitempty"`
	// GoInput 描述注册参数，用于评审核对；JSON map/struct 不要求客户端 Go 类型名相同。
	GoInput string `json:"go_input"`
	// GoOutput 描述声明的返回类型，不记录业务数据。
	GoOutput string `json:"go_output"`
}

// currentWorkflowID 精确匹配名称并翻页；官方 name 参数是搜索条件，Charge 也会命中 ChargeWithRetries。
func (b *Backend) currentWorkflowID(ctx context.Context, name string) (uuid.UUID, error) {
	// offset、limit 只描述当前查询页，删除后同名创建会重新取得身份。
	offset, limit := 0, 100
	for {
		listed, err := b.raw.API().WorkflowListWithResponse(ctx, uuid.MustParse(b.config.TenantID), &rest.WorkflowListParams{Name: &name, Offset: &offset, Limit: &limit})
		if err != nil {
			return uuid.Nil, Normalize(err)
		}
		if err := responseStatus(listed.StatusCode(), listed.JSON200 != nil); err != nil {
			return uuid.Nil, err
		}
		if listed.JSON200 == nil || listed.JSON200.Rows == nil {
			return uuid.Nil, status.Error(codes.DataLoss, "wego: missing workflow list")
		}
		for _, workflow := range *listed.JSON200.Rows {
			if workflow.Name != name {
				continue
			}
			id, err := uuid.Parse(workflow.Metadata.Id)
			if err != nil {
				return uuid.Nil, status.Error(codes.DataLoss, "wego: invalid workflow identity")
			}
			return id, nil
		}
		if len(*listed.JSON200.Rows) < limit {
			return uuid.Nil, status.Errorf(codes.NotFound, "wego: target %s is not registered", name)
		}
		offset += len(*listed.JSON200.Rows)
	}
}

// inputSchema 让 Console 接受裸对象或显式 Input；流输入要求非空提交键。
func inputSchema(shape string) map[string]any {
	// header 不携带方法或类型；未知可选字段由协议解释器决定是否可忽略。
	header := map[string]any{
		"type": "object", "required": []string{"version"},
		"properties": map[string]any{
			"version":         map[string]any{"const": wire.Version},
			"encoding":        map[string]any{"enum": []string{"json", "base64"}},
			"response":        map[string]any{"enum": []string{"native", "wego"}},
			"idempotency_key": map[string]any{"type": "string", "minLength": 1},
			"codec_ids":       map[string]any{"type": "array", "maxItems": 16, "items": map[string]any{"type": "string", "minLength": 1}},
		},
	}
	wrapped := map[string]any{
		"type": "object", "required": []string{"_wego", "value"},
		"properties": map[string]any{"_wego": header, "value": map[string]any{}, "routing": map[string]any{"type": "object"}},
	}
	// JSON 数组和变换后字符串是两种不同承载方式；先看 encoding，再验证 value 的形状。
	plainValue := map[string]any{}
	if shape == "client" || shape == "bidi" {
		plainValue = map[string]any{"type": "array", "items": map[string]any{}}
	}
	wrapped["if"] = map[string]any{"properties": map[string]any{"_wego": map[string]any{
		"required": []string{"encoding"}, "properties": map[string]any{"encoding": map[string]any{"const": "base64"}},
	}}}
	wrapped["then"] = map[string]any{"properties": map[string]any{"value": map[string]any{"type": "string"}}}
	wrapped["else"] = map[string]any{"properties": map[string]any{"value": plainValue}}
	// 非空 codec 链必须显式声明 base64；注册表未知 ID 仍由运行时在 I/O 前拒绝。
	header["if"] = map[string]any{"required": []string{"codec_ids"}, "properties": map[string]any{"codec_ids": map[string]any{"minItems": 1}}}
	header["then"] = map[string]any{"required": []string{"encoding"}, "properties": map[string]any{"encoding": map[string]any{"const": "base64"}}}
	if shape != "unary" {
		header["required"] = []string{"version", "idempotency_key"}
		return wrapped
	}
	return map[string]any{"type": "object", "oneOf": []any{map[string]any{"type": "object", "not": map[string]any{"required": []string{"_wego"}}}, wrapped}}
}

// targetProofKey 使目标校验凭据只能由本后端创建，业务 context 无法伪造。
type targetProofKey struct{}

// targetProof 仅在一次提交链路使用；名称和流契约与生成凭据时完全相同才复用。
type targetProof struct {
	// owner 防止其他 Backend 共用凭据。
	owner *Backend
	// name 是本次调用的原始地址，不是由输入内容猜测的目标。
	name string
	// shape 固定 unary/client/server/bidi。
	shape string
	// mode 固定 reliable/realtime。
	mode string
	// outputType 固定生成桩的响应类型；修改约束不能复用旧凭据。
	outputType string
	// types 是已经验证的序列化类型列表。
	types []string
}

// PrepareTarget 在 codec I/O 前核验目标，后端提交不再重复查询同一绑定。
func (b *Backend) PrepareTarget(ctx context.Context, name, shape, mode string, types ...string) (context.Context, error) {
	if err := b.ValidateTarget(ctx, name, shape, mode, types...); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, targetProofKey{}, targetProof{owner: b, name: name, shape: shape, mode: mode, types: append([]string(nil), types...), outputType: ports.ExpectedOutput(ctx)}), nil
}

// matches 无类型参数的提交阶段只检查已验证目标；显式类型校验仍要求相同冻结列表。
func (proof *targetProof) matches(owner *Backend, name, shape, mode string, types []string, outputType string) bool {
	if proof.owner != owner || proof.name != name || proof.shape != shape || proof.mode != mode || proof.outputType != outputType {
		return false
	}
	if len(types) == 0 {
		return true
	}
	return slices.Equal(proof.types, types)
}
