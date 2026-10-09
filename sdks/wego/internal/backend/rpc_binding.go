package backend

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
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
	streamShape := shape
	if shape == "unary" {
		streamShape = ""
	}
	return json.Marshal(map[string]any{
		"type": "object", "required": []string{"version", "method", "payload"},
		"properties":     map[string]any{"version": map[string]any{"const": wire.Version}, "method": map[string]any{"const": method}, "stream": map[string]any{"const": streamShape}, "stream_mode": map[string]any{"const": mode}},
		"x-wego-binding": rpcBinding{Version: wire.Version, Namespace: namespace, Method: method, Workflow: binding.WorkflowName(namespace, method), Action: binding.Action(namespace, method), Shape: shape, Mode: mode},
	})
}

// rpcBinding 是存储在 workflow version 的不可变协议约定，不包含实例随机身份
type rpcBinding struct {
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
}

// validateRPCBinding 从正式 API 读取当前版本；按 version ID 缓存约定，不将注册请求内存当作证据
func (b *Backend) validateRPCBinding(ctx context.Context, input ports.RPCInput) error {
	budget, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	method := input.RPCMethod()
	workflow, err := b.features.Workflows().Get(budget, binding.WorkflowName(b.config.Namespace, method))
	if err != nil {
		return Normalize(err)
	}
	id, err := uuid.Parse(workflow.Metadata.Id)
	if err != nil {
		return status.Error(codes.DataLoss, "wego: registered workflow identity is invalid")
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
	b.featureMu.Lock()
	stored, found := b.rpcBindings[version]
	b.featureMu.Unlock()
	if !found {
		data, err := json.Marshal((*response.JSON200.InputJsonSchema)["x-wego-binding"])
		if err != nil {
			return status.Error(codes.DataLoss, "wego: malformed workflow binding")
		}
		if err := json.Unmarshal(data, &stored); err != nil {
			return status.Error(codes.DataLoss, "wego: malformed workflow binding")
		}
		// 缓存只减少重复解析，当前版本仍每次读取；删除或升级不会使用过期名称缓存
		b.featureMu.Lock()
		if b.rpcBindings == nil || len(b.rpcBindings) >= 1024 {
			b.rpcBindings = map[string]rpcBinding{}
		}
		b.rpcBindings[version] = stored
		b.featureMu.Unlock()
	}
	if stored.Version != wire.Version || stored.Namespace != b.config.Namespace || stored.Method != method || stored.Workflow != binding.WorkflowName(b.config.Namespace, method) || stored.Action != binding.Action(b.config.Namespace, method) || stored.Shape != input.RPCShape() || stored.Mode != input.RPCMode() {
		return status.Error(codes.FailedPrecondition, "wego: registered RPC version, shape or stream mode differs from caller")
	}
	return nil
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
