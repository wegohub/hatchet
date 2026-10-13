package wire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/message"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// EncodeEvent 冻结事件默认值，多个接收 handler 共享同一份调度快照。
func EncodeEvent(ctx context.Context, config spec.Runtime, key string, value any) (Envelope, error) {
	if key == "" {
		return Envelope{}, fmt.Errorf("wego: event key cannot be empty")
	}
	routing, err := spec.SnapshotRouting(config.EventRoutingDefaults[key])
	if err != nil {
		return Envelope{}, err
	}
	if routing == nil {
		routing = map[string]any{}
	}
	value, explicit, err := InputValue(value)
	if err != nil {
		return Envelope{}, err
	}
	for key, item := range explicit {
		routing[key] = item
	}
	routing, err = spec.SnapshotRouting(routing)
	if err != nil {
		return Envelope{}, err
	}
	limits := config.PayloadLimits
	plain, typeName, err := MarshalValue(value, limits.MaxMessageBytes)
	if err != nil {
		return Envelope{}, err
	}
	envelope := NewEnvelope(ForTrigger(ctx), "", typeName, plain, routing)

	envelope.IdempotencyKey = "event:" + uuid.NewString()
	envelope.InputDigest, err = EventDigest(plain, routing)
	if err != nil {
		return Envelope{}, err
	}
	budget, cancel := context.WithTimeout(ctx, limits.EncodeTimeout)
	defer cancel()
	return Seal(budget, envelope, config, limits.MaxMessageBytes, limits.MaxMessageBytes+(64<<10), limits.MaxEncodedBytes)
}

// EventDigest 在 codec 前生成与接收方法无关的内容摘要；对象路径和加密 nonce 不参与。
func EventDigest(payload json.RawMessage, routing map[string]any) (string, error) {
	if !json.Valid(payload) {
		return "", fmt.Errorf("wego: event payload must be one JSON value")
	}
	if len(routing) == 0 {
		routing = nil
	}
	canonical, err := json.Marshal(struct {
		// Value 仅含 codec 前业务数据，类型和目标来自注册信息。
		Value json.RawMessage `json:"value"`
		// Routing 将不同调度策略区分为不同流输入。
		Routing map[string]any `json:"routing"`
	}{payload, routing})
	if err != nil {
		return "", err
	}
	canonical, err = message.Canonical(canonical)
	if err != nil {
		return "", err
	}

	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

// EncodeValue 为静态输入准备有界协议值，目标由注册位置提供。
func EncodeValue(ctx context.Context, config spec.Runtime, value any, routing map[string]any) (Envelope, error) {
	limits := config.PayloadLimits
	plain, typeName, err := MarshalValue(value, limits.MaxMessageBytes)
	if err != nil {
		return Envelope{}, err
	}
	envelope := NewEnvelope(ctx, "", typeName, plain, routing)
	budget, cancel := context.WithTimeout(ctx, limits.EncodeTimeout)
	defer cancel()
	return Seal(budget, envelope, config, limits.MaxMessageBytes, limits.MaxMessageBytes+(64<<10), limits.MaxEncodedBytes)
}

// EncodeFilter 保持业务附加值为 JSON 对象；CEL 读取独立 routing。
func EncodeFilter(ctx context.Context, config spec.Runtime, target string, value any) (Envelope, error) {
	value, routing, err := InputValue(value)
	if err != nil {
		return Envelope{}, err
	}
	snapshot, err := spec.SnapshotRouting(routing)
	if err != nil {
		return Envelope{}, err
	}
	plain, _, err := MarshalValue(value, config.PayloadLimits.MaxMessageBytes)
	if err != nil {
		return Envelope{}, err
	}
	// FilterPayload 的公开视图是 map，不能将 scalar 或 protobuf WKT 冒充对象。
	if len(plain) == 0 || plain[0] != '{' {
		return Envelope{}, fmt.Errorf("wego: filter payload must be a JSON object")
	}
	packet := NewEnvelope(ForTrigger(ctx), "", "", plain, snapshot)
	budget, cancel := context.WithTimeout(ctx, config.PayloadLimits.EncodeTimeout)
	defer cancel()
	return Seal(budget, packet, config, config.PayloadLimits.MaxMessageBytes, config.PayloadLimits.MaxMessageBytes+(64<<10), config.PayloadLimits.MaxEncodedBytes)
}

// InputValue 仅解开显式输入描述；业务 map 的 value/routing 字段不具有协议含义。
func InputValue(input any) (any, map[string]any, error) {
	switch value := input.(type) {
	case model.Input:
		return value.Value, value.Routing, nil
	case *model.Input:
		if value == nil {
			return nil, nil, fmt.Errorf("wego: nil input description")
		}
		return value.Value, value.Routing, nil
	default:
		return input, nil, nil
	}
}
