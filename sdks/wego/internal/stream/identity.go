package stream

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// IdempotencyKey 在租户共享的官方幂等空间内隔离 namespace、方法与业务键
// 同一 namespace 的同一方法复用 operationKey 才指向同一逻辑调用
func IdempotencyKey(namespace, method, operationKey string) string {
	identity := sha256.Sum256([]byte(namespace + "\x00" + method + "\x00" + operationKey))
	return "rpc:" + hex.EncodeToString(identity[:])
}

// InputDigest 计算 codec 之前的规范 JSON 输入和 routing，不将随机 nonce 或对象引用纳入调用身份
func InputDigest(method string, messages []json.RawMessage, routing map[string]any) (string, error) {
	inputs := make([]any, len(messages))
	for i, message := range messages {
		decoder := json.NewDecoder(bytes.NewReader(message))
		decoder.UseNumber()
		if err := decoder.Decode(&inputs[i]); err != nil {
			return "", status.Error(codes.InvalidArgument, "wego: input identity requires valid ProtoJSON")
		}
		// trailing 必须为 EOF；一个消息不能偷偷夹带第二个 JSON 值
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return "", status.Error(codes.InvalidArgument, "wego: input identity has trailing JSON")
		}
	}
	if routing == nil {
		routing = map[string]any{}
	}
	canonical, err := json.Marshal(struct {
		// Method 将相同业务形状的不同 RPC 区分开
		Method string `json:"method"`
		// Messages 保留请求顺序，空流规范表示 []
		Messages []any `json:"messages"`
		// Routing 纳入分组、成本等调度身份，不能把不同策略当作同一次重试
		Routing map[string]any `json:"routing"`
	}{method, inputs, routing})
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "wego: routing cannot be encoded as JSON: %v", err)
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

// VerifyRunIdentity 在幂等冲突恢复前核对持久输入，不能因相同 key 就消费另一个调用
func VerifyRunIdentity(input json.RawMessage, method, digest string) error {
	// identity 只读取明确的协议字段，不依赖业务解密结果或猜测 JSON 标签
	var identity struct {
		// Version 是实际提交的 RPC 协议版本
		Version uint32 `json:"version"`
		// Method 必须与当前生成客户端的完整方法名相同
		Method string `json:"method"`
		// Digest 来自 codec 之前的规范请求和 routing
		Digest string `json:"input_digest"`
	}
	if err := json.Unmarshal(input, &identity); err != nil {
		return status.Error(codes.DataLoss, "wego: persisted RPC input is not JSON")
	}
	if identity.Version != wire.LogVersion || identity.Method != method || identity.Digest == "" || identity.Digest != digest {
		return status.Error(codes.FailedPrecondition, "wego: idempotency key belongs to a different RPC input or routing")
	}
	return nil
}
