package stream

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

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

// VerifyRunIdentity 在幂等冲突恢复前核对持久输入，不能因相同 key 就消费另一个调用
func VerifyRunIdentity(input json.RawMessage, digest string) error {
	// packet 的响应契约和内容摘要来自持久输入，方法由运行绑定核验。
	var packet wire.Envelope
	if err := json.Unmarshal(input, &packet); err != nil {
		return err
	}
	if packet.Native || packet.Response != "wego" || packet.InputDigest == "" || packet.InputDigest != digest {
		return status.Error(codes.FailedPrecondition, "wego: idempotency key belongs to a different input or response contract")
	}
	return nil
}

// ExecutionDigest 将事件共享内容绑定到实际注册方法，同一事件不会混用多个输出 topic。
func ExecutionDigest(namespace, method, digest string) string {
	identity := sha256.Sum256([]byte(namespace + "\x00" + method + "\x00" + digest))
	return hex.EncodeToString(identity[:])
}
