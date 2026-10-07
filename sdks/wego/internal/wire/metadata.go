package wire

import (
	"context"
	"encoding/json"
)

// binaryMetadata 将所有 metadata 值视为字节，JSON 自动使用 base64，避免 ff00fe 被 UTF-8 替换。
type binaryMetadata map[string][][]byte

// metadataBytes 复制多值 metadata，例如 token-bin 的两项任意字节均独立保留。
func metadataBytes(md map[string][]string) binaryMetadata {
	// out 仅用于传输，不能把转换后的切片交回业务修改。
	out := make(binaryMetadata, len(md))
	// key 是 metadata 名称，values 按原顺序编码，不合并同名值。
	for key, values := range md {
		// value 是一项原始字节串，ASCII 和 -bin 使用同一无损编码。
		for _, value := range values {
			out[key] = append(out[key], []byte(value))
		}
	}
	return out
}

// strings 恢复 gRPC 要求的原始 string 字节，不做 UTF-8 校验或替换。
func (md binaryMetadata) strings() map[string][]string {
	// out 是独立的业务视图。
	out := make(map[string][]string, len(md))
	// key 与 values 保持原有多值顺序。
	for key, values := range md {
		// value 可以包含 00 或 ff，转换成 string 保留全部字节。
		for _, value := range values {
			out[key] = append(out[key], string(value))
		}
	}
	return out
}

// envelopeJSON 在别名字段上覆盖 metadata 的 JSON 表示，其余字段仍由标准编码器处理。
type envelopeJSON struct {
	// envelopeAlias 防止 MarshalJSON 递归调用自身。
	envelopeAlias
	// Metadata 使用 bytes/base64 表示请求 metadata。
	Metadata binaryMetadata `json:"metadata,omitempty"`
	// Headers 使用相同规则传输响应头。
	Headers binaryMetadata `json:"headers,omitempty"`
	// Trailers 在成功与业务错误中均保留任意字节。
	Trailers binaryMetadata `json:"trailers,omitempty"`
}

// envelopeAlias 与 Envelope 字段一致，但不带自定义 JSON 方法。
type envelopeAlias Envelope

// MarshalJSON 仅在传输边界编码 metadata，内存中的 Envelope 仍保存 gRPC 原始值。
func (e Envelope) MarshalJSON() ([]byte, error) {
	return json.Marshal(envelopeJSON{envelopeAlias(e), metadataBytes(e.Metadata), metadataBytes(e.Headers), metadataBytes(e.Trailers)})
}

// UnmarshalJSON 恢复全部多值字节；非法 base64 返回错误，不能静默替换认证数据。
func (e *Envelope) UnmarshalJSON(data []byte) error {
	// decoded 先完整校验，再替换目标，避免失败时留下半个 envelope。
	var decoded envelopeJSON
	// err 包括 JSON 格式和 base64 错误。
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*e = Envelope(decoded.envelopeAlias)
	e.Metadata, e.Headers, e.Trailers = decoded.Metadata.strings(), decoded.Headers.strings(), decoded.Trailers.strings()
	return nil
}

// triggerContextKey 只控制写入远期任务的 deadline，不改变当前编码操作的取消能力。
type triggerContextKey struct{}

// ForTrigger 保留发布请求的预算和 metadata，envelope 不携带其绝对 deadline。
// 例如今晚创建明天的 Schedule，今晚的 3 秒上传预算不能成为明天任务的截止时间。
func ForTrigger(ctx context.Context) context.Context {
	return context.WithValue(ctx, triggerContextKey{}, true)
}
