package wire

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"time"
	"unicode/utf8"
)

// binaryMetadata 将所有 metadata 值视为字节，JSON 自动使用 base64，避免 ff00fe 被 UTF-8 替换
type binaryMetadata map[string][][]byte

// metadataBytes 复制多值 metadata，例如 token-bin 的两项任意字节均独立保留
func metadataBytes(md map[string][]string) binaryMetadata {
	// out 仅用于传输，不能把转换后的切片交回业务修改
	out := make(binaryMetadata, len(md))
	// key 是 metadata 名称，values 按原顺序编码，不合并同名值
	for key, values := range md {
		// value 是一项原始字节串，ASCII 和 -bin 使用同一无损编码
		for _, value := range values {
			out[key] = append(out[key], []byte(value))
		}
	}
	return out
}

// strings 恢复 gRPC 要求的原始 string 字节，不做 UTF-8 校验或替换
func (md binaryMetadata) strings() map[string][]string {
	// out 是独立的业务视图
	out := make(map[string][]string, len(md))
	// key 与 values 保持原有多值顺序
	for key, values := range md {
		// value 可以包含 00 或 ff，转换成 string 保留全部字节
		for _, value := range values {
			out[key] = append(out[key], string(value))
		}
	}
	return out
}

// protocolHeader 保存按需出现的协议信息；方法和类型由注册绑定决定。
type protocolHeader struct {
	// Version 协议解释版本；不读取协议 5。
	Version int `json:"version"`
	// Response 结果契约，缺省 native；不用于身份认证。
	Response string `json:"response,omitempty"`
	// Encoding value 的 JSON 或 base64 字节表示。
	Encoding string `json:"encoding,omitempty"`
	// CodecIDs 实际发送步骤，最多 16 项；重复项仍执行。
	CodecIDs []string `json:"codec_ids,omitempty"`
	// Metadata 普通值可读，-bin 值使用 base64。
	Metadata map[string][]string `json:"metadata,omitempty"`
	// Trace 按需携带实例的 trace 传播字段。
	Trace map[string]string `json:"trace,omitempty"`
	// Deadline UTC RFC3339Nano，避免纳秒整数的浮点精度损失。
	Deadline string `json:"deadline,omitempty"`
	// IdempotencyKey 流提交键；unary 不依赖这个字段。
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	// InputDigest 流恢复或事件共享内容的规范摘要。
	InputDigest string `json:"input_digest,omitempty"`
	// StreamMode 直接流调用对输出传输的预期。
	StreamMode string `json:"stream_mode,omitempty"`
	// Headers 成功响应的业务 headers。
	Headers map[string][]string `json:"headers,omitempty"`
	// Trailers 最终状态对应的业务 trailers。
	Trailers map[string][]string `json:"trailers,omitempty"`
}

// transportEnvelope 只在最外层解释 _wego，value 内的保留字段仍是业务数据。
type transportEnvelope struct {
	// Header 最外层保留标记，value 内同名字段不会再次拆包。
	Header protocolHeader `json:"_wego"`
	// Value 业务序列化值或变换后的 base64 字符串。
	Value json.RawMessage `json:"value"`
	// Routing 独立明文调度对象；输出不携带输入 routing。
	Routing map[string]any `json:"routing,omitempty"`
}

// readableMetadata 只将 -bin 的原始字节转成 base64，普通请求头在 Console 中可读。
func readableMetadata(md map[string][]string, decode bool) (map[string][]string, error) {
	if len(md) == 0 {
		return nil, nil
	}
	out := make(map[string][]string, len(md))
	for key, values := range md {
		copied := make([]string, len(values))
		for i, value := range values {
			if strings.HasSuffix(key, "-bin") {
				if decode {
					data, err := base64.StdEncoding.DecodeString(value)
					if err != nil {
						return nil, fmt.Errorf("wego: invalid binary metadata")
					}
					copied[i] = string(data)
				} else {
					copied[i] = base64.StdEncoding.EncodeToString([]byte(value))
				}
			} else {
				if !utf8.ValidString(value) {
					return nil, fmt.Errorf("wego: non-binary metadata must be UTF-8")
				}
				copied[i] = value
			}
		}
		out[key] = copied
	}
	return out, nil
}

// MarshalJSON 值接收者保证 map 元素和 any 中的封包也执行相同传输规则。
func (e Envelope) MarshalJSON() ([]byte, error) {
	if e.opened {
		return nil, fmt.Errorf("wego: decoded value cannot be marshaled as its stored encoding; seal a new message")
	}
	if !e.sealed {
		return nil, fmt.Errorf("wego: envelope must be sealed before transport")
	}
	if e.Native {
		return append([]byte(nil), e.Payload...), nil
	}
	h, err := e.protocolHeader()
	if err != nil {
		return nil, err
	}
	return json.Marshal(transportEnvelope{Header: h, Value: e.Payload, Routing: e.Routing})
}

// protocolHeader 在变换前验证发送头；非法 metadata 不得先触发对象上传。
func (e Envelope) protocolHeader() (protocolHeader, error) {
	md, err := readableMetadata(e.Metadata, false)
	if err != nil {
		return protocolHeader{}, err
	}
	headers, err := readableMetadata(e.Headers, false)
	if err != nil {
		return protocolHeader{}, err
	}
	trailers, err := readableMetadata(e.Trailers, false)
	if err != nil {
		return protocolHeader{}, err
	}
	h := protocolHeader{Version: e.Version, Response: e.Response, Encoding: e.Encoding, CodecIDs: e.CodecIDs, Metadata: md, Trace: e.Trace, IdempotencyKey: e.IdempotencyKey, InputDigest: e.InputDigest, StreamMode: e.StreamMode, Headers: headers, Trailers: trailers}
	if err := validateHeader(h); err != nil {
		return protocolHeader{}, err
	}
	if e.Deadline != 0 {
		h.Deadline = time.Unix(0, e.Deadline).UTC().Format(time.RFC3339Nano)
	}
	head, err := json.Marshal(h)
	if err != nil {
		return protocolHeader{}, err
	}
	if len(head) > 64<<10 {
		return protocolHeader{}, status.Error(codes.ResourceExhausted, "wego: protocol header exceeds 64 KiB")
	}
	return h, nil
}

// UnmarshalJSON 原生值不拆包；标记存在但畸形时必须失败，不能绕过 codec。
func (e *Envelope) UnmarshalJSON(data []byte) error {
	if !json.Valid(data) {
		return status.Error(codes.DataLoss, "wego: expected one JSON value")
	}
	// root 只解析最外层；裸对象不按 value/routing 字段猜测协议。
	var root map[string]json.RawMessage
	// duplicate 只影响显式协议；原生业务对象由其自身 JSON 解码规则解释。
	var duplicate bool
	if len(bytes.TrimSpace(data)) != 0 && bytes.TrimSpace(data)[0] == '{' {
		// err 保留外层读取失败，不能留下半解析的协议对象。
		var err error
		root, duplicate, err = protocolObject(data)
		if err != nil {
			return err
		}
	}
	marker, marked := root["_wego"]
	if !marked {
		*e = Envelope{Version: Version, Native: true, Payload: append(json.RawMessage(nil), data...), sealed: true}
		return nil
	}
	if duplicate {
		return status.Error(codes.InvalidArgument, "wego: duplicate envelope field")
	}
	h, err := readHeader(marker)
	if err != nil {
		return err
	}
	value, present := root["value"]
	if !present {
		return status.Error(codes.InvalidArgument, "wego: missing value")
	}
	if h.Encoding == "base64" && (len(value) == 0 || value[0] != '"') {
		return status.Error(codes.InvalidArgument, "wego: base64 value must be a string")
	}
	routing, err := readRouting(root["routing"])
	if err != nil {
		return err
	}
	decoded, err := h.envelope(value, routing)
	if err != nil {
		return err
	}
	*e = decoded
	return nil
}

// readHeader 保留未知可选字段，但拒绝不支持的版本、编码或畸形保留标记。
func readHeader(marker json.RawMessage) (protocolHeader, error) {
	if len(marker) > 64<<10 {
		return protocolHeader{}, status.Error(codes.ResourceExhausted, "wego: protocol header exceeds 64 KiB")
	}
	if len(bytes.TrimSpace(marker)) == 0 || bytes.TrimSpace(marker)[0] != '{' {
		return protocolHeader{}, status.Error(codes.InvalidArgument, "wego: _wego must be an object")
	}
	// fields 同时完成字段快照与去重检查，不再次复制整个业务 value。
	fields, duplicate, err := protocolObject(marker)
	if err != nil {
		return protocolHeader{}, status.Error(codes.InvalidArgument, "wego: invalid protocol header")
	}
	if duplicate {
		return protocolHeader{}, status.Error(codes.InvalidArgument, "wego: duplicate protocol header field")
	}
	for key, raw := range fields {
		switch strings.ToLower(key) {
		case "version", "response", "encoding", "codec_ids", "metadata", "trace", "deadline", "idempotency_key", "input_digest", "stream_mode", "headers", "trailers":
			if key != strings.ToLower(key) || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return protocolHeader{}, status.Errorf(codes.InvalidArgument, "wego: invalid protocol field %q", key)
			}
		}
	}
	// h 仅含可选协议数据，不保存注册目标。
	var h protocolHeader
	if err := json.Unmarshal(marker, &h); err != nil {
		return protocolHeader{}, status.Errorf(codes.InvalidArgument, "wego: invalid protocol header: %v", err)
	}
	if err := validateHeader(h); err != nil {
		return protocolHeader{}, err
	}
	return h, nil
}

// protocolObject 一次读取当前层字段并记录重复键，不递归解释业务 value。
func protocolObject(data []byte) (map[string]json.RawMessage, bool, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if _, err := decoder.Token(); err != nil {
		return nil, false, err
	}
	fields := make(map[string]json.RawMessage)
	duplicate := false
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, false, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, false, status.Error(codes.InvalidArgument, "wego: protocol object requires string keys")
		}
		if _, exists := fields[key]; exists {
			duplicate = true
		}
		// value 保留原始 JSON；后续解析根据注册类型进行，不扫描嵌套协议标记。
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, false, err
		}
		fields[key] = value
	}
	_, err := decoder.Token()
	return fields, duplicate, err
}

// envelope 原子还原 metadata 与截止时间；出错时不修改调用方已有的 Envelope。
func (h *protocolHeader) envelope(value json.RawMessage, routing map[string]any) (Envelope, error) {
	// e 是完整解析后的候选值，仅在全部校验通过后交付。
	var e Envelope
	md, err := readableMetadata(h.Metadata, true)
	if err != nil {
		return Envelope{}, err
	}
	headers, err := readableMetadata(h.Headers, true)
	if err != nil {
		return Envelope{}, err
	}
	trailers, err := readableMetadata(h.Trailers, true)
	if err != nil {
		return Envelope{}, err
	}
	e = Envelope{Version: h.Version, Response: h.Response, Encoding: h.Encoding, CodecIDs: h.CodecIDs, Payload: value, Routing: routing, Metadata: md, Trace: h.Trace, Headers: headers, Trailers: trailers, IdempotencyKey: h.IdempotencyKey, InputDigest: h.InputDigest, StreamMode: h.StreamMode, sealed: true}
	if h.Deadline != "" {
		deadline, err := time.Parse(time.RFC3339Nano, h.Deadline)
		if err != nil {
			return Envelope{}, status.Error(codes.InvalidArgument, "wego: invalid deadline")
		}
		e.Deadline = deadline.UnixNano()
		if !time.Unix(0, e.Deadline).Equal(deadline) {
			return Envelope{}, status.Error(codes.InvalidArgument, "wego: deadline outside supported timestamp range")
		}
	}
	return e, nil
}

// readRouting 只接受明文对象，不对数值重新转换为 float64。
func readRouting(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, status.Error(codes.InvalidArgument, "wego: routing must be an object")
	}
	// routing 用 UseNumber 保存明文调度对象，不触发 codec。
	var routing map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&routing); err != nil {
		return nil, status.Error(codes.InvalidArgument, "wego: routing must be an object")
	}
	return routing, nil
}

// responseKey 的模式只属于本次执行，子调用仍显式创建自己的 SDK 请求。
type responseKey struct{}

// WithResponse 固定执行响应契约，业务 context 派生不改变该选择。
func WithResponse(ctx context.Context, wego bool) context.Context {
	return context.WithValue(ctx, responseKey{}, wego)
}

// WegoResponse 未绑定时供内部执行使用 wego 默认值；原生入口显式绑定 false。
func WegoResponse(ctx context.Context) bool {
	value, ok := ctx.Value(responseKey{}).(bool)
	return !ok || value
}

// Output 根据执行契约准备结果，原生结果不会经过 Worker 的编码链。
func Output(ctx context.Context, e Envelope, config spec.Runtime, maxBusiness, maxPacket, maxEncoded int) (Envelope, error) {
	e.Metadata, e.Trace, e.Routing = nil, nil, nil
	e.Deadline = 0
	if !WegoResponse(ctx) {
		if err := ctx.Err(); err != nil {
			return Envelope{}, err
		}
		if len(e.Payload) > maxBusiness {
			return Envelope{}, status.Error(codes.ResourceExhausted, "wego: native output exceeds payload limit")
		}
		if !json.Valid(e.Payload) {
			return Envelope{}, status.Error(codes.InvalidArgument, "wego: native output requires one JSON value")
		}
		e.Native = true
		e.Response = ""
		e.sealed = true
		e.opened = false
		return e, nil
	}
	return Seal(ctx, e, config, maxBusiness, maxPacket, maxEncoded)
}

// triggerContextKey 只影响未来任务 deadline，不改变创建操作自身的 I/O 预算。
type triggerContextKey struct{}

// ForTrigger 防止今晚的三秒创建预算成为明天的执行截止时间。
func ForTrigger(ctx context.Context) context.Context {
	return context.WithValue(ctx, triggerContextKey{}, true)
}

// validateHeader 在任何 codec I/O 前拒绝版本、模式和编码组合错误。
func validateHeader(h protocolHeader) error {
	if h.Version != Version {
		return status.Error(codes.FailedPrecondition, "wego: unsupported protocol version")
	}
	if h.Response != "" && h.Response != "native" && h.Response != "wego" {
		return status.Error(codes.InvalidArgument, "wego: unsupported response mode")
	}
	if h.Encoding != "" && h.Encoding != "json" && h.Encoding != "base64" {
		return status.Error(codes.FailedPrecondition, "wego: unsupported value encoding")
	}
	if h.StreamMode != "" && h.StreamMode != "reliable" && h.StreamMode != "realtime" {
		return status.Error(codes.InvalidArgument, "wego: unsupported stream mode")
	}
	if len(h.CodecIDs) > 16 {
		return status.Error(codes.ResourceExhausted, "wego: codec chain exceeds 16 steps")
	}
	if len(h.CodecIDs) > 0 && h.Encoding != "base64" {
		return status.Error(codes.InvalidArgument, "wego: codec value must use base64")
	}

	return nil
}
