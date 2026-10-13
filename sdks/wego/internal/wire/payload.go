package wire

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/message"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// MarshalValue 只选择序列化器；注册绑定负责决定任务种类和业务类型。
func MarshalValue(value any, maxBytes int) (json.RawMessage, string, error) {
	data, kind, err := message.Marshal(value, maxBytes)
	if err != nil {
		code := codes.InvalidArgument
		if strings.Contains(err.Error(), "exceeds limit") {
			code = codes.ResourceExhausted
		}
		return nil, "", status.Error(code, err.Error())
	}
	return data, kind, nil
}

// validatePlainValue 在任何变换前检查协议版本、业务预算和单个 JSON 值。
func validatePlainValue(e Envelope, limit int) error {
	if e.Version != Version {
		return status.Error(codes.FailedPrecondition, "wego: unsupported protocol version")
	}
	if len(e.Payload) > limit {
		return status.Error(codes.ResourceExhausted, "wego: message exceeds payload limit")
	}
	if !json.Valid(e.Payload) {
		return status.Error(codes.InvalidArgument, "wego: value requires one JSON value")
	}
	return nil
}

// prepareEncoding 冻结调度字段、发送链并校验明文头，任何 codec I/O 必须在此之后。
func prepareEncoding(e Envelope, config spec.Runtime, maxBusiness int) (Envelope, *spec.PayloadCodecs, error) {
	if err := validatePlainValue(e, maxBusiness); err != nil {
		return Envelope{}, nil, err
	}
	routing, err := spec.SnapshotRouting(e.Routing)
	if err != nil {
		return Envelope{}, nil, err
	}
	e.Routing = routing
	registry, err := config.Codecs()
	if err != nil {
		return Envelope{}, nil, err
	}
	e.CodecIDs = registry.EncodingIDs()
	// 明文头的错误先于 codec I/O；例如 70 KiB metadata 不能遗留 S3 对象。
	if len(e.CodecIDs) > 0 {
		e.Encoding = "base64"
		if _, err := e.protocolHeader(); err != nil {
			return Envelope{}, nil, err
		}
	}
	return e, registry, nil
}

// Seal 只变换业务值；routing、trace 和调用头保持独立、可读。
func Seal(ctx context.Context, e Envelope, config spec.Runtime, maxBusiness, maxPacket, maxEncoded int) (Envelope, error) {
	if err := ctx.Err(); err != nil {
		return Envelope{}, err
	}
	if e.sealed && !e.opened {
		return e, nil
	}
	e, registry, err := prepareEncoding(e, config, maxBusiness)
	if err != nil {
		return Envelope{}, err
	}
	data := []byte(e.Payload)
	if len(e.CodecIDs) != 0 {
		// 自定义组件可以原地处理输入；不能因此修改可复用的 codec 前快照。
		data = slices.Clone(data)
	}
	data, e.CodecIDs, err = encodeValueChain(ctx, registry, e.CodecIDs, data, maxEncoded)
	if err != nil {
		return Envelope{}, err
	}
	if err := ctx.Err(); err != nil {
		return Envelope{}, err
	}
	// 无变换时仍可字节承载不安全数字，避免服务端把 9007199254740993 转成 float64。
	if len(e.CodecIDs) != 0 || !message.Portable(data) {
		e.Encoding = "base64"
		e.Payload, err = json.Marshal(data)
		if err != nil {
			return Envelope{}, err
		}
	} else {
		e.Encoding = ""
		e.Payload = slices.Clone(e.Payload)
	}
	e.sealed, e.opened = true, false
	encoded, err := json.Marshal(e)
	if err != nil {
		return Envelope{}, err
	}
	if len(encoded) > maxEncoded {
		return Envelope{}, status.Error(codes.ResourceExhausted, "wego: encoded payload exceeds limit")
	}
	return e, nil
}

// encodeValueChain 串行执行已冻结发送链；每一步都检查取消和中间值预算。
func encodeValueChain(ctx context.Context, registry *spec.PayloadCodecs, ids []string, data []byte, limit int) ([]byte, []string, error) {
	// applied 仅记录实际执行步骤，跳过步骤不能出现在解码链中。
	applied := make([]string, 0, len(ids))
	// err 保存链中某一步失败，失败时禁止继续处理或交付。
	var err error
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if !registry.ShouldEncode(id, len(data)) {
			continue
		}
		component, _ := registry.Lookup(id)
		data, err = component.Encode(ctx, data)
		applied = append(applied, id)
		if err != nil {
			return nil, nil, err
		}
		if len(data) > limit {
			return nil, nil, status.Error(codes.ResourceExhausted, "wego: codec output exceeds limit")
		}
	}
	return data, applied, nil
}

// Open 按持久消息的 ID 解码；调用方法只用于上下文，不作为解密 AAD。
func Open(ctx context.Context, method string, e Envelope, config spec.Runtime, maxBusiness, maxEncoded int) (Envelope, error) {
	if err := ctx.Err(); err != nil {
		return Envelope{}, err
	}
	if e.Version != Version {
		return Envelope{}, status.Error(codes.FailedPrecondition, "wego: unsupported protocol version")
	}
	if e.opened {
		if len(e.Payload) > maxBusiness {
			return Envelope{}, status.Error(codes.ResourceExhausted, "wego: decoded value exceeds limit")
		}
		return e, nil
	}
	encoded, err := json.Marshal(e)
	if err != nil {
		return Envelope{}, err
	}
	if len(encoded) > maxEncoded {
		return Envelope{}, status.Error(codes.ResourceExhausted, "wego: encoded payload exceeds limit")
	}
	registry, err := config.Codecs()
	if err != nil {
		return Envelope{}, err
	}
	if err := validateValueDecoders(e.CodecIDs, registry); err != nil {
		return Envelope{}, err
	}
	data := []byte(e.Payload)
	if e.Encoding == "base64" {
		limit := maxEncoded
		if len(e.CodecIDs) == 0 {
			limit = maxBusiness
		}
		data, err = decodeBase64Value(e.Payload, limit)
		if err != nil {
			return Envelope{}, err
		}
	}
	data, err = decodeValueChain(ctx, registry, e.CodecIDs, data, maxBusiness, maxEncoded)
	if err != nil {
		return Envelope{}, err
	}
	if err := ctx.Err(); err != nil {
		return Envelope{}, err
	}
	if err := validateDecodedValue(data, maxBusiness); err != nil {
		return Envelope{}, err
	}
	e.Payload = slices.Clone(data)
	e.opened = true
	return e, nil
}

// validateDecodedValue 在业务反序列化前检查还原值，组件即使返回成功也不得越过预算或返回非 JSON。
func validateDecodedValue(data []byte, limit int) error {
	if len(data) > limit {
		return status.Error(codes.ResourceExhausted, "wego: decoded value exceeds limit")
	}
	if !json.Valid(data) {
		return status.Error(codes.DataLoss, "wego: decoded value is not JSON")
	}
	return nil
}

// validateValueDecoders 完整 ID 预检先于任何解压、解密和下载，也限制重复步骤的执行数量。
func validateValueDecoders(ids []string, registry *spec.PayloadCodecs) error {
	if len(ids) > 16 {
		return status.Error(codes.ResourceExhausted, "wego: codec chain exceeds 16 steps")
	}
	// 完整预检先于 base64 分配或对象下载，缺失内层组件不能先执行外层组件。
	for _, id := range ids {
		if _, ok := registry.Lookup(id); !ok {
			return status.Errorf(codes.FailedPrecondition, "wego: decoding CodecID %q is not registered", id)
		}
	}
	return nil
}

// decodeValueChain 按消息顺序逆序还原，最内层执行更小的业务值预算。
func decodeValueChain(ctx context.Context, registry *spec.PayloadCodecs, ids []string, data []byte, maxBusiness, maxEncoded int) ([]byte, error) {
	// err 保存链中某一步失败，失败时禁止继续处理或交付。
	var err error
	for i := len(ids) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		limit := maxEncoded
		if i == 0 {
			limit = maxBusiness
		}
		component, _ := registry.Lookup(ids[i])
		data, err = component.DecodeLimit(ctx, data, limit)
		if err != nil {
			return nil, err
		}
		if len(data) > limit {
			return nil, status.Error(codes.ResourceExhausted, "wego: restored codec value exceeds limit")
		}
	}
	return data, nil
}

// decodeBase64Value 在解码过程中截断读预算；无 codec 的精度载体也不能先分配超大业务值。
func decodeBase64Value(raw json.RawMessage, limit int) ([]byte, error) {
	// text 的大小已经由完整编码请求预算限制。
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return nil, status.Error(codes.DataLoss, "wego: invalid base64 value")
	}
	reader := base64.NewDecoder(base64.StdEncoding, strings.NewReader(text))
	data, err := io.ReadAll(io.LimitReader(reader, int64(limit)+1))
	if err != nil {
		return nil, status.Error(codes.DataLoss, "wego: invalid base64 value")
	}
	if len(data) > limit {
		return nil, status.Error(codes.ResourceExhausted, "wego: base64 value exceeds decoded limit")
	}
	return data, nil
}

// DecodeValue 对普通 JSON 与 protobuf 执行同一 codec；目标类型只决定最后的反序列化。
func DecodeValue(ctx context.Context, method string, e Envelope, target any, config spec.Runtime, maxBusiness, maxEncoded int) error {
	// 本地参数错误先于对象下载，例如 nil 目标不得触发 S3 请求。
	value := reflect.ValueOf(target)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("wego: decode target must be a non-nil pointer")
	}
	out, err := Open(ctx, method, e, config, maxBusiness, maxEncoded)
	if err != nil {
		return err
	}
	if message, ok := target.(proto.Message); ok {
		if out.Type != "" && out.Type != string(message.ProtoReflect().Descriptor().FullName()) {
			return status.Error(codes.InvalidArgument, "wego: protobuf message type mismatch")
		}
		decoded := message.ProtoReflect().New().Interface()
		if err := protojson.Unmarshal(out.Payload, decoded); err != nil {
			return status.Errorf(codes.DataLoss, "wego: invalid ProtoJSON: %v", err)
		}
		proto.Reset(message)
		proto.Merge(message, decoded)
		return nil
	}
	if !json.Valid(out.Payload) {
		return status.Error(codes.DataLoss, "wego: payload must be one JSON value")
	}
	decoded := reflect.New(value.Elem().Type())
	decoder := json.NewDecoder(bytes.NewReader(out.Payload))
	decoder.UseNumber()
	if err := decoder.Decode(decoded.Interface()); err != nil {
		return status.Errorf(codes.DataLoss, "wego: invalid JSON payload: %v", err)
	}
	value.Elem().Set(decoded.Elem())
	return nil
}

// ValueType 只提取序列化类型，普通 protobuf 任务仍通过目标注册信息识别。
func ValueType(value any) string {
	value, _, _ = InputValue(value)
	if message, ok := value.(proto.Message); ok && message != nil && message.ProtoReflect().IsValid() {
		return string(message.ProtoReflect().Descriptor().FullName())
	}
	return ""
}

// ValidateRPCBinding 仅验证本地提交绑定；传输消息的目标由实际注册 action 决定。
func ValidateRPCBinding(packet Envelope, method string) error {
	if packet.Method != "" && packet.Method != method {
		return status.Error(codes.FailedPrecondition, "wego: local RPC binding mismatch")
	}
	return nil
}

// PayloadCodecIDs 返回消息实际发送链，不读取接收注册顺序。
func PayloadCodecIDs(packet Envelope) ([]string, error) { return slices.Clone(packet.CodecIDs), nil }
