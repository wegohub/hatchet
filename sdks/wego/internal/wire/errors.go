package wire

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// ErrorMarker 标识可在控制台阅读的 wego 错误对象。
const ErrorMarker = `"_wego_error"`

// legacyErrorPrefix 标识持久化记录中的 v2 二进制错误。
const legacyErrorPrefix = "wego_error_v2:"

// errorEnvelope 把可读状态和客户端恢复所需的附加信息放在同一个 JSON 对象中。
type errorEnvelope struct {
	// Version 是错误对象的解释版本，与任务输入协议版本分开。
	Version int `json:"_wego_error"`
	// Code 是 gRPC 数值错误码，例如 InvalidArgument 为 3。
	Code int32 `json:"code"`
	// CodeName 供控制台直接辨认，例如 InvalidArgument。
	CodeName string `json:"code_name"`
	// Message 是业务可读的错误正文，例如 missing id。
	Message string `json:"message"`
	// Details 按原顺序保存业务 gRPC details，不包含内部控制标记。
	Details []errorDetail `json:"details,omitempty"`
	// NonRetryable 保存 wego 错误包装标记；服务端实际重试策略另由上报字段控制。
	NonRetryable bool `json:"non_retryable,omitempty"`
	// RPC 表示失败由 gRPC handler 返回，即使 headers 和 trailers 都为空也保留该身份。
	RPC bool `json:"rpc,omitempty"`
	// Headers 保存失败调用仍已发送的多值响应头，任意字节均按 base64 无损编码。
	Headers binaryMetadata `json:"headers,omitempty"`
	// Trailers 保存失败调用的多值响应尾部。
	Trailers binaryMetadata `json:"trailers,omitempty"`
	// Stream 是可靠流最终 attempt 的输出清单，普通错误不携带。
	Stream *model.StreamCompletion `json:"stream,omitempty"`
}

// errorDetail 同时保存可读视图与原始字节，客户端不依赖 Worker 的 protobuf 类型注册。
type errorDetail struct {
	// TypeURL 用来核对 JSON detail 或恢复未知 Any，例如 google.rpc.BadRequest。
	TypeURL string `json:"type_url"`
	// JSON 保存可读字段；客户端还原始终依据原始字节，不依赖本地类型注册。
	JSON json.RawMessage `json:"json,omitempty"`
	// ValueBase64 保存 Any 的原始字节；指针区分空字节与缺少该字段。
	ValueBase64 *string `json:"value_base64,omitempty"`
}

// EncodeError 生成可读 JSON 错误；业务载荷 codec 不参与错误上报。
func EncodeError(err error) string {
	s := status.Convert(err)
	if errors.Is(err, context.Canceled) {
		s = status.New(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		s = status.New(codes.DeadlineExceeded, err.Error())
	}
	packet := errorEnvelope{Version: 3, Code: int32(s.Code()), CodeName: s.Code().String(), Message: s.Message()}
	// nonretry 只表示业务错误包装；引擎是否重试仍由独立的 ShouldNotRetry 字段决定。
	var nonretry *model.NonRetryableError
	packet.NonRetryable = errors.As(err, &nonretry)
	// transport 保存失败时已经确定的 headers、trailers 和流结束身份。
	var transport *model.RPCError
	if errors.As(err, &transport) {
		packet.RPC = true
		packet.Headers = metadataBytes(transport.Headers)
		packet.Trailers = metadataBytes(transport.Trailers)
		packet.Stream = transport.Stream
	}
	for _, detail := range s.Proto().Details {
		item := errorDetail{TypeURL: detail.TypeUrl}
		encoded := base64.StdEncoding.EncodeToString(detail.Value)
		item.ValueBase64 = &encoded
		if readable, marshalErr := protojson.Marshal(detail); marshalErr == nil {
			item.JSON = readable
		}
		packet.Details = append(packet.Details, item)
	}
	encoded, marshalErr := json.Marshal(packet)
	if marshalErr != nil {
		return encodeLegacyError(err)
	}
	return string(encoded)
}

// ErrorReportPayload 让 Hatchet 将已验证的错误保存为 JSON 对象，而不是带引号的字符串。
func ErrorReportPayload(value any) any {
	text, ok := value.(string)
	if !ok || !strings.HasPrefix(text, "{") || !json.Valid([]byte(text)) || DecodeError(text) == nil {
		return value
	}
	return json.RawMessage(text)
}

// encodedLegacyError 从旧错误的包装文本中定位二进制状态。
var encodedLegacyError = regexp.MustCompile(`wego_error_v2:([A-Za-z0-9_-]+)`)

// encodedJSONError 定位对象起点，允许 Hatchet 在错误前添加任务失败说明。
var encodedJSONError = regexp.MustCompile(`\{\s*"_wego_error"\s*:`)

// DecodeError 还原 gRPC code、details 和响应 metadata；任务包装文本可位于 JSON 前面。
func DecodeError(text string) error {
	if start := encodedJSONError.FindStringIndex(text); start != nil {
		// packet 从第一个协议对象还原；合法 JSON 后的 Hatchet 包装文本不会混进字段。
		var packet errorEnvelope
		if err := json.NewDecoder(strings.NewReader(text[start[0]:])).Decode(&packet); err != nil || packet.Version != 3 {
			return status.Error(codes.DataLoss, "wego: malformed error report")
		}
		return decodeJSONError(packet)
	}
	return decodeLegacyError(text)
}

// decodeJSONError 校验明文和附加信息的结构，不能把畸形 detail 当成成功结果。
func decodeJSONError(packet errorEnvelope) error {
	if packet.Code <= int32(codes.OK) || packet.Code > int32(codes.Unauthenticated) || packet.CodeName != codes.Code(packet.Code).String() {
		return status.Error(codes.DataLoss, "wego: invalid error code")
	}
	s := status.New(codes.Code(packet.Code), packet.Message).Proto()
	for _, item := range packet.Details {
		if item.TypeURL == "" || item.ValueBase64 == nil {
			return status.Error(codes.DataLoss, "wego: malformed error detail")
		}
		value, err := base64.StdEncoding.DecodeString(*item.ValueBase64)
		if err != nil {
			return status.Error(codes.DataLoss, "wego: malformed error detail value")
		}
		if len(item.JSON) != 0 {
			// display 只核对可读视图的类型；原始 Any 字节始终是客户端状态的依据。
			var display struct {
				// TypeURL 核对可读视图所属的 protobuf 类型，避免跨类型展示。
				TypeURL string `json:"@type"`
			}
			if json.Unmarshal(item.JSON, &display) != nil || display.TypeURL != item.TypeURL {
				return status.Error(codes.DataLoss, "wego: malformed error detail")
			}
		}
		s.Details = append(s.Details, &anypb.Any{TypeUrl: item.TypeURL, Value: value})
	}
	// result 先保留业务状态，再按协议包装不可重试和失败响应 metadata。
	var result error = status.FromProto(s).Err()
	if packet.NonRetryable {
		result = &model.NonRetryableError{Err: result}
	}
	if packet.RPC || len(packet.Headers) > 0 || len(packet.Trailers) > 0 || packet.Stream != nil {
		return &model.RPCError{Err: result, Headers: packet.Headers.strings(), Trailers: packet.Trailers.strings(), Stream: packet.Stream}
	}
	return result
}

// encodeLegacyError 仅在 JSON 封装失败时保留现有错误信息的无损兜底。
func encodeLegacyError(err error) string {
	s := status.Convert(err)
	// nonretry 用旧协议控制标记保存不可重试身份。
	var nonretry *model.NonRetryableError
	if errors.As(err, &nonretry) {
		if marked, e := s.WithDetails(&errdetails.ErrorInfo{Reason: "WEGO_NON_RETRYABLE", Domain: "wego"}); e == nil {
			s = marked
		}
	}
	// transport 用旧协议的内部 detail 保存失败响应 metadata。
	var transport *model.RPCError
	if errors.As(err, &transport) {
		headers, _ := json.Marshal(metadataBytes(transport.Headers))
		trailers, _ := json.Marshal(metadataBytes(transport.Trailers))
		completion, _ := json.Marshal(transport.Stream)
		if marked, e := s.WithDetails(&errdetails.ErrorInfo{Reason: "WEGO_RPC_METADATA", Domain: "wego", Metadata: map[string]string{"headers": string(headers), "trailers": string(trailers), "stream": string(completion)}}); e == nil {
			s = marked
		}
	}
	data, _ := proto.Marshal(s.Proto())
	return legacyErrorPrefix + base64.RawURLEncoding.EncodeToString(data)
}

// decodeLegacyError 从持久化 v2 错误中还原 gRPC 状态。
func decodeLegacyError(text string) error {
	matches := encodedLegacyError.FindStringSubmatch(text)
	if len(matches) != 2 {
		return nil
	}
	data, err := base64.RawURLEncoding.DecodeString(matches[1])
	if err != nil {
		return nil
	}
	s := status.New(codes.Unknown, "").Proto()
	if proto.Unmarshal(data, s) != nil {
		return nil
	}
	// transport 接收旧协议中的失败响应 metadata，不把该标记交付业务。
	var transport *model.RPCError
	nonretry := false
	retained := s.Details[:0]
	for _, detail := range s.Details {
		info := &errdetails.ErrorInfo{}
		if detail.UnmarshalTo(info) == nil && info.Domain == "wego" {
			if info.Reason == "WEGO_NON_RETRYABLE" {
				nonretry = true
			}
			if info.Reason == "WEGO_RPC_METADATA" {
				transport = &model.RPCError{}
				// headers、trailers 逐项还原旧协议中的任意字节 metadata。
				var headers, trailers binaryMetadata
				if json.Unmarshal([]byte(info.Metadata["headers"]), &headers) != nil || json.Unmarshal([]byte(info.Metadata["trailers"]), &trailers) != nil {
					return status.Error(codes.DataLoss, "wego: malformed RPC metadata")
				}
				transport.Headers, transport.Trailers = headers.strings(), trailers.strings()
				if encoded := info.Metadata["stream"]; encoded != "" && json.Unmarshal([]byte(encoded), &transport.Stream) != nil {
					return status.Error(codes.DataLoss, "wego: malformed stream completion metadata")
				}
				continue
			}
		}
		retained = append(retained, detail)
	}
	s.Details = retained
	// result 保留已移除内部 metadata 标记的业务 gRPC status。
	var result error = status.FromProto(s).Err()
	if nonretry {
		result = &model.NonRetryableError{Err: result}
	}
	if transport != nil {
		transport.Err = result
		return transport
	}
	return result
}
