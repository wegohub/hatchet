package wire

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"regexp"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// ErrorPrefix 任务错误字符串的协议前缀，用于识别可恢复的 gRPC 状态编码。
const ErrorPrefix = "wego_error_v2:"

// EncodeError 将 gRPC status、details 和不可重试标记编码进引擎错误文本。
func EncodeError(err error) string {
	// s 构造或读取标准 gRPC 状态，保留 code 与业务 details。
	s := status.Convert(err)
	if errors.Is(err, context.Canceled) {
		s = status.New(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		s = status.New(codes.DeadlineExceeded, err.Error())
	}
	// nonretry 禁止重试错误的识别目标；转换后只保留 wego 或标准错误链。
	var nonretry *model.NonRetryableError
	if errors.As(err, &nonretry) {
		// marked 保存附加内部标记后的 status；附加失败时保留原业务错误而非丢弃其 code。
		if marked, e := s.WithDetails(&errdetails.ErrorInfo{Reason: "WEGO_NON_RETRYABLE", Domain: "wego"}); e == nil {
			s = marked
		}
	}
	// transport 携带响应 metadata 的 wego RPCError 目标，headers / trailers 不混入业务 details。
	var transport *model.RPCError
	if errors.As(err, &transport) {
		// headers, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
		headers, _ := json.Marshal(metadataBytes(transport.Headers))
		// trailers, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
		trailers, _ := json.Marshal(metadataBytes(transport.Trailers))
		// marked 保存附加内部标记后的 status；附加失败时保留原业务错误而非丢弃其 code。
		if marked, e := s.WithDetails(&errdetails.ErrorInfo{
			Reason:   "WEGO_RPC_METADATA",
			Domain:   "wego",
			Metadata: map[string]string{"headers": string(headers), "trailers": string(trailers)},
		}); e == nil {
			s = marked
		}
	}
	// data, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
	data, _ := proto.Marshal(s.Proto())
	return ErrorPrefix + base64.RawURLEncoding.EncodeToString(data)
}

// encodedError 错误编码的私有数据形状，保存状态、details 和不可重试标记。
var encodedError = regexp.MustCompile(`wego_error_v2:([A-Za-z0-9_-]+)`)

// DecodeError 还原公开错误，并移除仅用于携带 headers/trailers 的内部详情标记。
func DecodeError(text string) error {
	// matches 定位 wego 自有错误 envelope，没有匹配则保留普通错误语义。
	matches := encodedError.FindStringSubmatch(text)
	if len(matches) != 2 {
		return nil
	}

	// data, err 接收 base64.RawURLEncoding.DecodeString 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	data, err := base64.RawURLEncoding.DecodeString(matches[1])
	if err != nil {
		return nil
	}

	// s 构造或读取标准 gRPC 状态，保留 code 与业务 details。
	s := status.New(codes.Unknown, "").Proto()
	if proto.Unmarshal(data, s) != nil {
		return nil
	}

	// transport 携带响应 metadata 的 wego RPCError 目标，headers / trailers 不混入业务 details。
	var transport *model.RPCError
	// nonretry 是否从错误协议中识别禁止重试标记，用于恢复 wego 错误包装。
	nonretry := false
	// retained 复用 details 切片容量保存业务详情，去除 wego 控制标记而保留业务详情顺序。
	retained := s.Details[:0]
	// 逐项处理 s.Details，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, detail := range s.Details {
		// info 用于解码 ErrorInfo 的消息目标，以 Domain 和 Reason 识别 wego 控制标记。
		info := &errdetails.ErrorInfo{}
		// 检查 detail.UnmarshalTo(info) == nil && info.Domain == "wego"；不满足协议或配置约束时返回 DataLoss（wego: malformed RPC metadata）。
		if detail.UnmarshalTo(info) == nil && info.Domain == "wego" {
			if info.Reason == "WEGO_NON_RETRYABLE" {
				nonretry = true
			}
			// 检查 info.Reason == "WEGO_RPC_METADATA"；不满足协议或配置约束时返回 DataLoss（wego: malformed RPC metadata）。
			if info.Reason == "WEGO_RPC_METADATA" {
				transport = &model.RPCError{}
				// headers、trailers 使用与 envelope 相同的无损表示。
				var headers, trailers binaryMetadata
				// 解析协议 metadata 失败时返回 DataLoss；畸形 headers / trailers 不能污染业务 details 或伪装成正常响应。
				if json.Unmarshal([]byte(info.Metadata["headers"]), &headers) != nil || json.Unmarshal([]byte(info.Metadata["trailers"]), &trailers) != nil {
					return status.Error(codes.DataLoss, "wego: malformed RPC metadata")
				}

				transport.Headers, transport.Trailers = headers.strings(), trailers.strings()
				continue
			}
		}
		retained = append(retained, detail)
	}
	s.Details = retained
	// result 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
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
