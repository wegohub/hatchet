package wire

import (
	"encoding/base64"
	"fmt"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// EncodeFrame 用 protobuf 编码流帧，再用 base64 适配引擎的 string 订阅出口。
func EncodeFrame(frame *Frame) (string, error) {
	// data, err 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
	data, err := proto.Marshal(frame)
	return base64.StdEncoding.EncodeToString(data), err
}

// DecodeFrame 在解码前限制编码长度，并校验协议版本、会话身份和消息大小。
func DecodeFrame(value string, max int) (*Frame, error) {
	if len(value) > base64.StdEncoding.EncodedLen(max+65536) {
		return nil, fmt.Errorf("wego: oversized frame")
	}

	// data, err 接收 base64.StdEncoding.DecodeString 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	data, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, err
	}

	// frame 流帧的 protobuf 解码目标，解码后再检查版本、会话身份和消息大小。
	frame := &Frame{}
	if err = proto.Unmarshal(data, frame); err != nil {
		return nil, err
	}
	if frame.Version != Version || frame.StreamId == "" {
		return nil, fmt.Errorf("wego: invalid frame identity or version")
	}

	// 按协议、输入类型或状态分派处理；每个分支只应用与该类型匹配的转换和状态更新。
	switch frame.Kind {
	// 只接受协议定义的帧类型；START 由独立 Control 输入表达，不属于 Frame。
	case "PING", "READY", "OPEN", "DATA", "ACK", "END", "CANCEL", "HEADER":
	default:
		return nil, fmt.Errorf("wego: unknown frame kind %q", frame.Kind)
	}
	if len(frame.Payload) > max {
		return nil, fmt.Errorf("wego: oversized frame payload")
	}

	return frame, nil
}

// Metadata 把多值 metadata 转为协议 map，不丢失同名键的多个值。
func Metadata(md metadata.MD) map[string]*Values {
	// out 当前结果的按键映射，逐项写入转换后的输出；不会把后端对象直接放入公开返回值。
	out := map[string]*Values{}
	// 逐项处理 md，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for key, value := range md {
		// values 保存每项独立的原始字节，例如 ff00fe 不转换为文本。
		values := &Values{}
		// item 是同名 metadata 的一项。
		for _, item := range value {
			values.Values = append(values.Values, []byte(item))
		}
		out[key] = values
	}
	return out
}

// FromMetadata 从协议 map 恢复标准 gRPC 多值 metadata。
func FromMetadata(value map[string]*Values) metadata.MD {
	// out 构造当前步骤的数据对象，字段在提交、编码或断言之前一次性初始化。
	out := metadata.MD{}
	// 逐项处理 value，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for key, values := range value {
		// nil metadata 值直接略过，避免解引用空 Values；已有非 nil 列表仍按原顺序复制。
		if values == nil {
			continue
		}
		// item 的 string 转换保持字节不变。
		for _, item := range values.Values {
			out[key] = append(out[key], string(item))
		}
	}
	return out
}
