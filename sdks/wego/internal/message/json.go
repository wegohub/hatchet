// Package message 提供无配置、无 I/O 的业务序列化，供公开 Input 和传输层共用。
package message

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Version 是显式 Input 与任务输出共同使用的协议版本。
const Version = 6

// Marshal 对 protobuf 使用 ProtoJSON；数组元素类型不能改变任务身份。
func Marshal(value any, limit int) (json.RawMessage, string, error) {
	if m, ok := value.(proto.Message); ok {
		return marshalProto(m, limit)
	}
	v := reflect.ValueOf(value)
	if v.IsValid() && (v.Kind() == reflect.Array || v.Kind() == reflect.Slice) {
		isProto := protobufArray(v)
		if isProto {
			out, kind := []byte{'['}, ""
			for i := 0; i < v.Len(); i++ {
				m, ok := v.Index(i).Interface().(proto.Message)
				if !ok {
					return nil, "", fmt.Errorf("wego: mixed protobuf array at %d", i)
				}
				data, name, err := marshalProto(m, limit)
				if err != nil {
					return nil, "", err
				}
				if kind != "" && kind != name {
					return nil, "", fmt.Errorf("wego: mixed protobuf array types")
				}
				kind = name
				if i != 0 {
					out = append(out, ',')
				}
				if len(out)+len(data)+1 > limit {
					return nil, "", fmt.Errorf("wego: message exceeds limit")
				}
				out = append(out, data...)
			}
			return append(out, ']'), kind, nil
		}
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, "", err
	}
	if len(data) > limit {
		return nil, "", fmt.Errorf("wego: message exceeds limit")
	}
	return data, "", nil
}

// marshalProto 先检查源消息，再检查 JSON 膨胀；例如 bytes 会扩大为 base64。
func marshalProto(m proto.Message, limit int) (json.RawMessage, string, error) {
	if m == nil || !m.ProtoReflect().IsValid() {
		return nil, "", fmt.Errorf("wego: nil protobuf message")
	}
	if proto.Size(m) > limit {
		return nil, "", fmt.Errorf("wego: message exceeds limit")
	}
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(m)
	if err != nil {
		return nil, "", err
	}
	if len(data) > limit {
		return nil, "", fmt.Errorf("wego: message exceeds limit")
	}
	return data, string(m.ProtoReflect().Descriptor().FullName()), nil
}

// Portable 检查 JSON 数字经过 float64 和 JSON 重编码后数值是否相同。
// 9007199254740993 或 1e-400 需字节承载；字符串中的数字不参与检查。
func Portable(data []byte) bool {
	quoted, escaped := false, false
	for i := 0; i < len(data); i++ {
		c := data[i]
		if quoted {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		if c == '"' {
			quoted = true
			continue
		}
		if c != '-' && (c < '0' || c > '9') {
			continue
		}
		start := i
		for i+1 < len(data) && strings.ContainsRune("0123456789.eE+-", rune(data[i+1])) {
			i++
		}
		text := string(data[start : i+1])
		if !portableNumber(text) {
			return false
		}

	}
	return true
}

// normalizeNumber 使用十进制精确移位；1、1.0、10e-1 统一为 1e0。
// 指数用整数而非展开小数，1e100000 不会分配十万位零。
func normalizeNumber(text string) string {
	sign := ""
	if strings.HasPrefix(text, "-") {
		sign, text = "-", text[1:]
	}
	mantissa, exponent := text, new(big.Int)
	if at := strings.IndexAny(text, "eE"); at >= 0 {
		mantissa = text[:at]
		exponent.SetString(text[at+1:], 10)
	}
	if at := strings.IndexByte(mantissa, '.'); at >= 0 {
		exponent.Sub(exponent, big.NewInt(int64(len(mantissa)-at-1)))
		mantissa = mantissa[:at] + mantissa[at+1:]
	}
	mantissa = strings.TrimLeft(mantissa, "0")
	if mantissa == "" {
		return "0"
	}
	trimmed := strings.TrimRight(mantissa, "0")
	exponent.Add(exponent, big.NewInt(int64(len(mantissa)-len(trimmed))))
	return sign + trimmed + "e" + exponent.String()
}

// Canonical 只用于恢复身份和静态定义摘要，不在普通 unary 的热路径上调用。
func Canonical(data []byte) ([]byte, error) {
	if !json.Valid(data) {
		return nil, fmt.Errorf("wego: expected one JSON value")
	}
	// value 仅属于规范化解析，UseNumber 避免 float64 改写输入。
	var value any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	normalizeValue(value)
	if number, ok := value.(json.Number); ok {
		value = json.Number(normalizeNumber(string(number)))
	}
	return json.Marshal(value)
}

// normalizeValue 原地处理本次解析创建的容器，不修改业务方的 map。
func normalizeValue(value any) {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			if n, ok := item.(json.Number); ok {
				v[key] = json.Number(normalizeNumber(string(n)))
			} else {
				normalizeValue(item)
			}
		}
	case []any:
		for i, item := range v {
			if n, ok := item.(json.Number); ok {
				v[i] = json.Number(normalizeNumber(string(n)))
			} else {
				normalizeValue(item)
			}
		}
	}
}

// protobufArray 只决定数组序列化方式；空的 protobuf 切片仍编码为 []。
func protobufArray(v reflect.Value) bool {
	isProto := v.Type().Elem().Implements(reflect.TypeFor[proto.Message]())
	if v.Type().Elem().Kind() == reflect.Interface {
		for i := 0; i < v.Len(); i++ {
			if _, ok := v.Index(i).Interface().(proto.Message); ok {
				isProto = true
				break
			}
		}
	}

	return isProto
}

// portableNumber 比较 float64 重编码后的十进制值，避免把 1e-400 当作零。
func portableNumber(text string) bool {
	value, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
		return false
	}
	if normalizeNumber(text) != normalizeNumber(strconv.FormatFloat(value, 'g', -1, 64)) {
		return false
	}
	return true
}
