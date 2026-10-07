package wire

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// project 只允许明确指定的标量字段或嵌套标量路径，避免将整个业务对象暴露给调度器。
func project(message protoreflect.Message, path string) (any, error) {
	// parts 解析结构化名称或参数，后续分派只接受明确注册的操作。
	parts := strings.Split(path, ".")
	// 逐项处理 parts，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for i, part := range parts {
		// field 读取 protobuf 字段描述，用于显式调度投影及类型校验。
		field := message.Descriptor().Fields().ByName(protoreflect.Name(part))
		if field == nil {
			return nil, fmt.Errorf("wego: projection field %q not found", path)
		}
		if field.IsList() || field.IsMap() {
			return nil, fmt.Errorf("wego: projection %q must be scalar", path)
		}

		// value 取得明确类型的字段或上下文能力，存在标记为 false 时不得使用。
		value := message.Get(field)
		// 路径还有下一段时要求当前字段为消息，例如 profile.region 先读取 profile 再读取 region。
		if i < len(parts)-1 {
			if field.Message() == nil {
				return nil, fmt.Errorf("wego: projection %q traverses scalar", path)
			}

			message = value.Message()
			continue
		}
		// 按协议、输入类型或状态分派处理；每个分支只应用与该类型匹配的转换和状态更新。
		switch field.Kind() {
		case protoreflect.StringKind:
			return value.String(), nil
		case protoreflect.BoolKind:
			return value.Bool(), nil
		case protoreflect.EnumKind:
			return int32(value.Enum()), nil
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind, protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			return value.Int(), nil
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			return value.Uint(), nil
		case protoreflect.FloatKind, protoreflect.DoubleKind:
			return value.Float(), nil
		default:
			return nil, fmt.Errorf("wego: projection %q must be scalar", path)
		}
	}
	return nil, fmt.Errorf("wego: empty projection path")
}
