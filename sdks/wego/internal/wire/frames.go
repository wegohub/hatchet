package wire

import (
	"google.golang.org/grpc/metadata"
)

// Metadata 把多值 metadata 转为协议 map，不丢失同名键的多个值
func Metadata(md metadata.MD) map[string]*Values {
	// out 当前结果的按键映射，逐项写入转换后的输出；不会把后端对象直接放入公开返回值
	out := map[string]*Values{}
	// 逐项处理 md，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for key, value := range md {
		// values 保存每项独立的原始字节，例如 ff00fe 不转换为文本
		values := &Values{}
		// item 是同名 metadata 的一项
		for _, item := range value {
			values.Values = append(values.Values, []byte(item))
		}
		out[key] = values
	}
	return out
}

// FromMetadata 从协议 map 恢复标准 gRPC 多值 metadata
func FromMetadata(value map[string]*Values) metadata.MD {
	// out 构造当前步骤的数据对象，字段在提交、编码或断言之前一次性初始化
	out := metadata.MD{}
	// 逐项处理 value，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for key, values := range value {
		// nil metadata 值直接略过，避免解引用空 Values；已有非 nil 列表仍按原顺序复制
		if values == nil {
			continue
		}
		// item 的 string 转换保持字节不变
		for _, item := range values.Values {
			out[key] = append(out[key], string(item))
		}
	}
	return out
}
