package logging

import (
	"encoding/json"
	"log/slog"
)

// Metadata 保留结构化字段；不把 JSON 拼入 message，也不吞掉无法编码的业务值。
func Metadata(record slog.Record) ([]byte, error) {
	attrs := make([]slog.Attr, 0, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool { attrs = append(attrs, attr); return true })
	return json.Marshal(object(attrs))
}

// object 将已合并的 slog 属性转换为 JSON 对象，时间、错误和分组遵循标准日志表达。
func object(attrs []slog.Attr) map[string]any {
	result := make(map[string]any, len(attrs))
	for _, attr := range attrs {
		value := attr.Value.Resolve()
		switch value.Kind() {
		case slog.KindGroup:
			result[attr.Key] = object(value.Group())
		case slog.KindDuration:
			result[attr.Key] = int64(value.Duration())
		default:
			if err, ok := value.Any().(error); ok {
				result[attr.Key] = err.Error()
			} else {
				result[attr.Key] = value.Any()
			}
		}
	}
	return result
}
