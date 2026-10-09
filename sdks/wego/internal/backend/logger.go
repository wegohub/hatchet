package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
)

// slogWriter 将后端结构化日志交给实例 slog 日志器，不修改进程全局日志配置
type slogWriter struct {
	// logger 实例日志记录器，不替换进程全局 logger
	logger *slog.Logger
}

// Write 把写入字节转交实例日志器，并遵守 io.Writer 的长度和错误约定
func (w *slogWriter) Write(data []byte) (int, error) {
	// record 结构化日志解码目标，把 message / level 与其他属性分别传给 slog
	var record map[string]any
	if json.Unmarshal(data, &record) != nil {
		return len(data), nil
	}

	// level 当前日志等级的默认值，解析记录或实例配置后再选择实际级别
	level := slog.LevelInfo
	// 按协议、输入类型或状态分派处理；每个分支只应用与该类型匹配的转换和状态更新
	switch record["level"] {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	// args 按当前调用约定组织的参数列表，用于事件发布、反射调用或结构化日志属性
	args := []any{}
	// 逐项处理 record，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for k, v := range record {
		if k != "message" && k != "level" && k != "time" {
			args = append(args, k, v)
		}
	}
	w.logger.Log(context.Background(), level, fmt.Sprint(record["message"]), args...)
	return len(data), nil
}

// 编译期接口或签名检查，确保适配对象可以被标准 gRPC 或 wego 入口使用
var _ io.Writer = &slogWriter{}
