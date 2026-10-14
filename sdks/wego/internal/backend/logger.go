package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

// slogWriter 将后端结构化日志交给实例 slog 日志器，不修改进程全局日志配置
type slogWriter struct {
	// logger 实例日志记录器，不替换进程全局 logger
	logger *slog.Logger
	// actionNames 是当前 Worker 的只读注册映射，不影响真实调度或其他 Worker。
	actionNames map[string]string
	// actionText 在注册时构造，替换官方消息中的内部 ID；并发 Write 只读使用。
	actionText *strings.Replacer
}

// workerLogWriter 将官方执行器日志中的已注册 RPC action 显示为完整方法名。
// 例如失败和调试日志均显示 /demo.Greeter/SayHello，错误内容仍原样保留。
func workerLogWriter(logger *slog.Logger, namespace string, definitions []ports.Definition) *slogWriter {
	w := &slogWriter{logger: logger, actionNames: make(map[string]string)}
	pairs := make([]string, 0, len(definitions)*2)
	for _, definition := range definitions {
		if definition.RPCMethod == "" {
			continue
		}
		action := binding.Action(namespace, definition.RPCMethod)
		if _, exists := w.actionNames[action]; exists {
			continue
		}
		w.actionNames[action] = definition.RPCMethod
		pairs = append(pairs, action, definition.RPCMethod)
	}
	if len(pairs) > 0 {
		w.actionText = strings.NewReplacer(pairs...)
	}
	return w
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
			// 仅解释明确的 action 字段，不改写业务错误、payload 或运行身份。
			if k == "actionId" || k == "action_id" {
				if action, ok := v.(string); ok {
					if method, found := w.actionNames[action]; found {
						args = append(args, "rpc_method", method)
						continue
					}
				}
			}
			args = append(args, k, v)
		}
	}
	message := fmt.Sprint(record["message"])
	if w.actionText != nil {
		message = w.actionText.Replace(message)
	}
	w.logger.Log(context.Background(), level, message, args...)
	return len(data), nil
}

// 编译期接口或签名检查，确保适配对象可以被标准 gRPC 或 wego 入口使用
var _ io.Writer = &slogWriter{}
