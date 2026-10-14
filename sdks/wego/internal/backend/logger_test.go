package backend

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

// TestWorkerLogsReadableRPCNames 覆盖官方执行器的失败、完成、动作列表及结构化 action 日志。
func TestWorkerLogsReadableRPCNames(t *testing.T) {
	// method 是期望展示的完整业务地址。
	const method = "/demo.Greeter/SayHello"
	action := binding.Action("test", method)
	for _, message := range []string{
		"could not execute action: " + action,
		"action " + action + " completed",
		"worker is listening for actions: [" + action + " native:invoke]",
	} {
		t.Run(message[:strings.IndexByte(message, ' ')], func(t *testing.T) {
			// output 保存本次独立日志记录。
			var output bytes.Buffer
			writer := workerLogWriter(slog.New(slog.NewJSONHandler(&output, nil)), "test", []ports.Definition{{RPCMethod: method}})
			logger := zerolog.New(writer)
			// 错误字符串即便包含相同 ID 也不属于路由字段，必须完整保留。
			failure := "business error " + action
			logger.Error().Err(errors.New(failure)).Str("actionId", action).Str("run_id", "run-1").Msg(message)
			// record 验证最终呈现的消息与结构化属性。
			var record map[string]any
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if record["msg"] != strings.ReplaceAll(message, action, method) || record["rpc_method"] != method {
				t.Fatalf("unreadable RPC log: %v", record)
			}
			if record["error"] != failure || record["run_id"] != "run-1" || record["actionId"] != nil {
				t.Fatalf("unexpected field rewrite: %v", record)
			}
		})
	}
}

// TestWorkerLogMappingIsolation 验证不同 namespace 的映射互不影响，普通动作日志保持原样。
func TestWorkerLogMappingIsolation(t *testing.T) {
	// method 是期望展示的完整业务地址。
	const method = "/demo.Greeter/SayHello"
	// output 仅在全部写入完成后读取。
	var output bytes.Buffer
	writer := workerLogWriter(slog.New(slog.NewJSONHandler(&output, nil)), "first", []ports.Definition{{RPCMethod: method}, {RPCMethod: method}})
	logger := zerolog.New(writer)
	other := binding.Action("second", method)
	logger.Info().Str("action_id", other).Msg("action native:invoke completed")
	// record 验证最终呈现的消息与结构化属性。
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["msg"] != "action native:invoke completed" || record["action_id"] != other {
		t.Fatalf("unregistered action changed: %v", record)
	}
}

// TestWorkerLogConcurrentWrites 验证注册映射冻结后并发输出不会修改映射或消息。
func TestWorkerLogConcurrentWrites(t *testing.T) {
	// method 是期望展示的完整业务地址。
	const method = "/demo.Greeter/SayHello"
	// output 仅在全部写入完成后读取。
	var output bytes.Buffer
	writer := workerLogWriter(slog.New(slog.NewJSONHandler(&output, nil)), "", []ports.Definition{{RPCMethod: method}})
	logger := zerolog.New(writer)
	action := binding.Action("", method)
	// pending 确保读缓冲区前所有日志写入均已结束。
	var pending sync.WaitGroup
	for range 16 {
		pending.Add(1)
		go func() {
			defer pending.Done()
			logger.Error().Msg("could not execute action: " + action)
		}()
	}
	pending.Wait()
	if strings.Count(output.String(), method) != 16 || strings.Contains(output.String(), action) {
		t.Fatalf("unexpected concurrent output: %s", output.String())
	}
}
