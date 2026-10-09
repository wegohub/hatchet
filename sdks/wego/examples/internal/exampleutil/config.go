package exampleutil

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// Runtime 从示例环境读取实例配置，不打印令牌
func Runtime(namespace string) []runtime.Option {
	return []runtime.Option{
		runtime.WithToken(os.Getenv("HATCHET_CLIENT_TOKEN")),
		runtime.WithAddress("localhost:7077"),
		runtime.WithServerURL("http://localhost:8080"),
		runtime.WithTLSConfig(nil),
		runtime.WithNamespace(namespace),
		runtime.WithSlots(4),
		runtime.WithDurableSlots(2),
	}
}

// Context 返回当前执行上下文，包含取消与截止时间
func Context() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 45*time.Second)
}

// Print 输出可阅读的业务结果，编码错误直接报告
func Print(value any) {
	// data, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷
	data, err := json.Marshal(value)
	if err != nil {
		fmt.Fprintln(os.Stderr, "业务结果编码失败:", err)
		return
	}
	fmt.Println(string(data))
}

// Namespace 创建或读取当前示例的隔离任务前缀，避免与其他验收互相影响
func Namespace() string {
	return fmt.Sprintf("wego_example_%d_", time.Now().UnixNano())
}
