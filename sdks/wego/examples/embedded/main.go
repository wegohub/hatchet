//go:build wego_embedded

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// main 组织此示例或测试进程的初始化、执行和清理；错误以日志或非零退出码报告，不把启动成功代替业务成功。
func main() {
	// err 保存安全检查环境配置结果；设置失败时不能启动本测试引擎。
	if err := os.Setenv("SERVER_SECURITY_CHECK_ENABLED", "false"); err != nil {
		log.Fatal(err)
	}
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// conn, err 接收 client.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	conn, err := client.New(client.WithRuntime(
		runtime.WithEmbedded(runtime.EmbeddedConfig{DatabaseURL: os.Getenv("WEGO_EMBEDDED_DATABASE_URL"), LogLevel: "warn"}),
		runtime.WithNamespace(fmt.Sprintf("wego_embedded_example_%d_", time.Now().UnixNano())),
	))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// greet 构造原生独立任务定义，业务输入保持 JSON 语义。
	greet := conn.NewStandaloneTask("greet", func(ctx context.Context, in map[string]string) (map[string]string, error) {
		return map[string]string{"greeting": "Hello, " + in["name"] + "!"}, nil
	})
	// worker, err 接收 conn.NewWorker 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	worker, err := conn.NewWorker("embedded-worker", client.WithWorkflows(greet))
	if err != nil {
		log.Fatal(err)
	}
	// cleanup, err 接收 worker.Start 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	cleanup, err := worker.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	if err = worker.WaitReady(ctx); err != nil {
		log.Fatal(err)
	}
	// result, err 接收 greet.Run 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	result, err := greet.Run(ctx, map[string]string{"name": "embed"})
	if err != nil {
		log.Fatal(err)
	}
	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
	var out map[string]string
	if err = result.Into(&out); err != nil {
		log.Fatal(err)
	}
	fmt.Println(out["greeting"])
	if _, err = conn.Workflows().Delete(ctx, greet.GetName()); err != nil {
		log.Fatal(err)
	}
}
