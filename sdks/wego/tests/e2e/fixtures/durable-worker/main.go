package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/option"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/telemetry"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// main 组织此示例或测试进程的初始化、执行和清理；错误以日志或非零退出码报告，不把启动成功代替业务成功。
func main() {
	// err 接收 run 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run 执行此独立示例或 Worker 进程的连接、注册、业务调用及清理；context 控制总预算，错误传播到 main 退出码。
func run() error {
	// opts 复制或扩展当前列表，避免把内部可变容器直接交给调用方修改。
	opts := append(
		scenarios.Runtime(os.Getenv("WEGO_RESTART_NAMESPACE")),
		runtime.WithTelemetry(telemetry.Config{Trace: telemetry.TraceConfig{DisableWorkerExporter: true}}),
	)
	// address 仅在 memo 故障验收中连接测试代理，不改变进程外 SDK 或引擎配置。
	if address := os.Getenv("WEGO_RESTART_PROXY_ADDRESS"); address != "" {
		opts = append(opts, runtime.WithAddress(address))
	}
	// srv 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	srv := server.New(server.WithRuntime(opts...), server.WithWorker(worker.WithDurableTask(
		pb.UnaryGreeter_WaitHello_FullMethodName,
		task.WithExecutionTimeout(6*time.Minute),
		task.WithEviction(model.EvictionPolicy{TTL: option.Some(time.Duration(0)), AllowCapacityEviction: option.Some(false)}),
	)))

	// service 注入 durable 重启用例的业务 handler，等待和子结果来自原执行账本。
	service := &scenarios.Service{
		Wait: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// now, err 接收 task.Now 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
			now, err := task.Now(ctx)
			if err != nil {
				return nil, err
			}

			// conn, err 接收 task.Client 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
			conn, err := task.Client(ctx)
			if err != nil {
				return nil, err
			}

			// result, err 接收 conn.Run 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
			result, err := conn.Run(
				task.WithChildKey(ctx, "cached-child"),
				pb.UnaryGreeter_SayHello_FullMethodName,
				in,
				client.WithRunMetadata(map[string]string{"fixture": "restart"}),
			)
			if err != nil {
				return nil, err
			}

			// child 子调用的 protobuf 响应，检查 ChildRunID 和 ParentRunID 的关联。
			var child pb.Reply
			if err = result.Into(&child); err != nil {
				return nil, err
			}
			if err = task.Sleep(ctx, 300*time.Millisecond); err != nil {
				return nil, err
			}

			// out 附带实际任务身份和恢复结果，父子 RunID 必须在 Worker 重启后复用。
			out := scenarios.Reply(ctx, in)
			out.ChildRunId = child.RunId
			out.ProcessedAt = now.Format(time.RFC3339Nano)
			// data, err 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
			data, err := json.Marshal(out)
			if err != nil {
				return nil, err
			}

			// req, err 接收 http.NewRequestWithContext 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, os.Getenv("WEGO_RESTART_FIXTURE_URL")+"/snapshot", bytes.NewReader(data))
			if err != nil {
				return nil, err
			}

			// response, err 接收 http.DefaultClient.Do 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				return nil, err
			}

			response.Body.Close()
			if response.StatusCode != 200 {
				return nil, fmt.Errorf("snapshot fixture: %d", response.StatusCode)
			}

			_, err = task.WaitForEvent(ctx, "restart:finish", "input.run_id == "+strconv.Quote(out.RunId))
			return out, err
		},
	}
	pb.RegisterUnaryGreeterServer(srv, service)
	// done 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	done := make(chan error, 1)
	go func() {
		done <- srv.Serve()
	}()
	// conn, err 接收 client.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	conn, err := client.New(client.WithRuntime(opts...))
	if err != nil {
		return err
	}
	defer conn.Close()
	// readyCtx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
	readyCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// err 保存注册和消费就绪检查结果；未就绪时不得开始提交本场景业务。
	if err := support.WaitWorkers(readyCtx, conn, os.Getenv("WEGO_RESTART_NAMESPACE"), 1, done); err != nil {
		return err
	}

	// response, err 接收 http.Post 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	response, err := http.Post(os.Getenv("WEGO_RESTART_FIXTURE_URL")+"/ready", "text/plain", nil)
	if err != nil {
		return err
	}

	response.Body.Close()
	return <-done
}
