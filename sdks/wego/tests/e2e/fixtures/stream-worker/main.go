package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// restartService 仅注册 server stream，运行中的进程由父测试显式终止
type restartService struct {
	// UnimplementedGreeterServer 满足标准生成接口的兼容性约束
	pb.UnimplementedGreeterServer
}

// WatchHellos 首次确认两条输出后停留在 handler；重投从有限历史恢复，再继续连续输出
func (*restartService) WatchHellos(in *pb.Request, out pb.Greeter_WatchHellosServer) error {
	ctx := out.Context()
	history, err := task.Checkpoint(ctx)
	if err != nil {
		return err
	}
	defer history.Close()
	// next 由实际持久历史恢复，不依赖进程内计数或固定请求 hash
	var next int32
	for {
		// reply 是本条持久历史的独立解码目标，Count 必须连续
		var reply pb.Reply
		err := history.Next(&reply)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if reply.Count != next {
			return fmt.Errorf("fixture: history gap %d/%d", reply.Count, next)
		}
		next++
	}
	info, ok := task.Info(ctx)
	if !ok {
		return fmt.Errorf("fixture: task identity missing")
	}
	if info.RetryCount == 0 {
		for ; next < 2; next++ {
			reply := scenarios.Reply(ctx, in)
			reply.Count = next
			if err := out.Send(reply); err != nil {
				return err
			}
		}
		if err := notify(ctx, "/started", info); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	if next != 2 {
		return fmt.Errorf("fixture: recovered history count=%d", next)
	}
	if err := notify(ctx, "/resumed", info); err != nil {
		return err
	}
	for ; next < in.Count; next++ {
		reply := scenarios.Reply(ctx, in)
		reply.Count = next
		if err := out.Send(reply); err != nil {
			return err
		}
	}
	return nil
}

// notify 以有预算的 HTTP 事实屏障向父测试交付执行身份，不记录凭证
func notify(ctx context.Context, path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	budget, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(budget, http.MethodPost, os.Getenv("WEGO_STREAM_RESTART_FIXTURE")+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("fixture: notification status=%d", response.StatusCode)
	}
	return nil
}

// run 使用正式 SDK 启动一个实例；就绪后才通知父进程允许提交任务
func run() error {
	namespace := os.Getenv("WEGO_STREAM_RESTART_NAMESPACE")
	opts := scenarios.Runtime(namespace)
	srv := server.New(server.WithRuntime(opts...), server.WithWorker(worker.WithTask(pb.Greeter_WatchHellos_FullMethodName, task.WithRetries(1), task.WithExecutionTimeout(8*time.Second))))
	pb.RegisterGreeterServer(srv, &restartService{})
	done := make(chan error, 1)
	go func() { done <- srv.Serve() }()
	defer srv.Stop()
	conn, err := client.New(client.WithRuntime(opts...))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := support.WaitWorkers(ctx, conn, namespace, 1, done); err != nil {
		return err
	}
	if err := notify(ctx, "/ready", model.TaskInfo{}); err != nil {
		return err
	}
	return <-done
}

// main 把启动或执行故障转换为独立进程的非零退出状态
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
