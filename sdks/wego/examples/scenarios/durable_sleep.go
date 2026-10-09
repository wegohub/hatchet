package scenarios

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// DurableSleep 验证引擎记录的等待和可重放时间
func DurableSleep(ctx context.Context, report *Report) (err error) {
	// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份
	service := &Service{
		Wait: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// now, err 接收 task.Now 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
			now, err := task.Now(ctx)
			if err != nil {
				return nil, err
			}
			if err = task.Sleep(ctx, 300*time.Millisecond); err != nil {
				return nil, err
			}

			// again, err 接收 task.Now 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
			again, err := task.Now(ctx)
			if err != nil || !now.Equal(again) {
				return nil, fmt.Errorf("memoized Now")
			}

			// out 带当前任务身份的 protobuf 响应，业务字段与运行身份一起返回供断言
			out := Reply(ctx, in)
			out.ProcessedAt = now.Format(time.RFC3339Nano)
			return out, nil
		},
	}
	// h, err 接收 worker.WithDurableTask 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	h, err := Start(ctx, "durable-sleep", report, service, nil, worker.WithDurableTask(pb.UnaryGreeter_WaitHello_FullMethodName))
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// start 读取本机时钟，用于耗时或超时判断，不参与 durable 重放
	start := time.Now()
	// out, err 接收 h.RPC.WaitHello 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	out, err := h.RPC.WaitHello(ctx, &pb.Request{Message: "durable"})
	if err != nil {
		return err
	}
	if time.Since(start) < 300*time.Millisecond || out.ProcessedAt == "" {
		return fmt.Errorf("durable wait did not execute")
	}

	report.Add(h, "real durable Sleep and memoized Now", out)
	return nil
}
