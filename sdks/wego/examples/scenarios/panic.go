package scenarios

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// Panic 验证 panic 回调与 Worker 后续继续处理请求
func Panic(ctx context.Context, report *Report) (err error) {
	// recovered 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	recovered := make(chan string, 1)
	// service 在 Fail=true 时抛出受控 panic，验证状态转换和回调；正常请求仍返回业务响应
	service := &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			if in.Fail {
				panic("intentional panic")
			}
			return Reply(ctx, in), nil
		},
	}
	// h, err 接收 worker.WithPanicHandler 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	h, err := Start(ctx, "panic", report, service, nil, worker.WithPanicHandler(func(ctx context.Context, value any) {
		recovered <- fmt.Sprint(value)
	}))
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// 当前步骤失败时终止处理：panic returned success；不把无效结果交给下一步
	if _, err = h.RPC.SayHello(ctx, &pb.Request{Fail: true}); err == nil {
		return fmt.Errorf("panic returned success")
	}

	// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
	select {
	// value 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功
	case value := <-recovered:
		if value != "intentional panic" {
			return fmt.Errorf("panic payload: %q", value)
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	// out, err 接收 h.RPC.SayHello 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	out, err := h.RPC.SayHello(ctx, &pb.Request{Message: "healthy"})
	if err != nil {
		return err
	}

	report.Add(h, "panic handler and subsequent successful call", out)
	return nil
}
