package scenarios

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// Mergent 通过 Cron 和一次性调度实现受控迁移场景。 每次使用唯一 namespace，成功断言写入报告后清理可删除资源。
func Mergent(ctx context.Context, report *Report) (err error) {
	// called 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	called := make(chan *pb.Reply, 4)
	// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份。
	service := &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// 检查 in.ImageUrl == ""；不满足协议或配置约束时返回 InvalidArgument（processing failed to generate URL）。
			if in.ImageUrl == "" {
				return nil, status.Error(codes.InvalidArgument, "processing failed to generate URL")
			}

			// out 带当前任务身份的 protobuf 响应，业务字段与运行身份一起返回供断言。
			out := Reply(ctx, in)
			out.ProcessedUrl = in.ImageUrl
			out.ImageMetadata = &pb.ImageMetadata{Size: 100, Format: "png", AppliedFilters: in.Filters}
			called <- out
			return out, nil
		},
	}
	// h, err 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值。
	h, err := Start(ctx, "mergent", report, service, nil)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// input 当前提交的 protobuf 请求，分组、数量与故障标记在此固定，便于核对执行结果。
	input := &pb.Request{ImageUrl: "https://example.invalid/fixture.png", Filters: []string{"blur"}}
	// out, err 接收 h.RPC.SayHello 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	out, err := h.RPC.SayHello(ctx, input)
	if err != nil {
		return err
	}
	if out.ProcessedUrl != input.ImageUrl || out.ImageMetadata.Size != 100 || out.ImageMetadata.Format != "png" || len(out.ImageMetadata.AppliedFilters) != 1 || out.ImageMetadata.AppliedFilters[0] != "blur" {
		return fmt.Errorf("nested protobuf image output mismatch: %v", out)
	}

	report.Add(h, "image processor nested protobuf result", out)
	<-called
	if _, err = h.RPC.SayHello(ctx, &pb.Request{}); status.Code(err) != codes.InvalidArgument {
		return fmt.Errorf("image error code: %v", err)
	}

	err = nil
	// created, err 接收 h.Conn.Schedules 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	created, err := h.Conn.Schedules().Create(
		ctx,
		pb.UnaryGreeter_SayHello_FullMethodName,
		model.CreateScheduledRunTrigger{TriggerAt: time.Now().Add(3 * time.Second), Input: input},
	)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, cleanupSchedule(h, created.ID()))
	}()

	// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
	select {
	// out 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功。
	case out := <-called:
		if err = waitStatus(ctx, h, out.RunId, model.Completed); err != nil {
			return err
		}
		report.Add(h, "scheduled image processor invocation", out)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
