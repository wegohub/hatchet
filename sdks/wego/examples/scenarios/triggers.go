package scenarios

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// Cron 注册并验证分钟级 Cron 的真实触发 每次使用唯一 namespace，成功断言写入报告后清理可删除资源
func Cron(ctx context.Context, report *Report) (err error) {
	// called 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	called := make(chan *pb.Reply, 64)
	// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份
	service := &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// out 带当前任务身份的 protobuf 响应，业务字段与运行身份一起返回供断言
			out := Reply(ctx, in)
			out.ProcessedAt = time.Now().UTC().Format(time.RFC3339Nano)
			called <- out
			return out, nil
		},
	}
	// expressions 官方 Cron 示例的五种表达式，先验证注册，再使用分钟级表达式检查真实触发
	expressions := []string{
		"0 2 * * *",
		"0 * * * *",
		"0 9 * * 1",
		"0 9-17 * * 1-5",
		"0 12 * * 6",
	}
	// h, err 接收 worker.WithTask 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	h, err := Start(
		ctx,
		"cron",
		report,
		service,
		nil,
		worker.WithTask(pb.UnaryGreeter_SayHello_FullMethodName, task.WithCron(expressions...), task.WithCronInput(&pb.Request{Message: "definition"})),
	)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// ids Worker 名称到注册身份的映射
	ids := []string{}
	defer func() {
		// 逐项处理 ids，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for _, id := range ids {
			err = errors.Join(err, h.Conn.Crons().Delete(context.Background(), id))
		}
	}()

	// 逐项处理 expressions，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for i, expression := range expressions {
		// created, e 创建或查询实际周期调度资源，表达式注册与真实触发分别确认
		created, e := h.Conn.Crons().Create(
			ctx,
			pb.UnaryGreeter_SayHello_FullMethodName,
			model.CreateCronTrigger{
				Name:               fmt.Sprintf("%sdefinition_%d", h.Namespace, i),
				Expression:         expression,
				Input:              routedInput(&pb.Request{Message: fmt.Sprint(i)}),
				AdditionalMetadata: map[string]any{"acceptance": h.Namespace},
			},
		)
		if e != nil {
			return e
		}

		ids = append(ids, created.ID())
	}
	if _, err = h.Conn.Crons().List(ctx, model.Query{"additionalMetadata": []string{"acceptance:" + h.Namespace}}); err != nil {
		return err
	}
	if _, err = h.Conn.Crons().Create(
		ctx,
		pb.UnaryGreeter_SayHello_FullMethodName,
		model.CreateCronTrigger{
			Name:       h.Namespace + "minute",
			Expression: "* * * * *",
			Input:      routedInput(&pb.Request{Message: "minute"}),
		},
	); err != nil {
		return err
	}
	// 确认实际触发后，删除工作流会一并清理该 Cron
	for {
		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		// out 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功
		case out := <-called:
			if out.Message == "minute" {
				report.Add(h, "all five expressions registered; real minute trigger with protobuf input", out)
				return scheduleOnce(ctx, h, called)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// scheduleOnce 执行 h.Conn.Schedules/time.Now/created.ID 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误
func scheduleOnce(ctx context.Context, h *Harness, called <-chan *pb.Reply) (err error) {
	// created, err 接收 h.Conn.Schedules 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	created, err := h.Conn.Schedules().Create(
		ctx,
		pb.UnaryGreeter_SayHello_FullMethodName,
		model.CreateScheduledRunTrigger{
			TriggerAt: time.Now().Add(3 * time.Second),
			Input: model.RPCInput{
				Method:  pb.UnaryGreeter_SayHello_FullMethodName,
				Message: &pb.Request{Message: "scheduled"},
			},
			AdditionalMetadata: map[string]any{"acceptance": h.Namespace},
		},
	)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, cleanupSchedule(h, created.ID()))
	}()

	if _, err = h.Conn.Schedules().List(ctx, model.Query{}); err != nil {
		return err
	}

	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		// out 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功
		case out := <-called:
			if out.Message == "scheduled" {
				h.Report.Add(h, "real scheduled protobuf invocation", out)
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// cleanupSchedule 删除当前验收创建的一次性调度记录
func cleanupSchedule(h *Harness, id string) error {
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// err 接收 h.Conn.Schedules 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	err := h.Conn.Schedules().Delete(ctx, id)
	if err != nil && strings.Contains(err.Error(), "status 404") {
		h.Report.Add(h, "one-shot schedule removed after trigger", nil)
		return nil
	}

	return err
}
