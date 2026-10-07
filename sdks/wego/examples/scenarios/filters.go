package scenarios

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// Filters 验证动态事件过滤器及传入业务的 filter payload。 每次使用唯一 namespace，成功断言写入报告后清理可删除资源。
func Filters(ctx context.Context, report *Report) (err error) {
	// observed 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	observed := make(chan *pb.Reply, 4)
	// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份。
	service := &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// info, _ 读取实际执行身份，例如 RunID 与 WorkerID；普通网络 context 没有任务身份。
			info, _ := task.Info(ctx)
			if info.FilterPayload["marker"] != "fixture" {
				return nil, fmt.Errorf("filter payload missing: %v", info.FilterPayload)
			}

			// out 带当前任务身份的 protobuf 响应，业务字段与运行身份一起返回供断言。
			out := Reply(ctx, in)
			out.Message = strings.ToLower(in.Message)
			observed <- out
			return out, nil
		},
	}
	// h, err 接收 runtime.WithInputProjection 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	h, err := Start(
		ctx,
		"filters",
		report,
		service,
		[]runtime.Option{
			runtime.WithInputProjection(pb.UnaryGreeter_SayHello_FullMethodName, map[string]string{"skip": "fail", "group": "group_key"}),
		},
		worker.WithTask(
			pb.UnaryGreeter_SayHello_FullMethodName,
			task.WithEvents("filtered:create"),
			task.WithDefaultFilters(model.DefaultFilter{
				Expression: "!input.routing.skip",
				Scope:      "fixture-scope",
				Payload:    map[string]any{"marker": "fixture"},
			}),
		),
	)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// scope 当前步骤使用的字符串值 "fixture-scope"，用于路由、请求或断言。
	scope := "fixture-scope"
	// push 发送分别满足或不满足过滤条件的事件，以实际运行数量证明过滤生效。
	push := func(skip bool) error {
		return h.Conn.Events().Push(
			ctx,
			"filtered:create",
			model.RPCInput{
				Method:  pb.UnaryGreeter_SayHello_FullMethodName,
				Message: &pb.Request{Message: "Mixed CASE", Fail: skip},
			},
			&scope,
		)
	}
	if err = push(true); err != nil {
		return err
	}

	// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
	select {
	// out 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功。
	case out := <-observed:
		return fmt.Errorf("filtered event executed: %v", out)
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	if err = push(false); err != nil {
		return err
	}

	// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
	select {
	// out 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功。
	case out := <-observed:
		if out.Message != "mixed case" {
			return fmt.Errorf("event transformation")
		}
		report.Add(h, "filter rejects and accepts events with payload", out)
	case <-ctx.Done():
		return ctx.Err()
	}
	// workflow, err 接收 h.Conn.Workflows 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	workflow, err := h.Conn.Workflows().Get(ctx, TaskName(pb.UnaryGreeter_SayHello_FullMethodName))
	if err != nil {
		return err
	}

	// filter, err 接收 h.Conn.Filters 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	filter, err := h.Conn.Filters().Create(ctx, model.Resource{
		"workflowId": workflow.ID(),
		"expression": "true",
		"scope":      "management-scope",
		"payload":    map[string]any{"marker": "fixture"},
	})
	if err != nil {
		return err
	}

	// id 读取实际资源 ID，后续删除或查询必须使用同一身份。
	id := filter.ID()
	if id == "" {
		return fmt.Errorf("created filter ID missing: %v", filter)
	}
	if _, err = h.Conn.Filters().Get(ctx, id); err != nil {
		return err
	}
	if _, err = h.Conn.Filters().Update(ctx, id, model.Resource{"expression": "false"}); err != nil {
		return err
	}
	if _, err = h.Conn.Filters().List(ctx, model.Query{"workflowIds": []string{workflow.ID()}}); err != nil {
		return err
	}
	if err = h.Conn.Filters().Delete(ctx, id); err != nil {
		return err
	}

	report.Add(h, "filter create/get/update/list/delete", nil)
	return nil
}
