package scenarios

import (
	"context"
	"errors"
	"fmt"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// Affinity 执行 Affinity 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func Affinity(ctx context.Context, report *Report) error {
	return affinity(ctx, report, false)
}

// Sticky 执行 Sticky 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func Sticky(ctx context.Context, report *Report) error {
	// err 接收 affinity 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
	if err := affinity(ctx, report, true); err != nil {
		return err
	}

	return stickyDAG(ctx, report)
}

// stickyDAG 执行 h.Close/h.Conn.NewWorkflow/task.WithSticky 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func stickyDAG(ctx context.Context, report *Report) (err error) {
	// h, err 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值。
	h, err := Start(ctx, "sticky-workers", report, &Service{}, nil)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// w 构造原生工作流定义，后续 Worker 注册时才写入引擎。
	w := h.Conn.NewWorkflow("sticky-dag", task.WithSticky(model.StickySoft))
	// fn 返回当前执行的 WorkerID，验证粘性 DAG 的多个任务由同一实例执行。
	fn := func(ctx context.Context, in map[string]any) (map[string]string, error) {
		// info, _ 读取实际执行身份，例如 RunID 与 WorkerID；普通网络 context 没有任务身份。
		info, _ := task.Info(ctx)
		return map[string]string{"worker_id": info.WorkerID}, nil
	}
	w.NewTask("first", fn)
	w.NewTask("second", fn)
	h.Names = []string{
		w.GetName(),
		TaskName(pb.UnaryGreeter_SayHello_FullMethodName),
		TaskName(pb.UnaryGreeter_WaitHello_FullMethodName),
		TaskName(pb.UnaryGreeter_ChildHello_FullMethodName),
	}
	// 逐项处理 []string{"alpha", "beta"}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, label := range []string{"alpha", "beta"} {
		// worker, e 为任务定义创建独立 Worker，显示名称与注册身份分开。
		worker, e := h.Conn.NewWorker(h.Namespace+label, client.WithWorkflows(w), client.WithWorkerRuntime(runtime.WithLabels(map[string]any{"dag_label": label})))
		if e != nil {
			return e
		}

		defer func() {
			err = errors.Join(err, worker.Shutdown(ctx))
		}()

		if _, e = worker.Start(); e != nil {
			return e
		}
		if e = worker.WaitReady(ctx); e != nil {
			return e
		}
	}
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
	for i := 0; i < 8; i++ {
		// result, e 提交并等待业务结果，失败和取消不得作为有效输出继续使用。
		result, e := w.Run(ctx, map[string]any{})
		if e != nil {
			return e
		}

		// first, second 两次执行的结果视图，比较 Worker 身份以验证粘性调度。
		var first, second map[string]string
		if e = result.TaskOutput("first").Into(&first); e != nil {
			return e
		}
		if e = result.TaskOutput("second").Into(&second); e != nil {
			return e
		}
		if first["worker_id"] == "" || first["worker_id"] != second["worker_id"] {
			return fmt.Errorf("sticky DAG workers: %v/%v", first, second)
		}

		report.Add(
			h,
			"two-worker sticky DAG executes independent tasks on one worker",
			&pb.Reply{RunId: result.RunID, WorkerId: first["worker_id"]},
		)
	}
	return nil
}

// affinity 执行 task.Client/conn.Run/task.WithChildKey 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func affinity(ctx context.Context, report *Report, sticky bool) (err error) {
	// service 本场景的受控业务服务，handler 注入明确行为并记录实际调用结果。
	service := &Service{}
	if sticky {
		service.Say = func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// conn, e 借用当前执行所属连接，子调用复用父身份且不能关闭共享资源。
			conn, e := task.Client(ctx)
			if e != nil {
				return nil, e
			}

			// result, e 提交并等待业务结果，失败和取消不得作为有效输出继续使用。
			result, e := conn.Run(task.WithChildKey(ctx, "sticky-child"), pb.UnaryGreeter_WaitHello_FullMethodName, in, client.WithRunSticky(true))
			if e != nil {
				return nil, e
			}

			// child 子调用的 protobuf 响应，检查 ChildRunID 和 ParentRunID 的关联。
			var child pb.Reply
			if e = result.Into(&child); e != nil {
				return nil, e
			}

			// parent 父响应带有当前 RunID 和 WorkerID，子调用结果据此验证粘性与父子身份。
			parent := Reply(ctx, in)
			if child.WorkerId != parent.WorkerId {
				return nil, fmt.Errorf("sticky child worker %s differs from parent %s", child.WorkerId, parent.WorkerId)
			}

			parent.ChildRunId = child.RunId
			return parent, nil
		}
	}
	// policies 明确列出本段注册、传输或清理选项，应用顺序决定重复字段的覆盖关系。
	policies := []worker.Option{
		worker.WithTask(pb.UnaryGreeter_WaitHello_FullMethodName, task.WithSticky(model.StickySoft)),
	}
	// name 注册或查询时使用的名称；必须与提交任务的名称对应。
	name := "runtime-affinity"
	if sticky {
		name = "sticky-workers"
	}
	// h, err 接收 runtime.WithLabels 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	h, err := Start(ctx, name, report, service, []runtime.Option{runtime.WithLabels(map[string]any{"affinity": "alpha"})}, policies...)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// opts 复制或扩展当前列表，避免把内部可变容器直接交给调用方修改。
	opts := append(append([]runtime.Option{}, h.Runtime...), runtime.WithLabels(map[string]any{"affinity": "beta"}))
	// second 构造共享注册表的 Server，业务只在 Serve 启动后接收调用。
	second := wego.NewServer(server.WithRuntime(opts...), server.WithWorker(policies...))

	pb.RegisterUnaryGreeterServer(second, service)
	go func() {
		_ = second.Serve()
	}()
	if err = support.WaitWorkers(ctx, h.Conn, h.Namespace, 2, nil); err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, support.StopServer(context.Background(), second))
	}()

	// ids Worker 名称到注册身份的映射。
	ids := map[string]string{}
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
	for i := 0; i < 20; i++ {
		// label 交替使用 alpha、beta 的受控标签，使批提交覆盖两个亲和性分组。
		label := []string{"alpha", "beta"}[i%2]
		// result, e 提交并等待业务结果，失败和取消不得作为有效输出继续使用。
		result, e := h.Conn.Run(
			ctx,
			pb.UnaryGreeter_SayHello_FullMethodName,
			&pb.Request{Count: int32(i)},
			client.WithDesiredWorkerLabels(map[string]*model.DesiredWorkerLabel{"affinity": {Value: label, Required: true}}),
		)
		if e != nil {
			return e
		}

		// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
		var out pb.Reply
		if e = result.Into(&out); e != nil {
			return e
		}
		if out.WorkerId == "" {
			return fmt.Errorf("worker identity missing")
		}
		// id 是该标签第一次分配的 WorkerID，后续相同 Required 条件必须得到同一个实例。
		if id := ids[label]; id != "" && id != out.WorkerId {
			return fmt.Errorf("required affinity changed owner")
		}

		ids[label] = out.WorkerId
		report.Add(h, "required affinity "+label+"; sticky="+fmt.Sprint(sticky), &out)
	}
	if ids["alpha"] == ids["beta"] {
		return fmt.Errorf("distinct labels routed to same worker")
	}

	return nil
}
