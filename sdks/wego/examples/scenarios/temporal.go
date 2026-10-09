package scenarios

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	wlog "github.com/hatchet-dev/hatchet/sdks/wego/log"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/option"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// temporalService 迁移验收的 durable 业务服务，记录子调用与父输出快照
type temporalService struct {
	// pb.UnimplementedTemporalServer 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	pb.UnimplementedTemporalServer
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁
	mu sync.Mutex
	// counts 业务执行次数，用于断言重试、重放及幂等行为
	counts map[string]int
	// parentSnapshots 子任务读取到的父输出快照，用于断言 DAG 数据依赖
	parentSnapshots map[string][]*pb.Reply
	// called 业务 handler 调用通知，让测试在确定位置注入故障
	called chan *pb.Reply
}

// execute 执行 s.mu.Lock/s.mu.Unlock/status.Error 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误
func (s *temporalService) execute(ctx context.Context, in *pb.Request, method string) (out *pb.Reply, err error) {
	s.mu.Lock()
	s.counts[method]++
	s.mu.Unlock()
	out = Reply(ctx, in)
	// 检查 in.Fail；不满足协议或配置约束时返回 FailedPrecondition（fixture operation failed）
	if in.Fail {
		return nil, status.Error(codes.FailedPrecondition, "fixture operation failed")
	}

	// 按协议、输入类型或状态分派处理；每个分支只应用与该类型匹配的转换和状态更新
	switch method {
	case "Charge", "ChargeWithRetries", "LoggedCharge":
		// 检查 in.Id == ""；不满足协议或配置约束时返回 InvalidArgument（order id required）
		if in.Id == "" {
			return nil, status.Error(codes.InvalidArgument, "order id required")
		}
		// 检查 method == "ChargeWithRetries" && out.RetryCount < 2；不满足协议或配置约束时返回 Unavailable（fixture transient payment failure）
		if method == "ChargeWithRetries" && out.RetryCount < 2 {
			return nil, status.Error(codes.Unavailable, "fixture transient payment failure")
		}
		if method == "LoggedCharge" {
			if err = wlog.RInfo(ctx, "charging fixture order", "order_id", in.Id); err != nil {
				return nil, err
			}
		}
		out.Ok = true
		out.Count = 1999
	case "Validate":
		out.Ok = in.Id != ""
		// 检查 !out.Ok；不满足协议或配置约束时返回 InvalidArgument（order id required）
		if !out.Ok {
			return nil, status.Error(codes.InvalidArgument, "order id required")
		}
	case "Fulfill":
		out.Ok = true
		out.Message = "tracking-" + in.Id
	case "Welcome", "Followup", "Digest":
		// 检查 in.Message == ""；不满足协议或配置约束时返回 InvalidArgument（email required）
		if in.Message == "" {
			return nil, status.Error(codes.InvalidArgument, "email required")
		}
		if method == "Digest" {
			if err = task.Sleep(ctx, 300*time.Millisecond); err != nil {
				return nil, err
			}
		}
		out.Count = 1
		out.Ok = true
	case "Item":
		out.Ok = in.Count > 0 && in.Id != ""
		// 检查 !out.Ok；不满足协议或配置约束时返回 InvalidArgument（item and positive quantity required）
		if !out.Ok {
			return nil, status.Error(codes.InvalidArgument, "item and positive quantity required")
		}
	case "Report":
		out.Count = 100
		out.Ok = true
		s.called <- out
	case "SyncRecords":
		out.Count = 10
		out.Ok = true
	case "CallModel":
		out.Message = "completed: " + in.Message
	case "Approval":
		// 检查 in.CorrelationId == ""；不满足协议或配置约束时返回 InvalidArgument（opaque correlation id required）
		if in.CorrelationId == "" {
			return nil, status.Error(codes.InvalidArgument, "opaque correlation id required")
		}
		// quoted 生成当前数据的文本表示，用于名称、业务比较或报告，不包含凭证
		quoted := strconv.Quote(in.CorrelationId)
		// event 事件等待返回的普通 JSON 数据，用于检查匹配条件与 payload
		var event map[string]any
		event, err = task.WaitForEvent(ctx, "approval:granted", "input.correlation_id == "+quoted, model.EventWait{})
		if err != nil {
			return nil, err
		}
		out.Ok = true
		out.Message = fmt.Sprint(event["approved_by"])
	case "Process", "Onboard", "ShipItems":
		// conn, e 借用当前执行所属连接，子调用复用父身份且不能关闭共享资源
		conn, e := task.Client(ctx)
		if e != nil {
			return nil, e
		}
		// rpc 构造 durable 示例业务桩，生成接口仍遵循标准 gRPC
		rpc := pb.NewTemporalClient(conn)
		if method == "Process" {
			// now 通过 durable Now 取得的记录时间，用于验证重放一致性
			var now time.Time
			now, err = task.Now(ctx)
			if err != nil {
				return nil, err
			}

			out.ProcessedAt = now.Format(time.RFC3339Nano)
			// validated, e 校验当前定义或业务结果，不满足约束时在执行前失败
			validated, e := rpc.Validate(task.WithChildKey(ctx, "validate"), in)
			if e != nil {
				return nil, e
			}

			out.ChildRunIds = append(out.ChildRunIds, validated.RunId)
			// charged 是扣款子调用结果，业务拒绝时不能继续履约或伪造成功
			charged, e := rpc.Charge(task.WithChildKey(ctx, "charge"), in)
			if e != nil {
				return nil, e
			}

			out.ChildRunIds = append(out.ChildRunIds, charged.RunId)
			s.mu.Lock()
			s.parentSnapshots[out.RunId] = append(s.parentSnapshots[out.RunId], out)
			s.mu.Unlock()
			if err = task.Sleep(ctx, time.Duration(in.DelayMillis)*time.Millisecond); err != nil {
				return nil, err
			}

			// fulfilled 是履约子任务的实际响应，只有扣款成功后才提交该子调用
			fulfilled, e := rpc.Fulfill(task.WithChildKey(ctx, "fulfill"), in)
			if e != nil {
				return nil, e
			}

			out.ChildRunIds = append(out.ChildRunIds, fulfilled.RunId)
			out.Ok = fulfilled.Ok
			out.Message = fulfilled.Message
			out.Count = charged.Count
		} else if method == "Onboard" {
			// welcome 使用稳定 child key 发起欢迎邮件子调用，结果与后续 Sleep 各自记录
			welcome, e := rpc.Welcome(task.WithChildKey(ctx, "welcome"), in)
			if e != nil {
				return nil, e
			}

			out.ChildRunIds = append(out.ChildRunIds, welcome.RunId)
			if err = task.Sleep(ctx, 300*time.Millisecond); err != nil {
				return nil, err
			}

			// followup 使用稳定 child key 发起跟进邮件子调用，durable 重放不能重复生成子身份
			followup, e := rpc.Followup(task.WithChildKey(ctx, "followup"), in)
			if e != nil {
				return nil, e
			}

			out.ChildRunIds = append(out.ChildRunIds, followup.RunId)
			out.Count = welcome.Count + followup.Count
		} else {
			// wg 等待本组并发步骤结束，资源关闭前必须确认所有已启动步骤退出
			var wg sync.WaitGroup
			// outputs 分配当前步骤的结果列表，容量只用于聚合，不代表业务已经成功完成
			outputs := make([]*pb.Reply, len(in.Items))
			// errs 分配当前步骤的结果列表，容量只用于聚合，不代表业务已经成功完成
			errs := make([]error, len(in.Items))
			// 逐项处理 in.Items，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
			for i, item := range in.Items {
				wg.Add(1)
				go func() {
					defer wg.Done()

					outputs[i], errs[i] = rpc.Item(task.WithChildKey(ctx, fmt.Sprintf("item-%d", i)), &pb.Request{Id: item.ProductId, Count: item.Quantity})
				}()
			}
			wg.Wait()
			if err = errors.Join(errs...); err != nil {
				return nil, err
			}

			// 逐项处理 outputs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
			for _, child := range outputs {
				out.ChildRunIds = append(out.ChildRunIds, child.RunId)
			}
			out.Count = int32(len(outputs))
		}
	}
	return out, nil
}

// Temporal 验证 durable DAG、子调用、重试与业务数据依赖 每次使用唯一 namespace，成功断言写入报告后清理可删除资源
func Temporal(ctx context.Context, report *Report) (err error) {
	// key 生成当前数据的文本表示，用于名称、业务比较或报告，不包含凭证
	key := fmt.Sprintf("wego_model_budget_%d", time.Now().UnixNano())
	// setup, err 接收 client.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	setup, err := client.New(client.WithRuntime(Runtime("wego_temporal_setup_")...))
	if err != nil {
		return err
	}

	err = setup.RateLimits().Upsert(model.CreateRatelimitOpts{Key: key, Limit: 10, Duration: model.Second})
	err = errors.Join(err, setup.Close())
	if err != nil {
		return err
	}

	// s 当前场景服务或指定 StreamID 的端点，后续状态更新只作用于这一对象
	s := &temporalService{
		counts:          map[string]int{},
		parentSnapshots: map[string][]*pb.Reply{},
		called:          make(chan *pb.Reply, 16),
	}
	// methods 方法名称到 protobuf 绑定的映射
	methods := []string{}
	// names 本步骤需要遍历的名称或路径列表，注册、查询和清理使用同一集合以保持对应
	names := []string{}
	// opts 明确列出本段注册、传输或清理选项，应用顺序决定重复字段的覆盖关系
	opts := []runtime.Option{runtime.WithLogReport(true)}
	// 逐项处理 pb.Temporal_ServiceDesc.Methods，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, desc := range pb.Temporal_ServiceDesc.Methods {
		// method 本次调用用于 trace、投影及任务路由的方法名；RPCInput 提供完整方法时覆盖普通任务名
		method := "/wego.example.v1.Temporal/" + desc.MethodName
		methods = append(methods, method)
		names = append(names, method)
		opts = append(opts, runtime.WithRoutingDefaults(method, map[string]any{"id": "", "account": ""}))
	}
	// policies 按方法收集的 Worker 策略，普通与 durable 入口分别配置
	policies := []worker.Option{}
	// 逐项配置迁移服务的方法策略，普通任务与 durable 方法分别注册，避免把网络请求误当作具有任务身份的执行
	for _, method := range []string{
		pb.Temporal_Process_FullMethodName,
		pb.Temporal_Onboard_FullMethodName,
		pb.Temporal_Digest_FullMethodName,
		pb.Temporal_Approval_FullMethodName,
		pb.Temporal_ShipItems_FullMethodName,
	} {
		// ttl 初始化为未设置状态，需要驱逐的 durable 方法在后续分支明确覆盖
		ttl := time.Duration(0)
		if method == pb.Temporal_Process_FullMethodName {
			ttl = 3 * time.Second
		}
		policies = append(policies, worker.WithDurableTask(
			method,
			task.WithExecutionTimeout(3*time.Minute),
			task.WithEviction(model.EvictionPolicy{TTL: option.Some(ttl), AllowCapacityEviction: option.Some(true)}),
		))
	}
	// strategy 本场景需要验证的并发策略，实际执行峰值和终态用于判断其是否生效
	strategy := model.CancelInProgress
	policies = append(
		policies,
		worker.WithTask(pb.Temporal_ChargeWithRetries_FullMethodName, task.WithRetries(3), task.WithRetryBackoff(2, 3*time.Second)),
		worker.WithTask(pb.Temporal_Report_FullMethodName, task.WithCron("0 9 * * 1"), task.WithCronInput(&pb.Request{Message: "weekly"})),
		worker.WithTask(
			pb.Temporal_SyncRecords_FullMethodName,
			task.WithConcurrency(model.Concurrency{
				Expression:    "input.routing.account",
				MaxRuns:       pointer(int32(1)),
				LimitStrategy: &strategy,
			}),
		),
		worker.WithTask(pb.Temporal_CallModel_FullMethodName, task.WithRateLimits(model.RateLimit{Key: key, Units: pointer(1)})),
	)
	// h, err 接收 pb.RegisterTemporalServer 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	h, err := StartRegistered(ctx, "temporal", report, func(r grpc.ServiceRegistrar) {
		pb.RegisterTemporalServer(r, s)
	}, names, opts, policies...)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// rpc 构造 durable 示例业务桩，生成接口仍遵循标准 gRPC
	rpc := pb.NewTemporalClient(h.Conn)
	// order 受控订单请求，固定输入身份与测试邮箱用于核对迁移 handler 的数据依赖
	order := &pb.Request{Id: "fixture-order", Message: "fixture@example.invalid"}
	// 逐项配置迁移服务的方法策略，普通任务与 durable 方法分别注册，避免把网络请求误当作具有任务身份的执行
	for _, method := range []string{
		pb.Temporal_Charge_FullMethodName,
		pb.Temporal_Validate_FullMethodName,
		pb.Temporal_Fulfill_FullMethodName,
		pb.Temporal_Welcome_FullMethodName,
		pb.Temporal_Followup_FullMethodName,
		pb.Temporal_Digest_FullMethodName,
		pb.Temporal_ChargeWithRetries_FullMethodName,
		pb.Temporal_Report_FullMethodName,
		pb.Temporal_SyncRecords_FullMethodName,
		pb.Temporal_CallModel_FullMethodName,
		pb.Temporal_LoggedCharge_FullMethodName,
	} {
		// result, e 提交并等待业务结果，失败和取消不得作为有效输出继续使用
		result, e := h.Conn.Run(ctx, method, order)
		if e != nil {
			return e
		}

		// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
		var out pb.Reply
		if e = result.Into(&out); e != nil {
			return e
		}
		if method == pb.Temporal_ChargeWithRetries_FullMethodName && out.RetryCount != 2 {
			return fmt.Errorf("charge retries: %v", &out)
		}

		report.Add(h, method+" business result", &out)
	}
	order.DelayMillis = 4000
	// ref, err 接收 h.Conn.RunNoWait 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	ref, err := h.Conn.RunNoWait(ctx, pb.Temporal_Process_FullMethodName, order)
	if err != nil {
		return err
	}
	if err = waitEvicted(ctx, h, ref.RunID); err != nil {
		return err
	}

	// result, err 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据
	result, err := ref.Result(ctx)
	if err != nil {
		return err
	}

	// processed 处理步骤的 protobuf 输出，核对业务文本与执行身份
	var processed pb.Reply
	if err = result.Into(&processed); err != nil {
		return err
	}
	if processed.Count != 1999 || !processed.Ok || len(processed.ChildRunIds) != 3 || processed.InvocationCount < 2 {
		return fmt.Errorf("process order replay result: %v", &processed)
	}

	s.mu.Lock()
	// snapshots 当前 Process RunID 对应的父输出快照，逐条检查依赖结果已被下游读取
	snapshots := s.parentSnapshots[processed.RunId]
	s.mu.Unlock()
	if len(snapshots) < 2 || snapshots[0].ProcessedAt != snapshots[1].ProcessedAt || snapshots[0].ChildRunIds[0] != snapshots[1].ChildRunIds[0] || snapshots[0].ChildRunIds[1] != snapshots[1].ChildRunIds[1] {
		return fmt.Errorf("replay changed Now or child identities")
	}

	report.Add(h, "durable process order reused Now and completed child RunIDs", &processed)
	if _, err = rpc.Process(ctx, &pb.Request{Fail: true}); status.Code(err) != codes.FailedPrecondition {
		return fmt.Errorf("process failure: %v", err)
	}

	err = nil
	// onboarded, err 接收 rpc.Onboard 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	onboarded, err := rpc.Onboard(ctx, order)
	if err != nil {
		return err
	}
	if onboarded.Count != 2 || len(onboarded.ChildRunIds) != 2 {
		return fmt.Errorf("onboarding result: %v", onboarded)
	}

	report.Add(h, "durable onboarding sends two emails around sleep", onboarded)
	// shipped, err 接收 rpc.ShipItems 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	shipped, err := rpc.ShipItems(ctx, &pb.Request{
		Items: []*pb.Item{
			{ProductId: "item-a", Quantity: 1},
			{ProductId: "item-b", Quantity: 2},
			{ProductId: "item-c", Quantity: 3},
		},
	})
	if err != nil {
		return err
	}
	if shipped.Count != 3 || len(shipped.ChildRunIds) != 3 {
		return fmt.Errorf("parallel shipment result: %v", shipped)
	}

	report.Add(h, "durable concurrent child fanout", shipped)
	// approval, err 接收 h.Conn.RunNoWait 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	approval, err := h.Conn.RunNoWait(ctx, pb.Temporal_Approval_FullMethodName, routedInput(&pb.Request{CorrelationId: "approval-fixture"}))
	if err != nil {
		return err
	}
	if err = pushUntilCompleted(
		ctx,
		h,
		approval,
		"approval:granted",
		map[string]any{"correlation_id": "approval-fixture", "approved_by": "fixture-approver"},
		nil,
	); err != nil {
		return err
	}

	// approved, err 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据
	approved, err := approval.Result(ctx)
	if err != nil {
		return err
	}

	// approvalOut 审批等待后的响应，核对事件数据确实被 handler 消费
	var approvalOut pb.Reply
	if err = approved.Into(&approvalOut); err != nil {
		return err
	}
	if !approvalOut.Ok || approvalOut.Message != "fixture-approver" {
		return fmt.Errorf("approval result: %v", &approvalOut)
	}

	report.Add(h, "correlated approval event", &approvalOut)
	// cron, err 接收 h.Conn.Crons 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	cron, err := h.Conn.Crons().Create(
		ctx,
		pb.Temporal_Report_FullMethodName,
		model.CreateCronTrigger{
			Name:       h.Namespace + "weekly",
			Expression: "0 9 * * 1",
			Input:      routedInput(&pb.Request{Message: "weekly"}),
		},
	)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Conn.Crons().Delete(context.Background(), cron.ID()))
	}()

	// scheduled, err 接收 h.Conn.Schedules 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	scheduled, err := h.Conn.Schedules().Create(
		ctx,
		pb.Temporal_Report_FullMethodName,
		model.CreateScheduledRunTrigger{
			TriggerAt: time.Now().Add(3 * time.Second),
			Input:     routedInput(&pb.Request{Message: "scheduled"}),
		},
	)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, cleanupSchedule(h, scheduled.ID()))
	}()

	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		// out 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功
		case out := <-s.called:
			if out.Message == "scheduled" {
				if err = waitStatus(ctx, h, out.RunId, model.Completed); err != nil {
					return err
				}

				report.Add(h, "runtime weekly cron and one-shot report schedule", out)
				return temporalDAG(ctx, h)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// temporalDAG 执行 h.Conn.NewWorkflow/task.WithSticky/w.NewTask 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误
func temporalDAG(ctx context.Context, h *Harness) (err error) {
	// w 构造原生工作流定义，后续 Worker 注册时才写入引擎
	w := h.Conn.NewWorkflow("order-dag", task.WithSticky(model.StickySoft))
	// validate 注册工作流内任务及依赖关系，当前步骤尚未执行业务
	validate := w.NewTask("validate", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		return map[string]any{"valid": true}, nil
	})
	// charge 注册工作流内任务及依赖关系，当前步骤尚未执行业务
	charge := w.NewTask("charge", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		// value 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
		var value map[string]any
		// err 保存父输出解码结果，成功后 value 才能用于下游计算或断言
		if err := task.ParentOutput(ctx, validate.GetName(), &value); err != nil {
			return nil, err
		}
		if value["valid"] != true {
			return nil, fmt.Errorf("validation result missing")
		}

		return map[string]any{"amount": 1999}, nil
	}).WithParents(validate)
	w.NewTask("fulfill", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		// value 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
		var value map[string]any
		// err 保存父输出解码结果，成功后 value 才能用于下游计算或断言
		if err := task.ParentOutput(ctx, charge.GetName(), &value); err != nil {
			return nil, err
		}

		return map[string]any{"fulfilled": true, "amount": value["amount"]}, nil
	}).WithParents(charge)
	h.Names = append(h.Names, w.GetName())
	// worker, err 接收 h.Conn.NewWorker 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	worker, err := h.Conn.NewWorker(h.Namespace+"dag-worker", client.WithWorkflows(w))
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, worker.Shutdown(context.Background()))
	}()

	if _, err = worker.Start(); err != nil {
		return err
	}
	if err = worker.WaitReady(ctx); err != nil {
		return err
	}

	// result, err 接收 w.Run 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	result, err := w.Run(ctx, map[string]any{})
	if err != nil {
		return err
	}

	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
	var out map[string]any
	if err = result.TaskOutput("fulfill").Into(&out); err != nil {
		return err
	}
	if out["fulfilled"] != true || out["amount"] != float64(1999) {
		return fmt.Errorf("DAG result: %v", out)
	}

	h.Report.Add(h, "DAG dependency and parent output", &pb.Reply{RunId: result.RunID})
	return nil
}
