package scenarios

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/telemetry"
)

// spanCapture 并发安全的 span 捕获 exporter，用于断言追踪关系。
type spanCapture struct {
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁。
	mu sync.Mutex
	// spans 已捕获的追踪片段，供父子关系与 exporter 验收使用。
	spans []sdktrace.ReadOnlySpan
}

// ExportSpans 执行 e.mu.Lock/e.mu.Unlock 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (e *spanCapture) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.spans = append(e.spans, spans...)
	return nil
}

// Shutdown 在传入 context 的预算内排空调用并关闭资源，预算到期后取消剩余工作。
func (e *spanCapture) Shutdown(context.Context) error {
	return nil
}

// snapshot 执行 e.mu.Lock/e.mu.Unlock/sdktrace.ReadOnlySpan 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (e *spanCapture) snapshot() []sdktrace.ReadOnlySpan {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]sdktrace.ReadOnlySpan(nil), e.spans...)
}

// Observability 验证 trace 传播、多个 exporter 和实例 provider 隔离。 每次使用唯一 namespace，成功断言写入报告后清理可删除资源。
func Observability(ctx context.Context, report *Report) (err error) {
	// global 保存全局 provider 身份，实例初始化与关闭后必须保持相同对象。
	global := otel.GetTracerProvider()
	// one, two 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值。
	one, two := &spanCapture{}, &spanCapture{}
	// jaeger, err 接收 otlptracegrpc.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	jaeger, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint("localhost:4317"), otlptracegrpc.WithInsecure())
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, jaeger.Shutdown(context.Background()))
	}()

	// listener, err 接收 net.Listen 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}

	// address 保存实际绑定地址，测试使用空闲端口避免固定端口冲突。
	address := listener.Addr().String()
	listener.Close()
	// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份。
	service := &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// tracer 使用所属实例的 tracer，保持全局 OTel provider 不变。
			tracer := task.Tracer(ctx)
			// ctx, span 启动实例或 span，并保留返回的完成通道或句柄供后续确认。
			ctx, span := tracer.Start(ctx, "notification.render-template")
			defer span.End()

			// 检查 in.Count == 10；不满足协议或配置约束时返回 Aborted（fixture notification failed）。
			if in.Count == 10 {
				return nil, status.Error(codes.Aborted, "fixture notification failed")
			}

			// conn, e 借用当前执行所属连接，子调用复用父身份且不能关闭共享资源。
			conn, e := task.Client(ctx)
			if e != nil {
				return nil, e
			}

			// child, e 构造 unary 生成桩，使用 wego Conn 时通过任务引擎执行。
			child, e := pb.NewUnaryGreeterClient(conn).WaitHello(task.WithChildKey(ctx, "notification"), in)
			if e != nil {
				return nil, e
			}

			// out 带当前任务身份的 protobuf 响应，业务字段与运行身份一起返回供断言。
			out := Reply(ctx, in)
			out.ChildRunId = child.RunId
			return out, nil
		},
		Wait: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// tracer 使用所属实例的 tracer，保持全局 OTel provider 不变。
			tracer := task.Tracer(ctx)
			// _, span 启动实例或 span，并保留返回的完成通道或句柄供后续确认。
			_, span := tracer.Start(ctx, "notification.deliver.fixture")
			span.End()
			return Reply(ctx, in), nil
		},
	}
	// h, err 接收 runtime.WithSlots 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	h, err := Start(
		ctx,
		"observability",
		report,
		service,
		[]runtime.Option{
			runtime.WithSlots(32),
			runtime.WithTelemetry(telemetry.Config{
				Trace:   telemetry.TraceConfig{Exporters: []sdktrace.SpanExporter{one, two, jaeger}},
				Metrics: telemetry.MetricsConfig{Enabled: true, Addr: address},
			}),
		},
	)
	if err != nil {
		return err
	}

	// closed 是否已关闭或禁止接收新工作；已有工作按关闭策略处理。
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, h.Close())
		}
	}()

	// graphRunID, err 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值。
	graphRunID, err := observabilityGraph(ctx, h)
	if err != nil {
		return err
	}
	// _, e 通过标准生成桩执行 unary，请求和响应使用 protobuf 载荷。
	if _, e := h.RPC.SayHello(ctx, &pb.Request{Count: 10}); status.Code(e) != codes.Aborted {
		return fmt.Errorf("notification failure status: %v", e)
	}

	// out, err 接收 h.RPC.SayHello 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	out, err := h.RPC.SayHello(ctx, &pb.Request{Message: "notification"})
	if err != nil {
		return err
	}

	// response, err 接收 http.Get 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	response, err := http.Get("http://" + address + "/metrics")
	if err != nil {
		return err
	}

	// body, err 接收 io.ReadAll 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	response.Body.Close()
	if err != nil {
		return err
	}
	if !strings.Contains(string(body), "wego_rpc_calls_total") || !strings.Contains(string(body), "wego_rpc_duration_seconds") {
		return fmt.Errorf("metrics did not observe RPC")
	}

	closed = true
	if err = h.Close(); err != nil {
		return err
	}
	if otel.GetTracerProvider() != global {
		return fmt.Errorf("global provider changed")
	}

	// spans 在锁内复制 exporter 已收到的 span，后续断言不直接读可变缓冲。
	spans := one.snapshot()
	if len(spans) < 6 || len(spans) != len(two.snapshot()) {
		return fmt.Errorf("multiple exporter spans: %d / %d", len(spans), len(two.snapshot()))
	}

	// id 当前资源的唯一身份，用于查找对应运行或会话。
	var id trace.TraceID
	// 逐项处理 spans，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, span := range spans {
		if span.SpanKind() == trace.SpanKindServer {
			// 逐项处理 span.Attributes()，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
			for _, a := range span.Attributes() {
				if string(a.Key) == "hatchet.workflow_run_id" && a.Value.AsString() == out.RunId {
					id = span.SpanContext().TraceID()
				}
			}
		}
	}
	// serverCount 当前步骤的初始计数 0，后续根据实际执行或数据量更新。
	serverCount := 0
	// businessCount 当前步骤的初始计数 0，后续根据实际执行或数据量更新。
	businessCount := 0
	// graphTasks 当前步骤的初始计数 0，后续根据实际执行或数据量更新。
	graphTasks := 0
	// graphTraceID 父子调用图中选定的 TraceID，所有关联 span 必须属于同一条追踪。
	var graphTraceID trace.TraceID
	// 逐项处理 spans，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, span := range spans {
		// isBase 当前 span 是否属于基础调用图，后续据此筛选本实例的追踪记录。
		isBase := false
		// isGraph 当前 span 是否属于父子调用图，后续验证所有关联身份与层级。
		isGraph := false
		// 逐项处理 span.Attributes()，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
		for _, a := range span.Attributes() {
			if string(a.Key) == "hatchet.workflow_run_id" {
				if a.Value.AsString() == out.RunId || a.Value.AsString() == out.ChildRunId {
					isBase = true
				}
				if a.Value.AsString() == graphRunID {
					isGraph = true
				}
			}
		}
		if isGraph && span.SpanKind() == trace.SpanKindInternal {
			graphTasks++
			if graphTraceID.IsValid() && graphTraceID != span.SpanContext().TraceID() {
				return fmt.Errorf("DAG trace propagation changed")
			}

			graphTraceID = span.SpanContext().TraceID()
		}
		if isBase && span.SpanKind() == trace.SpanKindServer {
			serverCount++
			if id.IsValid() && id != span.SpanContext().TraceID() {
				return fmt.Errorf("child trace broke propagation")
			}

			id = span.SpanContext().TraceID()
			// found 当前查询是否找到必需的资源或观测记录，缺失不能作为成功验收。
			found := false
			// 逐项处理 span.Attributes()，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
			for _, a := range span.Attributes() {
				if string(a.Key) == "hatchet.step_run_id" && a.Value.AsString() != "" {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("task attributes missing")
			}
		}
		if span.SpanContext().TraceID() == id && strings.HasPrefix(span.Name(), "notification.") {
			businessCount++
		}
	}
	if serverCount != 2 || businessCount != 2 {
		return fmt.Errorf("server/business span counts %d/%d", serverCount, businessCount)
	}
	if graphTasks != 4 || !graphTraceID.IsValid() {
		return fmt.Errorf("DAG task spans: %d", graphTasks)
	}

	// deadline 读取本机时钟，用于耗时或超时判断，不参与 durable 重放。
	deadline := time.Now().Add(15 * time.Second)
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果。
	for {
		// request, e 将请求绑定到调用预算，取消后 HTTP I/O 必须结束。
		request, e := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:16686/api/traces/"+id.String(), nil)
		if e != nil {
			return e
		}

		// response 来自受控 HTTP 请求，状态和响应体共同验证业务交互，退出前必须关闭 Body。
		response, e := http.DefaultClient.Do(request)
		if e == nil {
			body, e = io.ReadAll(io.LimitReader(response.Body, 1<<20))
			response.Body.Close()
			if e == nil && response.StatusCode == 200 && strings.Contains(string(body), id.String()) {
				break
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("trace %s not found in Jaeger", id.String())
		}

		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	report.AddTrace(h, "trace propagation, task attributes, two exporters, Jaeger, metrics, unchanged global provider", out, id.String())
	// e 用于确认 exporter 端口实际可连接；失败时继续等待预算而不认定服务就绪。
	if _, e := net.DialTimeout("tcp", address, 200*time.Millisecond); e == nil {
		return fmt.Errorf("metrics listener survived shutdown")
	}

	return nil
}

// observabilityGraph 执行 h.Conn.NewWorkflow/task.WithEvents/audit.NewTask 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func observabilityGraph(ctx context.Context, h *Harness) (runID string, err error) {
	// audited 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	audited := make(chan *pb.Reply, 4)
	// audit 构造原生工作流定义，后续 Worker 注册时才写入引擎。
	audit := h.Conn.NewWorkflow("otel-audit", task.WithEvents("order:payment_charged"))
	audit.NewTask("write-audit-entry", func(ctx context.Context, in map[string]any) (map[string]bool, error) {
		// _, span 使用所属实例的 tracer，保持全局 OTel provider 不变。
		_, span := task.Tracer(ctx).Start(ctx, "audit.persist")
		span.End()
		audited <- Reply(ctx, &pb.Request{})
		return map[string]bool{"logged": true}, nil
	})
	// w 构造原生工作流定义，后续 Worker 注册时才写入引擎。
	w := h.Conn.NewWorkflow("otel-order")
	// validate 注册工作流内任务及依赖关系，当前步骤尚未执行业务。
	validate := w.NewTask("validate", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		// _, one 使用所属实例的 tracer，保持全局 OTel provider 不变。
		_, one := task.Tracer(ctx).Start(ctx, "order.validate.schema")
		one.End()
		// _, two 使用所属实例的 tracer，保持全局 OTel provider 不变。
		_, two := task.Tracer(ctx).Start(ctx, "order.validate.fraud-check")
		two.End()
		return map[string]any{"valid": true, "id": in["id"]}, nil
	})
	// charge 注册工作流内任务及依赖关系，当前步骤尚未执行业务。
	charge := w.NewTask("charge", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		// parent 父任务输出快照，验证 DAG 下游读取的是实际父结果。
		var parent map[string]any
		// e 读取依赖任务的业务结果，后续任务只能使用已经完成的父输出。
		if e := task.ParentOutput(ctx, validate.GetName(), &parent); e != nil {
			return nil, e
		}
		if parent["valid"] != true {
			return nil, fmt.Errorf("invalid parent output")
		}

		// paymentCtx, pay 使用所属实例的 tracer，保持全局 OTel provider 不变。
		paymentCtx, pay := task.Tracer(ctx).Start(ctx, "payment.process")
		defer pay.End()

		// _, token 使用所属实例的 tracer，保持全局 OTel provider 不变。
		_, token := task.Tracer(ctx).Start(paymentCtx, "payment.tokenize-card")
		token.End()
		// _, charged 使用所属实例的 tracer，保持全局 OTel provider 不变。
		_, charged := task.Tracer(ctx).Start(paymentCtx, "payment.charge")
		charged.End()
		// conn, e 借用当前执行所属连接，子调用复用父身份且不能关闭共享资源。
		conn, e := task.Client(ctx)
		if e != nil {
			return nil, e
		}
		if e = conn.Events().Push(paymentCtx, "order:payment_charged", map[string]any{"id": parent["id"]}); e != nil {
			return nil, e
		}

		return map[string]any{"amount": 1999, "transaction_id": "fixture-transaction"}, nil
	}).WithParents(validate)
	// reserve 注册工作流内任务及依赖关系，当前步骤尚未执行业务。
	reserve := w.NewTask("reserve", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		// _, one 使用所属实例的 tracer，保持全局 OTel provider 不变。
		_, one := task.Tracer(ctx).Start(ctx, "inventory.check-availability")
		one.End()
		// _, two 使用所属实例的 tracer，保持全局 OTel provider 不变。
		_, two := task.Tracer(ctx).Start(ctx, "inventory.reserve")
		two.End()
		return map[string]any{"items": 3}, nil
	}).WithParents(validate)
	w.NewTask("confirmation", func(ctx context.Context, in map[string]any) (map[string]any, error) {
		// payment, inventory 两个父任务的输出，分别验证金额与库存等结构字段被正确传给下游。
		var payment, inventory map[string]any
		// e 读取依赖任务的业务结果，后续任务只能使用已经完成的父输出。
		if e := task.ParentOutput(ctx, charge.GetName(), &payment); e != nil {
			return nil, e
		}
		// e 读取依赖任务的业务结果，后续任务只能使用已经完成的父输出。
		if e := task.ParentOutput(ctx, reserve.GetName(), &inventory); e != nil {
			return nil, e
		}
		if payment["amount"] != float64(1999) || inventory["items"] != float64(3) {
			return nil, fmt.Errorf("parallel DAG parent results")
		}

		// conn, e 借用当前执行所属连接，子调用复用父身份且不能关闭共享资源。
		conn, e := task.Client(ctx)
		if e != nil {
			return nil, e
		}

		// rpc 构造 unary 生成桩，使用 wego Conn 时通过任务引擎执行。
		rpc := pb.NewUnaryGreeterClient(conn)
		// wg 等待本组并发步骤结束，资源关闭前必须确认所有已启动步骤退出。
		var wg sync.WaitGroup
		// errs 分配当前步骤的结果列表，容量只用于聚合，不代表业务已经成功完成。
		errs := make([]error, 10)
		// 逐项处理 errs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
		for i := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// out, e 通过标准生成桩执行 unary，请求和响应使用 protobuf 载荷。
				out, e := rpc.SayHello(task.WithChildKey(ctx, fmt.Sprintf("notify-%d", i)), &pb.Request{
					Message: []string{
						"email",
						"sms",
						"push",
						"chat",
						"webhook",
					}[i%5],
					Count: int32(i),
				})
				if e == nil && (out.ChildRunId == "" || out.ParentRunId == "") {
					e = fmt.Errorf("notification child identities")
				}
				errs[i] = e
			}()
		}
		// other, e 执行具有等待策略的业务方法，deadline 同时覆盖排队和执行。
		other, e := rpc.WaitHello(task.WithChildKey(ctx, "other"), &pb.Request{Message: "other", Count: 1})
		wg.Wait()
		if e = errors.Join(e, errors.Join(errs...)); e != nil {
			return nil, e
		}

		return map[string]any{"sent": true, "notifications": 10, "other_run_id": other.RunId}, nil
	}).WithParents(charge, reserve)
	h.Names = []string{
		audit.GetName(),
		w.GetName(),
		TaskName(pb.UnaryGreeter_SayHello_FullMethodName),
		TaskName(pb.UnaryGreeter_WaitHello_FullMethodName),
		TaskName(pb.UnaryGreeter_ChildHello_FullMethodName),
	}
	// native, e 为任务定义创建独立 Worker，显示名称与注册身份分开。
	native, e := h.Conn.NewWorker(h.Namespace+"otel-dag", client.WithWorkflows(w, audit))
	if e != nil {
		return "", e
	}

	defer func() {
		err = errors.Join(err, native.Shutdown(ctx))
	}()

	if _, e = native.Start(); e != nil {
		return "", e
	}
	if e = native.WaitReady(ctx); e != nil {
		return "", e
	}

	// result, e 提交并等待业务结果，失败和取消不得作为有效输出继续使用。
	result, e := w.Run(ctx, map[string]any{"id": "fixture-order"})
	if e != nil {
		return "", e
	}

	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
	var out map[string]any
	if e = result.TaskOutput("confirmation").Into(&out); e != nil {
		return "", e
	}
	if out["sent"] != true || out["notifications"] != float64(10) {
		return "", fmt.Errorf("confirmation output: %v", out)
	}

	// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
	select {
	// event 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功。
	case event := <-audited:
		if e = waitStatus(ctx, h, event.RunId, "COMPLETED"); e != nil {
			return "", e
		}
		h.Report.Add(h, "audit event triggered workflow with traced persistence", event)
	case <-ctx.Done():
		return "", ctx.Err()
	}
	h.Report.Add(
		h,
		"traced DAG parallel payment/inventory and ten concurrent child notifications plus second standalone",
		&pb.Reply{RunId: result.RunID},
	)
	return result.RunID, nil
}
