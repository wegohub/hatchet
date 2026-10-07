package scenarios

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
)

// Shutdown 在传入 context 的预算内排空调用并关闭资源，预算到期后取消剩余工作。
func Shutdown(ctx context.Context, report *Report) (err error) {
	// 逐项处理 []runtime.ShutdownConfig{ {Mode: runtime.DrainUntilDone}, {Mode: runtime.DrainWithTimeout, Timeout: 5 * time.Second}, }，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, mode := range []runtime.ShutdownConfig{
		{Mode: runtime.DrainUntilDone},
		{Mode: runtime.DrainWithTimeout, Timeout: 5 * time.Second},
	} {
		if err = drainConn(ctx, report, mode); err != nil {
			return err
		}
	}
	// failed 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	failed := server.New(server.WithRuntime(Runtime("wego_failed_start_")...))
	// e 必须反映启动错误；无效配置不能成功启动。
	if e := failed.Serve(); e == nil {
		return fmt.Errorf("empty server started")
	}
	failed.Stop()

	return drainStream(ctx, report)
}

// drainConn 执行 ctx.Done/ctx.Err/runtime.WithShutdown 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func drainConn(ctx context.Context, report *Report, mode runtime.ShutdownConfig) (err error) {
	// started 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	started := make(chan struct{})
	// release 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	release := make(chan struct{})
	// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份。
	service := &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			close(started)
			// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
			select {
			case <-release:
				return Reply(ctx, in), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	// h, err 接收 runtime.WithShutdown 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	h, err := Start(ctx, "shutdown", report, service, []runtime.Option{runtime.WithShutdown(mode)})
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// e 必须拒绝停止后的重启，同一 Server 的资源生命周期不可复用。
	if e := h.Server.Serve(); e == nil {
		return fmt.Errorf("duplicate Serve succeeded")
	}

	// result 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	result := make(chan error, 1)
	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
	var out *pb.Reply
	go func() {
		// value, e 通过标准生成桩执行 unary，请求和响应使用 protobuf 载荷。
		value, e := h.RPC.SayHello(ctx, &pb.Request{Message: "drained result"})
		out = value
		result <- e
	}()
	// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
	select {
	case <-started:
	case <-ctx.Done():
		return ctx.Err()
	}
	// closed 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	closed := make(chan error, 1)
	go func() {
		closed <- h.Conn.Close()
	}()
	// 排空开始后，新的管理请求应立即被拒绝。
	for {
		// _, e 读取实际执行身份，例如 RunID 与 WorkerID；普通网络 context 没有任务身份。
		_, e := h.Conn.Info(ctx)
		if errors.Is(e, model.ErrClosed) {
			break
		}
		if e != nil {
			return e
		}

		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	// _, e 执行具有等待策略的业务方法，deadline 同时覆盖排队和执行。
	if _, e := h.RPC.WaitHello(ctx, &pb.Request{}); !errors.Is(e, model.ErrClosed) {
		return fmt.Errorf("new call admitted during drain: %v", e)
	}

	// 根据已经就绪的通知选择下一步，不通过轮询固定延迟猜测执行时机。
	select {
	// e 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功。
	case e := <-closed:
		return fmt.Errorf("connection closed before in-flight result: %v", e)
	default:
	}
	close(release)
	// e 从实际操作完成通道读取结果，失败时不能继续后续成功断言。
	if e := <-result; e != nil {
		return e
	}
	// e 从实际操作完成通道读取结果，失败时不能继续后续成功断言。
	if e := <-closed; e != nil {
		return e
	}
	if out == nil || out.Message != "drained result" {
		return fmt.Errorf("drained result missing")
	}

	// wg 等待本组并发步骤结束，资源关闭前必须确认所有已启动步骤退出。
	var wg sync.WaitGroup
	// errs 分配当前步骤的结果列表，容量只用于聚合，不代表业务已经成功完成。
	errs := make([]error, 8)
	// 逐项处理 errs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()

			errs[i] = h.Conn.Shutdown(ctx)
		}()
	}
	wg.Wait()
	// e 合并各步骤错误，不能因前一步失败遗漏后续清理错误。
	if e := errors.Join(errs...); e != nil {
		return e
	}

	report.Add(h, fmt.Sprintf("drain mode %d waits result, rejects new calls, repeated/concurrent close", mode.Mode), out)
	// 被测连接关闭后，资源删除通过另一条独立拥有的连接完成。
	h.Conn, err = client.New(client.WithRuntime(h.Runtime...))
	return err
}

// drainStream 执行 pb.RegisterGreeterServer/runtime.WithSlots/runtime.WithControlSlots 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func drainStream(ctx context.Context, report *Report) (err error) {
	// names 本步骤需要遍历的名称或路径列表，注册、查询和清理使用同一集合以保持对应。
	names := []string{"wego-session-control"}
	// 逐项处理 pb.Greeter_ServiceDesc.Methods，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, d := range pb.Greeter_ServiceDesc.Methods {
		names = append(names, TaskName("/wego.example.v1.Greeter/"+d.MethodName))
	}
	// 逐项处理 pb.Greeter_ServiceDesc.Streams，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, d := range pb.Greeter_ServiceDesc.Streams {
		names = append(names, TaskName("/wego.example.v1.Greeter/"+d.StreamName)+"-session")
		names = append(names, TaskName("/wego.example.v1.Greeter/"+d.StreamName)+"-start")
	}
	// h, err 接收 pb.RegisterGreeterServer 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	h, err := StartRegistered(
		ctx,
		"shutdown",
		report,
		func(r grpc.ServiceRegistrar) {
			pb.RegisterGreeterServer(r, &streamService{})
		},
		names,
		[]runtime.Option{runtime.WithSlots(1), runtime.WithControlSlots(4)},
	)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// stream, err 接收 pb.NewGreeterClient 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	stream, err := pb.NewGreeterClient(h.Conn).ChatHellos(ctx)
	if err != nil {
		return err
	}
	if err = stream.Send(&pb.Request{Message: "before drain"}); err != nil {
		return err
	}

	// out, err 接收 stream.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	out, err := stream.Recv()
	if err != nil {
		return err
	}

	// budget, stop 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
	budget, stop := context.WithTimeout(ctx, 15*time.Second)
	defer stop()

	// closed 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	closed := make(chan error, 1)
	go func() {
		closed <- support.StopServer(budget, h.Server)
	}()
	// 根据已经就绪的通知选择下一步，不通过轮询固定延迟猜测执行时机。
	select {
	// e 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功。
	case e := <-closed:
		return fmt.Errorf("server did not wait active stream: %v", e)
	case <-time.After(100 * time.Millisecond):
	}
	// 当前步骤失败时终止处理：draining control channel: %w；不把无效结果交给下一步。
	if err = stream.CloseSend(); err != nil {
		return fmt.Errorf("draining control channel: %w", err)
	}
	// _, e 接收下一项业务数据；EOF 只在对应方向正常结束后成立。
	if _, e := stream.Recv(); e != io.EOF {
		return fmt.Errorf("drained stream terminal: %v", e)
	}
	if err = <-closed; err != nil {
		return err
	}

	report.Add(h, "business worker drained while control END/ACK remained available", out)
	return nil
}
