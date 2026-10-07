package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/internal/exampleutil"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// greeter 示例或测试业务服务，用标准 gRPC 注册方式共享 handler。
type greeter struct {
	// pb.UnimplementedUnaryGreeterServer 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则。
	pb.UnimplementedUnaryGreeterServer
}

// SayHello 处理 unary 文本请求，返回可用于断言的响应。
func (g *greeter) SayHello(ctx context.Context, req *pb.Request) (*pb.Reply, error) {
	// info, _ 读取实际执行身份，例如 RunID 与 WorkerID；普通网络 context 没有任务身份。
	info, _ := task.Info(ctx)
	return &pb.Reply{
		Message:    "Processed: " + req.Message,
		RunId:      info.RunID,
		WorkerId:   info.WorkerID,
		RetryCount: int32(info.RetryCount),
	}, nil
}

// WaitHello 处理可等待或 durable 的请求，用于验证执行预算与等待能力。
func (g *greeter) WaitHello(ctx context.Context, req *pb.Request) (*pb.Reply, error) {
	// err 接收 task.Sleep 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
	if err := task.Sleep(ctx, 100*time.Millisecond); err != nil {
		return nil, err
	}

	return g.SayHello(ctx, req)
}

// ChildHello 通过任务上下文提交子调用，并返回父子运行身份。
func (g *greeter) ChildHello(ctx context.Context, req *pb.Request) (*pb.Reply, error) {
	// conn, err 接收 task.Client 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	conn, err := task.Client(ctx)
	if err != nil {
		return nil, err
	}

	return pb.NewUnaryGreeterClient(conn).SayHello(task.WithChildKey(ctx, "greeting"), req)
}

// run 执行此独立示例或 Worker 进程的连接、注册、业务调用及清理；context 控制总预算，错误传播到 main 退出码。
func run() error {
	// ctx, cancel 读取或创建当前操作预算，后续注册、提交和等待都使用这一预算。
	ctx, cancel := exampleutil.Context()
	defer cancel()

	// namespace 创建本轮隔离前缀，注册、调用与资源删除使用相同名称空间。
	namespace := exampleutil.Namespace()
	// options 取得客户端与 Worker 共用配置，投影、载荷链和地址必须一致。
	options := exampleutil.Runtime(namespace)
	// srv 构造共享注册表的 Server，业务只在 Serve 启动后接收调用。
	srv := wego.NewServer(server.WithRuntime(options...), server.WithWorker(worker.WithDurableTask(pb.UnaryGreeter_WaitHello_FullMethodName)))

	// 使用标准生成的注册函数；服务方法会绑定为独立任务。
	pb.RegisterUnaryGreeterServer(srv, &greeter{})
	// serveErr 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve()
	}()
	defer srv.Stop()

	// conn, err 接收 wego.NewConn 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	conn, err := wego.NewConn(client.WithRuntime(options...))
	if err != nil {
		return err
	}

	defer conn.Close()
	// err 保存注册和消费就绪检查结果；未就绪时不得开始提交本场景业务。
	if err := support.WaitWorkers(ctx, conn, namespace, 1, serveErr); err != nil {
		return err
	}

	// 同一个连接既支持标准 gRPC 客户端，也支持后面的 wego 原生执行入口。
	reply, err := pb.NewUnaryGreeterClient(conn).SayHello(ctx, &pb.Request{Message: "Hello, World!"})
	if err != nil {
		return err
	}
	if reply.Message != "Processed: Hello, World!" || reply.RunId == "" || reply.WorkerId == "" {
		return fmt.Errorf("invalid reply: %v", reply)
	}

	exampleutil.Print(reply)
	// 异步入口只提交任务；通过返回句柄显式等待并解码 protobuf 结果。
	ref, err := conn.RunNoWait(ctx, pb.UnaryGreeter_SayHello_FullMethodName, &pb.Request{Message: "async"})
	if err != nil {
		return err
	}

	// result, err 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据。
	result, err := ref.Result(ctx)
	if err != nil {
		return err
	}

	// async 异步运行等待后的 protobuf 响应，用于与同步结果比较。
	var async pb.Reply
	if err = result.Into(&async); err != nil {
		return err
	}
	if async.Message != "Processed: async" {
		return fmt.Errorf("invalid async reply")
	}

	// 批量提交保持输入与结果的顺序对应关系。
	inputs := []client.RunManyInput{{Input: &pb.Request{Message: "one"}}, {Input: &pb.Request{Message: "two"}}}
	// refs, err 接收 conn.RunMany 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	refs, err := conn.RunMany(ctx, pb.UnaryGreeter_SayHello_FullMethodName, inputs)
	if err != nil {
		return err
	}

	// 逐项处理 refs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, ref := range refs {
		// result, err 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据。
		result, err := ref.Result(ctx)
		if err != nil {
			return err
		}

		// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
		var out pb.Reply
		if err = result.Into(&out); err != nil {
			return err
		}
		if !strings.HasPrefix(out.Message, "Processed: ") {
			return fmt.Errorf("invalid batch reply")
		}
	}
	// handler 通过上下文借用连接发起子调用，稳定 child key 保留父子身份。
	child, err := pb.NewUnaryGreeterClient(conn).ChildHello(ctx, &pb.Request{Message: "child"})
	if err != nil {
		return err
	}
	if child.Message != "Processed: child" {
		return fmt.Errorf("invalid child reply")
	}

	// durable 明确标记的方法可以使用 Sleep，普通 handler 不具备此能力。
	durable, err := pb.NewUnaryGreeterClient(conn).WaitHello(ctx, &pb.Request{Message: "durable"})
	if err != nil || durable.Message != "Processed: durable" {
		return fmt.Errorf("durable result: %v / %v", durable, err)
	}
	// methods 是本例注册的三个任务名，完成后删除定义，运行历史保留在隔离 namespace。
	for _, method := range []string{pb.UnaryGreeter_SayHello_FullMethodName, pb.UnaryGreeter_WaitHello_FullMethodName, pb.UnaryGreeter_ChildHello_FullMethodName} {
		// 删除名称与服务注册使用同一规则。
		if _, err = conn.Workflows().Delete(ctx, scenarios.TaskName(method)); err != nil {
			return err
		}
	}
	return nil
}

// main 组织此示例或测试进程的初始化、执行和清理；错误以日志或非零退出码报告，不把启动成功代替业务成功。
func main() {
	// err 接收 run 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
