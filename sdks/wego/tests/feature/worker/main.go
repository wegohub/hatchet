package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// NewGreeter 实例化greeter服务
func NewGreeter(instanceName string) pb.GreeterServer {
	return &greeter{
		instanceName: instanceName,
	}
}

// greeter 示例或测试业务服务，用标准 gRPC 注册方式共享 handler
type greeter struct {
	// UnimplementedGreeterServer 按值嵌入，满足生成器的私有兼容性标记
	pb.UnimplementedGreeterServer
	// instanceName 当前实例展示名称，响应据此区分同进程中的多个 Worker。
	instanceName string
}

// SayHello 返回请求响应及实际执行身份，供本机 Worker 调用检查
func (obj *greeter) SayHello(ctx context.Context, request *pb.Request) (*pb.Reply, error) {
	// info, _ 读取实际执行身份，例如 RunID 与 WorkerID；普通网络 context 没有任务身份
	info, _ := task.Info(ctx)
	return &pb.Reply{
		Message:    "Processed: " + request.Message + "| " + obj.instanceName,
		RunId:      info.RunID,
		WorkerId:   info.WorkerID,
		RetryCount: int32(info.RetryCount),
	}, nil
}

// WaitHello 在 durable 等待完成后返回执行身份；100ms 等待可在重放时复用。
func (obj *greeter) WaitHello(ctx context.Context, request *pb.Request) (*pb.Reply, error) {
	// err 接收 task.Sleep 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
	if err := task.Sleep(ctx, 100*time.Millisecond); err != nil {
		return nil, err
	}

	return obj.SayHello(ctx, request)
}

// ChildHello 借用执行连接并以稳定子调用键 greeting 调用 unary 服务。
func (obj *greeter) ChildHello(ctx context.Context, request *pb.Request) (*pb.Reply, error) {
	// conn, err 接收 task.Client 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	conn, err := task.Client(ctx)
	if err != nil {
		return nil, err
	}

	return pb.NewUnaryGreeterClient(conn).SayHello(task.WithChildKey(ctx, "greeting"), request)
}

// UploadHellos 收完输入后返回数量；例如两条请求产生 Count=2 和对应 trailer。
func (obj *greeter) UploadHellos(stream grpc.ClientStreamingServer[pb.Request, pb.Reply]) error {
	// count 实际收到的业务消息计数，例如两条输入应产生 Count=2
	var count int32
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		// _, err 接收 stream.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		_, err := stream.Recv()
		if err == io.EOF {
			stream.SetTrailer(metadata.Pairs("count", strconv.Itoa(int(count))))
			return stream.SendAndClose(&pb.Reply{Count: count})
		}
		if err != nil {
			return err
		}

		count++
	}
}

// WatchHellos 连续输出 Count 条响应；例如 Count=3 的输出序号为 0、1、2。
func (obj *greeter) WatchHellos(request *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
	if err := stream.SendHeader(metadata.Pairs("mode", "server")); err != nil {
		return err
	}

	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
	for i := int32(0); i < request.Count; i++ {
		// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
		if err := stream.Send(&pb.Reply{Message: request.Message, Count: i}); err != nil {
			return err
		}
	}
	stream.SetTrailer(metadata.Pairs("count", strconv.Itoa(int(request.Count))))
	return nil
}

// ChatHellos 逐条收发，用同一标准 handler 验证任务与网络流入口
func (obj *greeter) ChatHellos(stream grpc.BidiStreamingServer[pb.Request, pb.Reply]) error {
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		// req, err 接收 stream.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = stream.Send(&pb.Reply{Message: req.Message, Count: req.Count}); err != nil {
			return err
		}
	}
}

// runSingle 阻塞运行指定展示名称的 Server，上下文取消后排空当前实例。
func runSingle(ctx context.Context, instanceName string) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// 实例化server
	s := wego.NewServer(
		server.WithRuntime(
			runtime.WithLogger(logger),
			runtime.WithToken(os.Getenv("HATCHET_CLIENT_TOKEN")),
			runtime.WithAddress(os.Getenv("HATCHET_BROKER_ADDR")),
			runtime.WithServerURL(os.Getenv("HATCHET_SERVER_ADDR")),
			runtime.WithTLSConfig(nil),
			runtime.WithSlots(10),
			runtime.WithDurableSlots(10),
			runtime.WithInstanceName(instanceName),
		),
	)

	// 注册服务
	pb.RegisterGreeterServer(s, NewGreeter(instanceName))

	// 上下文取消时排空已有业务，GracefulStop 完成后 Serve 解除阻塞。
	stopSignal := context.AfterFunc(ctx, s.GracefulStop)
	defer stopSignal()

	err := s.Serve()
	if err != nil {
		log.Error(ctx, "server stopped", "error", err)
		return
	}
}

// main 注册服务并阻塞运行；SDK 不自行接管应用的进程信号
func main() {
	// 应用显式接收进程信号，SDK 通过 Stop 关闭
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	instanceNum, err := strconv.Atoi(os.Getenv("INSTANCE_NUM"))
	if err != nil {
		panic(err)
	}

	// group 等待所有实例的 Serve 和关闭流程完成，主进程不能提前退出。
	var group sync.WaitGroup
	for i := range instanceNum {
		group.Go(func() {
			runSingle(ctx, fmt.Sprintf("feature-%d", i))
		})
	}
	group.Wait()
}
