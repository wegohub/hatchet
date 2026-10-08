package main

import (
	"context"
	"io"
	"os"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/log"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"google.golang.org/grpc"
)

// NewGreeter 实例化greeter服务
func NewGreeter() pb.GreeterServer {
	return &greeter{}
}

// greeter 示例或测试业务服务，用标准 gRPC 注册方式共享 handler
type greeter struct {
	pb.UnimplementedGreeterServer
}

func (obj *greeter) SayHello(ctx context.Context, request *pb.Request) (*pb.Reply, error) {
	// info, _ 读取实际执行身份，例如 RunID 与 WorkerID；普通网络 context 没有任务身份。
	info, _ := task.Info(ctx)
	return &pb.Reply{
		Message:    "Processed: " + request.Message,
		RunId:      info.RunID,
		WorkerId:   info.WorkerID,
		RetryCount: int32(info.RetryCount),
	}, nil
}

//func (obj *greeter) WaitHello(ctx context.Context, request *pb.Request) (*pb.Reply, error) {
//	// err 接收 task.Sleep 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
//	if err := task.Sleep(ctx, 100*time.Millisecond); err != nil {
//		return nil, err
//	}
//
//	return obj.SayHello(ctx, request)
//}
//
//func (obj *greeter) ChildHello(ctx context.Context, request *pb.Request) (*pb.Reply, error) {
//	// conn, err 接收 task.Client 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
//	conn, err := task.Client(ctx)
//	if err != nil {
//		return nil, err
//	}
//
//	return pb.NewUnaryGreeterClient(conn).SayHello(task.WithChildKey(ctx, "greeting"), request)
//}
//
//func (obj *greeter) UploadHellos(stream grpc.ClientStreamingServer[pb.Request, pb.Reply]) error {
//	// count 实际收到的业务消息计数，例如两条输入应产生 Count=2。
//	var count int32
//	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果。
//	for {
//		// _, err 接收 stream.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
//		_, err := stream.Recv()
//		if err == io.EOF {
//			stream.SetTrailer(metadata.Pairs("count", strconv.Itoa(int(count))))
//			return stream.SendAndClose(&pb.Reply{Count: count})
//		}
//		if err != nil {
//			return err
//		}
//
//		count++
//	}
//}
//
//func (obj *greeter) WatchHellos(request *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
//	// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功。
//	if err := stream.SendHeader(metadata.Pairs("mode", "server")); err != nil {
//		return err
//	}
//
//	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
//	for i := int32(0); i < request.Count; i++ {
//		// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功。
//		if err := stream.Send(&pb.Reply{Message: request.Message, Count: i}); err != nil {
//			return err
//		}
//	}
//	stream.SetTrailer(metadata.Pairs("count", strconv.Itoa(int(request.Count))))
//	return nil
//}

func (obj *greeter) ChatHellos(stream grpc.BidiStreamingServer[pb.Request, pb.Reply]) error {
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果。
	for {
		// req, err 接收 stream.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
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

func main() {
	ctx := context.Background()

	// 实例化server
	s := wego.NewServer(
		server.WithRuntime(
			runtime.WithToken(os.Getenv("HATCHET_CLIENT_TOKEN")),
			runtime.WithAddress(os.Getenv("HATCHET_BROKER_ADDR")),
			runtime.WithServerURL(os.Getenv("HATCHET_SERVER_ADDR")),
			runtime.WithTLSConfig(nil),
			runtime.WithSlots(10),
			runtime.WithDurableSlots(10),
			runtime.WithControlSlots(20),
		),
	)

	// 注册服务
	pb.RegisterGreeterServer(s, NewGreeter())

	// 启动服务
	err := s.Serve()
	if err != nil {
		log.Error(ctx, "start server", "error", err.Error())
		return
	}
}
