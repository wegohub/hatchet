package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/internal/exampleutil"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
)

// service 三种流的示例业务服务
type service struct {
	// pb.UnimplementedGreeterServer 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	pb.UnimplementedGreeterServer
}

// UploadHellos 接收多条 Request，收到输入 EOF 后返回最终 Reply，例如两条输入得到 Count=2
func (*service) UploadHellos(stream grpc.ClientStreamingServer[pb.Request, pb.Reply]) error {
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

// WatchHellos 处理单条 Request 并连续发送 Reply，例如 Count=3 时发送三条输出
func (*service) WatchHellos(req *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
	if err := stream.SendHeader(metadata.Pairs("mode", "server")); err != nil {
		return err
	}

	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
	for i := int32(0); i < req.Count; i++ {
		// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
		if err := stream.Send(&pb.Reply{Message: req.Message, Count: i}); err != nil {
			return err
		}
	}
	stream.SetTrailer(metadata.Pairs("count", strconv.Itoa(int(req.Count))))
	return nil
}

// ChatHellos 边接收 Request 边发送 Reply，输入 EOF 只结束输入方向
func (*service) ChatHellos(stream grpc.BidiStreamingServer[pb.Request, pb.Reply]) error {
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

// run 执行此独立示例或 Worker 进程的连接、注册、业务调用及清理；context 控制总预算，错误传播到 main 退出码
func run() error {
	// ctx, cancel 读取或创建当前操作预算，后续注册、提交和等待都使用这一预算
	ctx, cancel := exampleutil.Context()
	defer cancel()

	// namespace 创建本轮隔离前缀，注册、调用与资源删除使用相同名称空间
	namespace := exampleutil.Namespace()
	// opts 复制或扩展当前列表，避免把内部可变容器直接交给调用方修改
	opts := append(
		exampleutil.Runtime(namespace),
		runtime.WithLogger(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))),
	)
	// srv 构造共享注册表的 Server，业务只在 Serve 启动后接收调用
	srv := wego.NewServer(server.WithRuntime(opts...))

	pb.RegisterGreeterServer(srv, &service{})
	// serveErr 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()
	defer srv.Stop()

	// conn, err 接收 wego.NewConn 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	conn, err := wego.NewConn(client.WithRuntime(opts...))
	if err != nil {
		return err
	}

	defer conn.Close()
	// err 保存注册和消费就绪检查结果；未就绪时不得开始提交本场景业务
	if err := support.WaitWorkers(ctx, conn, namespace, 2, serveErr); err != nil {
		return err
	}

	// c 构造标准 gRPC 流服务客户端，传输由传入连接决定
	c := pb.NewGreeterClient(conn)
	// 逐项处理 []int{0, 3}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, n := range []int{0, 3} {
		// upload, err 接收 c.UploadHellos 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		upload, err := c.UploadHellos(ctx)
		// 当前步骤失败时终止处理：upload open: %w；不把无效结果交给下一步
		if err != nil {
			return fmt.Errorf("upload open: %w", err)
		}

		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
		for i := 0; i < n; i++ {
			if err = upload.Send(&pb.Request{Message: "hello"}); err != nil {
				return err
			}
		}
		// reply, err 接收 upload.CloseAndRecv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		reply, err := upload.CloseAndRecv()
		// 当前步骤失败时终止处理：upload receive: %w；不把无效结果交给下一步
		if err != nil {
			return fmt.Errorf("upload receive: %w", err)
		}
		if reply.Count != int32(n) {
			return fmt.Errorf("upload count %d != %d", reply.Count, n)
		}
		if upload.Trailer().Get("count")[0] != strconv.Itoa(n) {
			return fmt.Errorf("upload trailer")
		}
	}
	// watch, err 接收 c.WatchHellos 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	watch, err := c.WatchHellos(ctx, &pb.Request{Message: "watch", Count: 3})
	// 当前步骤失败时终止处理：watch open: %w；不把无效结果交给下一步
	if err != nil {
		return fmt.Errorf("watch open: %w", err)
	}

	// header, err 接收 watch.Header 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	header, err := watch.Header()
	if err != nil || len(header.Get("mode")) != 1 || header.Get("mode")[0] != "server" {
		return fmt.Errorf("watch header %v: %v", header, err)
	}

	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
	for i := int32(0); i < 3; i++ {
		// reply, err 接收 watch.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		reply, err := watch.Recv()
		if err != nil {
			return err
		}
		if reply.Count != i {
			return fmt.Errorf("watch sequence")
		}
	}
	if _, err = watch.Recv(); err != io.EOF {
		return fmt.Errorf("watch terminal: %v", err)
	}

	// chat, err 接收 c.ChatHellos 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	chat, err := c.ChatHellos(ctx)
	// 当前步骤失败时终止处理：chat open: %w；不把无效结果交给下一步
	if err != nil {
		return fmt.Errorf("chat open: %w", err)
	}

	// sent 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	sent := make(chan error, 1)
	go func() {
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
		for i := int32(0); i < 3; i++ {
			// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
			if err := chat.Send(&pb.Request{Message: "chat", Count: i}); err != nil {
				sent <- err
				return
			}
		}
		sent <- chat.CloseSend()
	}()
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
	for i := int32(0); i < 3; i++ {
		// reply, err 接收 chat.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		reply, err := chat.Recv()
		if err != nil {
			return err
		}
		if reply.Count != i {
			return fmt.Errorf("chat sequence")
		}
	}
	if err = <-sent; err != nil {
		return err
	}
	if _, err = chat.Recv(); err != io.EOF {
		return fmt.Errorf("chat terminal: %v", err)
	}

	exampleutil.Print(map[string]any{"client_stream": true, "server_stream": true, "bidi_stream": true})
	return nil
}

// main 组织此示例或测试进程的初始化、执行和清理；错误以日志或非零退出码报告，不把启动成功代替业务成功
func main() {
	// err 接收 run 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
