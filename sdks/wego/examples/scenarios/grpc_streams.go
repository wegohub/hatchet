package scenarios

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// streamService 三种流的验收业务服务
type streamService struct {
	// pb.UnimplementedGreeterServer 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	pb.UnimplementedGreeterServer
}

// UploadHellos 接收多条 Request，收到输入 EOF 后返回最终 Reply，例如两条输入得到 Count=2
func (s *streamService) UploadHellos(stream grpc.ClientStreamingServer[pb.Request, pb.Reply]) error {
	// count 实际收到的业务消息计数，例如两条输入应产生 Count=2
	var count int32
	// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
	if err := stream.SendHeader(metadata.Pairs("mode", "client")); err != nil {
		return err
	}

	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		// in, err 接收 stream.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		in, err := stream.Recv()
		if err == io.EOF {
			stream.SetTrailer(metadata.Pairs("count", strconv.Itoa(int(count))))
			return stream.SendAndClose(Reply(stream.Context(), &pb.Request{Count: count}))
		}
		if err != nil {
			return err
		}
		// 检查 in.Fail；不满足协议或配置约束时返回 InvalidArgument（upload rejected）
		if in.Fail {
			return status.Error(codes.InvalidArgument, "upload rejected")
		}

		count++
		if in.Message == "early" {
			return stream.SendAndClose(Reply(stream.Context(), &pb.Request{Count: count}))
		}
	}
}

// WatchHellos 处理单条 Request 并连续发送 Reply，例如 Count=3 时发送三条输出
func (s *streamService) WatchHellos(in *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
	if err := stream.SendHeader(metadata.Pairs("mode", "server")); err != nil {
		return err
	}

	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
	for i := int32(0); i < in.Count; i++ {
		// out 将业务数据与本次任务身份组成响应，流输出断言使用真实 RunID 和 WorkerID
		out := Reply(stream.Context(), in)
		out.Count = i
		// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
		if err := stream.Send(out); err != nil {
			return err
		}
	}
	stream.SetTrailer(metadata.Pairs("count", strconv.Itoa(int(in.Count))))
	// 检查 in.Fail；不满足协议或配置约束时返回 FailedPrecondition（watch rejected after output）
	if in.Fail {
		return status.Error(codes.FailedPrecondition, "watch rejected after output")
	}

	return nil
}

// ChatHellos 边接收 Request 边发送 Reply，输入 EOF 只结束输入方向
func (s *streamService) ChatHellos(stream grpc.BidiStreamingServer[pb.Request, pb.Reply]) error {
	// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
	if err := stream.SendHeader(metadata.Pairs("mode", "bidi")); err != nil {
		return err
	}

	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		// in, err 接收 stream.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		in, err := stream.Recv()
		if err == io.EOF {
			stream.SetTrailer(metadata.Pairs("state", "complete"))
			return nil
		}
		if err != nil {
			return err
		}
		// 检查 in.Fail；不满足协议或配置约束时返回 Aborted（chat rejected）
		if in.Fail {
			return status.Error(codes.Aborted, "chat rejected")
		}
		if err = stream.Send(Reply(stream.Context(), in)); err != nil {
			return err
		}
	}
}

// GRPCStreams 验证 client、server、bidi 三种标准 gRPC 桩调用 每次使用唯一 namespace，成功断言写入报告后清理可删除资源
func GRPCStreams(ctx context.Context, report *Report) (err error) {
	return grpcStreams(ctx, report, nil)
}

// grpcStreams 执行 runtime.WithSlots/runtime.WithStream 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误
func grpcStreams(ctx context.Context, report *Report, extra []runtime.Option) (err error) {
	// names 本步骤需要遍历的名称或路径列表，注册、查询和清理使用同一集合以保持对应
	names := []string{}
	// 逐项处理 pb.Greeter_ServiceDesc.Methods，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, desc := range pb.Greeter_ServiceDesc.Methods {
		names = append(names, "/wego.example.v1.Greeter/"+desc.MethodName)
	}
	// 逐项处理 pb.Greeter_ServiceDesc.Streams，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, desc := range pb.Greeter_ServiceDesc.Streams {
		names = append(names, "/wego.example.v1.Greeter/"+desc.StreamName)
	}
	// opts 复制或扩展当前列表，避免把内部可变容器直接交给调用方修改
	opts := append([]runtime.Option{
		runtime.WithSlots(1),
		runtime.WithStream(runtime.StreamConfig{
			Window:          2,
			BufferBytes:     4 << 20,
			MaxMessageBytes: 1 << 20,
		}),
	}, extra...)
	// h, err 接收 pb.RegisterGreeterServer 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	h, err := StartRegistered(ctx, "grpc-streams", report, func(r grpc.ServiceRegistrar) {
		pb.RegisterGreeterServer(r, &streamService{})
	}, names, opts)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// rpc 构造标准 gRPC 流服务客户端，传输由传入连接决定
	rpc := pb.NewGreeterClient(h.Conn)
	// 逐项处理 []int{0, 5}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, n := range []int{0, 5} {
		// upload, e 建立 client stream；发送 END 只半关闭输入，响应仍需完整接收
		upload, e := rpc.UploadHellos(ctx)
		if e != nil {
			return e
		}

		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
		for i := 0; i < n; i++ {
			if e = upload.Send(&pb.Request{Count: int32(i)}); e != nil {
				return e
			}
		}
		// out, e 关闭发送方向并等待唯一最终响应，不能把 END 的确认当作成功结果
		out, e := upload.CloseAndRecv()
		if e != nil {
			return e
		}
		if out.Count != int32(n) || len(upload.Trailer().Get("count")) != 1 || upload.Trailer().Get("count")[0] != strconv.Itoa(n) {
			return fmt.Errorf("client stream output/trailer mismatch")
		}

		report.Add(h, fmt.Sprintf("client stream %d messages, CloseAndRecv, trailers", n), out)
	}
	// 逐项处理 []int32{0, 5}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, n := range []int32{0, 5} {
		// watch, e 建立 server stream，输出通过订阅传递并由累计 ACK 释放窗口
		watch, e := rpc.WatchHellos(ctx, &pb.Request{Count: n})
		if e != nil {
			return e
		}

		// header, e 读取响应头快照，必须保留同名键的多个值
		header, e := watch.Header()
		if e != nil || len(header.Get("mode")) != 1 || header.Get("mode")[0] != "server" {
			return fmt.Errorf("server stream header: %v", e)
		}

		// last 最近一次控制消息，供测试制造重复帧
		var last *pb.Reply
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
		for i := int32(0); i < n; i++ {
			// out, e 接收下一项业务数据；EOF 只在对应方向正常结束后成立
			out, e := watch.Recv()
			if e != nil {
				return e
			}
			if out.Count != i {
				return fmt.Errorf("server stream order")
			}

			last = out
			time.Sleep(20 * time.Millisecond)
		}
		if _, e = watch.Recv(); e != io.EOF {
			return fmt.Errorf("server stream terminal: %v", e)
		}
		if len(watch.Trailer().Get("count")) != 1 {
			return fmt.Errorf("server stream trailer missing")
		}

		report.Add(h, fmt.Sprintf("server stream %d messages, window 2, slow consumer", n), last)
	}
	// 逐项处理 []int32{0, 8}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, n := range []int32{0, 8} {
		// chat, e 建立 bidi stream，两个方向分别维护序号与背压
		chat, e := rpc.ChatHellos(ctx)
		if e != nil {
			return e
		}

		// Worker 模式先冻结整批输入，再开始消费输出；网络入口仍允许同时收发
		for i := int32(0); i < n; i++ {
			if e := chat.Send(&pb.Request{Count: i, Message: "buffered"}); e != nil {
				return e
			}
		}
		if e = chat.CloseSend(); e != nil {
			return e
		}
		// last 最近一次控制消息，供测试制造重复帧
		var last *pb.Reply
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
		for i := int32(0); i < n; i++ {
			// out, e 接收下一项业务数据；EOF 只在对应方向正常结束后成立
			out, e := chat.Recv()
			if e != nil {
				return e
			}
			if out.Count != i {
				return fmt.Errorf("bidi order")
			}

			last = out
		}
		if _, e = chat.Recv(); e != io.EOF {
			return fmt.Errorf("bidi terminal: %v", e)
		}

		report.Add(h, "bidi batch input and streaming output with one business slot", last)
	}
	// watch, err 接收 rpc.WatchHellos 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	watch, err := rpc.WatchHellos(ctx, &pb.Request{Count: 1, Fail: true})
	if err != nil {
		return fmt.Errorf("business-error watch handshake: %w", err)
	}
	if _, err = watch.Recv(); err != nil {
		return fmt.Errorf("business-error watch output: %w", err)
	}
	if _, err = watch.Recv(); status.Code(err) != codes.FailedPrecondition {
		return fmt.Errorf("stream status lost: %v", err)
	}

	err = nil
	// upload, err 接收 rpc.UploadHellos 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	upload, err := rpc.UploadHellos(ctx)
	if err != nil {
		return fmt.Errorf("early-return upload handshake: %w", err)
	}
	if err = upload.Send(&pb.Request{Message: "early"}); err != nil {
		return err
	}

	// early, err 接收 upload.CloseAndRecv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	early, err := upload.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("early-return upload response: %w", err)
	}
	if early.Count != 1 {
		return fmt.Errorf("early handler result")
	}

	report.Add(h, "early client-stream handler return", early)
	// cancelled, stop 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	cancelled, stop := context.WithCancel(ctx)
	// chat, err 接收 rpc.ChatHellos 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	chat, err := rpc.ChatHellos(cancelled)
	// 当前步骤失败时终止处理：cancellation stream handshake: %w；不把无效结果交给下一步
	if err != nil {
		stop()
		return fmt.Errorf("cancellation stream handshake: %w", err)
	}

	stop()
	if _, err = chat.Recv(); status.Code(err) != codes.Canceled {
		return fmt.Errorf("stream cancellation: %v", err)
	}

	err = nil
	report.Add(h, "stream outputs precede business status; cancellation terminates receive", nil)
	return nil
}
