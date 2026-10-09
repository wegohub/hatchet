package server

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// 编译期接口或签名检查，确保适配对象可以被标准 gRPC 或 wego 入口使用
var _ func(...Option) *Server = New

// greeter 示例或测试业务服务，用标准 gRPC 注册方式共享 handler
type greeter struct {
	// pb.UnimplementedGreeterServer 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	pb.UnimplementedGreeterServer
	// say 测试注入的 unary handler，控制返回值或执行时机
	say func(context.Context, *pb.Request) (*pb.Reply, error)
}

// SayHello 处理 unary 文本请求，返回可用于断言的响应
func (g *greeter) SayHello(ctx context.Context, req *pb.Request) (*pb.Reply, error) {
	if g.say != nil {
		return g.say(ctx, req)
	}
	// 检查 ok；不满足协议或配置约束时返回 Internal（unexpected task context）
	if _, ok := task.Info(ctx); ok {
		return nil, status.Error(codes.Internal, "unexpected task context")
	}
	_ = grpc.SetHeader(ctx, metadata.Pairs("reply", "header"))
	grpc.SetTrailer(ctx, metadata.Pairs("reply", "trailer"))
	if req.Fail {
		// s, _ 构造或读取标准 gRPC 状态，保留 code 与业务 details
		s, _ := status.New(codes.InvalidArgument, "invalid request").WithDetails(&errdetails.ErrorInfo{Reason: "TEST"})
		return nil, s.Err()
	}
	return &pb.Reply{Message: req.Message}, nil
}

// UploadHellos 接收多条 Request，收到输入 EOF 后返回最终 Reply，例如两条输入得到 Count=2
func (g *greeter) UploadHellos(stream grpc.ClientStreamingServer[pb.Request, pb.Reply]) error {
	// count 此步骤已经观察到的执行或消息次数，后续与预期重试数、输入数或峰值比较
	count := int32(0)
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		// _, err 接收 stream.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		_, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.Reply{Count: count})
		}
		if err != nil {
			return err
		}
		count++
	}
}

// WatchHellos 处理单条 Request 并连续发送 Reply，例如 Count=3 时发送三条输出
func (g *greeter) WatchHellos(req *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
	for i := int32(0); i < req.Count; i++ {
		// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
		if err := stream.Send(&pb.Reply{Count: i}); err != nil {
			return err
		}
	}
	return nil
}

// ChatHellos 边接收 Request 边发送 Reply，输入 EOF 只结束输入方向
func (g *greeter) ChatHellos(stream grpc.BidiStreamingServer[pb.Request, pb.Reply]) error {
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
		// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
		if err := stream.Send(&pb.Reply{Message: req.Message}); err != nil {
			return err
		}
	}
}

// startNetwork 以禁用 Worker 的配置启动回环网络服务并建立原生 gRPC 连接，注册测试清理回调
func startNetwork(t *testing.T, handler *greeter, options ...grpc.ServerOption) (*Server, pb.GreeterClient, <-chan error) {
	t.Helper()
	// srv 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理
	srv := New(WithGRPC("127.0.0.1:0", options...), WithRuntime(runtime.WithDisableWorker(), runtime.WithToken("invalid")),
		// 禁用 Worker 时不应用任务策略或容量校验
		WithWorker(
			worker.WithTask("/unknown/method"),
			worker.WithDisableMethod("/unknown/method"),
			worker.WithDisableMethod(pb.Greeter_SayHello_FullMethodName),
			worker.WithDisableMethod(pb.Greeter_UploadHellos_FullMethodName),
			worker.WithDisableMethod(pb.Greeter_WatchHellos_FullMethodName),
			worker.WithDisableMethod(pb.Greeter_ChatHellos_FullMethodName),
		))
	pb.RegisterGreeterServer(srv, handler)
	// done 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	done := make(chan error, 1)
	go func() { done <- srv.Serve() }()
	// 根据已经就绪的通知选择下一步，不通过轮询固定延迟猜测执行时机
	select {
	case <-srv.initialized:
	case <-time.After(3 * time.Second):
		t.Fatal("initialization timeout")
	}
	if srv.network == nil {
		t.Fatalf("network startup: %v", <-done)
	}
	// conn, err 接收 grpc.NewClient 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	conn, err := grpc.NewClient(srv.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop(); conn.Close() })
	return srv, pb.NewGreeterClient(conn), done
}

// TestNetworkUnaryAndStreams 在无任务后端配置的纯网络模式验证 unary 和 client/server/bidi 三种流
func TestNetworkUnaryAndStreams(t *testing.T) {
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁
	var mu sync.Mutex
	// calls 本地调用编号到取消函数的映射，排空等待此表清空
	calls := 0
	// interceptor 检查客户端 metadata 是否到达 handler，并将请求继续交给原生业务入口
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		// values 接收 metadata.ValueFromIncomingContext 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
		if values := metadata.ValueFromIncomingContext(ctx, "request"); len(values) != 1 || values[0] != "metadata" {
			t.Error("metadata missing")
		}
		return handler(ctx, req)
	}
	// rpc 来自真实纯网络 gRPC 服务，原生 interceptor 必须只影响网络入口
	_, rpc, _ := startNetwork(t, &greeter{}, grpc.ChainUnaryInterceptor(interceptor))
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "request", "metadata")
	// headers, trailers 响应头缓存；调用方读取时返回副本 响应尾部 metadata，随最终状态交付
	var headers, trailers metadata.MD
	// out, err 接收 rpc.SayHello 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	out, err := rpc.SayHello(ctx, &pb.Request{Message: "hello"}, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil || out.Message != "hello" || headers.Get("reply")[0] != "header" || trailers.Get("reply")[0] != "trailer" {
		t.Fatalf("unary: %v %v %v %v", out, err, headers, trailers)
	}
	_, err = rpc.SayHello(ctx, &pb.Request{Fail: true})
	if status.Code(err) != codes.InvalidArgument || len(status.Convert(err).Details()) != 1 {
		t.Fatalf("status details: %v", err)
	}
	mu.Lock()
	if calls != 2 {
		t.Error("interceptor count", calls)
	}
	mu.Unlock()
	// 逐项处理 []int32{0, 3}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, count := range []int32{0, 3} {
		// upload, err 接收 rpc.UploadHellos 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		upload, err := rpc.UploadHellos(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
		for i := int32(0); i < count; i++ {
			// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
			if err := upload.Send(&pb.Request{}); err != nil {
				t.Fatal(err)
			}
		}
		// out, err 接收 upload.CloseAndRecv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		out, err := upload.CloseAndRecv()
		if err != nil || out.Count != count {
			t.Fatalf("client stream: %v %v", out, err)
		}
		// watch, err 接收 rpc.WatchHellos 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		watch, err := rpc.WatchHellos(ctx, &pb.Request{Count: count})
		if err != nil {
			t.Fatal(err)
		}
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
		for i := int32(0); i < count; i++ {
			// out, err 接收 watch.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
			out, err := watch.Recv()
			if err != nil || out.Count != i {
				t.Fatalf("server stream: %v %v", out, err)
			}
		}
		// _, err 保存一次接收结果；按断言区分正常 EOF、业务状态错误及协议 DataLoss
		if _, err := watch.Recv(); err != io.EOF {
			t.Fatal(err)
		}
		// chat, err 接收 rpc.ChatHellos 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		chat, err := rpc.ChatHellos(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
		for i := int32(0); i < count; i++ {
			// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
			if err := chat.Send(&pb.Request{Message: "echo"}); err != nil {
				t.Fatal(err)
			}
			// out, err 接收 chat.Recv 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
			out, err := chat.Recv()
			if err != nil || out.Message != "echo" {
				t.Fatalf("bidi: %v %v", out, err)
			}
		}
		// err 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功
		if err := chat.CloseSend(); err != nil {
			t.Fatal(err)
		}
		// _, err 保存一次接收结果；按断言区分正常 EOF、业务状态错误及协议 DataLoss
		if _, err := chat.Recv(); err != io.EOF {
			t.Fatal(err)
		}
	}
}

// TestGracefulDrainAndForceStop 先触发 GracefulStop 保留正在执行的业务，再 Stop 中断排空并取消 handler
func TestGracefulDrainAndForceStop(t *testing.T) {
	// 逐项处理 []bool{false, true}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, force := range []bool{false, true} {
		// name 注册或查询时使用的名称；必须与提交任务的名称对应
		name := "complete"
		if force {
			name = "force"
		}
		t.Run(name, func(t *testing.T) {
			// started, release 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
			started, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			// srv, rpc, serve 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值
			srv, rpc, serve := startNetwork(t, &greeter{say: func(ctx context.Context, req *pb.Request) (*pb.Reply, error) {
				close(started)
				// 故意忽略取消，验证 Stop 不等待不合作的业务
				<-release
				return &pb.Reply{Message: "done"}, nil
			}})
			// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			// result 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
			result := make(chan error, 1)
			// err 接收后台 RPC 的实际结果并交给 result 通道；测试据此断言取消或强制停止，不只等待 goroutine 启动
			go func() { _, err := rpc.SayHello(ctx, &pb.Request{}); result <- err }()
			<-started
			// stopped 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
			stopped := make(chan struct{})
			go func() { srv.GracefulStop(); close(stopped) }()
			// 根据已经就绪的通知选择下一步，不通过轮询固定延迟猜测执行时机
			select {
			case <-stopped:
				t.Fatal("did not drain")
			case <-time.After(30 * time.Millisecond):
			}
			if force {
				go srv.Stop()
			} else {
				release <- struct{}{}
			}
			// 根据已经就绪的通知选择下一步，不通过轮询固定延迟猜测执行时机
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("stop blocked")
			}
			// err 保存当前步骤返回或收到的值，紧接着按错误、类型或内容校验再继续
			if err := <-serve; err != nil {
				t.Fatal(err)
			}
			// err 保存当前步骤返回或收到的值，紧接着按错误、类型或内容校验再继续
			if err := <-result; !force && err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestRegistrationAndConfigurationErrors 构造重复服务、错误 handler 及关闭全部入口等配置，断言 Serve 返回明确错误
func TestRegistrationAndConfigurationErrors(t *testing.T) {
	// 逐项构造独立错误配置：全入口关闭、空地址、nil 描述及不匹配的 handler；每项都必须让 Serve 明确失败
	for _, configure := range []func() *Server{
		func() *Server { return New(WithRuntime(runtime.WithDisableWorker())) },
		func() *Server { return New(WithGRPC(""), WithRuntime(runtime.WithDisableWorker())) },
		// s 是本配置错误用例的独立 Server，注册该行指定的非法描述后用 Serve 校验错误
		func() *Server { s := New(); s.RegisterService(nil, nil); return s },
		func() *Server {
			// s 当前场景服务或指定 StreamID 的端点，后续状态更新只作用于这一对象
			s := New()
			pb.RegisterGreeterServer(s, &greeter{})
			pb.RegisterGreeterServer(s, &greeter{})
			return s
		},
		// s 是本配置错误用例的独立 Server，注册该行指定的非法描述后用 Serve 校验错误
		func() *Server { s := New(); s.RegisterService(&pb.Greeter_ServiceDesc, struct{}{}); return s },
	} {
		// s 当前场景服务或指定 StreamID 的端点，后续状态更新只作用于这一对象
		s := configure()
		// err 保存入口启动或执行结果，明确区分预期启动失败与正常退出
		if err := s.Serve(); err == nil {
			t.Fatal("invalid server started")
		}
		s.Stop()
	}
	// s 当前场景服务或指定 StreamID 的端点，后续状态更新只作用于这一对象
	s := New()
	// wg 等待本组并发步骤结束，资源关闭前必须确认所有已启动步骤退出
	var wg sync.WaitGroup
	// 逐项处理 8，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for range 8 {
		wg.Go(s.Stop)
	}
	wg.Wait()
	// err 保存入口启动或执行结果，明确区分预期启动失败与正常退出
	if err := s.Serve(); !errors.Is(err, grpc.ErrServerStopped) {
		t.Fatal(err)
	}
}

// TestServiceInfoAndMessageLimits 修改 GetServiceInfo 返回值后再读取，确认注册表未被改写，并验证原生消息上限
func TestServiceInfoAndMessageLimits(t *testing.T) {
	// srv 和 rpc 组成纯网络测试实例，用实际调用校验消息限制或拦截器，并自动清理端口
	srv, rpc, _ := startNetwork(t, &greeter{}, grpc.MaxRecvMsgSize(32))
	// info 复制服务信息快照，调用方修改 map 不影响注册表
	info := srv.GetServiceInfo()
	// name 注册或查询时使用的名称；必须与提交任务的名称对应
	name := pb.Greeter_ServiceDesc.ServiceName
	info[name].Methods[0].Name = "changed"
	if srv.GetServiceInfo()[name].Methods[0].Name == "changed" {
		t.Fatal("shared method list")
	}
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// _, err 保存调用错误并核对预期 gRPC code；例如输出缺口必须是 DataLoss，不能是成功 EOF
	if _, err := rpc.SayHello(ctx, &pb.Request{Message: strings.Repeat("x", 100)}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
}

// TestOccupiedPort 提前占用监听端口，断言 Serve 返回监听错误并完成清理
func TestOccupiedPort(t *testing.T) {
	// listener, err 接收 net.Listen 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	// s 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理
	s := New(WithGRPC(listener.Addr().String()), WithRuntime(runtime.WithDisableWorker()))
	// err 保存入口启动或执行结果，明确区分预期启动失败与正常退出
	if err := s.Serve(); err == nil {
		t.Fatal("occupied port accepted")
	}
}

// TestRegistrationAfterStartFailsServe 启动后尝试注册服务，断言生命周期约束通过 Serve 错误报告
func TestRegistrationAfterStartFailsServe(t *testing.T) {
	// srv, _, done 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值
	srv, _, done := startNetwork(t, &greeter{})
	pb.RegisterGreeterServer(srv, &greeter{})
	// 根据已经就绪的通知选择下一步，不通过轮询固定延迟猜测执行时机
	select {
	// err 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "registration after") {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("late registration error was not delivered to Serve")
	}
}
