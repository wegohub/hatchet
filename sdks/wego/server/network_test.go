package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// rpcStats 测试用原生 stats handler，统计实际 RPC 结束事件。
type rpcStats struct{ ended atomic.Int32 }

// TagRPC 登记传输层 RPC 的整个生命周期，包含原生 interceptor 外层执行。
func (s *rpcStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }

// HandleRPC 收到 stats.End 后释放传输层调用记录，通知排空等待者。
func (s *rpcStats) HandleRPC(_ context.Context, event stats.RPCStats) {
	// _, ok 接收 event. 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
	if _, ok := event.(*stats.End); ok {
		s.ended.Add(1)
	}
}

// TagConn 保留连接上下文；连接级事件不代表某次业务 RPC 已完成。
func (*rpcStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }

// HandleConn 处理连接级观测事件；业务完成由 RPC 结束事件判断。
func (*rpcStats) HandleConn(context.Context, stats.ConnStats) {}

// TestTLSAndNativeStats 使用受控证书建立 TLS 网络调用，验证原生 stats 收到结束事件。
func TestTLSAndNativeStats(t *testing.T) {
	// public, private, err 接收 ed25519.GenerateKey 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	// cert 是受控本机 TLS 证书，测试只为回环地址建立信任，避免依赖外部 CA。
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Minute),
		NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	// der, err 接收 x509.CreateCertificate 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, public, private)
	if err != nil {
		t.Fatal(err)
	}
	// certificate, err 接收 x509.ParseCertificate 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	// roots 建立测试 TLS 信任链，只接受受控测试证书。
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	// counts 业务执行次数，用于断言重试、重放及幂等行为。
	counts := &rpcStats{}
	// srv 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	srv := New(WithGRPC("127.0.0.1:0",
		grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12,
			Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}})),
		grpc.StatsHandler(counts),
	), WithRuntime(runtime.WithDisableWorker(), runtime.WithSlots(0), runtime.WithEmbedded(runtime.EmbeddedConfig{})))
	pb.RegisterGreeterServer(srv, &greeter{})
	// done 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	done := make(chan error, 1)
	go func() { done <- srv.Serve() }()
	<-srv.initialized
	if srv.network == nil {
		t.Fatalf("startup: %v", <-done)
	}
	defer srv.Stop()
	// conn, err 接收 grpc.NewClient 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	conn, err := grpc.NewClient(srv.listener.Addr().String(), grpc.WithTransportCredentials(
		credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "localhost"})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// out, err 接收 pb.NewGreeterClient 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	out, err := pb.NewGreeterClient(conn).SayHello(ctx, &pb.Request{Message: "TLS"})
	if err != nil || out.Message != "TLS" {
		t.Fatalf("TLS RPC: %v %v", out, err)
	}
	for counts.ended.Load() == 0 {
		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
		select {
		case <-ctx.Done():
			t.Fatal("stats callback missing")
		case <-time.After(time.Millisecond):
		}
	}
	if srv.engine != nil {
		t.Fatal("disabled Worker initialized")
	}
}

// TestManualServiceDescriptionWithoutProtoDescriptor 纯网络注册手写 ServiceDesc，验证不要求 Worker 的 protobuf 绑定能力。
func TestManualServiceDescriptionWithoutProtoDescriptor(t *testing.T) {
	// srv 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	srv := New(WithGRPC("127.0.0.1:0"), WithRuntime(runtime.WithDisableWorker()))
	// desc 服务描述的本地副本，测试修改时不能污染生成代码中的共享 ServiceDesc。
	desc := pb.Greeter_ServiceDesc
	desc.ServiceName = "manual.Greeter"
	srv.RegisterService(&desc, &greeter{})
	// done 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	done := make(chan error, 1)
	go func() { done <- srv.Serve() }()
	<-srv.initialized
	if srv.network == nil {
		t.Fatal(<-done)
	}
	defer srv.Stop()
	// conn, err 接收 grpc.NewClient 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	conn, err := grpc.NewClient(srv.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
	var out pb.Reply
	// err 接收 conn.Invoke 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
	if err := conn.Invoke(ctx, "/manual.Greeter/SayHello", &pb.Request{Message: "manual"}, &out); err != nil || out.Message != "manual" {
		t.Fatalf("manual service: %v %v", &out, err)
	}
}

// TestDeadlineAndCancellationReachHandler 客户端 deadline 或主动取消后，断言网络 handler 的 context 收到对应错误。
func TestDeadlineAndCancellationReachHandler(t *testing.T) {
	// 逐项处理 []bool{false, true}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			// started, canceled 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
			started, canceled := make(chan struct{}), make(chan struct{})
			// rpc 来自真实纯网络 gRPC 服务，原生 interceptor 必须只影响网络入口。
			_, rpc, _ := startNetwork(t, &greeter{say: func(ctx context.Context, _ *pb.Request) (*pb.Reply, error) {
				close(started)
				<-ctx.Done()
				close(canceled)
				return nil, ctx.Err()
			}})
			// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			// result 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
			result := make(chan error, 1)
			// err 接收后台 RPC 的实际结果并交给 result 通道；测试据此断言取消或强制停止，不只等待 goroutine 启动。
			go func() { _, err := rpc.SayHello(ctx, &pb.Request{}); result <- err }()
			<-started
			// want 本用例预期的状态或数量，后续将真实调用结果与此值比较。
			want := codes.DeadlineExceeded
			if !deadline {
				cancel()
				want = codes.Canceled
			}
			// err 保存调用错误并核对预期 gRPC code；例如输出缺口必须是 DataLoss，不能是成功 EOF。
			if err := <-result; status.Code(err) != want {
				t.Fatal(err)
			}
			// 根据已经就绪的通知选择下一步，不通过轮询固定延迟猜测执行时机。
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("handler did not observe cancellation")
			}
		})
	}
}

// TestForceStopIncludesNativeInterceptor 让原生 interceptor 的外层阻塞，再强制 Stop，验证传输级跟踪覆盖完整执行范围。
func TestForceStopIncludesNativeInterceptor(t *testing.T) {
	// entered, release 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	// interceptor 在原生拦截器外层通知 entered 并等待 release，用来验证强制停止覆盖业务 handler 外的在途执行。
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		close(entered)
		<-release
		return handler(ctx, req)
	}
	// srv 和 rpc 组成纯网络测试实例，用实际调用校验消息限制或拦截器，并自动清理端口。
	srv, rpc, _ := startNetwork(t, &greeter{}, grpc.UnaryInterceptor(interceptor))
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// result 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	result := make(chan error, 1)
	// err 接收后台 RPC 的实际结果并交给 result 通道；测试据此断言取消或强制停止，不只等待 goroutine 启动。
	go func() { _, err := rpc.SayHello(ctx, &pb.Request{}); result <- err }()
	<-entered
	// drained 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	drained := make(chan struct{})
	go func() { srv.GracefulStop(); close(drained) }()
	// 根据已经就绪的通知选择下一步，不通过轮询固定延迟猜测执行时机。
	select {
	case <-drained:
		t.Fatal("native interceptor was not drained")
	case <-time.After(20 * time.Millisecond):
	}
	go srv.Stop()
	// 等待排空状态或强制停止信号；Stop 可以打断 GracefulStop 的持续等待。
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("force stop blocked on interceptor")
	}
	// err 保存当前步骤返回或收到的值，紧接着按错误、类型或内容校验再继续。
	if err := <-result; err == nil {
		t.Fatal("canceled RPC succeeded")
	}
}

// TestFailedWorkerStartupReleasesNetworkPort 先绑定端口再制造 Worker 启动失败，断言网络资源释放且端口可重新监听。
func TestFailedWorkerStartupReleasesNetworkPort(t *testing.T) {
	// srv 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	srv := New(WithGRPC("127.0.0.1:0"), WithRuntime(runtime.WithLogger(nil)))
	pb.RegisterGreeterServer(srv, &greeter{})
	// err 保存入口启动或执行结果，明确区分预期启动失败与正常退出。
	if err := srv.Serve(); err == nil {
		t.Fatal("invalid Worker started")
	}
	if srv.engine != nil {
		t.Fatal("invalid configuration created engine")
	}
	// err 保存资源释放结果，清理错误同样必须报告，不能因业务已成功而忽略。
	if err := srv.listener.Close(); err == nil {
		t.Fatal("listener remained open")
	}
}

// TestServeStopRace 并发启动和停止多次，断言 Serve 正常退出且停止流程没有竞态。
func TestServeStopRace(t *testing.T) {
	// 逐项处理 50，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for range 50 {
		// srv 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
		srv := New(WithGRPC("127.0.0.1:0"), WithRuntime(runtime.WithDisableWorker()))
		pb.RegisterGreeterServer(srv, &greeter{})
		// done 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
		done := make(chan error, 1)
		go func() { done <- srv.Serve() }()
		srv.Stop()
		// 根据已经就绪的通知选择下一步，不通过轮询固定延迟猜测执行时机。
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("startup-stop race blocked")
		}
	}
}
