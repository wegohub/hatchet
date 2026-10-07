package scenarios

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
)

// DualEntry 验证同一 handler 的两种执行路径，以及 Conn 不受 Worker 开关限制。
func DualEntry(ctx context.Context, report *Report) (err error) {
	// listener, err 接收 net.Listen 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	// address 保存实际绑定地址，测试使用空闲端口避免固定端口冲突。
	address := listener.Addr().String()
	if err = listener.Close(); err != nil {
		return err
	}

	// namespace 生成当前数据的文本表示，用于名称、业务比较或报告，不包含凭证。
	namespace := fmt.Sprintf("wego_accept_dual_%d_", time.Now().UnixNano())
	// opts 当前入口使用的原生 gRPC 选项。
	opts := Runtime(namespace)
	// srv 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	srv := server.New(server.WithRuntime(opts...), server.WithGRPC(address))
	pb.RegisterUnaryGreeterServer(srv, &Service{})
	// serve 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	serve := make(chan error, 1)
	go func() { serve <- srv.Serve() }()
	defer srv.Stop()

	// 开关只禁止本实例创建 Worker，不限制远端任务执行。
	connOpts := append(append([]runtime.Option(nil), opts...), runtime.WithDisableWorker())
	// conn, err 接收 client.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	conn, err := client.New(client.WithRuntime(connOpts...))
	if err != nil {
		return err
	}
	// h 当前场景的生命周期夹具，集中保存 Server、Conn、隔离前缀和真实验收报告。
	h := &Harness{Scenario: "dual-entry", Namespace: namespace, Server: srv, Conn: conn, Report: report}
	defer func() { err = errors.Join(err, h.Close()) }()
	if err = support.WaitWorkers(ctx, conn, namespace, 1, serve); err != nil {
		return err
	}
	// e 必须拒绝 DisableWorker 与原生 Worker 创建的配置冲突。
	if _, e := conn.NewWorker("disabled"); e == nil {
		return fmt.Errorf("disabled Conn created Worker")
	}

	// network, err 接收 grpc.NewClient 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	network, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer network.Close()
	// direct, err 接收 pb.NewUnaryGreeterClient 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	direct, err := pb.NewUnaryGreeterClient(network).SayHello(ctx, &pb.Request{Message: "direct"})
	if err != nil {
		return err
	}
	if direct.Message != "direct" || direct.RunId != "" || direct.WorkerId != "" {
		return fmt.Errorf("network path created task context: %v", direct)
	}
	report.Add(h, "network gRPC directly executes shared handler without task identity", direct)

	// queued, err 接收 pb.NewUnaryGreeterClient 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	queued, err := pb.NewUnaryGreeterClient(conn).SayHello(ctx, &pb.Request{Message: "queued"})
	if err != nil {
		return err
	}
	if queued.Message != "queued" || queued.RunId == "" || queued.WorkerId == "" {
		return fmt.Errorf("Worker path did not produce execution identity: %v", queued)
	}
	report.Add(h, "Worker executes shared handler with RunID; disabled Conn still invokes tasks", queued)
	return nil
}
