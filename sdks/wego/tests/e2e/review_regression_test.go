//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// routingService 验证同一 namespace 下只有部分流方法的实例能接到对应 START
type routingService struct {
	// UnimplementedGreeterServer 提供无关方法的默认实现
	pb.UnimplementedGreeterServer
}

// UploadHellos 以收到的消息数返回结果，零消息也必须正确完成
func (s *routingService) UploadHellos(stream grpc.ClientStreamingServer[pb.Request, pb.Reply]) error {
	// count 只记录本会话输入，两个实例不共享业务状态
	var count int32
	// 每轮接收一条消息；例如两次 DATA 后 END 得到 Count=2
	for {
		// err 的 EOF 只半关闭输入，最终响应仍须通过任务结果返回
		_, err := stream.Recv()
		if err == io.EOF {
			// info 取自实际会话任务，记录 RunID 与 owner，便于关联调度和清理证据
			info, _ := task.Info(stream.Context())
			return stream.SendAndClose(&pb.Reply{Count: count, RunId: info.RunID, WorkerId: info.WorkerID})
		}
		if err != nil {
			return err
		}
		count++
	}
}

// WatchHellos 返回一个输出，验证服务端流被路由到支持 Watch 的实例
func (s *routingService) WatchHellos(in *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	// info 同样来自实际会话任务，不能由测试预先指定 owner 身份
	info, _ := task.Info(stream.Context())
	return stream.Send(&pb.Reply{Message: in.Message, RunId: info.RunID, WorkerId: info.WorkerID})
}

// TestReviewHeterogeneousStreamOwners 将 Upload 与 Watch 分配到两个不同注册集合的 Server
func TestReviewHeterogeneousStreamOwners(t *testing.T) {
	preflight(t)
	// ctx 约束注册、流交互和清理总时间
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	// namespace 是本轮隔离前缀，滚动发布的两个服务实例使用同一个前缀
	namespace := fmt.Sprintf("wego_routing_%d_", time.Now().UnixNano())
	// conn 只负责调用与资源删除，不拥有两个 Server 的执行连接
	conn, err := client.New(client.WithRuntime(scenarios.Runtime(namespace)...))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// servers 保存全部入口，以便断言失败时仍释放端口、Worker 与流会话
	servers := []*server.Server{}
	defer func() {
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
		for _, s := range servers {
			s.Stop()
		}
	}()
	// names 包含所有真实注册资源，每个 RPC 只有一份持久定义
	names := []string{}
	defer func() {
		// cleanup 不沿用可能已到期的业务预算
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		// name 是唯一注册名，公共控制入口只删除一次
		for _, name := range names {
			// _, e 操作实际工作流定义，删除成功会使共享名称缓存失效
			if _, e := conn.Workflows().Delete(cleanup, name); e != nil {
				t.Error(e)
			}
		}
	}()
	// descriptor 只注册一项流；服务名称相同也不能假定方法能力一致
	for _, descriptor := range pb.Greeter_ServiceDesc.Streams[:2] {
		// desc 复制描述，避免修改全局生成代码的注册表
		desc := pb.Greeter_ServiceDesc
		desc.Methods = nil
		desc.Streams = []grpc.StreamDesc{descriptor}
		// srv 不同实例只有一个流方法，不能消费另一个方法的任务
		srv := server.New(server.WithRuntime(scenarios.Runtime(namespace)...))
		srv.RegisterService(&desc, &routingService{})
		servers = append(servers, srv)
		// exited 捕获启动错误，就绪检查不得只靠等待固定时间
		exited := make(chan error, 1)
		go func() { exited <- srv.Serve() }()
		// method 用完整名称确定唯一注册名
		method := "/wego.example.v1.Greeter/" + descriptor.StreamName
		names = append(names, method)
		if err = support.WaitWorkers(ctx, conn, namespace, len(servers), exited); err != nil {
			t.Fatal(err)
		}
	}
	// rpc 使用同一 Conn 连续交替调用两个不重叠的服务方法
	rpc := pb.NewGreeterClient(conn)
	// i 的多次交替防止偶然选中正确实例掩盖无能力选路
	for i := 0; i < 8; i++ {
		// upload 向 Upload owner 发送一条业务消息并半关闭输入
		upload, e := rpc.UploadHellos(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if e = upload.Send(&pb.Request{Message: "one"}); e != nil {
			t.Fatal(e)
		}
		// reply 必须证明消息被处理，而非仅 START 注册成功
		reply, e := upload.CloseAndRecv()
		if e != nil || reply.Count != 1 || reply.RunId == "" || reply.WorkerId == "" {
			t.Fatalf("upload result: %v / %v", reply, e)
		}
		t.Logf("namespace=%s method=Upload iteration=%d run_id=%s worker_id=%s", namespace, i, reply.RunId, reply.WorkerId)
		// watch 的首条输出和最终 EOF 必须都来自支持 Watch 的 Worker
		watch, e := rpc.WatchHellos(ctx, &pb.Request{Message: "watch"})
		if e != nil {
			t.Fatal(e)
		}
		reply, e = watch.Recv()
		if e != nil || reply.Message != "watch" || reply.RunId == "" || reply.WorkerId == "" {
			t.Fatalf("watch result: %v / %v", reply, e)
		}
		t.Logf("namespace=%s method=Watch iteration=%d run_id=%s worker_id=%s", namespace, i, reply.RunId, reply.WorkerId)
		if _, e = watch.Recv(); e != io.EOF {
			t.Fatalf("watch terminal: %v", e)
		}
	}
}

// TestReviewSameNameWorkerInstances 验证同名 Worker 的身份、独立关闭与剩余消费能力
func TestReviewSameNameWorkerInstances(t *testing.T) {
	preflight(t)
	// ctx 包含两次注册和关闭后的实际执行预算
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	// namespace 隔离本轮任务和引擎运行记录
	namespace := fmt.Sprintf("wego_identity_%d_", time.Now().UnixNano())
	// conn 的两个 Worker 共享后端与名称，但必须使用独立注册身份
	conn, err := client.New(client.WithRuntime(scenarios.Runtime(namespace)...))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// definition 返回引擎实际分配的 WorkerID，供 Required affinity 检验身份
	definition := conn.NewStandaloneTask("identity", func(ctx context.Context, input map[string]any) (any, error) { // info 必须来自实际任务上下文，网络 handler 没有任务身份
		// info, ok 读取实际执行身份，例如 RunID 与 WorkerID；普通网络 context 没有任务身份
		info, ok := task.Info(ctx)
		if !ok {
			return nil, fmt.Errorf("missing task execution identity")
		}
		return info, nil
	})
	// workers 保存两个同名实例，任何错误都不允许残留监听器
	workers := []*client.Worker{}
	defer func() {
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
		for _, w := range workers {
			_ = w.Shutdown(ctx)
		}
		_, _ = conn.Workflows().Delete(context.Background(), "identity")
	}()
	// i 只标识实例创建次序，两个显示名称均为 worker
	for i := 0; i < 2; i++ {
		// worker 与另一个实例名称相同，但标签唯一
		worker, e := conn.NewWorker("worker", client.WithWorkflows(definition))
		if e != nil {
			t.Fatal(e)
		}
		workers = append(workers, worker)
		if _, e = worker.Start(); e != nil {
			t.Fatal(e)
		}
		if e = worker.WaitReady(ctx); e != nil {
			t.Fatal(e)
		}
	}
	if workers[0].ID() == "" || workers[0].ID() == workers[1].ID() {
		t.Fatal("same-name workers did not retain independent IDs")
	}
	t.Logf("namespace=%s first_worker_id=%s second_worker_id=%s", namespace, workers[0].ID(), workers[1].ID())
	if err = workers[0].Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	// result 证明关闭第一个实例没有暂停或注销第二个实例
	result, err := conn.Run(ctx, "identity", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	// info 保留实际分配记录，不能从本地 Worker 对象伪造成功
	var info model.TaskInfo
	if err = result.Into(&info); err != nil {
		t.Fatal(err)
	}
	t.Logf("namespace=%s run_id=%s worker_id=%s", namespace, result.RunID, info.WorkerID)
	if info.WorkerID != workers[1].ID() {
		t.Fatalf("remaining owner: %s", info.WorkerID)
	}
}
