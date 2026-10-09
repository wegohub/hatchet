//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// selectiveService 用执行身份区分同一 handler 的网络和调度调用，流方向复用有序 fixture。
type selectiveService struct {
	// faultService 提供 client / server / bidi 流及生成接口的默认实现。
	faultService
}

// SayHello 供禁用调度后的网络调用验证，不要求任务上下文。
func (s *selectiveService) SayHello(ctx context.Context, request *pb.Request) (*pb.Reply, error) {
	return scenarios.Reply(ctx, request), nil
}

// WaitHello 供保留的调度方法验证，响应包含真实 RunID 和 WorkerID。
func (s *selectiveService) WaitHello(ctx context.Context, request *pb.Request) (*pb.Reply, error) {
	return scenarios.Reply(ctx, request), nil
}

// TestDisabledWorkerMethods 在真实引擎确认只注册两个 workflow，禁用项仍能从网络执行。
func TestDisabledWorkerMethods(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	namespace := fmt.Sprintf("selective_%d", time.Now().UnixNano())
	disabled := []string{
		pb.Greeter_SayHello_FullMethodName,
		pb.Greeter_ChildHello_FullMethodName,
		pb.Greeter_UploadHellos_FullMethodName,
		pb.Greeter_ChatHellos_FullMethodName,
	}
	// 禁用项即使配置不合法的执行策略，也不能被绑定成 workflow 或阻止其他方法启动。
	options := []worker.Option{
		worker.WithTask(pb.Greeter_SayHello_FullMethodName, task.WithExecutionTimeout(-time.Second)),
		worker.WithDurableTask(pb.Greeter_UploadHellos_FullMethodName),
		worker.WithDisableMethod(disabled...),
	}
	srv := server.New(server.WithRuntime(scenarios.Runtime(namespace)...), server.WithGRPC(address), server.WithWorker(options...))
	pb.RegisterGreeterServer(srv, &selectiveService{})
	serve := make(chan error, 1)
	go func() { serve <- srv.Serve() }()
	defer srv.Stop()
	conn, err := client.New(client.WithRuntime(scenarios.Runtime(namespace)...))
	if err != nil {
		t.Fatal(err)
	}
	report := &scenarios.Report{}
	harness := &scenarios.Harness{
		Scenario: "disabled-methods", Namespace: namespace, Server: srv, Conn: conn, Report: report,
		Names: []string{pb.Greeter_WaitHello_FullMethodName, pb.Greeter_WatchHellos_FullMethodName},
	}
	closed := false
	defer func() {
		if !closed {
			if err := harness.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if err := support.WaitWorkers(ctx, conn, namespace, 1, serve); err != nil {
		t.Fatal(err)
	}
	if len(srv.GetServiceInfo()[pb.Greeter_ServiceDesc.ServiceName].Methods) != 6 {
		t.Fatal("network service info lost disabled methods")
	}

	// 查询引擎持久定义，不能仅凭本地绑定表推断禁用成功。
	resource, err := conn.Workflows().List(ctx, model.Query{"name": namespace, "limit": 100})
	if err != nil {
		t.Fatal(err)
	}
	rows, ok := resource["rows"].([]any)
	if !ok || len(rows) != len(harness.Names) {
		t.Fatalf("workflow count=%d, expected 2: %v", len(rows), resource)
	}
	expected := make(map[string]bool)
	for _, method := range harness.Names {
		expected[binding.WorkflowName(namespace, method)] = false
	}
	for _, value := range rows {
		row, ok := value.(map[string]any)
		if !ok {
			t.Fatal("invalid workflow row", value)
		}
		name, _ := row["name"].(string)
		if seen, exists := expected[name]; !exists || seen {
			t.Fatal("unexpected or duplicate workflow", name)
		}
		expected[name] = true
	}
	report.Add(harness, "only enabled unary and server-stream workflows registered", nil)

	// 通过同一 handler 验证两个真实任务运行，输出与终态均有 deadline 约束。
	queued := pb.NewGreeterClient(conn)
	reply, err := queued.WaitHello(ctx, &pb.Request{Message: "scheduled"})
	if err != nil || reply.GetMessage() != "scheduled" || reply.GetRunId() == "" || reply.GetWorkerId() == "" {
		t.Fatal("enabled unary execution", reply, err)
	}
	report.Add(harness, "enabled unary produces actual task identity", reply)
	watch, err := queued.WatchHellos(ctx, &pb.Request{Count: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 2; i++ {
		output, err := watch.Recv()
		if err != nil || output.GetCount() != i || output.GetRunId() == "" || output.GetWorkerId() != reply.WorkerId {
			t.Fatal("enabled stream execution", output, err)
		}
		report.Add(harness, "enabled server stream produces ordered task output", output)
	}
	if _, err := watch.Recv(); err != io.EOF {
		t.Fatal("missing verified stream EOF", err)
	}

	// 网络入口不受禁用影响：unary、client stream 和 bidi stream 均直接执行，不产生任务身份。
	network, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer network.Close()
	direct := pb.NewGreeterClient(network)
	plain, err := direct.SayHello(ctx, &pb.Request{Message: "direct"})
	if err != nil || plain.GetMessage() != "direct" || plain.GetRunId() != "" || plain.GetWorkerId() != "" {
		t.Fatal("disabled unary network call", plain, err)
	}
	upload, err := direct.UploadHellos(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 2; i++ {
		if err := upload.Send(&pb.Request{Count: i}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := upload.CloseAndRecv()
	if err != nil || result.GetCount() != 2 || result.GetRunId() != "" || result.GetWorkerId() != "" {
		t.Fatal("disabled client stream network call", result, err)
	}
	chat, err := direct.ChatHellos(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := chat.Send(&pb.Request{Message: "echo"}); err != nil {
		t.Fatal(err)
	}
	output, err := chat.Recv()
	if err != nil || output.GetMessage() != "echo" || output.GetRunId() != "" || output.GetWorkerId() != "" {
		t.Fatal("disabled bidi network call", output, err)
	}
	if err := chat.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := chat.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	report.Add(harness, "disabled methods still execute directly over network gRPC", nil)

	// 先关闭并删除本次注册的两个定义，再保存通过断言的运行证据；历史记录由引擎保留。
	info, err := conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if err := <-serve; err != nil {
		t.Fatal(err)
	}
	evidence := map[string]any{"sdk_version": wego.Version, "protocol_version": wire.Version, "engine": info, "disabled_methods": disabled, "records": report.Records}
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join("..", "..", ".test-results")
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, namespace+"-methods.json")
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("method selection evidence:", path)
}
