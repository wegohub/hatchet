//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	wlog "github.com/hatchet-dev/hatchet/sdks/wego/log"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// logDrainGreeter 在停止接收新调用后继续发布流输出和任务日志。
type logDrainGreeter struct {
	// UnimplementedGreeterServer 补齐此 fixture 不使用的方法。
	pb.UnimplementedGreeterServer
	// release 是测试确认排空已开始之后解除业务等待的屏障。
	release <-chan struct{}
}

// SayHello 用于观察网络入口是否已拒绝新增调用，不提交任务。
func (*logDrainGreeter) SayHello(context.Context, *pb.Request) (*pb.Reply, error) {
	return &pb.Reply{Message: "accepted"}, nil
}

// WatchHellos 就绪后等待屏障，显式关闭期间仍须完成第二条输出和日志。
func (g *logDrainGreeter) WatchHellos(_ *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	ctx := wlog.With(stream.Context(), slog.String("phase", "drain"), slog.Int("count", 3))
	info, queued := task.Info(ctx)
	if err := stream.Send(&pb.Reply{Message: "ready", RunId: info.RunID, WorkerId: info.WorkerID}); err != nil {
		return err
	}
	select {
	case <-g.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if queued {
		if err := wlog.RInfo(ctx, "drain completed", "count", 4); err != nil {
			return err
		}
	} else {
		wlog.Info(ctx, "network drain completed", "count", 4)
	}
	return stream.Send(&pb.Reply{Message: "completed", RunId: info.RunID, WorkerId: info.WorkerID})
}

// TestLoggingShutdown 在真实双入口中显式排空，确认日志和流终态先于资源释放完成。
func TestLoggingShutdown(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	namespace := fmt.Sprintf("wego_log_drain_%d_", time.Now().UnixNano())
	release := make(chan struct{})
	// releaseOnce 保证失败清理与正常完成共用同一个屏障，不重复关闭 channel。
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	options := append(scenarios.Runtime(namespace), runtime.WithLogger(slog.New(slog.NewJSONHandler(io.Discard, nil))), runtime.WithLogReport(true))
	srv := server.New(server.WithRuntime(options...), server.WithGRPC(address))
	pb.RegisterGreeterServer(srv, &logDrainGreeter{release: release})
	serve := make(chan error, 1)
	go func() { serve <- srv.Serve() }()
	defer srv.Stop()
	conn, err := client.New(client.WithRuntime(scenarios.Runtime(namespace)...))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := support.WaitWorkers(ctx, conn, namespace, 1, serve); err != nil {
		t.Fatal(err)
	}
	network, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer network.Close()
	queued, err := pb.NewGreeterClient(conn).WatchHellos(ctx, &pb.Request{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := queued.Recv()
	if err != nil || first.Message != "ready" || first.RunId == "" {
		t.Fatal(first, err)
	}
	direct, err := pb.NewGreeterClient(network).WatchHellos(ctx, &pb.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if first, err := direct.Recv(); err != nil || first.Message != "ready" || first.RunId != "" {
		t.Fatal(first, err)
	}
	closed := make(chan struct{})
	go func() { srv.GracefulStop(); close(closed) }()
	// 新网络请求被拒绝是排空开始的可观测屏障，不能用固定 sleep 猜测时序。
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := pb.NewGreeterClient(network).SayHello(ctx, &pb.Request{})
		if status.Code(err) == codes.Unavailable {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	releaseOnce.Do(func() { close(release) })
	for _, stream := range []grpc.ServerStreamingClient[pb.Reply]{queued, direct} {
		if reply, err := stream.Recv(); err != nil || reply.Message != "completed" {
			t.Fatal(reply, err)
		}
		if _, err := stream.Recv(); err != io.EOF {
			t.Fatal("missing verified final EOF", err)
		}
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := <-serve; err != nil {
		t.Fatal(err)
	}
	logs := awaitReportedLog(t, ctx, conn, first.RunId)
	info, err := conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 仅删除当前唯一 namespace 的定义；运行、Worker 与 topic 历史由服务端保留。
	for _, method := range pb.Greeter_ServiceDesc.Methods {
		if _, err := conn.Workflows().Delete(ctx, "/wego.example.v1.Greeter/"+method.MethodName); err != nil {
			t.Fatal(err)
		}
	}
	for _, method := range pb.Greeter_ServiceDesc.Streams {
		if _, err := conn.Workflows().Delete(ctx, "/wego.example.v1.Greeter/"+method.StreamName); err != nil {
			t.Fatal(err)
		}
	}
	report := map[string]any{"sdk_version": wego.Version, "protocol_version": 4, "run_id": first.RunId, "worker_id": first.WorkerId, "engine": info, "log": logs, "cleanup": "explicit GracefulStop completed; workflow definitions deleted", "history": "run, Worker and topic history retained"}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("..", "..", ".test-results", namespace+"logging.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("log drain evidence:", path)
}

// awaitReportedLog 等待读模型可查询，metadata 必须保存 count=4 及正确任务身份。
func awaitReportedLog(t *testing.T, ctx context.Context, conn *client.Conn, runID string) model.Resource {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		logs, err := conn.Logs().List(ctx, runID, model.Query{})
		if err != nil {
			t.Fatal(err)
		}
		rows, _ := logs["rows"].([]any)
		for _, value := range rows {
			row, _ := value.(map[string]any)
			if row["message"] != "drain completed" {
				continue
			}
			metadata, _ := row["metadata"].(map[string]any)
			if metadata["count"] != float64(4) || metadata["phase"] != "drain" || metadata["run_id"] != runID {
				t.Fatal("reported metadata mismatch", row)
			}
			return row
		}
		select {
		case <-ctx.Done():
			t.Fatal("log did not become queryable", ctx.Err())
		case <-ticker.C:
		}
	}
}
