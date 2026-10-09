//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// TestWorkerEvents 让两个实际 Worker 的业务 slots 全满，验证独立通知、定向寻址和新进程加入边界
func TestWorkerEvents(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	namespace := fmt.Sprintf("events_%d", time.Now().UnixNano())
	conn, err := client.New(client.WithRuntime(scenarios.Runtime(namespace)...))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// 此广播先于两个 JOIN 持久发布，新 Worker 不应把它交给业务回调
	if err := conn.Workers().BroadcastEvent(ctx, model.WorkerEvent{ID: "before-join", Type: "fixture"}); err != nil {
		t.Fatal(err)
	}
	// events 保留实际回调；entered 与 release 是业务占满 slots 的事实屏障
	events := make(chan model.WorkerEvent, 16)
	entered := make(chan model.TaskInfo, 2)
	release := make(chan struct{})
	// releaseOnce 同时保护成功路径和提前失败清理，不重复关闭业务屏障
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	service := &scenarios.Service{Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
		info, ok := task.Info(ctx)
		if !ok {
			return nil, status.Error(codes.Internal, "fixture: task identity missing")
		}
		select {
		case entered <- info:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-release:
			return scenarios.Reply(ctx, in), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	// servers 同 namespace、同方法共享定义，每个实例只有一个业务 slot
	servers := make([]*server.Server, 2)
	serveErrors := make(chan error, 2)
	for i := range servers {
		opts := append(scenarios.Runtime(namespace), runtime.WithSlots(1), runtime.WithWorkerEventHandler(func(ctx context.Context, event model.WorkerEvent) error {
			select {
			case events <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}))
		servers[i] = server.New(server.WithRuntime(opts...))
		pb.RegisterUnaryGreeterServer(servers[i], service)
		go func(s *server.Server) { serveErrors <- s.Serve() }(servers[i])
	}
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()
	if err := support.WaitWorkers(ctx, conn, namespace, 2, serveErrors); err != nil {
		t.Fatal(err)
	}
	// 分别占用两个 Worker，任务身份直接给出当前随机 worker_key
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, err := pb.NewUnaryGreeterClient(conn).SayHello(ctx, &pb.Request{}); results <- err }()
	}
	keys := map[string]bool{}
	runIDs := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case info := <-entered:
			keys[info.WorkerKey] = true
			runIDs = append(runIDs, info.RunID)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if len(keys) != 2 || keys[""] {
		t.Fatalf("worker identities=%v", keys)
	}
	// 广播须同时到达两个已加入实例，业务 slots 的占用不能阻塞此路径
	if err := conn.Workers().BroadcastEvent(ctx, model.WorkerEvent{ID: "both", Type: "fixture"}); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case event := <-events:
			if event.ID != "both" || !keys[event.WorkerKey] || seen[event.WorkerKey] {
				t.Fatalf("unexpected broadcast: %+v", event)
			}
			seen[event.WorkerKey] = true
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	// 相同事件 ID 的明确 producer 重发只存一条，定向回调必须来自指定实例
	var target string
	for key := range keys {
		target = key
		break
	}
	event := model.WorkerEvent{ID: "one", Type: "fixture", SentAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Minute).UTC()}
	for i := 0; i < 2; i++ {
		if err := conn.Workers().SendEvent(ctx, target, event); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case got := <-events:
		if got.ID != "one" || got.WorkerKey != target {
			t.Fatalf("wrong directed event: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// 同一个定向 topic 的尾部屏障到达后，确认重复事件和加入前广播都未被交付
	if err := conn.Workers().SendEvent(ctx, target, model.WorkerEvent{ID: "tail", Type: "fixture"}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-events:
		if got.ID != "tail" || got.WorkerKey != target {
			t.Fatalf("duplicate or historic event: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	releaseOnce.Do(func() { close(release) })
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	for _, s := range servers {
		s.GracefulStop()
	}
	for i := 0; i < 2; i++ {
		if err := <-serveErrors; err != nil {
			t.Fatal(err)
		}
	}
	// 定义按本轮 namespace 清理；topic 和运行历史由服务端保留策略管理
	for _, method := range pb.UnaryGreeter_ServiceDesc.Methods {
		if _, err := conn.Workflows().Delete(ctx, "/wego.example.v1.UnaryGreeter/"+method.MethodName); err != nil {
			t.Fatal(err)
		}
	}
	info, err := conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writeStreamEvidence(t, "worker-events", map[string]any{"server_version": info.Version, "namespace": namespace, "run_ids": runIDs, "worker_keys": keys, "assertions": []string{"two random worker identities", "broadcast while all business slots occupied", "directed event reaches matching instance", "repeated event ID stored once", "pre-JOIN broadcast excluded"}, "cleanup": "workers stopped; definitions deleted; topic and run history retained"})
}
