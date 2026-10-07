package backend_test

import (
	"context"
	"errors"
	"fmt"
	admin "github.com/hatchet-dev/hatchet/internal/services/admin/contracts"
	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/hatchet-dev/hatchet/sdks/go"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/backend"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	wruntime "github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/telemetry"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// rereviewPayload 模拟可取消的对象上传；release 只用于断言失败后的测试清理。
type rereviewPayload struct {
	// entered 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
	entered chan struct{}
	// release 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
	release chan struct{}
}

// rereviewBulkAdmin 第一块明确成功，第二块模拟取消或返回结构化幂等冲突。
type rereviewBulkAdmin struct {
	// admin.UnimplementedWorkflowServiceServer 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
	admin.UnimplementedWorkflowServiceServer
	// calls 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
	calls atomic.Int32
	// collision 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
	collision bool
}

// BulkTriggerWorkflow 为批量错误构造真实 gRPC 响应，记录已确定提交成功的输入数。
func (a *rereviewBulkAdmin) BulkTriggerWorkflow(ctx context.Context, req *admin.BulkTriggerWorkflowRequest) (*admin.BulkTriggerWorkflowResponse, error) {
	if a.collision {
		// st 标准 gRPC 状态，保留业务 code 和 Google details。
		st, err := status.New(codes.AlreadyExists, "collision").WithDetails(&v1.BulkTriggerIdempotencyCollisionError{
			SuccessfulWorkflowRunExternalIds: []string{"successful-run"},
			Collisions:                       []*v1.IdempotencyCollisionError{{ExistingRunExternalId: "existing-run"}},
		})
		if err != nil {
			return nil, err
		}
		return nil, st.Err()
	}
	if a.calls.Add(1) == 1 {
		// ids 引擎确认的运行身份集合，不通过提交输入猜测身份。
		ids := make([]string, len(req.Workflows))
		// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2。
		for i := range ids {
			ids[i] = fmt.Sprintf("accepted-run-%d", i)
		}
		return &admin.BulkTriggerWorkflowResponse{WorkflowRunIds: ids}, nil
	}
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

// rereviewBulkBackend 使用本地真实 gRPC 传输，所有凭证来自已有受控 fixture。
func rereviewBulkBackend(t *testing.T, a *rereviewBulkAdmin) *backend.Backend {
	t.Helper()
	// config 取得 fixture 的结果，确认成功后才进入下一处理阶段。
	config := fixture(t, nil, nil)
	// listener 根 Worker 拥有的 durable 监听器，子等待复用相同执行账本。
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// srv 取得 grpc.NewServer 的结果，确认成功后才进入下一处理阶段。
	srv := grpc.NewServer()
	admin.RegisterWorkflowServiceServer(srv, a)
	go srv.Serve(listener)
	t.Cleanup(srv.Stop)
	config.Address = listener.Addr().String()
	// b 隔离后端实例，连接及监听器由创建方统一关闭。
	b, err := backend.New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// TestReReviewBulkCollisionThroughGRPC 验证真正 RunMany 路径也应返回自有结构化错误。
func TestReReviewBulkCollisionThroughGRPC(t *testing.T) {
	// b 隔离后端实例，连接及监听器由创建方统一关闭。
	b := rereviewBulkBackend(t, &rereviewBulkAdmin{collision: true})
	// _, err 取得 b.RunMany 的结果，确认成功后才进入下一处理阶段。
	_, err := b.RunMany(context.Background(), "bulk", []model.RunManyInput{{Input: 1}, {Input: 2}})
	// collision 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
	var collision *model.BulkIdempotencyCollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("RunMany lost successful and conflicting RunIDs: %T: %v", err, err)
	}
	if len(collision.SuccessfulRunIDs) != 1 || collision.SuccessfulRunIDs[0] != "successful-run" || len(collision.Collisions) != 1 || collision.Collisions[0].ExistingRunID != "existing-run" {
		t.Fatalf("incomplete bulk identities: %+v", collision)
	}
}

// TestReReviewBulkCancellationRetainsAcceptedRuns 第一块的 1000 个 RunID 已返回，后一块取消不应丢弃它们。
func TestReReviewBulkCancellationRetainsAcceptedRuns(t *testing.T) {
	// a 取得 &rereviewBulkAdmin{} 的结果，确认成功后才进入下一处理阶段。
	a := &rereviewBulkAdmin{}
	// b 隔离后端实例，连接及监听器由创建方统一关闭。
	b := rereviewBulkBackend(t, a)
	// ctx 当前操作预算，用于传输取消；不得替换为无预算的 Background。
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	// refs 已由引擎确认接受的运行句柄，后续分块失败不能清空它们。
	refs, err := b.RunMany(ctx, "bulk", make([]model.RunManyInput, 1001))
	if a.calls.Load() != 2 || err == nil {
		t.Fatalf("fixture did not reach partial success: calls=%d, err=%v", a.calls.Load(), err)
	}
	if len(refs) != 1000 {
		t.Errorf("engine accepted 1000 runs; caller retained %d refs; err=%v", len(refs), err)
	}
	// partial 的类型及身份必须与成功句柄一致，不能仅用普通错误描述“部分失败”。
	var partial *model.PartialSubmissionError
	if !errors.As(err, &partial) || len(partial.SuccessfulRunIDs) != 1000 {
		t.Fatalf("partial result lost: %v", err)
	}
	// 每个句柄仍对应原始输入位置，不能把后续分块的输入方法套到前一块。
	for i, ref := range refs {
		if ref.InputIndex != i || ref.ID != partial.SuccessfulRunIDs[i] {
			t.Fatalf("misaligned accepted input %d: %+v", i, ref)
		}
	}

}

// Encode 等待实际收到的预算取消，不能把测试清理当成 Stop 生效。
func (p *rereviewPayload) Encode(ctx context.Context, _ string, data []byte) ([]byte, error) {
	close(p.entered)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.release:
		return data, nil
	}
}

// Decode 在本次启动期测试中不改变数据。
func (p *rereviewPayload) Decode(_ context.Context, _ string, data []byte) ([]byte, error) {
	return data, nil
}

// TestReReviewStopCancelsCronEncoding 验证 Stop 覆盖默认 Cron 输入的编码 I/O。
func TestReReviewStopCancelsCronEncoding(t *testing.T) {
	// config 和 payload 分别提供真实底层传输配置与受控上传点。
	config := fixture(t, nil, nil)
	// payload 取得 &rereviewPayload{entered: make 的结果，确认成功后才进入下一处理阶段。
	payload := &rereviewPayload{entered: make(chan struct{}), release: make(chan struct{})}
	// srv 取得 server.New 的结果，确认成功后才进入下一处理阶段。
	srv := server.New(server.WithRuntime(
		wruntime.WithToken(config.Token), wruntime.WithAddress(config.Address), wruntime.WithTLSConfig(nil),
		wruntime.WithLogger(config.Logger), wruntime.WithShutdown(wruntime.ShutdownConfig{Timeout: 50 * time.Millisecond}),
		wruntime.WithTelemetry(telemetry.Config{Trace: telemetry.TraceConfig{DisableWorkerExporter: true}}),
		wruntime.WithMiddleware(middleware.WithPayload(payload)),
	), server.WithWorker(worker.WithTask(pb.UnaryGreeter_SayHello_FullMethodName,
		task.WithCron("* * * * *"), task.WithCronInput(&pb.Request{Message: "scheduled"}))))
	pb.RegisterUnaryGreeterServer(srv, &pb.UnimplementedUnaryGreeterServer{})
	// serve 取得 make 的结果，确认成功后才进入下一处理阶段。
	serve := make(chan error, 1)
	go func() { serve <- srv.Serve() }()
	select {
	case <-payload.entered:
	// err 当前操作错误，失败时不继续使用对应结果。
	case err := <-serve:
		t.Fatalf("unexpected startup exit: %v", err)
	case <-time.After(time.Second):
		t.Fatal("encoding was not reached")
	}
	// stopped 取得 make 的结果，确认成功后才进入下一处理阶段。
	stopped := make(chan struct{})
	go func() { srv.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(250 * time.Millisecond):
		t.Error("Stop is blocked by CronInput.Encode with context.Background; 50ms shutdown budget cannot cancel it")
	}
	close(payload.release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not complete after releasing payload")
	}
	<-serve
}

// TestReReviewNormalizeSDKBulkCollision 验证实际 SDK 返回类型的批量成功和冲突身份均被保留。
func TestReReviewNormalizeSDKBulkCollision(t *testing.T) {
	// 输入来自 sdks/go.runManyBulk 的真实错误形状，而不是未被 wego 直接接收的 v0 错误。
	err := backend.Normalize(&sdk.BulkTriggerIdempotencyCollisionError{
		SuccessfulRunExternalIds: []string{"successful-run"},
		Collisions:               []*sdk.IdempotencyCollisionError{{ExistingRunExternalId: "existing-run"}},
	})
	// collision 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
	var collision *model.BulkIdempotencyCollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("typed IDs lost: %T: %v", err, err)
	}
	if len(collision.SuccessfulRunIDs) != 1 || collision.SuccessfulRunIDs[0] != "successful-run" || collision.Collisions[0].ExistingRunID != "existing-run" {
		t.Fatalf("incomplete identities: %+v", collision)
	}
}
