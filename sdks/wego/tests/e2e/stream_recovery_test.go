//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	coreclient "github.com/hatchet-dev/hatchet/sdks/wego/internal/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// recoveryService 用真实应用重试驱动有限历史恢复，外部业务不依赖原 Worker 名称
type recoveryService struct {
	// UnimplementedGreeterServer 保留未覆盖方法的标准生成行为
	pb.UnimplementedGreeterServer
	// executions 统计实际业务执行，不统计冲突恢复或消费重放
	executions atomic.Int32
}

// WatchHellos 首次执行发布两条后失败；重试从持久 checkpoint 的末条继续
func (s *recoveryService) WatchHellos(in *pb.Request, out grpc.ServerStreamingServer[pb.Reply]) error {
	s.executions.Add(1)
	history, err := task.Checkpoint(out.Context())
	if err != nil {
		return err
	}
	start := int32(0)
	for {
		item := &pb.Reply{}
		err := history.Next(item)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if item.Count != start {
			return fmt.Errorf("history discontinuity: %d/%d", item.Count, start)
		}
		start++
	}
	info, ok := task.Info(out.Context())
	if !ok {
		return model.ErrTaskContext
	}
	for i := start; i < in.Count; i++ {
		if err := out.Send(scenarios.Reply(out.Context(), &pb.Request{Count: i})); err != nil {
			return err
		}
		if info.RetryCount == 0 && i == 1 {
			return status.Error(codes.Unavailable, "retry after acknowledged output")
		}
	}
	return nil
}

// TestReliableStreamRecovery 验证 handler 复用历史、消费 checkpoint JSON 往返和幂等冲突复用 RunID
func TestReliableStreamRecovery(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	service := &recoveryService{}
	harness, err := scenarios.StartRegistered(ctx, "stream-recovery", &scenarios.Report{}, func(registrar grpc.ServiceRegistrar) { pb.RegisterGreeterServer(registrar, service) }, streamNames(), nil, worker.WithTask(pb.Greeter_WatchHellos_FullMethodName, task.WithRetries(1)))
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			if err := harness.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	operation := client.WithIdempotencyKey(ctx, "persisted-operation")
	watch, err := pb.NewGreeterClient(harness.Conn).WatchHellos(operation, &pb.Request{Count: 4})
	if err != nil {
		t.Fatal(err)
	}
	first, err := watch.Recv()
	if err != nil || first.Count != 0 {
		t.Fatal(first, err)
	}
	cp, err := client.StreamCheckpoint(watch.Context())
	if err != nil {
		t.Fatal(err)
	}
	if cp.OutputSeq != 1 || cp.Cursor == "" || cp.RunID != first.RunId {
		t.Fatalf("invalid delivered checkpoint: %+v", cp)
	}
	// serialized 使用公开 JSON 类型模拟客户端独立保存，不共享原始 headers map
	serialized, err := json.Marshal(cp)
	if err != nil {
		t.Fatal(err)
	}
	// saved 是经过持久化往返的自有断点
	var saved model.StreamCheckpoint
	if err := json.Unmarshal(serialized, &saved); err != nil {
		t.Fatal(err)
	}
	for i := int32(1); i < 4; i++ {
		reply, err := watch.Recv()
		if err != nil || reply.Count != i || reply.RunId != cp.RunID {
			t.Fatal(reply, err)
		}
	}
	if _, err := watch.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	if service.executions.Load() != 2 {
		t.Fatalf("attempts=%d", service.executions.Load())
	}
	// 独立 Conn 创建新的观察资源，恢复不复用原 ClientStream 或重新执行 handler
	config := spec.Defaults()
	for _, option := range scenarios.Runtime(harness.Namespace) {
		option(&config)
	}
	e, err := engine.New(config)
	if err != nil {
		t.Fatal(err)
	}
	resumedConn := coreclient.FromEngine(e, false)
	defer resumedConn.Close()
	raw, err := resumedConn.ResumeStream(ctx, saved)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.SendMsg(&pb.Request{}); err != io.EOF {
		t.Fatalf("resumed input reopened: %v", err)
	}
	resumed := &grpc.GenericClientStream[pb.Request, pb.Reply]{ClientStream: raw}
	for i := int32(1); i < 4; i++ {
		reply, err := resumed.Recv()
		if err != nil || reply.Count != i || reply.RunId != saved.RunID {
			t.Fatal(reply, err)
		}
	}
	if _, err := resumed.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	// 相同逻辑键和输入必须通过冲突身份复用历史，而不是产生新的逻辑任务
	duplicate, err := pb.NewGreeterClient(resumedConn).WatchHellos(operation, &pb.Request{Count: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 4; i++ {
		reply, err := duplicate.Recv()
		if err != nil || reply.Count != i || reply.RunId != saved.RunID {
			t.Fatal(reply, err)
		}
	}
	if _, err := duplicate.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	if service.executions.Load() != 2 {
		t.Fatal("resume retriggered handler")
	}
	_, err = pb.NewGreeterClient(resumedConn).WatchHellos(operation, &pb.Request{Count: 5})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("different input reused existing key: %v", err)
	}
	info, err := harness.Conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumedConn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := harness.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	writeStreamEvidence(t, "stream-recovery", map[string]any{"server_version": info.Version, "namespace": harness.Namespace, "run_id": saved.RunID, "task_id": saved.TaskID, "cursor": saved.Cursor, "output_seq": saved.OutputSeq, "handler_attempts": 2, "assertions": []string{"application retry restores historical outputs", "checkpoint survives JSON serialization", "independent Conn resumes the same run without submission", "same key and input reuse existing run", "different input with same key rejected"}, "cleanup": "workers and connections stopped; definitions deleted; run/topic history retained"})
	t.Logf("run_id=%s task_id=%s cursor=%s output_seq=%d handler_attempts=2", saved.RunID, saved.TaskID, saved.Cursor, saved.OutputSeq)
}
