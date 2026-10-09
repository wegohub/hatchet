//go:build e2e

package e2e

import (
	"context"
	"errors"
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
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// submissionLoss 转发真实提交，只在引擎已接受后丢弃响应并模拟调用方取消
type submissionLoss struct {
	// faultBackend 转发正式读取、持久传输和实例关闭
	*faultBackend
	// cancel 只取消本次调用，不能取消测试清理预算
	cancel context.CancelFunc
	// accepted 保存真实创建的唯一运行供终态断言
	accepted ports.Run
}

// submissionService 等待远端取消，验证恢复身份后的取消确实作用于引擎运行
type submissionService struct {
	// UnimplementedGreeterServer 提供其余 RPC 的标准生成实现
	pb.UnimplementedGreeterServer
}

// WatchHellos 不自行结束，使取消终态只能来自已接受运行的主动取消
func (*submissionService) WatchHellos(_ *pb.Request, output grpc.ServerStreamingServer[pb.Reply]) error {
	<-output.Context().Done()
	return output.Context().Err()
}

// Run 只触发一次真实执行，后续身份恢复必须走只读查询
func (b *submissionLoss) Run(ctx context.Context, method string, input any, options model.RunOptions) (ports.Run, error) {
	ref, err := b.faultBackend.Run(ctx, method, input, options)
	if err != nil {
		return ref, err
	}
	b.accepted = ref
	b.cancel()
	return ports.Run{}, status.Error(codes.Unavailable, "accepted submission response lost")
}

// FindSubmission 转发正式只读检索，不以已保存的 fixture RunID 伪造恢复成功
func (b *submissionLoss) FindSubmission(ctx context.Context, nonce, method, digest string) (ports.Run, error) {
	return b.Backend.(ports.SubmissionFinder).FindSubmission(ctx, nonce, method, digest)
}

// TestStreamAmbiguousSubmissionCancellation 验证取消后查回 RunID、请求远端取消，且没有第二次提交或失踪任务
func TestStreamAmbiguousSubmissionCancellation(t *testing.T) {
	preflight(t)
	ctx, stop := context.WithTimeout(context.Background(), 45*time.Second)
	defer stop()
	service := &submissionService{}
	harness, err := scenarios.StartRegistered(ctx, "submission-loss", &scenarios.Report{}, func(registrar grpc.ServiceRegistrar) { pb.RegisterGreeterServer(registrar, service) }, streamNames(), nil)
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
	config := spec.Defaults()
	for _, option := range scenarios.Runtime(harness.Namespace) {
		option(&config)
	}
	e, err := engine.New(config)
	if err != nil {
		t.Fatal(err)
	}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	proxy := &submissionLoss{faultBackend: &faultBackend{Backend: e.Backend, mode: "none", authority: make(chan struct{})}, cancel: cancel}
	e.Backend = proxy
	conn := coreclient.FromEngine(e, false)
	defer conn.Close()
	_, err = pb.NewGreeterClient(conn).WatchHellos(client.WithIdempotencyKey(callCtx, "saved-operation"), &pb.Request{Count: 4})
	// failure 必须由真实 REST 查询取得身份，错误中同时保留业务在提交前保存的原始 key
	var failure *model.SubmissionError
	if !errors.As(err, &failure) || status.Code(err) != codes.Canceled || failure.OperationKey != "saved-operation" || failure.RunID == "" || failure.RunID != proxy.accepted.ID {
		t.Fatalf("ambiguous cancellation lost recoverable identity: %T %v", err, err)
	}
	if proxy.calls.Load() != 1 {
		t.Fatal("cancellation retriggered a task")
	}
	if _, err := proxy.accepted.Wait(ctx); status.Code(err) != codes.Canceled {
		t.Fatal("accepted run was not canceled", err)
	}
	info, err := conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := harness.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	writeStreamEvidence(t, "stream-submission", map[string]any{"server_version": info.Version, "namespace": harness.Namespace, "run_id": failure.RunID, "submitted_tasks": 1, "assertions": []string{"stored submission ACK lost", "read-only nonce lookup recovers actual RunID", "original operation key preserved", "no submission after cancellation", "accepted run reaches canceled terminal"}, "cleanup": "workers and connections stopped; definitions deleted; run/topic history retained"})
}
