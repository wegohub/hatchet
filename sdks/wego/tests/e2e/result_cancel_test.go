//go:build e2e

package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestResultCancellationCancelsSharedRun 观察者超时主动取消实际运行，其他观察者也获得引擎的取消终态
func TestResultCancellationCancelsSharedRun(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	entered := make(chan struct{}, 1)
	service := &scenarios.Service{Say: func(ctx context.Context, _ *pb.Request) (*pb.Reply, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	harness, err := scenarios.Start(ctx, "result-cancel", &scenarios.Report{}, service, nil)
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
	ref, err := harness.Conn.RunNoWait(ctx, pb.UnaryGreeter_SayHello_FullMethodName, model.RPCInput{Message: &pb.Request{}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitCtx, stopWait := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err = ref.Result(waitCtx)
	stopWait()
	if !errors.Is(err, context.DeadlineExceeded) && status.Code(err) != codes.DeadlineExceeded {
		t.Fatal("observer deadline lost", err)
	}
	if _, err := ref.Result(ctx); status.Code(err) != codes.Canceled {
		t.Fatal("shared run was not canceled", err)
	}
	info, err := harness.Conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	writeStreamEvidence(t, "result-cancel", map[string]any{"server_version": info.Version, "run_id": ref.RunID, "assertions": []string{"asynchronous submission lifetime detached", "Result deadline requests remote cancellation", "independent observation sees authoritative Canceled"}, "cleanup": "worker and connection stopped; definitions deleted; run history retained"})
}
