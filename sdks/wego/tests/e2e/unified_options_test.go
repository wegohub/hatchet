//go:build e2e

package e2e

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// TestUnifiedCallOptions 在真实调度中验证生成客户端透传 RunOption、大小限制与完成通知。
func TestUnifiedCallOptions(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	// calls 仅统计真正启动的 handler，发送前拒绝的消息不得创建业务执行。
	var calls atomic.Int32
	service := &scenarios.Service{Say: func(ctx context.Context, request *pb.Request) (*pb.Reply, error) {
		calls.Add(1)
		info, ok := task.Info(ctx)
		if !ok {
			return nil, model.ErrTaskContext
		}
		if info.AdditionalMetadata["source"] != "generated" || info.WorkerLabels["region"] != "fixture" {
			return nil, status.Error(codes.FailedPrecondition, "run options missing from task")
		}
		_ = grpc.SetHeader(ctx, metadata.Pairs("entry", "worker"))
		grpc.SetTrailer(ctx, metadata.Pairs("result", "complete"))
		return scenarios.Reply(ctx, request), nil
	}}
	harness, err := scenarios.Start(ctx, "unified-options", &scenarios.Report{}, service,
		[]runtime.Option{runtime.WithLabels(map[string]any{"region": "fixture"})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := harness.Close(); err != nil {
			t.Error(err)
		}
	}()
	options := client.WithRunOptions(client.WithRouting(map[string]any{"group": "one"}),
		client.WithRunPriority(3), client.WithRunMetadata(map[string]string{"source": "generated"}),
		client.WithDesiredWorkerLabels(map[string]*model.DesiredWorkerLabel{"region": {Value: "fixture", Required: true}}))
	// finished 记录同步调用终态，过滤和成功调用都只能通知一次。
	var finished atomic.Int32
	// headers 与 trailers 保存真实 Worker 返回的协议元数据。
	var headers, trailers metadata.MD
	reply, err := harness.RPC.SayHello(ctx, &pb.Request{Message: "hello"}, options,
		grpc.Header(&headers), grpc.Trailer(&trailers),
		grpc.MaxCallSendMsgSize(0), grpc.MaxCallSendMsgSize(1024),
		grpc.MaxCallRecvMsgSize(0), grpc.MaxCallRecvMsgSize(1024),
		grpc.OnFinish(func(error) { finished.Add(1) }))
	if err != nil || reply.RunId == "" || len(headers.Get("entry")) != 1 || len(trailers.Get("result")) != 1 || headers.Get("entry")[0] != "worker" || trailers.Get("result")[0] != "complete" || finished.Load() != 1 {
		t.Fatal("generated options or completion failed", reply, err)
	}
	// 零发送预算仅接受编码长度为零的 protobuf，非空输入在提交前失败。
	_, err = harness.RPC.SayHello(ctx, &pb.Request{Message: "blocked"}, options,
		grpc.MaxCallSendMsgSize(0), grpc.OnFinish(func(error) { finished.Add(1) }))
	if status.Code(err) != codes.ResourceExhausted || calls.Load() != 1 || finished.Load() != 2 {
		t.Fatal("send limit failed before submission", err, calls.Load(), finished.Load())
	}
	// 空请求可以提交，但非空执行身份超过零接收预算，不能被静默交付。
	_, err = harness.RPC.SayHello(ctx, &pb.Request{}, options,
		grpc.MaxCallSendMsgSize(0), grpc.MaxCallRecvMsgSize(0))
	if status.Code(err) != codes.ResourceExhausted || calls.Load() != 2 {
		t.Fatal("empty protobuf or receive limit failed", err, calls.Load())
	}
}
