//go:build e2e

package e2e

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// TestRealtimeStreams 验证 PutStream 的三种调用、完整 EOF 与明确拒绝恢复；不通过持久出口伪造实时传输
func TestRealtimeStreams(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	options := []runtime.Option{runtime.WithStreamMode(pb.Greeter_WatchHellos_FullMethodName, runtime.Realtime), runtime.WithStreamMode(pb.Greeter_ChatHellos_FullMethodName, runtime.Realtime)}
	harness, err := scenarios.StartRegistered(ctx, "realtime", &scenarios.Report{}, func(registrar grpc.ServiceRegistrar) { pb.RegisterGreeterServer(registrar, &faultService{}) }, streamNames(), options)
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
	rpc := pb.NewGreeterClient(harness.Conn)
	watch, err := rpc.WatchHellos(ctx, &pb.Request{Count: 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 3; i++ {
		reply, err := watch.Recv()
		if err != nil || reply.Count != i {
			t.Fatal(reply, err)
		}
	}
	if _, err := watch.Recv(); err != io.EOF {
		t.Fatal("unverified realtime EOF", err)
	}
	if _, err := client.StreamCheckpoint(watch.Context()); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("realtime checkpoint advertised recovery", err)
	}
	chat, err := rpc.ChatHellos(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 3; i++ {
		if err := chat.Send(&pb.Request{Count: i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := chat.CloseSend(); err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 3; i++ {
		reply, err := chat.Recv()
		if err != nil || reply.Count != i {
			t.Fatal(reply, err)
		}
	}
	if _, err := chat.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	upload, err := rpc.UploadHellos(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := upload.Send(&pb.Request{Count: 0}); err != nil {
		t.Fatal(err)
	}
	if response, err := upload.CloseAndRecv(); err != nil || response.Count != 1 {
		t.Fatal(response, err)
	}
	info, err := harness.Conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	writeStreamEvidence(t, "stream-realtime", map[string]any{"server_version": info.Version, "assertions": []string{"formal PutStream carries CLAIM HEADERS DATA END", "server and bidi response order and engine-verified EOF", "client stream final response", "realtime checkpoint rejected"}, "cleanup": "workers and connections stopped; definitions deleted; run history retained"})
}
