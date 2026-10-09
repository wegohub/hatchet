package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	wlog "github.com/hatchet-dev/hatchet/sdks/wego/log"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// receiveServe 限时等待 SDK 清理完成，避免测试自身永久阻塞。
func receiveServe(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("server cleanup did not finish")
		return nil
	}
}

// TestNetworkLogs 普通网络请求与三种流共享 Runtime 输出，R 系列打印后返回能力错误。
func TestNetworkLogs(t *testing.T) {
	// logs 保存本机 JSON 输出，不保存凭证或业务请求。
	var logs bytes.Buffer
	// mu 保护测试输出容器，并发 handler 不能同时修改 buffer。
	var mu sync.Mutex
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	// 拦截器串行记录，以免测试 bytes.Buffer 本身产生 race。
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		mu.Lock()
		wlog.Info(ctx, "network unary")
		err := wlog.RInfo(ctx, "network report")
		mu.Unlock()
		if !errors.Is(err, model.ErrTaskContext) {
			t.Error(err)
		}
		return next(ctx, req)
	}
	stream := func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		mu.Lock()
		wlog.Info(stream.Context(), "network stream")
		mu.Unlock()
		return next(service, stream)
	}
	srv := New(WithGRPC("127.0.0.1:0", grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream)), WithRuntime(runtime.WithDisableWorker(), runtime.WithLogger(logger)))
	pb.RegisterGreeterServer(srv, &greeter{})
	done := make(chan error, 1)
	go func() { done <- srv.Serve() }()
	<-srv.initialized
	conn, err := grpc.NewClient(srv.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	defer srv.Stop()
	rpc := pb.NewGreeterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := rpc.SayHello(ctx, &pb.Request{}, grpc.WaitForReady(true)); err != nil {
		t.Fatal(err)
	}
	upload, err := rpc.UploadHellos(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := upload.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}
	watch, err := rpc.WatchHellos(ctx, &pb.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	chat, err := rpc.ChatHellos(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := chat.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := chat.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	srv.Stop()
	if err := receiveServe(t, done); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Count(logs.String(), "network stream") != 3 || !strings.Contains(logs.String(), "rpc_method") || strings.Contains(logs.String(), "task_run_id") {
		t.Fatal(logs.String())
	}
}
