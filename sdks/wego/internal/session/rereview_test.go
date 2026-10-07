package session

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestReReviewDecodeFailureIsTerminal 即使下一条 DATA 已缓冲，也不能在解码失败之后再次返回成功。
func TestReReviewDecodeFailureIsTerminal(t *testing.T) {
	// c 当前协议详情或流客户端，类型确认后才读取具体字段。
	c := receivingClient(t)
	c.backend = rereviewControl{}
	c.messages[1] = []byte{0xff}
	c.messages[2], _ = proto.Marshal(&pb.Reply{Message: "later"})
	c.inputBytes = len(c.messages[1]) + len(c.messages[2])
	// first 取得 c.RecvMsg 的结果，确认成功后才进入下一处理阶段。
	first := c.RecvMsg(&pb.Reply{})
	if first == nil {
		t.Fatal("invalid first payload succeeded")
	}
	// second 取得 c.RecvMsg 的结果，确认成功后才进入下一处理阶段。
	second := c.RecvMsg(&pb.Reply{})
	if c.consumed != 1 {
		t.Errorf("terminal failure consumed buffered DATA: %d", c.consumed)
	}
	if !errors.Is(second, first) && (second == nil || second.Error() != first.Error()) {
		t.Errorf("terminal error changed: first=%v; second=%v; consumed=%d", first, second, c.consumed)
	}
}

// rereviewControl 与真实 backend 一样拒绝已取消的提交。
type rereviewControl struct{ ports.Backend }

// Run 在调用已取消时返回取消状态，避免用忽略 context 的假后端掩盖错误覆盖。
func (b rereviewControl) Run(ctx context.Context, name string, input any, options model.RunOptions) (ports.Run, error) {
	// err 当前操作错误，失败时不继续使用对应结果。
	if err := ctx.Err(); err != nil {
		return ports.Run{}, status.FromContextError(err).Err()
	}
	return acceptingControl{}.Run(ctx, name, input, options)
}

// rereviewSessionBackend 完成真实客户端状态机握手，业务 RUN 等待调用方取消。
type rereviewSessionBackend struct {
	// ports.Backend 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
	ports.Backend
	// frames 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
	frames chan string
}

// Run 提供 START、PING 和控制任务确认，RUN 的结果等待由收到的 context 控制。
func (b *rereviewSessionBackend) Run(_ context.Context, _ string, input any, _ model.RunOptions) (ports.Run, error) {
	// c 当前协议详情或流客户端，类型确认后才读取具体字段。
	c := input.(Control)
	if c.Kind == "RUN" {
		return ports.Run{ID: "session-run", Wait: func(ctx context.Context) (ports.Result, error) {
			<-ctx.Done()
			return ports.Result{}, ctx.Err()
		}}, nil
	}
	if c.Kind == "PING" {
		// f, _ 取得 wire.DecodeFrame 的结果，确认成功后才进入下一处理阶段。
		f, _ := wire.DecodeFrame(c.Frame, 1<<20)
		// ready, _ 取得 wire.EncodeFrame 的结果，确认成功后才进入下一处理阶段。
		ready, _ := wire.EncodeFrame(&wire.Frame{Version: wire.Version, StreamId: c.StreamID, Kind: "READY", Nonce: f.Nonce})
		b.frames <- ready
	}
	return ports.Run{ID: "control", Wait: func(context.Context) (ports.Result, error) {
		return ports.Result{Outputs: map[string]any{"control": Acknowledgment{Owner: "owner", Open: true}}}, nil
	}}, nil
}

// Stream 转发 READY 并随 context 取消，避免引入额外传输错误。
func (b *rereviewSessionBackend) Stream(ctx context.Context, _ string, consume func(string) error) error {
	for {
		select {
		// case frame 取得 <-b.frames: 的结果，确认成功后才进入下一处理阶段。
		case frame := <-b.frames:
			// err 当前操作错误，失败时不继续使用对应结果。
			if err := consume(frame); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// TestReReviewCancellationRetainsGRPCCode 在后台结果等待退出后读取，取消不能被替换成 Unavailable。
func TestReReviewCancellationRetainsGRPCCode(t *testing.T) {
	// ctx 当前操作预算，用于传输取消；不得替换为无预算的 Background。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// c 当前协议详情或流客户端，类型确认后才读取具体字段。
	c, err := NewClient(ctx, &rereviewSessionBackend{frames: make(chan string, 8)}, spec.Defaults(), &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/fixture/Chat")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	// 等待后台 RUN 等待完成；测试特意覆盖 Recv 比取消清理更晚的合法调用顺序。
	until := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		// failure 首个终态故障，后续取消与清理错误不能覆盖它。
		failure := c.failure
		c.mu.Unlock()
		if failure != nil {
			break
		}
		if time.Now().After(until) {
			t.Fatal("result waiter did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	// err 当前操作错误，失败时不继续使用对应结果。
	if err := c.RecvMsg(&pb.Reply{}); status.Code(err) != codes.Canceled {
		t.Errorf("caller cancellation became %v: %v", status.Code(err), err)
	}
	<-c.cleanupDone
}

// TestReReviewClientStreamMissingResponse 未发送任何响应的 client-stream 不能伪造出一个空 protobuf 成功结果。
func TestReReviewClientStreamMissingResponse(t *testing.T) {
	// c 当前协议详情或流客户端，类型确认后才读取具体字段。
	c := receivingClient(t)
	c.description.ClientStreams = true
	c.final = &wire.StreamResult{}
	// err 当前操作错误，失败时不继续使用对应结果。
	if err := c.RecvMsg(&pb.Reply{}); status.Code(err) != codes.Internal {
		t.Error("client-stream with zero server responses returned a successful empty message")
	}
}

// TestReReviewEmptyFinalResponseHasPresence 一次合法的空 protobuf 响应也应占用唯一响应位置。
func TestReReviewEmptyFinalResponseHasPresence(t *testing.T) {
	// s, _ 取得 endpoint 的结果，确认成功后才进入下一处理阶段。
	s, _ := endpoint(t)
	// empty, err 取得 proto.Marshal 的结果，确认成功后才进入下一处理阶段。
	empty, err := proto.Marshal(&pb.Reply{})
	if err != nil {
		t.Fatal(err)
	}
	// err 当前操作错误，失败时不继续使用对应结果。
	if err := s.SetResponse(empty); err != nil {
		t.Fatal(err)
	}
	// err 当前操作错误，失败时不继续使用对应结果。
	if err := s.SetResponse(empty); err == nil {
		t.Error("two client-stream final responses were accepted because both encoded to empty bytes")
	}
}

// rereviewNoResponseServer 模拟合法返回 nil、但遗漏 SendAndClose 的业务 handler。
type rereviewNoResponseServer struct{ pb.UnimplementedGreeterServer }

// UploadHellos 不发送响应，用原生网络传输确认 response 数量约束。
func (*rereviewNoResponseServer) UploadHellos(grpc.ClientStreamingServer[pb.Request, pb.Reply]) error {
	return nil
}

// TestReReviewNativeClientStreamMissingResponse 为 wego 对照测试提供同版本 grpc 的真实网络基准。
func TestReReviewNativeClientStreamMissingResponse(t *testing.T) {
	// listener 绑定随机回环端口，不依赖本机 Hatchet 或业务服务。
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// srv 取得 grpc.NewServer 的结果，确认成功后才进入下一处理阶段。
	srv := grpc.NewServer()
	pb.RegisterGreeterServer(srv, &rereviewNoResponseServer{})
	go srv.Serve(listener)
	defer srv.Stop()
	// conn 本实例拥有的协议连接，所有成功和失败路径都必须关闭。
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// ctx 当前操作预算，用于传输取消；不得替换为无预算的 Background。
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// stream 使用相同的生成桩；同样遗漏响应时，原生实现必须返回 Internal。
	stream, err := pb.NewGreeterClient(conn).UploadHellos(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.CloseAndRecv()
	if status.Code(err) != codes.Internal {
		t.Fatalf("unexpected native result: %v", err)
	}
	t.Logf("native grpc rejects absent response: %v", err)
}

// TestEmptyClientStreamResponseRoundTrip 零字节 protobuf 可以是唯一最终响应，随后才返回 EOF。
func TestEmptyClientStreamResponseRoundTrip(t *testing.T) {
	// s 收集真实 Endpoint 的响应存在性，不能直接伪造一个非空 payload 代替空消息。
	s, _ := endpoint(t)
	// err 当前操作错误，失败时不继续使用对应结果。
	if err := s.SetResponse(nil); err != nil {
		t.Fatal(err)
	}
	// final 保留 has_response=true，即使 response 字节数组为空。
	final := s.finish(nil)
	if !final.HasResponse || len(final.Response) != 0 {
		t.Fatalf("empty response presence lost: %v", final)
	}
	// c 使用终态解码路径，模拟生成桩的 CloseAndRecv。
	c := receivingClient(t)
	c.final = final
	// err 当前操作错误，失败时不继续使用对应结果。
	if err := c.RecvMsg(&pb.Reply{}); err != nil {
		t.Fatal(err)
	}
	// err 当前操作错误，失败时不继续使用对应结果。
	if err := c.RecvMsg(&pb.Reply{}); err != io.EOF {
		t.Fatalf("expected EOF after one response: %v", err)
	}
}

// TestStreamWaitErrorPreservesDeadlineAndStatus 同一归类函数覆盖取消、deadline 和业务 status，避免后台等待改写状态。
func TestStreamWaitErrorPreservesDeadlineAndStatus(t *testing.T) {
	// cases 的输入错误分别代表三种必须保留的 gRPC 语义。
	cases := []struct {
		// err 为原始等待错误。
		err error
		// code 为公开调用期望观察到的状态。
		code codes.Code
	}{{context.Canceled, codes.Canceled}, {context.DeadlineExceeded, codes.DeadlineExceeded}, {status.Error(codes.Aborted, "business"), codes.Aborted}}
	// 当前上下文仍有效时，也必须识别错误链中的取消和已有 status。
	for _, test := range cases {
		// actual 取得 status.Code 的结果，确认成功后才进入下一处理阶段。
		if actual := status.Code(waitError(context.Background(), test.err)); actual != test.code {
			t.Fatalf("lost code: %v -> %v", test.err, actual)
		}
	}
}
