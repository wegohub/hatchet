package session

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// receivingClient 构造已初始化窗口和终态字段的接收端 fixture，便于测试协议边界。
func receivingClient(t *testing.T) *Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// config 创建默认配置副本，后续显式选项可以覆盖 0、false 或 nil。
	config := spec.Defaults()
	config.Stream.HandshakeTimeout = 20 * time.Millisecond
	return &Client{
		ctx:         ctx,
		cancel:      cancel,
		config:      config,
		messages:    map[uint64][]byte{},
		id:          "fixture",
		description: &grpc.StreamDesc{},
		finalAt:     time.Now(),
	}
}

// TestFinalResponseAppliesCallMetadataOptions 注入终态响应的 headers 和 trailers，验证 grpc.Header / Trailer 的目标被填写。
func TestFinalResponseAppliesCallMetadataOptions(t *testing.T) {
	// c 带有真实接收窗口与状态字段的客户端 fixture，后续注入帧及终态检测协议边界。
	c := receivingClient(t)
	// headers, trailers 响应头缓存；调用方读取时返回副本。 响应尾部 metadata，随最终状态交付。
	var headers, trailers metadata.MD
	c.opts = []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	c.headersSeen = true
	c.headers = metadata.Pairs("mode", "upload")
	c.trailers = metadata.Pairs("state", "complete")
	data, err := proto.Marshal(&pb.Reply{Count: 3})
	if err != nil {
		t.Fatal(err)
	}
	c.final = &wire.StreamResult{HasResponse: true, Response: data}
	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
	var out pb.Reply
	if err = c.RecvMsg(&out); err != nil {
		t.Fatal(err)
	}
	if out.Count != 3 || headers.Get("mode")[0] != "upload" || trailers.Get("state")[0] != "complete" {
		t.Fatalf("response/metadata: %v/%v/%v", &out, headers, trailers)
	}
	if err = c.RecvMsg(&out); err != io.EOF {
		t.Fatal(err)
	}
}

// TestOutputGapCannotBecomeSuccessfulEOF 构造最后序号为 2 但缺少序号 1 的输出，断言不能返回成功 EOF。
func TestOutputGapCannotBecomeSuccessfulEOF(t *testing.T) {
	// c 带有真实接收窗口与状态字段的客户端 fixture，后续注入帧及终态检测协议边界。
	c := receivingClient(t)
	c.description.ServerStreams = true
	c.final = &wire.StreamResult{LastSeq: 1}
	// err 保存一次接收结果；按断言区分正常 EOF、业务状态错误及协议 DataLoss。
	if err := c.RecvMsg(&pb.Reply{}); status.Code(err) != codes.DataLoss {
		t.Fatal(err)
	}
	if c.ctx.Err() != context.Canceled {
		t.Fatal("gap failure left client lifetime open")
	}
}

// TestProtocolFailureWinsCancellationRace 先记录 DataLoss 再触发清理取消，断言最终仍返回 DataLoss。
func TestProtocolFailureWinsCancellationRace(t *testing.T) {
	// c 带有真实接收窗口与状态字段的客户端 fixture，后续注入帧及终态检测协议边界。
	c := receivingClient(t)
	frame, _ := wire.EncodeFrame(&wire.Frame{
		Version:   wire.Version,
		StreamId:  c.id,
		Kind:      "DATA",
		Direction: "input",
		Seq:       1,
	})
	// err 保存调用错误并核对预期 gRPC code；例如输出缺口必须是 DataLoss，不能是成功 EOF。
	if err := c.receiveFrame(frame); status.Code(err) != codes.DataLoss {
		t.Fatal(err)
	}
	// err 保存一次接收结果；按断言区分正常 EOF、业务状态错误及协议 DataLoss。
	if err := c.RecvMsg(&pb.Reply{}); status.Code(err) != codes.DataLoss {
		t.Fatal("protocol error lost to context cancellation:", err)
	}
}
