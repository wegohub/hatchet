package session

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// 评审专用：非法终态应结束会话，而不是遗留在途调用。
func TestReviewMalformedFinalReleasesClientLifetime(t *testing.T) {
	// c 构造带独立取消函数的接收端，致命错误必须结束此会话。
	c := receivingClient(t)
	c.final = &wire.StreamResult{Status: []byte{0xff}}
	c.headersReady = make(chan struct{})
	// err 接收并解码业务输出，失败时应取消订阅并保存稳定错误。
	if err := c.RecvMsg(&pb.Reply{}); status.Code(err) != codes.DataLoss {
		t.Fatal(err)
	}
	if c.ctx.Err() == nil {
		t.Error("malformed terminal returned an error but left stream context alive")
	}
}

// acceptingControl 模拟可确认的累计 ACK，解码错误发生在真正消费了 DATA 之后。
type acceptingControl struct {
	// Backend 委托无关操作，当前测试只需要控制请求成功。
	ports.Backend
}

// Run 确认控制任务，业务 DATA 的协议序号与 ACK 不依赖测试 goroutine 调度。
func (b acceptingControl) Run(_ context.Context, _ string, _ any, _ model.RunOptions) (ports.Run, error) {
	return ports.Run{ID: "control", Wait: func(context.Context) (ports.Result, error) {
		return ports.Result{Outputs: map[string]any{"control": Acknowledgment{}}}, nil
	}}, nil
}

// TestMalformedBusinessPayloadReleasesLifetime 非法 protobuf DATA 返回解码错误后须结束订阅与调用登记。
func TestMalformedBusinessPayloadReleasesLifetime(t *testing.T) {
	// c 是可观察取消状态的真实接收端，第一条输出包含非法 protobuf 字节。
	c := receivingClient(t)
	c.backend = acceptingControl{}
	c.messages[1] = []byte{0xff}
	c.inputBytes = 1
	// err 是 RecvMsg 内部解码错误，不能继续等待后续消息。
	err := c.RecvMsg(&pb.Reply{})
	if err == nil || c.ctx.Err() == nil {
		t.Fatalf("decode error did not terminate stream: %v", err)
	}
	if c.failure != err {
		t.Fatal("stream did not retain original decode error")
	}
}

// rejectedDecode 模拟对象下载或解密失败，错误实例必须作为会话终态保留。
type rejectedDecode struct {
	// err 是受控 I/O 原因，不能转换成成功 EOF 或吞掉。
	err error
}

// Encode 保持业务编码，本测试只针对接收侧失败清理。
func (p rejectedDecode) Encode(_ context.Context, _ string, data []byte) ([]byte, error) {
	return data, nil
}

// Decode 返回既定失败，模拟权限拒绝或对象损坏。
func (p rejectedDecode) Decode(context.Context, string, []byte) ([]byte, error) { return nil, p.err }

// TestMiddlewareDecodeFailureReleasesLifetime 在无 deadline 条件下，变换失败仍须取消订阅和调用登记。
func TestMiddlewareDecodeFailureReleasesLifetime(t *testing.T) {
	// cause 表示对象存储下载失败，后续读取必须保留这一终态。
	cause := errors.New("object payload unavailable")
	// c 先确认真实 DATA 的 ACK，再进入受控解码错误。
	c := receivingClient(t)
	c.backend = acceptingControl{}
	c.config.Middleware = []middleware.Option{middleware.WithPayload(rejectedDecode{err: cause})}
	c.messages[1] = []byte{1}
	c.inputBytes = 1
	// err 必须保留原因，且退出后无须调用方额外取消。
	err := c.RecvMsg(&pb.Reply{})
	if !errors.Is(err, cause) || c.ctx.Err() == nil || c.failure != err {
		t.Fatalf("decode lifetime not released: %v", err)
	}
}

// TestMalformedFinalResponseRetainsFailure client stream 最终响应解码失败后不可被成功状态覆盖。
func TestMalformedFinalResponseRetainsFailure(t *testing.T) {
	// c 没有 DATA，响应只从最终结果读取，status 表示 OK 也不能掩盖损坏 protobuf。
	c := receivingClient(t)
	c.final = &wire.StreamResult{HasResponse: true, Response: []byte{0xff}}
	// err 应同时释放生命周期、保存原因并让观测报告失败。
	err := c.RecvMsg(&pb.Reply{})
	if err == nil || c.ctx.Err() == nil || c.failure != err || c.completionError() != err {
		t.Fatalf("final response error lost: %v", err)
	}
}
