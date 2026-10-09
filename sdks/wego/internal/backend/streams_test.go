package backend

import (
	"context"
	"errors"
	"io"
	"testing"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

// durableClientFixture 提供正式生成客户端的替身，回调关闭仍走真实 backend 订阅管理
type durableClientFixture struct {
	// V1StreamsClient 委托未使用的协议方法；本测试只替换 Subscribe
	v1.V1StreamsClient
	// receiver 保存当前子订阅的生命周期和待返回消息
	receiver *durableReceiverFixture
}

// Subscribe 保存 backend 新建的派生 context，便于验证回调提前返回会释放远端订阅
func (f *durableClientFixture) Subscribe(ctx context.Context, _ *v1.SubscribeStreamRequest, _ ...grpc.CallOption) (v1.V1Streams_SubscribeClient, error) {
	f.receiver.ctx = ctx
	return f.receiver, nil
}

// durableReceiverFixture 模拟最后一批消息、hangup 和显式接收错误
type durableReceiverFixture struct {
	// ClientStream 提供此测试未调用的通用流方法
	grpc.ClientStream
	// ctx 是 backend 拥有的订阅 context
	ctx context.Context
	// response 只交付一次的最后批次
	response *v1.StreamMessage
	// err 在最后批次之后返回，EOF 必须被视为可重连故障
	err error
}

// Recv 按批次再错误的顺序返回，不用 sleep 伪造网络状态
func (f *durableReceiverFixture) Recv() (*v1.StreamMessage, error) {
	if f.response != nil {
		response := f.response
		f.response = nil
		return response, nil
	}
	return nil, f.err
}

// TestDurableSubscriptionFailureAndRelease 回调、hangup、EOF 和远端错误均保持明确语义
func TestDurableSubscriptionFailureAndRelease(t *testing.T) {
	for _, scenario := range []string{"callback", "hangup", "eof", "remote", "missing_cursor"} {
		t.Run(scenario, func(t *testing.T) {
			callbackError := errors.New("fixture consumer finished")
			receiver := &durableReceiverFixture{err: io.EOF}
			if scenario != "eof" && scenario != "remote" {
				receiver.response = &v1.StreamMessage{Entries: []*v1.StreamEntry{{Payload: []byte("fixture"), Cursor: "cursor"}}, Hangup: scenario == "hangup"}
			}
			if scenario == "remote" {
				receiver.err = status.Error(codes.OutOfRange, "expired cursor")
			}
			if scenario == "missing_cursor" {
				receiver.response.Entries[0].Cursor = ""
			}
			b := &Backend{durableStreams: &durableClientFixture{receiver: receiver}}
			calls := 0
			err := b.SubscribeDurable(context.Background(), ports.DurableSubscription{Topic: "fixture"}, func(ports.DurableEntry) error {
				calls++
				if scenario == "callback" {
					return callbackError
				}
				return nil
			})
			if receiver.ctx.Err() != context.Canceled {
				t.Fatal("remote subscription was retained after consumer exit")
			}
			switch scenario {
			case "callback":
				if !errors.Is(err, callbackError) {
					t.Fatal(err)
				}
			case "hangup":
				if calls != 1 || status.Code(err) != codes.Unavailable {
					t.Fatalf("last batch lost: calls=%d err=%v", calls, err)
				}
			case "eof":
				if status.Code(err) != codes.Unavailable {
					t.Fatal(err)
				}
			case "remote":
				if status.Code(err) != codes.OutOfRange {
					t.Fatal(err)
				}
			case "missing_cursor":
				if status.Code(err) != codes.DataLoss || calls != 0 {
					t.Fatal(err)
				}
			}
		})
	}
}

// TestDurablePublishLimits 在任何真实连接调用之前验证完整请求和输入范围
func TestDurablePublishLimits(t *testing.T) {
	b := &Backend{}
	for _, scenario := range []struct {
		// name 是限制场景，message 是尚未提交的明确输入
		name string
		// message 不含凭证，失败路径不得调用未配置的连接
		message ports.DurableMessage
		// code 是调用方可以诊断的预期状态
		code codes.Code
	}{
		{"oversized_payload", ports.DurableMessage{Topic: "fixture", Producer: "producer", Payload: make([]byte, durablePayloadLimit+1)}, codes.ResourceExhausted},
		{"negative_sequence", ports.DurableMessage{Topic: "fixture", Producer: "producer", Sequence: -1, Payload: []byte("x")}, codes.InvalidArgument},
		{"empty_topic", ports.DurableMessage{Producer: "producer", Payload: []byte("x")}, codes.InvalidArgument},
		{"invalid_utf8", ports.DurableMessage{Topic: "\xff", Producer: "producer", Payload: []byte("x")}, codes.InvalidArgument},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			if err := b.PublishDurable(context.Background(), scenario.message); status.Code(err) != scenario.code {
				t.Fatal(err)
			}
		})
	}
}
