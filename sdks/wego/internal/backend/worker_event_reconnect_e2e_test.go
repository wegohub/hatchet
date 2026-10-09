//go:build e2e

package backend

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// eventReconnectClient 保留真实发布和存储，只在定向订阅读到一次事件后制造断流
type eventReconnectClient struct {
	// V1StreamsClient 是未经修改的正式实例连接
	v1.V1StreamsClient
	// codec 用与 Worker 相同的解码判断注入位置，JOIN 不能触发注入
	codec *wire.FrameCodec
	// subscriptions 记录定向订阅实际重新建立的次数
	subscriptions atomic.Int32
	// disconnected 限制一次故障，重连不能重复破坏消息
	disconnected atomic.Bool
}

// Subscribe 委托实际远端订阅；广播订阅不参与本次定向故障
func (p *eventReconnectClient) Subscribe(ctx context.Context, request *v1.SubscribeStreamRequest, options ...grpc.CallOption) (v1.V1Streams_SubscribeClient, error) {
	receiver, err := p.V1StreamsClient.Subscribe(ctx, request, options...)
	if err != nil {
		return nil, err
	}
	if request.Topic == stream.BroadcastTopic {
		return receiver, nil
	}
	p.subscriptions.Add(1)
	return &eventReconnectReceiver{V1Streams_SubscribeClient: receiver, peer: p, ctx: ctx}, nil
}

// eventReconnectReceiver 丢弃后续接收连接，不丢弃已经确认交给订阅者的事件
type eventReconnectReceiver struct {
	// V1Streams_SubscribeClient 提供实际 gRPC 收取和关闭上下文
	v1.V1Streams_SubscribeClient
	// peer 保存唯一故障和解码契约
	peer *eventReconnectClient
	// ctx 与当前底层订阅绑定
	ctx context.Context
	// failNext 在完整 EVENT 批次交付后才断开
	failNext bool
}

// Recv 返回真实远端帧，下一次接收只注入一次 Unavailable
func (r *eventReconnectReceiver) Recv() (*v1.StreamMessage, error) {
	if r.failNext {
		r.failNext = false
		return nil, status.Error(codes.Unavailable, "injected directed subscription loss")
	}
	message, err := r.V1Streams_SubscribeClient.Recv()
	if err != nil {
		return nil, err
	}
	for _, entry := range message.Entries {
		frame, decodeErr := r.peer.codec.Decode(r.ctx, stream.EventMethod, entry.Payload)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if frame.Kind == "EVENT" && r.peer.disconnected.CompareAndSwap(false, true) {
			r.failNext = true
		}
	}
	return message, nil
}

// TestWorkerEventReconnect 验证真实持久回读、重连后的通知与稳定实例身份，不创建控制任务或额外 Worker
func TestWorkerEventReconnect(t *testing.T) {
	b, config, ctx := p0Backend(t)
	codec, err := wire.NewFrameCodec(nil, 4<<20, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	peer := &eventReconnectClient{V1StreamsClient: b.durableStreams, codec: codec}
	b.durableStreams = peer
	received := make(chan string, 4)
	config.WorkerEventHandler = func(ctx context.Context, event model.WorkerEvent) error {
		select {
		case received <- event.ID:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w, err := b.Worker(ctx, "", []ports.Definition{{Name: "notification-probe", Policy: spec.Task{}, Function: func(context.Context, any) (any, error) { return "ready", nil }}}, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		budget, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := w.Close(budget); err != nil {
			t.Error(err)
		}
	})
	key, id := w.(ports.WorkerIdentity).WorkerKey(), w.ID()
	for _, eventID := range []string{"before-disconnect", "after-disconnect"} {
		if err := b.Feature(ctx, ports.WorkerSendEvent{WorkerKey: key, Event: model.WorkerEvent{ID: eventID, Type: "fixture"}}, nil); err != nil {
			t.Fatal(err)
		}
		select {
		case actual := <-received:
			if actual != eventID {
				t.Fatal("duplicate or reordered notification", actual, eventID)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if !peer.disconnected.Load() || peer.subscriptions.Load() < 2 || w.ID() != id || w.(ports.WorkerIdentity).WorkerKey() != key {
		t.Fatal("subscription did not recover stable Worker identity")
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	writeP0Report(t, b, "v020-event-reconnect", map[string]any{"worker_key": key, "worker_id": id, "subscriptions": peer.subscriptions.Load(), "assertions": []string{"real EVENT accepted before disconnect", "second EVENT recovered from real persistent log", "random worker_key and registered WorkerID remain unchanged"}, "cleanup": "worker stopped; probe definition and topic history retained"})
}
