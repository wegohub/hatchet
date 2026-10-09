package backend

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
)

// eventRecoveryPeer 第一次断连，随后可选择读回探针或持续不可达
type eventRecoveryPeer struct {
	// V1StreamsClient 保留未调用的正式协议方法
	v1.V1StreamsClient
	// calls 统计实际订阅，读回恢复不能只依靠 Publish 返回成功
	calls atomic.Int32
	// mu 保护已发布完整探针字节
	mu sync.Mutex
	// payload 是正式 Publish 请求中的冻结帧
	payload []byte
	// deliver 决定第二个订阅能否真正读取探针
	deliver bool
	// healthy 只有首次帧已被消费者处理并再次 Recv 才关闭
	healthy chan struct{}
	// once 防止重连关闭两次事实屏障
	once sync.Once
}

// Publish 保存实际经过 codec 的完整帧，不直接调用事件解释器
func (p *eventRecoveryPeer) Publish(_ context.Context, request *v1.PublishStreamMessageRequest, _ ...grpc.CallOption) (*v1.PublishStreamMessageResponse, error) {
	p.mu.Lock()
	p.payload = append([]byte(nil), request.Payload...)
	p.mu.Unlock()
	return &v1.PublishStreamMessageResponse{}, nil
}

// Subscribe 首次返回传输错误，之后按真实帧读取与 context 生命周期工作
func (p *eventRecoveryPeer) Subscribe(ctx context.Context, _ *v1.SubscribeStreamRequest, _ ...grpc.CallOption) (v1.V1Streams_SubscribeClient, error) {
	if p.calls.Add(1) == 1 {
		return nil, status.Error(codes.Unavailable, "disconnected")
	}
	p.mu.Lock()
	payload := append([]byte(nil), p.payload...)
	p.mu.Unlock()
	if !p.deliver {
		payload = nil
	}
	return &eventRecoveryReceiver{ctx: ctx, payload: payload, peer: p}, nil
}

// eventRecoveryReceiver 提供一个真实探针，随后保持正常空闲
type eventRecoveryReceiver struct {
	// ClientStream 未使用的方法不影响 Recv 生命周期
	grpc.ClientStream
	// ctx 是 backend 为本次订阅拥有的可取消上下文
	ctx context.Context
	// payload 只交付一次，不能反复发探针掩盖空闲超时
	payload []byte
	// peer 保存恢复完成的事实屏障
	peer *eventRecoveryPeer
}

// Recv 读回实际 Publish 字节；静默连接只能由订阅预算或关闭取消
func (r *eventRecoveryReceiver) Recv() (*v1.StreamMessage, error) {
	if r.payload != nil {
		payload := r.payload
		r.payload = nil
		return &v1.StreamMessage{Entries: []*v1.StreamEntry{{Payload: payload, Cursor: "recovered"}}}, nil
	}
	if r.peer.deliver {
		r.peer.once.Do(func() { close(r.peer.healthy) })
	}
	<-r.ctx.Done()
	return nil, r.ctx.Err()
}

// TestWorkerEventRecoveryBudget 真实读回会停止恢复计时器，未读回则有限失败并交给 Worker 错误通道
func TestWorkerEventRecoveryBudget(t *testing.T) {
	for _, deliver := range []bool{true, false} {
		t.Run(map[bool]string{true: "recovered_idle", false: "unreachable"}[deliver], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			config := spec.Defaults()
			config.WorkerEvents.RecoveryTimeout = 300 * time.Millisecond
			peer := &eventRecoveryPeer{deliver: deliver, healthy: make(chan struct{})}
			w := &worker{key: "fixture-key", backend: &Backend{config: config, durableStreams: peer}}
			n, err := newWorkerEvents(w, config)
			if err != nil {
				t.Fatal(err)
			}
			n.ctx = ctx
			done := make(chan struct{})
			n.subscriptions.Add(1)
			go func() { n.listen(stream.WorkerTopic(w.key), "initial"); close(done) }()
			defer func() { cancel(); <-done }()
			if !deliver {
				select {
				case err := <-n.errors:
					if status.Code(err) != codes.Unavailable {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("recovery never terminated", ctx.Err())
				}
				return
			}
			select {
			case <-peer.healthy:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// 等待超过明确的恢复预算，验证正常空闲不会继承故障计时器
			timer := time.NewTimer(2 * config.WorkerEvents.RecoveryTimeout)
			defer timer.Stop()
			select {
			case err := <-n.errors:
				t.Fatal("healthy idle channel timed out", err)
			case <-timer.C:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}
