package backend

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// workerEvents 复用实例传输，两个订阅与业务 slots、业务回调队列完全独立
type workerEvents struct {
	// worker 只持有所属进程逻辑身份及本地旧执行索引
	worker *worker
	// config 是此 Worker 的独立配置快照
	config spec.Runtime
	// options 是合并默认值后的通知预算
	options spec.WorkerEventOptions
	// codec 对 JOIN、EVENT、CANCEL 统一进行有界还原
	codec *wire.FrameCodec
	// ctx 与 cancel 管理两个订阅的生命周期
	ctx context.Context
	// cancel 释放订阅，不能等待不响应取消的业务回调
	cancel context.CancelFunc
	// subscriptions 只等待 SDK 拥有的传输退出
	subscriptions sync.WaitGroup
	// errors 提供异步不可恢复协议故障
	errors chan error
	// queue 给业务回调提供独立有界缓冲，取消不会排队到此通道
	queue chan queuedEvent
	// mu 保护已接受但尚未处理完成的业务字节总量
	mu sync.Mutex
	// bytes 同时包含队列和正在处理的事件
	bytes int
	// dropped 统计由于队列预算拒绝交付的业务通知
	dropped atomic.Uint64
	// seen 在 mu 下按有效期维护有界事件 ID 缓存；超出覆盖范围不承诺 exactly-once
	seen map[string]int64
}

// queuedEvent 将完整 trace 载体与业务事件一起保存，不暴露内部协议帧
type queuedEvent struct {
	// event 是已经校验并复制的业务 JSON
	event model.WorkerEvent
	// carrier 保留实例事件 trace 传播
	carrier propagation.MapCarrier
	// cost 是完整编码前事件 JSON 的字节成本
	cost int
}

// newWorkerEvents 不启动资源，缺少有界 codec 能力在接收任务之前失败
func newWorkerEvents(w *worker, config spec.Runtime) (*workerEvents, error) {
	o := config.WorkerEventOptionsFor()
	codec, err := wire.NewFrameCodec(config.Middleware, o.MaxFrameBytes, o.MaxEncodedBytes, w.backend.FrameObserver())
	if err != nil {
		return nil, err
	}
	return &workerEvents{worker: w, config: config, options: o, codec: codec, errors: make(chan error, 1), queue: make(chan queuedEvent, o.QueueMessages), seen: map[string]int64{}}, nil
}

// start 先持久读回两个 JOIN，再启动可恢复订阅，最后才允许 native Worker 消费任务
func (n *workerEvents) start(ctx context.Context) error {
	n.ctx, n.cancel = context.WithCancel(ctx)
	topics := []string{stream.WorkerTopic(n.worker.key), stream.BroadcastTopic}
	cursors := make([]string, len(topics))
	for i, topic := range topics {
		cursor, err := stream.Join(n.ctx, n.worker.backend, n.codec, n.config.Namespace, topic, n.worker.key, n.options)
		if err != nil {
			n.cancel()
			return err
		}
		cursors[i] = cursor
	}
	for i, topic := range topics {
		n.subscriptions.Add(1)
		go n.listen(topic, cursors[i])
	}
	go n.callbacks()
	return nil
}

// close 只等待受 context 约束的传输；业务回调与 handler 一样由应用负责响应取消
func (n *workerEvents) close(ctx context.Context) error {
	if n == nil || n.cancel == nil {
		return nil
	}
	n.cancel()
	done := make(chan struct{})
	go func() { n.subscriptions.Wait(); close(done) }()
	select {
	case <-done:
		// 订阅停止后不会再入队，回收未交付通知；正在运行的业务回调独立占用自己的条目
		for {
			select {
			case item := <-n.queue:
				n.worker.backend.ObserveProtocol(ports.ProtocolMetric{Name: "wego_worker_event_queue_size", Method: stream.EventMethod, Mode: "worker", Outcome: "active", Value: -1})
				n.mu.Lock()
				n.bytes -= item.cost
				n.mu.Unlock()
			default:
				n.mu.Lock()
				n.seen = nil
				n.mu.Unlock()
				return nil
			}
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// listen 从已接受位置重连；新进程只读取自己的 JOIN 之后的消息
func (n *workerEvents) listen(topic, cursor string) {
	defer n.subscriptions.Done()
	delay := 100 * time.Millisecond
	// recoveryStarted 只在通道故障时启动预算；正常无通知的订阅不会因空闲而失败
	var recoveryStarted time.Time
	// probeMessage 冻结本次故障恢复的探针，发布响应不明确时复用完整编码字节
	var probeMessage *ports.DurableMessage
	for n.ctx.Err() == nil {
		attemptCtx, cancel := context.WithCancel(n.ctx)
		// deadline 在收到实际的新帧后停止；连接建立本身没有官方订阅就绪 ACK
		var deadline *time.Timer
		if !recoveryStarted.IsZero() {
			remaining := n.options.RecoveryTimeout - time.Since(recoveryStarted)
			if remaining <= 0 {
				cancel()
				n.reportError(status.Error(codes.Unavailable, "wego: worker event recovery budget exhausted"))
				return
			}
			deadline = time.AfterFunc(remaining, cancel)
			// 持久 JOIN 只作为恢复探针，不变更已处理游标，也不绕过失败帧
			publishCtx, stopPublish := context.WithTimeout(attemptCtx, n.options.PublishTimeout)
			// publishErr 保留编码或固定字节发布的真实故障；没有完成编码不能创建另一个发布操作
			var publishErr error
			if probeMessage == nil {
				probe := &wire.LogFrame{Version: wire.LogVersion, Kind: "JOIN", Method: stream.EventMethod, FrameId: uuid.NewString(), WorkerKey: n.worker.key}
				data, err := n.codec.Encode(publishCtx, stream.EventMethod, probe)
				publishErr = err
				if err == nil {
					probeMessage = &ports.DurableMessage{Namespace: n.config.Namespace, Topic: topic, Producer: stream.IdempotencyKey(n.config.Namespace, stream.EventMethod+":"+topic, probe.FrameId), Sequence: 0, Payload: data}
				}
			}
			if publishErr == nil {
				publishErr = stream.PublishFixed(publishCtx, n.worker.backend, *probeMessage)
			}
			stopPublish()
			if err := publishErr; err != nil {
				deadline.Stop()
				cancel()
				if !retryObservation(err) {
					n.reportError(err)
					return
				}
				if err := waitObservationRetry(n.ctx, delay); err != nil {
					return
				}
				delay = min(delay*2, 2*time.Second)
				continue
			}
		}
		err := n.worker.backend.SubscribeDurable(attemptCtx, ports.DurableSubscription{Namespace: n.config.Namespace, Topic: topic, Cursor: &cursor}, func(entry ports.DurableEntry) error {
			if entry.Cursor == cursor {
				return nil
			}
			budget, cancel := context.WithTimeout(attemptCtx, n.options.DecodeTimeout)
			frame, err := n.codec.Decode(budget, stream.EventMethod, entry.Payload)
			cancel()
			if err != nil {
				return err
			}
			if err := n.accept(topic, frame); err != nil {
				return err
			}
			cursor = entry.Cursor
			delay = 100 * time.Millisecond
			recoveryStarted = time.Time{}
			probeMessage = nil
			if deadline != nil {
				deadline.Stop()
			}
			return nil
		})
		if deadline != nil {
			deadline.Stop()
		}
		cancel()
		if n.ctx.Err() != nil {
			return
		}
		if !recoveryStarted.IsZero() && time.Since(recoveryStarted) >= n.options.RecoveryTimeout {
			n.reportError(status.Error(codes.Unavailable, "wego: worker event recovery budget exhausted"))
			return
		}
		if !retryObservation(err) {
			n.reportError(err)
			return
		}
		if recoveryStarted.IsZero() {
			recoveryStarted = time.Now()
		}
		if err := waitObservationRetry(n.ctx, delay); err != nil {
			return
		}
		delay = min(delay*2, 2*time.Second)
	}
}

// reportError 使事件通道故障进入所属 Worker 的启动/运行错误通道，不能带着失效取消通道继续分配
func (n *workerEvents) reportError(err error) {
	select {
	case n.errors <- err:
	default:
	}
}

// accept 将取消直接发送到执行索引，业务事件只进行非阻塞有界入队
func (n *workerEvents) accept(topic string, frame *wire.LogFrame) error {
	if frame.Version != wire.LogVersion || frame.Method != stream.EventMethod || frame.FrameId == "" {
		return status.Error(codes.DataLoss, "wego: invalid worker notification identity")
	}
	if frame.Kind == "JOIN" {
		return nil
	}
	if frame.ExpiresAt <= time.Now().UnixNano() {
		n.worker.backend.ObserveProtocol(ports.ProtocolMetric{Name: "wego_worker_event_rejected_total", Method: stream.EventMethod, Mode: "worker", Outcome: "expired", Value: 1})
		return nil
	}
	if topic == stream.BroadcastTopic && frame.WorkerKey != "" || topic != stream.BroadcastTopic && frame.WorkerKey != n.worker.key {
		return status.Error(codes.DataLoss, "wego: worker notification address mismatch")
	}
	if frame.Kind == "CANCEL" {
		if topic == stream.BroadcastTopic {
			return status.Error(codes.DataLoss, "wego: internal cancellation cannot broadcast")
		}
		n.worker.runner.cancelNotice(frame)
		return nil
	}
	if frame.Kind != "EVENT" {
		return status.Error(codes.DataLoss, "wego: unknown worker notification kind")
	}
	if len(frame.Payload) > n.options.MaxMessageBytes {
		return status.Error(codes.ResourceExhausted, "wego: worker event payload exceeds limit")
	}
	// event 必须与完整协议帧的消息身份和有效期一致
	var event model.WorkerEvent
	if err := json.Unmarshal(frame.Payload, &event); err != nil || event.ID != frame.FrameId || event.Type == "" || event.ExpiresAt.UnixNano() != frame.ExpiresAt {
		return status.Error(codes.DataLoss, "wego: malformed worker event payload")
	}
	// 相同 ID 在有效期内只处理一次；清除过期项，满缓存时淘汰最早到期的项
	n.mu.Lock()
	now := time.Now().UnixNano()
	oldest, expires := "", int64(^uint64(0)>>1)
	for id, deadline := range n.seen {
		if deadline <= now {
			delete(n.seen, id)
		} else if deadline < expires {
			oldest, expires = id, deadline
		}
	}
	if _, duplicate := n.seen[event.ID]; duplicate {
		n.mu.Unlock()
		return nil
	}
	if len(n.seen) >= n.options.DedupEntries {
		delete(n.seen, oldest)
	}
	n.seen[event.ID] = frame.ExpiresAt
	n.mu.Unlock()
	if n.config.WorkerEventHandler == nil {
		return nil
	}
	event.WorkerKey = n.worker.key
	md := wire.FromMetadata(frame.Metadata)
	item := queuedEvent{event: event, carrier: propagation.MapCarrier{"traceparent": firstMetadata(md["traceparent"]), "tracestate": firstMetadata(md["tracestate"])}, cost: len(frame.Payload)}
	n.mu.Lock()
	if n.bytes+item.cost > n.options.QueueBytes {
		n.mu.Unlock()
		n.drop(event)
		return nil
	}
	n.bytes += item.cost
	select {
	case n.queue <- item:
		n.worker.backend.ObserveProtocol(ports.ProtocolMetric{Name: "wego_worker_event_queue_size", Method: stream.EventMethod, Mode: "worker", Outcome: "active", Value: 1})
		n.mu.Unlock()
	default:
		n.bytes -= item.cost
		n.mu.Unlock()
		n.drop(event)
	}
	return nil
}

// firstMetadata 不从空列表读取第一个值，事件 trace 只使用规范的单值载体
func firstMetadata(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// drop 记录可诊断的拒绝，不承诺队列满后的至少一次业务处理
func (n *workerEvents) drop(event model.WorkerEvent) {
	count := n.dropped.Add(1)
	n.worker.backend.ObserveProtocol(ports.ProtocolMetric{Name: "wego_worker_event_rejected_total", Method: stream.EventMethod, Mode: "worker", Outcome: "full", Value: 1})
	n.config.Logger.Warn("worker event rejected by queue budget", "worker_key", n.worker.key, "event_id", event.ID, "event_type", event.Type, "rejected_total", count)
}

// callbacks 串行处理业务通知；慢回调只能阻塞业务通知队列，不能阻塞定向取消
func (n *workerEvents) callbacks() {
	for {
		if n.ctx.Err() != nil {
			return
		}
		select {
		case <-n.ctx.Done():
			return
		case item := <-n.queue:
			n.worker.backend.ObserveProtocol(ports.ProtocolMetric{Name: "wego_worker_event_queue_size", Method: stream.EventMethod, Mode: "worker", Outcome: "active", Value: -1})
			if n.ctx.Err() != nil || item.event.ExpiresAt.Before(time.Now()) {
				n.mu.Lock()
				n.bytes -= item.cost
				n.mu.Unlock()
				continue
			}
			ctx, cancel := context.WithTimeout(propagation.TraceContext{}.Extract(n.ctx, item.carrier), n.options.CallbackTimeout)
			if n.worker.backend.Bind != nil {
				ctx = n.worker.backend.Bind(ctx)
			}
			err := eventCallback(n.config.WorkerEventHandler, ctx, item.event)
			cancel()
			n.mu.Lock()
			n.bytes -= item.cost
			n.mu.Unlock()
			if err != nil {
				n.config.Logger.Warn("worker event callback failed", "worker_key", n.worker.key, "event_id", item.event.ID, "error", err)
			}
		}
	}
}

// eventCallback 把业务回调 panic 转为诊断，不能杀死内部取消订阅
func eventCallback(handler func(context.Context, model.WorkerEvent) error, ctx context.Context, event model.WorkerEvent) (err error) {
	defer func() {
		if recover() != nil {
			err = status.Error(codes.Internal, "wego: worker event callback panic")
		}
	}()
	return handler(ctx, event)
}

// mergeErrors 用同一实例生命周期合并监听和通知故障，关闭后停止发送
func mergeErrors(ctx context.Context, left, right <-chan error) <-chan error {
	out := make(chan error, 1)
	go func() {
		defer close(out)
		for left != nil || right != nil {
			// err 是被选择入口的诊断，通道结束不产生伪造故障
			var err error
			select {
			case <-ctx.Done():
				return
			case value, ok := <-left:
				if !ok {
					left = nil
					continue
				}
				err = value
			case value, ok := <-right:
				if !ok {
					right = nil
					continue
				}
				err = value
			}
			if err != nil {
				select {
				case out <- Normalize(err):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
