package backend

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// publishWorkerEvent 冻结事件快照，完整 EVENT 统一经过实例 codec
func (b *Backend) publishWorkerEvent(ctx context.Context, request ports.WorkerSendEvent) error {
	o := b.config.WorkerEventOptionsFor()
	if request.WorkerKey != "" {
		if err := uuid.Validate(request.WorkerKey); err != nil {
			return status.Error(codes.InvalidArgument, "wego: directed worker key must be UUID")
		}
	}
	event := request.Event
	if event.ID == "" {
		event.ID = uuid.NewString()
	}
	if event.Type == "" || len(event.ID) > 255 || len(event.Type) > 255 {
		return status.Error(codes.InvalidArgument, "wego: invalid worker event identity or type")
	}
	if len(event.Payload) > o.MaxMessageBytes {
		return status.Error(codes.ResourceExhausted, "wego: worker event payload exceeds limit")
	}
	if len(event.Payload) > 0 && !json.Valid(event.Payload) {
		return status.Error(codes.InvalidArgument, "wego: worker event requires bounded valid JSON")
	}
	if event.SentAt.IsZero() {
		event.SentAt = time.Now().UTC()
	}
	if event.ExpiresAt.IsZero() {
		event.ExpiresAt = event.SentAt.Add(o.TTL)
	}
	if !event.ExpiresAt.After(event.SentAt) || !event.ExpiresAt.After(time.Now()) {
		return status.Error(codes.InvalidArgument, "wego: worker event is already expired")
	}
	event.WorkerKey = ""
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if len(payload) > o.MaxMessageBytes {
		return status.Error(codes.ResourceExhausted, "wego: complete worker event exceeds limit")
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	frame := &wire.LogFrame{Version: wire.LogVersion, Kind: "EVENT", Method: stream.EventMethod, WorkerKey: request.WorkerKey, FrameId: event.ID, Payload: payload, ExpiresAt: event.ExpiresAt.UnixNano(), Metadata: wire.Metadata(metadata.Pairs("traceparent", carrier["traceparent"], "tracestate", carrier["tracestate"]))}
	return b.publishWorkerFrame(ctx, request.WorkerKey, frame)
}

// publishWorkerCancel 明确限定旧代次及 writer，发送失败不会更换消息身份
func (b *Backend) publishWorkerCancel(ctx context.Context, notice ports.WorkerCancelNotice) error {
	if uuid.Validate(notice.WorkerKey) != nil || notice.TaskID == "" || notice.OldEpoch < 0 || notice.NewEpoch <= notice.OldEpoch || notice.OldWriter == "" {
		return status.Error(codes.InvalidArgument, "wego: invalid old execution cancellation identity")
	}
	frame := &wire.LogFrame{Version: wire.LogVersion, Kind: "CANCEL", Method: stream.EventMethod, FrameId: uuid.NewString(), TaskRunId: notice.TaskID, WorkerKey: notice.WorkerKey, OldEpoch: notice.OldEpoch, OldWriter: notice.OldWriter, Epoch: notice.NewEpoch, ExpiresAt: time.Now().Add(30 * time.Second).UnixNano()}
	return b.publishWorkerFrame(ctx, notice.WorkerKey, frame)
}

// publishWorkerFrame 复用实例连接和固定编码字节；每个消息使用作用域内稳定 producer 的 seq=0
func (b *Backend) publishWorkerFrame(ctx context.Context, key string, frame *wire.LogFrame) error {
	o := b.config.WorkerEventOptionsFor()
	codec, err := wire.NewFrameCodec(b.config.Middleware, o.MaxFrameBytes, o.MaxEncodedBytes, b.FrameObserver())
	if err != nil {
		return err
	}
	budget, cancel := context.WithTimeout(ctx, o.PublishTimeout)
	defer cancel()
	data, err := codec.Encode(budget, stream.EventMethod, frame)
	if err != nil {
		return err
	}
	topic := stream.BroadcastTopic
	if key != "" {
		topic = stream.WorkerTopic(key)
	}
	producer := stream.IdempotencyKey(b.config.Namespace, stream.EventMethod+":"+topic, frame.FrameId)
	return stream.PublishFixed(budget, b, ports.DurableMessage{Namespace: b.config.Namespace, Topic: topic, Producer: producer, Sequence: 0, Payload: data})
}
