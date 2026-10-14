package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// EventMethod 为定向和广播消息使用相同 codec 路径，不把 topic 地址当业务 RPC 方法
const EventMethod = "/wego.WorkerEvents"

// Join 通过完整 codec 编码、持久发布和读回自己的 JOIN 确定加入位置
// Publish 的空响应不能作为订阅游标；新 Worker 不交付此游标之前的业务广播
func Join(ctx context.Context, transport ports.DurableStreams, codec *wire.FrameCodec, namespace, topic, workerKey string, options ...spec.WorkerEventOptions) (string, error) {
	if workerKey == "" {
		return "", status.Error(codes.InvalidArgument, "wego: JOIN requires worker key")
	}
	// defaults 保存可寻址的默认配置，读取预算不改变这份配置。
	defaults := spec.Defaults()
	o := defaults.WorkerEventOptionsFor()
	if len(options) > 0 {
		o = options[0]
	}
	ctx, cancel := context.WithTimeout(ctx, o.JoinTimeout)
	defer cancel()
	id := uuid.NewString()
	frame := &wire.LogFrame{Version: wire.LogVersion, Kind: "JOIN", Method: EventMethod, WorkerKey: workerKey, FrameId: id}
	publishCtx, stopPublish := context.WithTimeout(ctx, o.PublishTimeout)
	defer stopPublish()
	encoded, err := codec.Encode(publishCtx, EventMethod, frame)
	if err != nil {
		return "", err
	}
	message := ports.DurableMessage{Namespace: namespace, Topic: topic, Producer: id, Sequence: 0, Payload: encoded}
	if err := PublishFixed(publishCtx, transport, message); err != nil {
		return "", err
	}
	cursor := ""
	frames, encodedBytes := 0, 0
	completed := errors.New("join barrier confirmed")
	err = SubscribeResumable(ctx, transport, ports.DurableSubscription{Namespace: namespace, Topic: topic}, func(entry ports.DurableEntry) error {
		frames++
		if frames > o.MaxReplayFrames || len(entry.Payload) > o.MaxReplayBytes-encodedBytes {
			return status.Error(codes.ResourceExhausted, "wego: JOIN scan exceeds frame/encoded-byte budget")
		}
		encodedBytes += len(entry.Payload)
		// 新实例只从自己的 JOIN 之后接收通知。此前的历史可能引用已过期对象，
		// 因此先比较本次发布时冻结的完整字节，不对无关历史执行 codec I/O。
		if !bytes.Equal(entry.Payload, encoded) {
			return nil
		}
		// 命中的 JOIN 必须能完整还原；字节匹配本身不能证明对象仍存在或密钥正确。
		decodeCtx, stopDecode := context.WithTimeout(ctx, o.DecodeTimeout)
		decoded, err := codec.Decode(decodeCtx, EventMethod, entry.Payload)
		stopDecode()
		if err != nil {
			return fmt.Errorf("wego: JOIN readback topic=%q: %w", topic, err)
		}
		if proto.Size(decoded) > o.MaxReplayBytes {
			return status.Error(codes.ResourceExhausted, "wego: JOIN scan exceeds restored-byte budget")
		}
		if decoded.Kind != "JOIN" || decoded.Method != EventMethod || decoded.FrameId != id || decoded.WorkerKey != workerKey || entry.Cursor == "" {
			return status.Error(codes.DataLoss, "wego: JOIN readback does not match published barrier")
		}
		cursor = entry.Cursor
		return completed
	})
	if errors.Is(err, completed) {
		return cursor, nil
	}
	return "", err
}
