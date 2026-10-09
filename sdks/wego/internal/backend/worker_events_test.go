package backend

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestBlockedEventCallbackDoesNotBlockCancellation 用事实屏障阻塞业务回调，填满队列后确认取消仍能直接完成
func TestBlockedEventCallbackDoesNotBlockCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	business, stopBusiness := context.WithCancel(ctx)
	defer stopBusiness()
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	// once 使回调屏障只在首次进入时关闭，其余队列项不能制造 panic
	var once sync.Once
	config := spec.Defaults()
	config.WorkerEvents.QueueMessages = 64
	config.WorkerEventHandler = func(ctx context.Context, _ model.WorkerEvent) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w := &worker{key: "current", backend: &Backend{}, runner: &gatedRunner{dispatcher: &dispatcher{key: "current"}, latest: map[string]*gatedAttempt{"task": {action: gateAction(0), writer: "writer", cancel: stopBusiness}}}}
	n, err := newWorkerEvents(w, config)
	if err != nil {
		t.Fatal(err)
	}
	n.ctx = ctx
	go func() { n.callbacks(); close(returned) }()
	defer func() { cancel(); close(release); <-returned }()
	// frame 来自完整 codec 之后；消息有效期及身份与 payload 完整一致
	event := model.WorkerEvent{ID: "event", Type: "fixture", SentAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute)}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	frame := &wire.LogFrame{Version: wire.LogVersion, Kind: "EVENT", Method: stream.EventMethod, FrameId: event.ID, WorkerKey: w.key, Payload: payload, ExpiresAt: event.ExpiresAt.UnixNano()}
	if err := n.accept(stream.WorkerTopic(w.key), frame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for i := 0; i < 65; i++ {
		event.ID = "queued-" + strconv.Itoa(i)
		frame.FrameId, frame.Payload = event.ID, mustEventJSON(t, event)
		if err := n.accept(stream.WorkerTopic(w.key), frame); err != nil {
			t.Fatal(err)
		}
	}
	if n.dropped.Load() != 1 || len(n.queue) != 64 {
		t.Fatal("business queue is not bounded")
	}
	if err := n.accept(stream.WorkerTopic(w.key), &wire.LogFrame{Version: wire.LogVersion, Kind: "CANCEL", Method: stream.EventMethod, FrameId: "cancel", WorkerKey: w.key, TaskRunId: "task", OldEpoch: 0, OldWriter: "writer", Epoch: 1, ExpiresAt: event.ExpiresAt.UnixNano()}); err != nil {
		t.Fatal(err)
	}
	if business.Err() != context.Canceled {
		t.Fatal("full business queue blocked internal cancellation")
	}
}

// mustEventJSON 只编码测试 fixture，失败不能被忽略后当作空事件
func mustEventJSON(t *testing.T, event model.WorkerEvent) []byte {
	t.Helper()
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
