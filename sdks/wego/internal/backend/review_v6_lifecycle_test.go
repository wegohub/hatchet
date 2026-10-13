package backend

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// reviewFilterContext 提供事件 Filter 和最小执行身份，不建立真实后端连接。
type reviewFilterContext struct {
	// memoContext 复用任务身份读取接口，未涉及的 durable 能力不会被调用。
	memoContext
	// filter 是已封包的 Filter 载荷，用于模拟下载阶段。
	filter map[string]any
}

// GetContext 提供可由 wrapper 独立取消的根上下文。
func (*reviewFilterContext) GetContext() context.Context { return context.Background() }

// FilterPayload 返回测试构造的消息，业务 handler 前必须先还原。
func (c *reviewFilterContext) FilterPayload() map[string]any { return c.filter }

// reviewBlockingCodec 在解码时等待取消，模拟遵守 context 的对象下载。
type reviewBlockingCodec struct {
	// entered 作为确定性屏障，测试不会在下载开始前发出停止。
	entered chan struct{}
}

// CodecID 使用固定测试格式标识。
func (*reviewBlockingCodec) CodecID() string { return "test.blocking.v1" }

// Encode 保留原字节，只测试接收端下载的生命周期。
func (*reviewBlockingCodec) Encode(_ context.Context, data []byte) ([]byte, error) {
	return data, nil
}

// Decode 与 SDK 使用的有界入口共享取消行为。
func (c *reviewBlockingCodec) Decode(ctx context.Context, data []byte) ([]byte, error) {
	return c.DecodeLimit(ctx, data, len(data))
}

// DecodeLimit 用屏障通知外层下载已开始，然后等待本地执行取消。
func (c *reviewBlockingCodec) DecodeLimit(ctx context.Context, _ []byte, _ int) ([]byte, error) {
	close(c.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestReviewV6FilterDecodeRegisteredForCancellation 下载开始前已有取消句柄，停止后登记必须释放。
func TestReviewV6FilterDecodeRegisteredForCancellation(t *testing.T) {
	codec := &reviewBlockingCodec{entered: make(chan struct{})}
	config := spec.Defaults()
	config.Middleware = []middleware.Option{middleware.WithPayload(codec)}
	packet, err := wire.Seal(context.Background(), wire.NewEnvelope(context.Background(), "", "", json.RawMessage(`{}`), nil), config, 1024, 2048, 4096)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(packet)
	input := &reviewFilterContext{}
	if err := json.Unmarshal(encoded, &input.filter); err != nil {
		t.Fatal(err)
	}
	b := &Backend{observer: &transport{cancels: map[uint64]executionCancel{}}}
	wrapped := b.wrap(func(context.Context, any) (any, error) {
		t.Error("canceled Filter reached business handler")
		return nil, nil
	}, config, "task", "", spec.Task{}, "task")
	done := make(chan error, 1)
	go func() { _, err := wrapped(input, map[string]any{}); done <- err }()
	select {
	case <-codec.entered:
	case <-time.After(time.Second):
		t.Fatal("Filter decode did not start")
	}
	b.observer.mu.Lock()
	count := len(b.observer.cancels)
	for _, execution := range b.observer.cancels {
		execution.cancel()
	}
	b.observer.mu.Unlock()
	if count != 1 {
		t.Fatal("Filter I/O has no execution cancellation registration")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || len(b.observer.cancels) != 0 {
			t.Fatal("cancellation or cleanup lost", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Filter download survived cancellation")
	}
}

// TestReviewV6ClosePreservesCleanupFailure 监听退出取消不能覆盖注销失败，并发 Close 只清理一次。
func TestReviewV6ClosePreservesCleanupFailure(t *testing.T) {
	// calls 由并发关闭者观察，清理操作必须只执行一次。
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observer := &transport{unregisterBudgets: map[string]context.Context{}, listenerDone: map[string]chan struct{}{"key": make(chan struct{})}}
	w := &worker{key: "key", backend: &Backend{observer: observer}, cleanup: func(context.Context) error {
		calls.Add(1)
		// 注销回调内触发取消，避免用极短 deadline 猜测并发调度时机。
		cancel()
		return errors.New("unregister failed")
	}}
	errorsOut := make(chan error, 2)
	for range 2 {
		go func() { errorsOut <- w.Close(ctx) }()
	}
	for range 2 {
		err := <-errorsOut
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "unregister failed") {
			t.Fatal("cleanup cause overwritten", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("duplicate cleanup", calls.Load())
	}
}
