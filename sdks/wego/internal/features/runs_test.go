package features

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// subscriptionBackend 用建立订阅的屏障控制持久输入读取，证明实时订阅不等待 REST 查询。
type subscriptionBackend struct {
	// Backend 只嵌入本测试不使用的端口。
	ports.Backend
	// started 由 Stream 关闭；RunInput 必须在此之后才能返回。
	started chan struct{}
	// done 在全部订阅与输入读取完成后关闭。
	done chan struct{}
	// input 保存明确的响应契约，不从消息外形猜测模式。
	input json.RawMessage
	// chunk 是实际收到的实时业务消息或 wego 字节封包。
	chunk string
	// inputErr 模拟持久契约读取失败，必须保留而非被取消错误覆盖。
	inputErr error
}

// Begin 验证生命周期登记直到两个并行步骤都退出才释放。
func (b *subscriptionBackend) Begin(ctx context.Context) (context.Context, func(), error) {
	return ctx, func() { close(b.done) }, nil
}

// RunInput 等待订阅建立；若实现先读取输入再订阅，本测试会在预算内失败。
func (b *subscriptionBackend) RunInput(ctx context.Context, _ string) (json.RawMessage, error) {
	select {
	case <-b.started:
		return b.input, b.inputErr
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Stream 先确认消费能力，再交付输出；契约读取失败时等待取消。
func (b *subscriptionBackend) Stream(ctx context.Context, _ string, receive func(string) error) error {
	close(b.started)
	if b.inputErr != nil {
		<-ctx.Done()
		return ctx.Err()
	}
	return receive(b.chunk)
}

// TestRealtimeSubscriptionParallelContractRead 覆盖原生与 wego 实时字节输出及查询失败，使用屏障而非延迟。
func TestRealtimeSubscriptionParallelContractRead(t *testing.T) {
	for _, mode := range []string{"native", "wego", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			config := spec.Defaults()
			backend := &subscriptionBackend{started: make(chan struct{}), done: make(chan struct{}), input: json.RawMessage(`{"message":"native"}`), chunk: "business"}
			if mode == "wego" {
				backend.input = json.RawMessage(`{"_wego":{"version":6,"response":"wego"},"value":{}}`)
				encoded, err := wire.EncodeNativeStream(ctx, config, []byte(backend.chunk))
				if err != nil {
					t.Fatal(err)
				}
				backend.chunk = string(encoded)
			}
			if mode == "invalid" {
				backend.inputErr = errors.New("persistent contract unavailable")
			}
			messages, failures := New(backend, config, nil).Runs.SubscribeToStream(ctx, "run")
			if mode != "invalid" {
				select {
				case message, open := <-messages:
					if !open || message != "business" {
						t.Fatal("decoded business output missing", message, open)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			select {
			case err, open := <-failures:
				if mode == "invalid" && (!open || !errors.Is(err, backend.inputErr)) || mode != "invalid" && open {
					t.Fatal("subscription failure changed", err, open)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-backend.done:
			case <-ctx.Done():
				t.Fatal("SDK subscription registration leaked")
			}
		})
	}
}
