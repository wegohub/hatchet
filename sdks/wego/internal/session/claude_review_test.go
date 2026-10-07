package session

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// readinessBackend 模拟 READY 已到但 PING 结果等待未完成，验证两个信号彼此独立。
type readinessBackend struct {
	// Backend 未涉及的端口由嵌入占位，不在本测试中调用。
	ports.Backend
	// client 接收匹配的订阅 nonce，不通过任务结果伪造 READY。
	client *Client
	// pings 累计实际重发次数，确保最多一个在途请求且有退避。
	pings atomic.Int32
	// ready 为 true 时主动交付匹配 nonce，false 时直到预算到期也不就绪。
	ready bool
	// pingStopped 确认等待被取消且退出，OPEN 不能先于该退出屏障。
	pingStopped chan struct{}
}

// Run 在 READY 场景故意阻塞 PING 的结果读取，但输出订阅已经证明会话就绪。
func (b *readinessBackend) Run(ctx context.Context, _ string, input any, _ model.RunOptions) (ports.Run, error) {
	// control 由生产 client.control 编码，不手动调用握手函数内部信号。
	control := input.(Control)
	if control.Kind == "CANCEL" {
		<-ctx.Done()
		return ports.Run{}, ctx.Err()
	}
	if control.Kind == "PING" {
		b.pings.Add(1)
		if b.ready {
			return ports.Run{ID: "ping", Wait: func(budget context.Context) (ports.Result, error) {
				b.client.ready <- "wrong-nonce"
				b.client.ready <- b.client.id
				<-budget.Done()
				close(b.pingStopped)
				return ports.Result{}, budget.Err()
			}}, nil
		}
	}
	if control.Kind == "OPEN" {
		select {
		case <-b.pingStopped:
		default:
			return ports.Run{}, errors.New("PING wait still owns resources")
		}
	}
	return ports.Run{ID: "control", Wait: func(context.Context) (ports.Result, error) {
		return ports.Result{Outputs: map[string]any{"control": Acknowledgment{}}}, nil
	}}, nil
}

// TestReadyDoesNotWaitForPingResult 验证 READY 能及时打断仍在等结果的 PING，OPEN 前回收等待。
func TestReadyDoesNotWaitForPingResult(t *testing.T) {
	// c 使用真实客户端状态，仅传输出口由本测试控制。
	c := receivingClient(t)
	c.id, c.ready, c.config = "session", make(chan string, 8), spec.Defaults()
	// backend 的 PING 永不返回成功结果，只有输出订阅交付 READY。
	backend := &readinessBackend{client: c, ready: true, pingStopped: make(chan struct{})}
	c.backend = backend
	// ctx 限制整个握手；若仍依赖 PING 结果，测试必然超时。
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// err 保留真实握手或预算结果，不以人工 READY 代替 OPEN 确认。
	if err := c.openWhenReady(ctx); err != nil {
		t.Fatal(err)
	}
	if backend.pings.Load() != 1 {
		t.Fatal("READY caused redundant PING submissions")
	}
}

// TestHandshakePingsAreBounded 验证缺失 READY 不会以紧密循环持续向控制队列发任务。
func TestHandshakePingsAreBounded(t *testing.T) {
	// c 的 READY 通道永不交付；所有 PING 本身立即确认。
	c := receivingClient(t)
	c.id, c.ready, c.config = "session", make(chan string, 8), spec.Defaults()
	// backend 只统计请求，没有人为调度等待。
	backend := &readinessBackend{client: c}
	c.backend = backend
	// ctx 的 350ms 预算最多允许立即一次和 250ms 后一次 PING。
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	// err 保留真实握手或预算结果，不以人工 READY 代替 OPEN 确认。
	if err := c.openWhenReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if backend.pings.Load() > 2 {
		t.Fatalf("PING flood: %d", backend.pings.Load())
	}
}

// TestCancelControlUsesCleanupBudget 验证一分钟握手配置不会延长已取消会话的远端清理。
func TestCancelControlUsesCleanupBudget(t *testing.T) {
	// c 的清理预算为 20ms，CANCEL 出口直到取消才返回。
	c := receivingClient(t)
	c.backend, c.config = &readinessBackend{}, spec.Defaults()
	c.config.Stream.HandshakeTimeout = time.Minute
	c.config.Shutdown.Timeout = 20 * time.Millisecond
	// started 用于核对预算归属，不要求精确到毫秒的墙钟时间。
	started := time.Now()
	c.cancelControl()
	if time.Since(started) > 200*time.Millisecond {
		t.Fatal("CANCEL inherited handshake timeout")
	}
}
