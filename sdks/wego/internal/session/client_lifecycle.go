package session

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

const (
	// pingMinInterval 限制重发速度；首次立即发送，之后至少等待 250ms。
	pingMinInterval = 250 * time.Millisecond
	// pingMaxInterval 控制退避上限；长时间订阅延迟时每秒至多重发一次。
	pingMaxInterval = time.Second
	// defaultCancelTimeout 在未显式配置时为取消控制提供独立短预算。
	defaultCancelTimeout = 5 * time.Second
)

// openWhenReady 只允许一个在途 PING，匹配 READY 后及时撤销结果等待并回收 goroutine。
// 握手 context 是唯一总预算；次数随配置的时长和退避受限，不另设会误伤长预算的固定次数。
func (c *Client) openWhenReady(ctx context.Context) error {
	// pingContext 独立取消 PING 的结果等待，取消它不撤销整个会话或后续 OPEN。
	pingContext, cancelPing := context.WithCancel(ctx)
	// pending 跟踪唯一 PING goroutine，函数返回前必须确认它已退出。
	var pending sync.WaitGroup
	defer func() { cancelPing(); pending.Wait() }()
	// timer 首次立即发 PING；只有上次请求结束才重置，不积累 ticker 的过期事件。
	timer := time.NewTimer(0)
	defer timer.Stop()
	interval := pingMinInterval
	// result 为 nil 时没有在途 PING，select 自动禁用该分支。
	var result <-chan error
	attempts := 0
	// open 要先确认匹配 READY 的输出订阅已建立，再取消 PING 结果等待并提交 OPEN。
	open := func() error {
		cancelPing()
		pending.Wait()
		if c.config.Logger != nil {
			c.config.Logger.Debug("流订阅 READY", "stream_id", c.id, "method", c.method, "ping_attempts", attempts)
		}
		_, err := c.control(ctx, &wire.Frame{Kind: "OPEN"})
		return err
	}
	for {
		// 已经排队的 READY 优先于重发 PING；错误 nonce 不能开启当前会话。
		select {
		case nonce := <-c.ready:
			if nonce == c.id {
				return open()
			}
		default:
		}
		select {
		case nonce := <-c.ready:
			if nonce == c.id {
				return open()
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-c.ctx.Done():
			return c.ctx.Err()
		case err := <-result:
			result = nil
			if err != nil && status.Code(err) != codes.Unavailable {
				return err
			}
			timer.Reset(interval)
			interval = min(2*interval, pingMaxInterval)
		case <-timer.C:
			// completed 容量为 1，取消后结果发送也不会阻塞退出。
			completed := make(chan error, 1)
			result = completed
			attempts++
			pending.Add(1)
			go func() {
				defer pending.Done()
				_, err := c.control(pingContext, &wire.Frame{Kind: "PING", Nonce: c.id})
				completed <- err
			}()
		}
	}
}

// cancelControl 在有限独立预算内取消远端会话，失败只作诊断，不改写原业务终态。
func (c *Client) cancelControl() {
	timeout := c.config.Stream.CancelTimeout
	if timeout <= 0 {
		timeout = defaultCancelTimeout
	}
	if c.config.Shutdown.Timeout > 0 {
		timeout = min(timeout, c.config.Shutdown.Timeout)
	}
	// cleanup 独立于已经取消的业务 context，远端仍有机会处理控制请求。
	cleanup, stop := context.WithTimeout(context.Background(), timeout)
	defer stop()
	started := time.Now()
	_, err := c.control(cleanup, &wire.Frame{Kind: "CANCEL"})
	c.logLifecycle("流 CANCEL 清理完成", started, err)
}

// logLifecycle 使用有限身份与耗时字段定位阶段，不写入业务 payload、metadata 或 token。
func (c *Client) logLifecycle(message string, started time.Time, err error) {
	if c.config.Logger != nil {
		c.config.Logger.Debug(message, "stream_id", c.id, "method", c.method,
			"elapsed", time.Since(started), "failed", err != nil, "grpc_code", status.Code(err).String())
	}
}
