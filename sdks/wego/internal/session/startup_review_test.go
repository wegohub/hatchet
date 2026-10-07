package session

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// startupBackend 只阻塞 RUN，START 与取消控制均可立即确认。
type startupBackend struct{ ports.Backend }

// Run 区分 START、会话 RUN 和控制请求，为握手及关闭提供受控协议边界。
func (startupBackend) Run(ctx context.Context, name string, input any, _ model.RunOptions) (ports.Run, error) {
	if strings.HasSuffix(name, "-session") {
		<-ctx.Done()
		return ports.Run{}, ctx.Err()
	}
	return ports.Run{ID: "start", Wait: func(context.Context) (ports.Result, error) {
		return ports.Result{Outputs: map[string]any{"start": Acknowledgment{Owner: "owner"}}}, nil
	}}, nil
}

// TestSyncReviewHandshakeBudgetIncludesRUN 验证 RUN 提交不能超出 20ms 握手预算。
func TestSyncReviewHandshakeBudgetIncludesRUN(t *testing.T) {
	// config 基于合法默认配置，只覆盖当前测试所需的投影、载荷或握手预算。
	config := spec.Defaults()
	config.Stream.HandshakeTimeout = 20 * time.Millisecond
	// ctx 和 cancel 为测试设置有限预算，退出时取消全部受控等待。
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	// started 用于比较 RUN 实际等待与 20ms 握手预算。
	started := time.Now()
	// err 保留调用的失败原因；本场景只验证错误或合并详情，不使用普通返回值。
	_, err := NewClient(ctx, startupBackend{}, config, &grpc.StreamDesc{ServerStreams: true}, "/fixture/Watch")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RUN must consume the handshake deadline: %v", err)
	}
	// elapsed 应受握手预算约束，不能退化为调用方较长 deadline。
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("20ms handshake waited %v on RUN", elapsed)
	}
}

// exitingBackend 模拟取消后仍需完成清理的订阅和结果等待。
type exitingBackend struct {
	// Backend 满足内部端口，只替换测试涉及的操作。
	ports.Backend
	// callback 发布已建立的订阅，PING 只能通过这一出口响应 READY。
	callback chan func(string) error
	// release 控制传输清理完成，取消本身不代表资源已退出。
	release chan struct{}
	// streamDone 确认输出订阅 goroutine 已释放传输。
	streamDone chan struct{}
	// waitDone 确认结果等待 goroutine 已释放传输。
	waitDone chan struct{}
}

// Run 区分 START、会话 RUN 和控制请求，为握手及关闭提供受控协议边界。
func (b *exitingBackend) Run(_ context.Context, name string, input any, _ model.RunOptions) (ports.Run, error) {
	// c 是 Conn 或流控制请求的自有视图，不持有独立后端连接。
	c := input.(Control)
	if strings.HasSuffix(name, "-session") {
		return ports.Run{ID: "session", Wait: func(ctx context.Context) (ports.Result, error) {
			<-ctx.Done()
			<-b.release
			close(b.waitDone)
			return ports.Result{}, ctx.Err()
		}}, nil
	}
	if c.Kind == "PING" {
		// cb 是输出订阅入口，READY 必须经过它才能证明订阅已建立。
		cb := <-b.callback
		// f 还原受控 PING 帧，READY 保留相同 nonce 来匹配本次握手。
		f, _ := wire.DecodeFrame(c.Frame, 1<<20)
		// value 将 READY 编码为真实协议字符串，复用本次会话身份和 nonce。
		value, _ := wire.EncodeFrame(&wire.Frame{Version: wire.Version, StreamId: c.StreamID, Kind: "READY", Nonce: f.Nonce})
		// err 检查当前操作结果，失败时不能使用未解码或未交付的输出。
		if err := cb(value); err != nil {
			return ports.Run{}, err
		}
	}
	return ports.Run{ID: "control", Wait: func(context.Context) (ports.Result, error) {
		return ports.Result{Outputs: map[string]any{"control": Acknowledgment{Owner: "owner"}}}, nil
	}}, nil
}

// Stream 在取消后仍等待清理屏障，模拟尚未释放的传输资源。
func (b *exitingBackend) Stream(ctx context.Context, _ string, fn func(string) error) error {
	b.callback <- fn
	<-ctx.Done()
	<-b.release
	close(b.streamDone)
	return ctx.Err()
}

// TestSyncReviewStreamCleanupJoinsOwnedGoroutines 验证实例登记只在全部传输 goroutine 退出后释放。
func TestSyncReviewStreamCleanupJoinsOwnedGoroutines(t *testing.T) {
	// config 基于合法默认配置，只覆盖当前测试所需的投影、载荷或握手预算。
	config := spec.Defaults()
	// ctx 和 cancel 为测试设置有限预算，退出时取消全部受控等待。
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// b 用受控通道阻塞订阅清理与结果清理，验证实例登记的释放时点。
	b := &exitingBackend{callback: make(chan func(string) error, 1), release: make(chan struct{}), streamDone: make(chan struct{}), waitDone: make(chan struct{})}
	defer func() { close(b.release); <-b.streamDone; <-b.waitDone }()
	// closed 发布实例登记释放；两个传输操作退出前不得关闭它。
	closed := make(chan struct{})
	// err 保留调用的失败原因；本场景只验证错误或合并详情，不使用普通返回值。
	_, err := NewClientManaged(ctx, b, config, &grpc.StreamDesc{ServerStreams: true}, "/fixture/Watch", func(error) { close(closed) })
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-closed:
		t.Fatal("Engine call released while Stream and Wait goroutines still own transport resources")
	case <-time.After(50 * time.Millisecond):
	}
}
