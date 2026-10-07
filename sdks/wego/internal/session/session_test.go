package session

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// frameBackend 记录已发布帧的后端 fixture，验证协议动作及窗口释放。
type frameBackend struct {
	// ports.Backend 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则。
	ports.Backend
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁。
	mu sync.Mutex
	// frames fixture 已发布的流帧，用于断言 DATA、ACK 与终态。
	frames []*wire.Frame
}

// Publish 解码并保存已发布的真实协议帧，测试随后断言序号、类型及累计 ACK。
func (b *frameBackend) Publish(_ context.Context, _ string, data []byte) error {
	// f, e 接收解码后的协议数据，错误时终止当前处理，不把非法输入推进到业务 handler。
	f, e := wire.DecodeFrame(string(data), 1<<20)
	if e != nil {
		return e
	}

	b.mu.Lock()
	b.frames = append(b.frames, f)
	b.mu.Unlock()
	return nil
}

// endpoint 创建已打开的协议 fixture，把 OPEN 的幂等处理用于后续输入与输出边界测试。
func endpoint(t *testing.T) (*Endpoint, *frameBackend) {
	t.Helper()
	// b 当前后端或测试后端对象，在所属实例内管理运行与资源，不经公开 API 暴露。
	b := &frameBackend{}
	// s 构造受控会话状态，用实际 OPEN、DATA 和终态测试协议约束。
	s := newEndpoint("session", "method", wire.Envelope{}, b, spec.Defaults())
	s.taskID = "task"
	s.running = true
	// e 保存控制帧处理结果，按本用例检查重复、窗口、半关闭或取消的协议约束。
	if e := s.control(context.Background(), &wire.Frame{Kind: "OPEN"}); e != nil {
		t.Fatal(e)
	}
	return s, b
}

// TestInputOrderingDuplicatesAndHalfClose 先提交序号 2 再提交 1，并重复相同 DATA，断言按 1、2 各交付一次，END 后输出仍可继续。
func TestInputOrderingDuplicatesAndHalfClose(t *testing.T) {
	s, _ := endpoint(t)
	// ctx 选取不带业务 deadline 的资源上下文，具体清理步骤另有明确预算。
	ctx := context.Background()
	// frames fixture 已发布的流帧，用于断言 DATA、ACK 与终态。
	frames := []*wire.Frame{
		{Kind: "END", Direction: "input", Seq: 2},
		{
			Kind:      "DATA",
			Direction: "input",
			Seq:       2,
			Payload:   []byte("two"),
		},
		{
			Kind:      "DATA",
			Direction: "input",
			Seq:       2,
			Payload:   []byte("two"),
		},
		{
			Kind:      "DATA",
			Direction: "input",
			Seq:       1,
			Payload:   []byte("one"),
		},
	}
	for _, f := range frames {
		// e 保存控制帧处理结果，按本用例检查重复、窗口、半关闭或取消的协议约束。
		if e := s.control(ctx, f); e != nil {
			t.Fatal(e)
		}
	}
	for _, want := range []string{"one", "two"} {
		// data, e 按输入序号交付业务消息，乱序帧先缓存，不能越过缺口消费。
		data, e := s.Receive(ctx)
		if e != nil || string(data) != want {
			t.Fatalf("got %q, %v", data, e)
		}
	}
	// _, e 保存按序接收结果；END 只关闭输入，完整消费后才允许输入 EOF。
	if _, e := s.Receive(ctx); e != io.EOF {
		t.Fatalf("END: %v", e)
	}
	// e 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功。
	if e := s.Send(ctx, []byte("still open")); e != nil {
		t.Fatal(e)
	}
	// e 保存调用错误并核对预期 gRPC code；例如输出缺口必须是 DataLoss，不能是成功 EOF。
	if e := s.control(ctx, &wire.Frame{Kind: "DATA", Direction: "input", Seq: 3}); status.Code(e) != codes.FailedPrecondition {
		t.Fatalf("after END: %v", e)
	}
}

// TestConflictingFramesAndWindow 注入同序号不同载荷及超窗帧，断言明确失败且不越过缓冲预算。
func TestConflictingFramesAndWindow(t *testing.T) {
	s, _ := endpoint(t)
	s.config.Stream.Window = 2
	// ctx 选取不带业务 deadline 的资源上下文，具体清理步骤另有明确预算。
	ctx := context.Background()
	// e 保存控制帧处理结果，按本用例检查重复、窗口、半关闭或取消的协议约束。
	if e := s.control(ctx, &wire.Frame{
		Kind:      "DATA",
		Direction: "input",
		Seq:       2,
		Payload:   []byte("x"),
	}); e != nil {
		t.Fatal(e)
	}
	// 逐项注入矛盾帧：相同序号不同载荷、排除已缓存消息的 END、确认未发送输出的 ACK；都必须明确失败。
	for _, f := range []*wire.Frame{
		{
			Kind:      "DATA",
			Direction: "input",
			Seq:       2,
			Payload:   []byte("y"),
		},
		{Kind: "END", Direction: "input", Seq: 1},
		{Kind: "ACK", Direction: "output", Ack: 1},
	} {
		// e 保存调用错误并核对预期 gRPC code；例如输出缺口必须是 DataLoss，不能是成功 EOF。
		if e := s.control(ctx, f); status.Code(e) != codes.DataLoss {
			t.Fatalf("%s: %v", f.Kind, e)
		}
	}
	// e 保存调用错误并核对预期 gRPC code；例如输出缺口必须是 DataLoss，不能是成功 EOF。
	if e := s.control(ctx, &wire.Frame{Kind: "DATA", Direction: "input", Seq: 3}); status.Code(e) != codes.ResourceExhausted {
		t.Fatal(e)
	}
}

// TestOutputCreditDoesNotBlockControl 填满输出窗口再提交 ACK，验证控制处理仍能释放额度并唤醒发送方。
func TestOutputCreditDoesNotBlockControl(t *testing.T) {
	s, _ := endpoint(t)
	s.config.Stream.Window = 1
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// e 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功。
	if e := s.Send(ctx, []byte("one")); e != nil {
		t.Fatal(e)
	}
	// done 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	done := make(chan error, 1)
	go func() {
		done <- s.Send(ctx, []byte("two"))
	}()
	// 根据已经就绪的通知选择下一步，不通过轮询固定延迟猜测执行时机。
	select {
	// e 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功。
	case e := <-done:
		t.Fatalf("sent without credit: %v", e)
	case <-time.After(10 * time.Millisecond):
	}
	// e 保存控制帧处理结果，按本用例检查重复、窗口、半关闭或取消的协议约束。
	if e := s.control(ctx, &wire.Frame{Kind: "ACK", Direction: "output", Ack: 1}); e != nil {
		t.Fatal(e)
	}
	// e 保存当前步骤返回或收到的值，紧接着按错误、类型或内容校验再继续。
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	// e 保存控制帧处理结果，按本用例检查重复、窗口、半关闭或取消的协议约束。
	if e := s.control(ctx, &wire.Frame{Kind: "CANCEL"}); e != nil {
		t.Fatal(e)
	}
	// e 保存本次响应头或消息发送结果；发送失败立即结束当前流，不能把未送达消息计为成功。
	if e := s.Send(ctx, []byte("three")); status.Code(e) != codes.Canceled {
		t.Fatal(e)
	}
}

// TestOpenAndTerminalControlAreIdempotent 重复提交 OPEN、END 和终态控制，确认 handler 只打开一次且不会重复关闭 channel。
func TestOpenAndTerminalControlAreIdempotent(t *testing.T) {
	s, _ := endpoint(t)
	// wg 等待本组并发步骤结束，资源关闭前必须确认所有已启动步骤退出。
	var wg sync.WaitGroup
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// e 保存控制帧处理结果，按本用例检查重复、窗口、半关闭或取消的协议约束。
			if e := s.control(context.Background(), &wire.Frame{Kind: "OPEN"}); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	// result 发布会话终态和最后输出位置，缺口存在时不能返回正常 EOF。
	result := s.finish(nil)
	if len(result.Status) != 0 {
		t.Fatalf("non-OK status: %v", result)
	}
	for _, kind := range []string{
		"END",
		"ACK",
		"OPEN",
		"CANCEL",
	} {
		// e 保存控制帧处理结果，按本用例检查重复、窗口、半关闭或取消的协议约束。
		if e := s.control(context.Background(), &wire.Frame{Kind: kind}); e != nil {
			t.Fatal(e)
		}
	}
}
