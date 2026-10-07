package session

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestCumulativeOutputACKBoundaries 检查累计、重复、较旧、未来 ACK，以及最大序号不会回绕或重复扣减。
func TestCumulativeOutputACKBoundaries(t *testing.T) {
	// s 已 OPEN，输出成本按真实序号构造；ACK=2 后只能保留第 3 条。
	s, _ := endpoint(t)
	s.outputSeq = 3
	s.outputBytes = 60
	s.outgoing = map[uint64]int{1: 10, 2: 20, 3: 30}
	// ack 覆盖推进与重复，所有较旧确认均保持累计位置和成本不变。
	for _, ack := range []uint64{0, 2, 2, 1} {
		// err 验证合法控制确认不会因为已确认位置被重复删除而失败。
		if err := s.control(context.Background(), &wire.Frame{Kind: "ACK", Direction: "output", Ack: ack}); err != nil {
			t.Fatal(err)
		}
	}
	if s.outputAck != 2 || s.outputBytes != 30 || len(s.outgoing) != 1 {
		t.Fatalf("ACK credit: ack=%d bytes=%d pending=%v", s.outputAck, s.outputBytes, s.outgoing)
	}
	// err 拒绝未发送序号；失败不能改写已经正确的额度。
	if err := s.control(context.Background(), &wire.Frame{Kind: "ACK", Direction: "output", Ack: 4}); status.Code(err) != codes.DataLoss {
		t.Fatal(err)
	}
	if s.outputAck != 2 || s.outputBytes != 30 {
		t.Fatal("invalid ACK changed credit")
	}
	s.outputSeq = math.MaxUint64
	s.outputAck = math.MaxUint64 - 2
	s.outputBytes = 7
	s.outgoing = map[uint64]int{math.MaxUint64 - 1: 3, math.MaxUint64: 4}
	// ack 最大累计序号和重复确认都须有界结束，最后不留下字节成本。
	for _, ack := range []uint64{math.MaxUint64, math.MaxUint64} {
		// err 最大序号不是非法 ACK，不得溢出而无限循环。
		if err := s.control(context.Background(), &wire.Frame{Kind: "ACK", Direction: "output", Ack: ack}); err != nil {
			t.Fatal(err)
		}
	}
	if s.outputBytes != 0 || len(s.outgoing) != 0 || s.outputAck != math.MaxUint64 {
		t.Fatal("last sequence credit not released")
	}
}

// TestCumulativeInputACKBoundaries 验证客户端订阅出口也按新增范围释放窗口，未来 ACK 保留 DataLoss。
func TestCumulativeInputACKBoundaries(t *testing.T) {
	// c 设置三条未确认输入，长度与服务端 ACK 用例相同，验证两个方向一致。
	c := receivingClient(t)
	c.sent = 3
	c.sendBytes = 60
	c.pending = map[uint64]int{1: 10, 2: 20, 3: 30}
	// ack 先确认前两条，再重复和回退，最后确认第三条。
	for _, ack := range []uint64{2, 2, 1, 3} {
		// encoded 通过真实 protobuf 帧出口输入客户端，而非直接调用额度辅助函数。
		encoded, err := wire.EncodeFrame(&wire.Frame{Version: wire.Version, StreamId: c.id, Kind: "ACK", Direction: "input", Ack: ack})
		if err != nil {
			t.Fatal(err)
		}
		if err = c.receiveFrame(encoded); err != nil {
			t.Fatal(err)
		}
	}
	if c.sendBytes != 0 || len(c.pending) != 0 || c.acked != 3 {
		t.Fatal("input credit not released exactly once")
	}
	// encoded 是超过实际发送位置的确认，必须拒绝并取消整个会话。
	encoded, err := wire.EncodeFrame(&wire.Frame{Version: wire.Version, StreamId: c.id, Kind: "ACK", Direction: "input", Ack: math.MaxUint64})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.receiveFrame(encoded); status.Code(err) != codes.DataLoss || c.ctx.Err() == nil {
		t.Fatalf("future ACK: %v", err)
	}
}

// TestBroadcastRegistrationAndCancellation 验证多个等待者、广播先于 select、重复空广播和独立取消。
func TestBroadcastRegistrationAndCancellation(t *testing.T) {
	// s 只使用状态广播，测试不依赖后台 goroutine 调度时机。
	s, _ := endpoint(t)
	s.mu.Lock()
	// first 与 second 代表两个已检查条件的等待者，必须共享同一代通道。
	first, second := s.watch(), s.watch()
	s.notify()
	s.notify()
	// next 是广播后的新等待代，不能复用已经关闭的通道。
	next := s.watch()
	s.mu.Unlock()
	if first != second || first == next {
		t.Fatal("waiters did not share one notification generation")
	}
	// changed 即使 select 在广播之后才执行，也必须立即观察到之前的状态变化。
	for _, changed := range []<-chan struct{}{first, second} {
		select {
		case <-changed:
		default:
			t.Fatal("registered waiter lost broadcast")
		}
	}
	select {
	case <-next:
		t.Fatal("new waiter observed obsolete broadcast")
	default:
	}
	// ctx 已取消且状态未改变，等待须直接退出，不创建取消桥接 goroutine。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(waitForChange(ctx, next), context.Canceled) {
		t.Fatal("cancelled waiter did not exit")
	}
	s.mu.Lock()
	s.notify()
	s.mu.Unlock()
}

// TestFailureDoesNotConsumeBufferedInput 故障先于 Receive 线性化时，不得从已缓冲 DATA 继续交付。
func TestFailureDoesNotConsumeBufferedInput(t *testing.T) {
	// s 先接受一条真实 DATA，再接收 CANCEL；故障后仍有缓存，不能靠空缓冲掩盖错误。
	s, _ := endpoint(t)
	// err 保存输入接收结果，正常 DATA 必须成功进入缓存。
	if err := s.control(context.Background(), &wire.Frame{Kind: "DATA", Direction: "input", Seq: 1, Payload: []byte("buffered")}); err != nil {
		t.Fatal(err)
	}
	// err 确保 CANCEL 已完成状态更新，然后才尝试业务消费。
	if err := s.control(context.Background(), &wire.Frame{Kind: "CANCEL"}); err != nil {
		t.Fatal(err)
	}
	// data、err 应为空载荷和 Canceled；消费位置、缓冲和字节成本都不能改变。
	data, err := s.Receive(context.Background())
	if status.Code(err) != codes.Canceled || data != nil || s.consumed != 0 || len(s.incoming) != 1 || s.inputBytes != len("buffered") {
		t.Fatalf("failure consumed input: %q, %v, consumed=%d", data, err, s.consumed)
	}
}

// TestWindowNearSequenceLimit 序号窗口接近 uint64 上限时仍接受合法末项，并拒绝发送回绕序号。
func TestWindowNearSequenceLimit(t *testing.T) {
	// s 将输入消费位置推进到倒数第二项；窗口比较不能以 consumed+Window 产生溢出。
	s, _ := endpoint(t)
	s.consumed = math.MaxUint64 - 1
	// err 验证最后一个输入序号仍在窗口内。
	if err := s.control(context.Background(), &wire.Frame{Kind: "DATA", Direction: "input", Seq: math.MaxUint64}); err != nil {
		t.Fatal(err)
	}
	s.outputSeq = math.MaxUint64
	// err 输出序号耗尽必须明确报错，不能创建 DATA=0。
	if err := s.Send(context.Background(), []byte("last")); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	// ok 检查非法零序号是否被错误写入发送窗口。
	if _, ok := s.outgoing[0]; ok {
		t.Fatal("output sequence wrapped to zero")
	}
	// c 同样禁止客户端发送方向回绕。
	c := receivingClient(t)
	c.sent = math.MaxUint64
	c.pending = map[uint64]int{}
	// err 客户端在提交任何控制任务前应发现序号耗尽。
	if err := c.SendMsg(&pb.Request{GroupKey: "limit"}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
}

// excessiveACKBackend 模拟控制任务返回未来确认，确保结果出口与订阅出口使用相同的校验规则。
type excessiveACKBackend struct {
	// Backend 委托不参与本场景的能力，当前 fixture 只实现 Run。
	ports.Backend
}

// Run 返回最大序号确认；客户端实际只发送一条消息，不能进入无界额度释放循环。
func (b *excessiveACKBackend) Run(context.Context, string, any, model.RunOptions) (ports.Run, error) {
	return ports.Run{ID: "control", Wait: func(context.Context) (ports.Result, error) {
		return ports.Result{Outputs: map[string]any{"control": Acknowledgment{Consumed: math.MaxUint64}}}, nil
	}}, nil
}

// TestControlResultRejectsFutureACK 确认 DATA 的控制结果不能绕过序号上界校验。
func TestControlResultRejectsFutureACK(t *testing.T) {
	// c 只提交一个 protobuf 请求，因此 Consumed=MaxUint64 必须被判定为 DataLoss。
	c := receivingClient(t)
	c.pending = map[uint64]int{}
	c.backend = &excessiveACKBackend{}
	// err 通过真实 SendMsg、control、Decode 路径观察终态错误。
	if err := c.SendMsg(&pb.Request{GroupKey: "input"}); status.Code(err) != codes.DataLoss || c.ctx.Err() == nil {
		t.Fatal(err)
	}
}

// unlockedPublisher 检查远程发布前端点锁已释放，防止 READY 的后端耗时阻塞控制通道。
type unlockedPublisher struct {
	// Backend 是不参与当前测试的其他后端能力。
	ports.Backend
	// endpoint 是被测会话；Publish 仅检查锁所有权，不修改状态。
	endpoint *Endpoint
}

// Publish 使用 TryLock 检查调用边界；持锁发布会明确失败而不会让测试挂死。
func (b *unlockedPublisher) Publish(context.Context, string, []byte) error {
	if !b.endpoint.mu.TryLock() {
		return errors.New("publish holds endpoint mutex")
	}
	b.endpoint.mu.Unlock()
	return nil
}

// TestControlPublishesOutsideLock 握手 READY 的远程发布与状态临界区分离。
func TestControlPublishesOutsideLock(t *testing.T) {
	// s 处于可以响应 PING 的状态，fixture 检查 backend 调用发生在解锁后。
	s, _ := endpoint(t)
	s.backend = &unlockedPublisher{endpoint: s}
	// err 通过完整 PING 路径检查解锁，不直接调用发布辅助函数。
	if err := s.control(context.Background(), &wire.Frame{Kind: "PING", Nonce: "handshake"}); err != nil {
		t.Fatal(err)
	}
}

// TestSessionDecodeSources 字符串、原始 JSON、普通对象以及非法输入都遵循相同确认结构。
func TestSessionDecodeSources(t *testing.T) {
	// value 覆盖引擎实际提供的出口类型；[]byte 必须解释为 JSON 而非 base64 字符串。
	for name, value := range map[string]any{
		"string": `{"owner":"instance","consumed":3}`,
		"bytes":  []byte(`{"owner":"instance","consumed":3}`),
		"raw":    json.RawMessage(`{"owner":"instance","consumed":3}`),
		"object": Acknowledgment{Owner: "instance", Consumed: 3},
	} {
		t.Run(name, func(t *testing.T) {
			// target 应从任意合法源还原成同一确认，源类型不能改变业务语义。
			var target Acknowledgment
			// err 区分解析失败与合法但内容错误。
			if err := Decode(value, &target); err != nil || target.Owner != "instance" || target.Consumed != 3 {
				t.Fatalf("decode: %+v, %v", target, err)
			}
		})
	}
	// value 非法 JSON、错误结构和不可编码对象均不得静默产生零值确认。
	for _, value := range []any{[]byte("not-json"), json.RawMessage("{"), 7, make(chan int)} {
		// target 接收非法输入，要求显式错误。
		var target Acknowledgment
		if Decode(value, &target) == nil {
			t.Fatalf("accepted malformed input type %T", value)
		}
	}
	// value 显式 nil 与带类型的 nil JSON 字节都遵循 null，不把原始字节优化变成语义变更。
	for _, value := range []any{nil, []byte(nil), json.RawMessage(nil)} {
		// target 是 null 测试的指针槽，非 nil 初始化确保解码确实清空而非未操作。
		target := &Acknowledgment{Owner: "instance"}
		// err nil 输入应还原为 null，不能误当作空文本解析失败。
		if err := Decode(value, &target); err != nil || target != nil {
			t.Fatalf("null: %v", err)
		}
	}
}

// TestPayloadCopyAndBoundedCapacity 缓存保留独立所有权、nil 与空切片语义，预分配不受巨大窗口控制。
func TestPayloadCopyAndBoundedCapacity(t *testing.T) {
	// payload 同时包含 nil、非 nil 空消息和非整齐长度，覆盖容量取整与空消息语义。
	for _, payload := range [][]byte{nil, {}, make([]byte, 253)} {
		// copied 必须长度与容量一致，而且不能借用调用者可变内存。
		copied := copyPayload(payload)
		if (payload == nil) != (copied == nil) || len(copied) != len(payload) || cap(copied) != len(payload) {
			t.Fatal("copy changed ownership or empty payload semantics")
		}
		if len(payload) != 0 {
			payload[0] = 1
			if copied[0] != 0 {
				t.Fatal("payload copy aliases caller memory")
			}
		}
	}
	// window 极大或为零时都不能触发不受控的初始容量；正常窗口保留预分配收益。
	for window, want := range map[int]int{-1: 0, 0: 0, 1: 1, 64: 64, math.MaxInt: 64} {
		if initialWindowCapacity(window) != want {
			t.Fatalf("capacity for %d", window)
		}
	}
}

// TestBroadcastConcurrentWaiters 多个实际阻塞的等待者必须全部观察广播，无需为取消创建额外 goroutine。
func TestBroadcastConcurrentWaiters(t *testing.T) {
	// s 共享一个端点锁与广播状态，所有等待者先登记再允许测试发送通知。
	s, _ := endpoint(t)
	// ctx 给失败路径设置明确预算，防止丢失唤醒使测试永久挂起。
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// ready 汇总八个等待者已经登记同一代通道的事实，不依赖 sleep 猜测执行顺序。
	ready := make(chan struct{}, 8)
	// results 收集每个等待者的结果，主测试 goroutine 统一检查。
	results := make(chan error, 8)
	// group 确认所有测试自有 goroutine 在结束前退出。
	var group sync.WaitGroup
	// i 为每个独立等待者登记执行与退出，不共享可变结果变量。
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			s.mu.Lock()
			// changed 在锁内捕获通知；解锁后即使尚未进入 select 也不会遗漏广播。
			changed := s.watch()
			s.mu.Unlock()
			ready <- struct{}{}
			results <- s.wait(ctx, changed)
		}()
	}
	// i 确认全部等待者已登记；预算到期仍取消并回收，不能留下测试 goroutine。
	for i := 0; i < 8; i++ {
		select {
		case <-ready:
		case <-ctx.Done():
			group.Wait()
			t.Fatal("waiter registration timed out")
		}
	}
	s.mu.Lock()
	s.notify()
	s.mu.Unlock()
	group.Wait()
	// i 检查一次广播已唤醒每个等待者，不能只唤醒任意一个。
	for i := 0; i < 8; i++ {
		// err 已完成等待者的结果，正确广播不得依赖预算超时才退出。
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

// TestWaitKeepsProtocolFailure 业务上下文取消与故障广播同时就绪时，首个协议错误仍具有优先级。
func TestWaitKeepsProtocolFailure(t *testing.T) {
	// s 在登记等待之后失败，保留的 DataLoss 不能被派生 ctx 取消替代。
	s, _ := endpoint(t)
	s.mu.Lock()
	// changed 保存失败前登记的等待代。
	changed := s.watch()
	s.mu.Unlock()
	// failure 表示真正的协议故障，后续取消只用于资源清理。
	failure := status.Error(codes.DataLoss, "output gap")
	s.fail(failure)
	// ctx 在故障已经登记后取消，模拟广播与 Done 同时就绪的竞态。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// i 多次观察两种已就绪通知，所有结果都必须保留 DataLoss。
	for i := 0; i < 100; i++ {
		// err 广播路径返回 nil 供调用方重新检查；取消路径必须返回实际故障。
		if err := s.wait(ctx, changed); err != nil && status.Code(err) != codes.DataLoss {
			t.Fatal(err)
		}
	}
}
