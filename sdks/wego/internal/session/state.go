package session

import (
	"context"
	"time"

	"google.golang.org/grpc/metadata"
)

// changeSignal 合并无人等待期间的状态变化；所有方法必须持有所属会话的 mu。
// 等待者先在锁内检查业务条件，再取得同一代通道，避免解锁与订阅之间丢失唤醒。
// 例如 DATA、ACK 连续到达但业务没有等待时不分配；两个等待者共享一个通道并一起醒来。
type changeSignal struct {
	// changed 仅在存在等待者时创建；广播后归零，下一次实际等待才创建新一代。
	changed chan struct{}
}

// watch 在条件检查的同一临界区内取得广播通道；零值可用，不为每次状态更新预分配。
func (s *changeSignal) watch() <-chan struct{} {
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	return s.changed
}

// notify 唤醒当前代的全部等待者；无人等待时合并变化，不创建通道或后台 goroutine。
func (s *changeSignal) notify() {
	if s.changed != nil {
		close(s.changed)
		s.changed = nil
	}
}

// waitForChange 等待状态广播或调用方取消；醒来后调用方必须重新加锁检查完整业务条件。
// 故障与消费的先后关系由锁内检查决定，通道就绪仅表示应重新检查，不能代表调用成功。
func waitForChange(ctx context.Context, changed <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
		return nil
	}
}

// endpointLifecycle 汇集握手、取消和终态状态；按值嵌入 Endpoint，始终由同一把 mu 保护。
// 不得复制正在运行的 Endpoint 或单独拷贝此状态，否则通知与取消的所有权会被分裂。
type endpointLifecycle struct {
	// changeSignal 是会话状态广播，输入、输出和故障共享同一等待代。
	changeSignal
	// ctx 在 RUN 时恢复请求 deadline 和 metadata，取消作用于业务 handler。
	ctx context.Context
	// cancel 终止业务执行；初始化时为空操作，RUN 后绑定到 ctx。
	cancel context.CancelFunc
	// taskID 是业务任务执行身份，输出发布必须等待该身份就绪。
	taskID string
	// timer 管理初始化或终态记录的回收预算。
	timer *time.Timer
	// running 表示业务任务已经进入 RUN，禁止重复启动。
	running bool
	// open 表示握手完成；重复 OPEN 保持幂等。
	open bool
	// opened 仅关闭一次，通知 RUN 可以调用业务 handler。
	opened chan struct{}
	// failed 通知尚未 OPEN 的 RUN 退出；业务窗口等待使用状态广播重新检查 failure。
	failed chan struct{}
	// failure 保存首个故障，后续清理取消不得覆盖它。
	failure error
	// final 表示终态已经生成；之后 ACK、END、OPEN、CANCEL 幂等返回。
	final bool
}

// endpointInput 保存输入方向的排序、半关闭和缓冲成本，不与输出方向的序号混用。
type endpointInput struct {
	// incoming 缓存窗口内的乱序 DATA；例如先到 2 时仍等待 1。
	incoming map[uint64][]byte
	// consumed 是已经连续交付的末序号，也是累计输入 ACK。
	consumed uint64
	// inputBytes 是尚未交付消息的业务字节数。
	inputBytes int
	// ended 表示收到输入 END，输出方向仍可发送。
	ended bool
	// lastInput 是 END 声明的最后序号；消费到该位置才返回输入 EOF。
	lastInput uint64
}

// endpointOutput 保存输出方向的连续发送位置与未确认成本。
type endpointOutput struct {
	// outputSeq 是已经分配的最后输出序号，首条为 1。
	outputSeq uint64
	// outputAck 是客户端累计确认位置，重复或较旧 ACK 不重复释放额度。
	outputAck uint64
	// outgoing 保存未确认消息的字节成本；ACK=3 仅删除此前尚未确认的 1～3。
	outgoing map[uint64]int
	// outputBytes 是尚未确认的输出字节总数。
	outputBytes int
}

// endpointResponse 保存响应 metadata 与 client stream 的唯一最终响应。
type endpointResponse struct {
	// headers 是响应头缓存，发布前可合并。
	headers metadata.MD
	// trailers 随最终状态交付，读取时使用副本。
	trailers metadata.MD
	// headersSent 防止重复发布 HEADER。
	headersSent bool
	// finalResponse 是最终响应的独立业务字节副本。
	finalResponse []byte
	// responseSent 独立记录存在性，合法的零字节 protobuf 也算已发送响应。
	responseSent bool
}

// initialWindowCapacity 为默认窗口预留空间，但不按任意大的配置一次性申请内存。
// 例如 Window=64 预留 64 项，Window=百万仍只预留 64 项，后续实际流量按窗口与字节预算增长。
func initialWindowCapacity(window int) int {
	return max(0, min(window, 64))
}

// releaseCredit 仅访问新增确认的序号，复杂度为 O(k)，k 为 ack-previous；调用方必须先校验 ack 不超过已发送位置。
// 例如此前 ACK=3、新 ACK=5 时只访问 4、5；先递增再处理保证 ACK=MaxUint64 时循环也能结束。
func releaseCredit(pending map[uint64]int, previous, ack uint64) int {
	released := 0
	for seq := previous; seq < ack; {
		seq++
		released += pending[seq]
		delete(pending, seq)
	}
	return released
}

// copyPayload 创建长度与容量相同的独立缓存，避免 append 的容量取整保留多余可写空间。
// nil 保持 nil，非 nil 空载荷保持非 nil；业务消息所有权不能借用解码器或调用者的缓冲。
func copyPayload(payload []byte) []byte {
	if payload == nil {
		return nil
	}
	copied := make([]byte, len(payload))
	copy(copied, payload)
	return copied
}
