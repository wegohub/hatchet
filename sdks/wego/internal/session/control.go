package session

import (
	"bytes"
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// control 在锁内校验并更新协议状态，锁外执行发布；慢消费者不能阻塞 ACK 或终态处理。
func (s *Endpoint) control(ctx context.Context, frame *wire.Frame) error {
	reply, err := s.updateControl(frame)
	if err != nil || reply == nil {
		return err
	}
	return s.publish(ctx, reply)
}

// updateControl 集中管理临界区，所有分支均由 defer 解锁；不得在此调用后端或业务 handler。
func (s *Endpoint) updateControl(frame *wire.Frame) (*wire.Frame, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if frame == nil {
		return nil, status.Error(codes.InvalidArgument, "wego: missing control frame")
	}
	// 终态保留期内接受迟到的幂等动作，不修改已经发布的业务结果。
	if s.final && (frame.Kind == "ACK" || frame.Kind == "END" || frame.Kind == "OPEN" || frame.Kind == "CANCEL") {
		return nil, nil
	}
	if s.failure != nil {
		return nil, s.failure
	}

	switch frame.Kind {
	case "PING":
		if s.taskID == "" {
			return nil, nil
		}
		return &wire.Frame{Kind: "READY", Nonce: frame.Nonce}, nil
	case "OPEN":
		if !s.running {
			return nil, status.Error(codes.Unavailable, "wego: session task not ready")
		}
		if !s.open {
			s.open = true
			close(s.opened)
			s.notify()
		}
		return nil, nil
	case "DATA":
		return nil, s.acceptInput(frame)
	case "END":
		return nil, s.endInput(frame)
	case "ACK":
		return nil, s.acknowledgeOutput(frame)
	case "CANCEL":
		// 故障与输入交付在同一把锁下排序，取消后不能再从缓存交付业务消息。
		s.failLocked(status.Error(codes.Canceled, "wego: client canceled stream"))
		return nil, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "wego: invalid control frame")
	}
}

// acceptInput 在持锁条件下保存窗口内的 DATA；相同序号重发幂等，不同内容则明确失败。
func (s *Endpoint) acceptInput(frame *wire.Frame) error {
	if frame.Direction != "input" || s.final {
		return status.Error(codes.FailedPrecondition, "wego: input direction is not open")
	}
	if !s.open {
		return status.Error(codes.FailedPrecondition, "wego: stream not open")
	}
	if frame.Seq == 0 {
		return status.Error(codes.InvalidArgument, "wego: DATA sequence starts at one")
	}
	if s.ended && frame.Seq > s.lastInput {
		return status.Error(codes.FailedPrecondition, "wego: DATA after END")
	}
	if frame.Seq <= s.consumed {
		return nil
	}
	if data, ok := s.incoming[frame.Seq]; ok {
		if !bytes.Equal(data, frame.Payload) {
			return status.Error(codes.DataLoss, "wego: duplicate DATA differs")
		}
		return nil
	}
	if s.inputWindowFull(frame.Seq, len(frame.Payload)) {
		return status.Error(codes.ResourceExhausted, "wego: input window full")
	}
	s.incoming[frame.Seq] = copyPayload(frame.Payload)
	s.inputBytes += len(frame.Payload)
	s.notify()
	return nil
}

// inputWindowFull 检查新 DATA 的序号、条数和字节预算；调用方须持锁且已排除 seq<=consumed。
// 用差值比较序号，避免 consumed 接近 MaxUint64 时加窗口发生溢出。
func (s *Endpoint) inputWindowFull(seq uint64, payloadBytes int) bool {
	return seq-s.consumed > uint64(s.config.Stream.Window) ||
		len(s.incoming) >= s.config.Stream.Window ||
		payloadBytes > s.config.Stream.BufferBytes-s.inputBytes
}

// endInput 在持锁条件下半关闭输入，允许 END 先于窗口内的 DATA 到达。
// 例如 END=2、当前 consumed=0，仍须交付 1、2；输出方向保持开放。
func (s *Endpoint) endInput(frame *wire.Frame) error {
	if frame.Direction != "input" || !s.open {
		return status.Error(codes.InvalidArgument, "wego: invalid input END")
	}
	// seq 检查 END 未排除已经接受的输入；例如缓存 3 时 END=2 是矛盾帧。
	for seq := range s.incoming {
		if seq > frame.Seq {
			return status.Error(codes.DataLoss, "wego: END excludes buffered input")
		}
	}
	if s.ended && s.lastInput != frame.Seq {
		return status.Error(codes.DataLoss, "wego: conflicting END")
	}
	if frame.Seq < s.consumed {
		return status.Error(codes.DataLoss, "wego: END precedes consumed input")
	}
	s.ended = true
	s.lastInput = frame.Seq
	s.notify()
	return nil
}

// acknowledgeOutput 在持锁条件下归还新增确认的输出成本，重复 ACK 不扫描或重复扣减。
func (s *Endpoint) acknowledgeOutput(frame *wire.Frame) error {
	if frame.Direction != "output" {
		return status.Error(codes.InvalidArgument, "wego: invalid output ACK direction")
	}
	// 必须先校验上界，再遍历新增序号，避免恶意 ACK 使控制 Worker 长时间循环。
	if frame.Ack > s.outputSeq {
		return status.Error(codes.DataLoss, "wego: output ACK exceeds sequence")
	}
	if frame.Ack > s.outputAck {
		s.outputBytes -= releaseCredit(s.outgoing, s.outputAck, frame.Ack)
		s.outputAck = frame.Ack
		s.notify()
	}
	return nil
}
