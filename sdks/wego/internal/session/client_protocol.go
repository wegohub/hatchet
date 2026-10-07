package session

import (
	"bytes"
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// options 要求会话和后续控制任务落到 START 选择的同一实例。
func (c *Client) options() model.RunOptions {
	return model.RunOptions{
		Labels: map[string]*model.DesiredWorkerLabel{OwnerLabel: {Value: c.owner, Required: true}},
	}
}

// control 处理会话控制帧，按方向、序号和窗口约束更新状态；ACK 不等待业务消费。
func (c *Client) control(ctx context.Context, frame *wire.Frame) (Acknowledgment, error) {
	frame.Version = wire.Version
	frame.StreamId = c.id
	encoded, err := wire.EncodeFrame(frame)
	if err != nil {
		return Acknowledgment{}, err
	}

	ref, err := c.backend.Run(ctx, ControlName, Control{StreamID: c.id, Kind: frame.Kind, Frame: encoded}, c.options())
	if err != nil {
		return Acknowledgment{}, err
	}

	result, err := ref.Wait(ctx)
	if err != nil {
		return Acknowledgment{}, err
	}

	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
	var out Acknowledgment
	err = single(result.Outputs, &out)
	return out, err
}

// receiveFrame 只校验协议和缓存消息，不等待应用读取，保证 ACK 能及时推进发送窗口。
// 协议错误统一在返回时终止会话；锁的 defer 先执行，随后才取消和广播。
func (c *Client) receiveFrame(value string) (resultErr error) {
	defer func() {
		if resultErr != nil {
			c.abort(status.Error(codes.DataLoss, resultErr.Error()))
		}
	}()

	frame, err := wire.DecodeFrame(value, c.config.Stream.MaxMessageBytes)
	if err != nil {
		return err
	}
	if frame.StreamId != c.id {
		return status.Error(codes.DataLoss, "wego: stream identity mismatch")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	switch frame.Kind {
	case "READY":
		select {
		case c.ready <- frame.Nonce:
		default:
		}
	case "HEADER":
		if !c.headersSeen {
			c.headers = wire.FromMetadata(frame.Metadata)
			c.headersSeen = true
			close(c.headersReady)
		}
	case "ACK":
		if frame.Direction != "input" || frame.Ack > c.sent {
			return status.Error(codes.DataLoss, "wego: invalid input ACK")
		}
		// 仅推进更大的累计 ACK；例如从 ACK=1 到 ACK=3 释放序号 2、3，重复 ACK=1 不重复扣减预算。
		if frame.Ack <= c.acked {
			return nil
		}
		c.sendBytes -= releaseCredit(c.pending, c.acked, frame.Ack)
		c.acked = frame.Ack
	case "DATA":
		if frame.Direction != "output" || frame.Seq == 0 {
			return status.Error(codes.DataLoss, "wego: invalid output DATA")
		}
		if frame.Seq <= c.consumed {
			return nil
		}
		if c.final != nil && frame.Seq > c.final.LastSeq {
			return status.Error(codes.DataLoss, "wego: DATA beyond final output sequence")
		}
		if old, ok := c.messages[frame.Seq]; ok {
			if !bytes.Equal(old, frame.Payload) {
				return status.Error(codes.DataLoss, "wego: conflicting output DATA")
			}

			return nil
		}
		// 对端输出超出序号窗口、条数或字节预算时视为 DataLoss，不接受任意扩大缓存的帧。
		if frame.Seq-c.consumed > uint64(c.config.Stream.Window) ||
			len(c.messages) >= c.config.Stream.Window ||
			len(frame.Payload) > c.config.Stream.BufferBytes-c.inputBytes {
			return status.Error(codes.DataLoss, "wego: peer exceeded receive window")
		}
		c.messages[frame.Seq] = copyPayload(frame.Payload)
		c.inputBytes += len(frame.Payload)
		if frame.Seq > c.received {
			c.received = frame.Seq
		}
	default:
		return status.Error(codes.DataLoss, "wego: unexpected server frame")
	}
	c.notify()
	return nil
}

// abort 记录首个故障并取消会话；后续取消不能覆盖该故障。
func (c *Client) abort(err error) {
	c.mu.Lock()
	// 仅保存首个错误；例如先发生 DataLoss，清理再产生 Canceled 时仍保留 DataLoss。
	if c.failure == nil {
		c.failure = err
	}
	c.notify()
	c.mu.Unlock()
	c.cancel()
}

// failCall 结束发生错误的传输操作，并返回会话首个故障；只能在解锁后传入非 nil 错误。
// 例如先收到 DataLoss、随后清理产生 Canceled，调用方仍得到 DataLoss。
func (c *Client) failCall(err error) error {
	c.abort(err)
	return c.terminalError(err)
}

// hasOutputGap 在持锁且 final 非 nil 时判断缺口已不可恢复；完整输出须由调用方先判断。
// 终态先到仍可等待迟到 DATA，只有矛盾末序号、订阅结束或缺口预算到期才失败。
func (c *Client) hasOutputGap() bool {
	return c.final.LastSeq < c.consumed || c.subscriptionClosed || time.Since(c.finalAt) >= c.config.Stream.HandshakeTimeout
}

// completionError 确认最后输出已经消费后读取终态；有缺口不能返回成功 EOF。
func (c *Client) completionError() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.failure != nil {
		return c.failure
	}
	if c.final != nil && c.final.LastSeq == c.consumed {
		st := status.New(codes.OK, "").Proto()
		if err := proto.Unmarshal(c.final.Status, st); err != nil {
			return status.Error(codes.DataLoss, "wego: malformed final status")
		}

		return status.FromProto(st).Err()
	}

	return status.FromContextError(c.ctx.Err()).Err()
}

// terminalError 优先返回已经观察到的协议故障，避免清理触发的取消掩盖真正原因。
func (c *Client) terminalError(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.failure != nil {
		err = c.failure
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}

	return err
}

// waitError 保留调用方取消和已有 status；只有没有分类的传输故障才标为 Unavailable。
func waitError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Unavailable, err.Error())
}
