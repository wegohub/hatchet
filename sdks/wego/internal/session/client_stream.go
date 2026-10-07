package session

import (
	"context"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// Context 返回当前执行上下文，包含取消与截止时间。
func (c *Client) Context() context.Context {
	return c.ctx
}

// Header 等待响应头到达或调用结束，返回头部副本。
func (c *Client) Header() (metadata.MD, error) {
	select {
	case <-c.headersReady:
		c.mu.Lock()
		defer c.mu.Unlock()
		c.applyHeaders()
		return c.headers.Copy(), c.failure
	case <-c.ctx.Done():
		return nil, c.terminalError(c.ctx.Err())
	}
}

// Trailer 读取响应尾部副本，不把内部 map 直接交给调用方修改。
func (c *Client) Trailer() metadata.MD {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.trailers.Copy()
}

// applyHeaders 把已缓存响应头复制给 grpc.Header 指定的目标，避免重复应用。
func (c *Client) applyHeaders() {
	if c.headerOptionsApplied || !c.headersSeen {
		return
	}

	c.headerOptionsApplied = true
	for _, opt := range c.opts {
		if header, ok := opt.(grpc.HeaderCallOption); ok {
			*header.HeaderAddr = c.headers.Copy()
		}
	}
}

// applyTrailers 把最终 trailer 复制给 grpc.Trailer 指定的目标。
func (c *Client) applyTrailers() {
	for _, opt := range c.opts {
		if trailer, ok := opt.(grpc.TrailerCallOption); ok {
			*trailer.TrailerAddr = c.trailers.Copy()
		}
	}
}

// SendMsg 编码并发送一条 protobuf 消息；窗口或字节预算不足时等待累计 ACK。
func (c *Client) SendMsg(message any) error {
	p, err := protobufMessage(message, "stream message")
	if err != nil {
		return err
	}

	envelope, err := wire.Encode(c.ctx, c.method, p, nil, c.config.Middleware, c.config.Stream.MaxMessageBytes)
	if err != nil {
		return err
	}

	// payload 载荷变换后的业务字节，消息窗口按这些实际发送字节计算成本。
	payload := envelope.Payload
	for {
		c.mu.Lock()
		if c.failure != nil {
			err := c.failure
			c.mu.Unlock()
			return err
		}
		if c.sendClosed {
			c.mu.Unlock()
			return status.Error(codes.FailedPrecondition, "wego: send direction closed")
		}
		if c.final != nil {
			c.mu.Unlock()
			return io.EOF
		}
		if len(c.pending) < c.config.Stream.Window && len(payload) <= c.config.Stream.BufferBytes-c.sendBytes {
			// 输入序号耗尽时拒绝新消息，禁止回绕成 DATA=0 或复用已有序号。
			if c.sent == ^uint64(0) {
				c.mu.Unlock()
				return status.Error(codes.ResourceExhausted, "wego: input sequence exhausted")
			}
			c.sent++
			seq := c.sent
			c.pending[seq] = len(payload)
			c.sendBytes += len(payload)
			c.mu.Unlock()
			for {
				ack, err := c.control(c.ctx, &wire.Frame{
					Kind:      "DATA",
					Direction: "input",
					Seq:       seq,
					Payload:   payload,
				})
				if err == nil {
					c.mu.Lock()
					// 控制任务返回的累计确认也属于对端协议，不能确认尚未发送的输入。
					if ack.Consumed > c.sent {
						c.mu.Unlock()
						c.abort(status.Error(codes.DataLoss, "wego: control ACK exceeds input sequence"))
						return c.terminalError(c.ctx.Err())
					}
					if ack.Consumed > c.acked {
						c.sendBytes -= releaseCredit(c.pending, c.acked, ack.Consumed)
						c.acked = ack.Consumed
					}
					c.notify()
					c.mu.Unlock()
					return nil
				}
				if status.Code(err) != codes.ResourceExhausted {
					return c.failCall(err)
				}

				// 对端窗口可能尚未消费到这一条，只重发同一序号；其余错误立即终止。
				select {
				case <-c.ctx.Done():
					return c.terminalError(c.ctx.Err())
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
		// changed 在锁内登记广播代，解锁后等待不会丢失紧接着到达的 DATA 或 ACK。
		changed := c.watch()
		c.mu.Unlock()
		if err := waitForChange(c.ctx, changed); err != nil {
			return c.terminalError(err)
		}
	}
}

// CloseSend 发送输入 END，仅关闭发送方向；客户端仍需读取剩余输出和最终状态。
func (c *Client) CloseSend() error {
	c.mu.Lock()
	if c.sendClosed {
		c.mu.Unlock()
		return nil
	}

	c.sendClosed = true
	seq := c.sent
	c.mu.Unlock()
	_, err := c.control(c.ctx, &wire.Frame{Kind: "END", Direction: "input", Seq: seq})
	if err != nil {
		return c.terminalError(err)
	}

	return nil
}

// RecvMsg 按序交付输出，消费后确认；收齐输出且最终 status 为 OK 时才返回 EOF。
// 已收到终态但仍缺少输出时，等待超时或订阅结束会返回 DataLoss。
// 例如输出到达顺序为 2、1、3，业务读取顺序仍为 1、2、3；最后状态非 OK 时返回错误。
func (c *Client) RecvMsg(message any) error {
	p, err := protobufMessage(message, "stream response")
	if err != nil {
		return err
	}

	for {
		c.mu.Lock()
		if c.failure != nil {
			err := c.failure
			c.mu.Unlock()
			return c.terminalError(err)
		}
		c.applyHeaders()
		// gap 终态输出缺口的超时通知，只有观察终态但缺消息时才启用。
		var gap <-chan time.Time
		// gapTimer 输出缺口计时器，成功补齐或返回前必须停止。
		var gapTimer *time.Timer
		if payload, ok := c.messages[c.consumed+1]; ok {
			c.consumed++
			seq := c.consumed
			delete(c.messages, seq)
			c.inputBytes -= len(payload)
			c.notify()
			c.mu.Unlock()
			_, err := c.control(c.ctx, &wire.Frame{Kind: "ACK", Direction: "output", Ack: seq})
			if err != nil {
				return c.failCall(err)
			}

			err = wire.Decode(c.ctx, c.method, wire.Envelope{Version: wire.Version, Payload: payload}, p, c.config.Middleware, c.config.Stream.MaxMessageBytes)
			if err != nil {
				c.abort(err)
			}
			return err
		}
		if c.final != nil {
			final := c.final
			st := status.New(codes.OK, "").Proto()
			if err := proto.Unmarshal(final.Status, st); err != nil {
				c.mu.Unlock()
				failure := status.Error(codes.DataLoss, "wego: malformed final status")
				c.abort(failure)
				return failure
			}
			if final.LastSeq == c.consumed {
				c.applyTrailers()
				if st.Code != 0 {
					c.mu.Unlock()
					c.cancel()
					return status.FromProto(st).Err()
				}
				if !c.description.ServerStreams && !c.responseRead {
					// 存在性独立于字节长度；空 protobuf 合法，未发送响应违反单响应约定。
					if !final.HasResponse {
						c.mu.Unlock()
						failure := status.Error(codes.Internal, "wego: client stream completed without response")
						c.abort(failure)
						return failure
					}
					c.responseRead = true
					payload := copyPayload(final.Response)
					c.mu.Unlock()
					err := wire.Decode(c.ctx, c.method, wire.Envelope{Version: wire.Version, Payload: payload}, p, c.config.Middleware, c.config.Stream.MaxMessageBytes)
					if err != nil {
						c.abort(err)
					} else {
						c.cancel()
					}
					return err
				}

				c.mu.Unlock()
				c.cancel()
				return io.EOF
			}
			if c.hasOutputGap() {
				c.mu.Unlock()
				failure := status.Error(codes.DataLoss, "wego: output sequence gap")
				c.abort(failure)
				return failure
			}

			gapTimer = time.NewTimer(time.Until(c.finalAt.Add(c.config.Stream.HandshakeTimeout)))
			gap = gapTimer.C
		}
		// changed 在锁内登记广播代，解锁后等待不会丢失紧接着到达的 DATA 或 ACK。
		changed := c.watch()
		c.mu.Unlock()
		select {
		case <-changed:
			if gapTimer != nil {
				gapTimer.Stop()
			}
		case <-gap:
			c.abort(status.Error(codes.DataLoss, "wego: output sequence gap"))
		case <-c.ctx.Done():
			if gapTimer != nil {
				gapTimer.Stop()
			}
			return c.terminalError(c.ctx.Err())
		}
	}
}

// protobufMessage 统一流发送与接收的类型校验，不把非 protobuf 对象当成空消息。
// 例如 (*Reply)(nil) 虽满足接口，也必须在消费缓存或发送控制任务之前拒绝。
func protobufMessage(value any, role string) (proto.Message, error) {
	message, ok := value.(proto.Message)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "wego: %s must be protobuf", role)
	}
	if !message.ProtoReflect().IsValid() {
		return nil, status.Error(codes.InvalidArgument, "wego: nil protobuf message")
	}
	return message, nil
}

// Cancel 在独立预算内发送取消控制，随后终止本地调用；在途计数由后台清理完成后释放。
func (c *Client) Cancel() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, _ = c.control(ctx, &wire.Frame{Kind: "CANCEL"})
	c.abort(context.Canceled)
}
