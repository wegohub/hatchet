package rpc

import (
	"context"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// waitInput 等待显式 EOF 提交，不从 Recv 或 Header 自动提交部分输入
func (s *taskClientStream) waitInput() error {
	select {
	case <-s.inputClosed:
	default:
		select {
		case <-s.inputClosed:
		case <-s.ctx.Done():
			return status.FromContextError(s.ctx.Err()).Err()
		}
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.submitErr
}

// Header 等待输入提交和真实响应头；等待受调用 context 约束
func (s *taskClientStream) Header() (metadata.MD, error) {
	if err := s.waitInput(); err != nil {
		return nil, err
	}
	// 完成后的缺头部对账使用独立预算，正常长任务不设置空闲失败期限
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		s.mu.Lock()
		if s.headersReady {
			s.headersDelivered = true
			md := s.headers.Copy()
			s.mu.Unlock()
			return md, nil
		}
		if s.resultDone && s.resultErr != nil || s.streamErr != nil {
			err := s.resultErr
			if err == nil {
				err = s.streamErr
			}
			md := s.headers.Copy()
			s.mu.Unlock()
			return md, err
		}
		if s.resultDone && timer == nil {
			timer = time.NewTimer(s.options.CompletionTimeout)
		}
		changed := s.changed
		s.mu.Unlock()
		// timeout 为 nil 时不消耗等待预算，业务运行多久由调用方决定
		var timeout <-chan time.Time
		if timer != nil {
			timeout = timer.C
		}
		select {
		case <-s.ctx.Done():
			return nil, status.FromContextError(s.ctx.Err()).Err()
		case <-changed:
		case <-timeout:
			// 没有读到帧可能是传输不可达，不能仅凭等待超时断言持久历史损坏
			err := status.Error(codes.Unavailable, "wego: terminal response headers unavailable within reconciliation budget")
			s.finish(err)
			return nil, err
		}
	}
}

// Trailer 返回独立多值副本，在最终 Recv 之后读取能得到权威 attempt 的尾部
func (s *taskClientStream) Trailer() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trailers.Copy()
}

// RecvMsg 只在成功解码并交付后推进进度；EOF 必须同时满足引擎结果和完整结束清单
func (s *taskClientStream) RecvMsg(value any) error {
	message, ok := value.(proto.Message)
	if !ok || message == nil || !message.ProtoReflect().IsValid() || message.ProtoReflect().Descriptor().FullName() != s.outputType {
		return status.Error(codes.InvalidArgument, "wego: stream response protobuf type mismatch")
	}
	if !s.recvMu.TryLock() {
		return status.Error(codes.FailedPrecondition, "wego: concurrent stream receives")
	}
	defer s.recvMu.Unlock()
	if err := s.waitInput(); err != nil {
		return err
	}
	// timer 仅在正在等待缺失的最终输出时计时；业务暂停消费不消耗对账预算
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		s.mu.Lock()
		if s.finished {
			err := s.terminalErr
			s.mu.Unlock()
			if err != nil {
				return err
			}
			return io.EOF
		}
		if err := s.ctx.Err(); err != nil {
			if s.streamErr != nil {
				err := s.streamErr
				s.mu.Unlock()
				return err
			}
			s.mu.Unlock()
			return status.FromContextError(err).Err()
		}
		if len(s.queue) > 0 {
			item := s.queue[0]
			if err := protojson.Unmarshal(item.data, message); err != nil {
				s.mu.Unlock()
				s.fail(status.Error(codes.DataLoss, "wego: output protobuf decode failed"))
				return status.Error(codes.DataLoss, "wego: output protobuf decode failed")
			}
			if s.maxRecv > 0 && proto.Size(message) > s.maxRecv {
				s.mu.Unlock()
				err := status.Error(codes.ResourceExhausted, "wego: protobuf response exceeds call receive limit")
				s.finish(err)
				return err
			}
			s.queue[0] = bufferedOutput{}
			s.queue = s.queue[1:]
			s.queueBytes -= len(item.data)
			s.recordMetric("wego_output_prefetch_messages", "active", -1)
			s.recordMetric("wego_output_prefetch_bytes", "active", -float64(len(item.data)))
			s.delivered, s.deliveredCursor = item.state.LastOutput, item.cursor
			s.deliveredState = item.state
			s.headersDelivered = s.headersReady
			s.recordMetric("wego_output_messages_total", "success", 1)
			s.notify()
			s.applyOptionsLocked()
			s.mu.Unlock()
			return nil
		}
		if s.streamErr != nil {
			err := s.streamErr
			s.applyOptionsLocked()
			s.mu.Unlock()
			s.finish(err)
			return err
		}
		if s.resultDone && s.resultErr != nil {
			// 错误清单存在时，先交付此前有效 DATA；没有结束帧的取消或崩溃直接返回引擎错误
			if s.resultEnd == nil || s.state.End != nil && s.state.End.FrameId == s.resultEnd.EndID {
				err := s.resultErr
				if s.resultEnd != nil {
					if verifyErr := stream.VerifyFailureCompletion(s.state, s.delivered, *s.resultEnd, s.resultErr); verifyErr != nil {
						err = verifyErr
					}
				}
				s.applyOptionsLocked()
				s.mu.Unlock()
				s.finish(err)
				return err
			}
			if timer == nil {
				timer = time.NewTimer(s.options.CompletionTimeout)
			}
		}

		if s.resultDone && s.resultErr == nil {
			if !s.desc.ServerStreams {
				if s.responseRead {
					s.applyOptionsLocked()
					s.mu.Unlock()
					s.finish(nil)
					return io.EOF
				}
				// result 在结果观察器完成后不再修改；解码可能下载对象，必须先释放状态锁
				result := *s.result
				s.mu.Unlock()
				return s.receiveResponse(result, message)
			}
			final, err := streamCompletion(*s.result)
			if err != nil {
				s.mu.Unlock()
				s.finish(err)
				return err
			}
			if s.state.End != nil && s.state.End.FrameId == final.EndID {
				err = stream.VerifyCompletion(s.state, s.delivered, final)
				s.trailers = wire.FromMetadata(s.state.End.Trailers)
				s.applyOptionsLocked()
				s.mu.Unlock()
				s.finish(err)
				if err != nil {
					return err
				}
				return io.EOF
			}
			if timer == nil {
				timer = time.NewTimer(s.options.CompletionTimeout)
			}
		}
		changed := s.changed
		s.mu.Unlock()
		// timeout 为 nil 时 select 禁用此分支，尚未完成的引擎任务只受调用方预算约束
		var timeout <-chan time.Time
		if timer != nil {
			timeout = timer.C
		}
		select {
		case <-s.ctx.Done():
			return status.FromContextError(s.ctx.Err()).Err()
		case <-changed:
		case <-timeout:
			s.mu.Lock()
			unconfirmed := s.disconnected || s.state.End == nil
			s.mu.Unlock()
			err := status.Error(codes.DataLoss, "wego: terminal run contradicts output log")
			if unconfirmed {
				err = status.Error(codes.Unavailable, "wego: terminal output could not be confirmed within reconciliation budget; checkpoint can be resumed")
			}
			s.finish(err)
			return err
		}
	}
}

// applyOptionsLocked 在状态锁内写回标准 metadata 指针，只复制自有内容
func (s *taskClientStream) applyOptionsLocked() { applyMetadata(s.opts, s.headers, s.trailers) }

// receiveResponse 还原 client stream 的唯一响应，远程 codec 不持有状态锁
// recvMu 保证只有一个接收者；例如对象下载期间 Stop 可以先取消订阅和释放登记
func (s *taskClientStream) receiveResponse(result ports.Result, message proto.Message) error {
	out, err := SingleOutput(result.Outputs)
	envelope := wire.Envelope{}
	if err == nil {
		envelope, err = wire.AsEnvelope(out)
	}
	if err == nil {
		budget, cancel := context.WithTimeout(s.ctx, s.options.DecodeTimeout)
		err = wire.DecodeWithLimits(budget, s.method, envelope, message, s.engine.Config.Middleware, s.options.MaxMessageBytes, s.options.MaxEncodedMessageBytes)
		if budget.Err() != nil {
			err = status.FromContextError(budget.Err()).Err()
		}
		cancel()
	}
	if err == nil && s.maxRecv > 0 && proto.Size(message) > s.maxRecv {
		err = status.Error(codes.ResourceExhausted, "wego: protobuf response exceeds call receive limit")
	}
	s.mu.Lock()
	// 关闭可与下载并行，迟到的解码结果不能覆盖终态或推进响应位置
	if s.finished {
		terminal := s.terminalErr
		s.mu.Unlock()
		if terminal != nil {
			return terminal
		}
		return io.EOF
	}
	if s.ctx.Err() != nil {
		err = status.FromContextError(s.ctx.Err()).Err()
	}
	if err == nil {
		s.headers = metadata.MD(envelope.Headers).Copy()
		s.headersReady = true
		s.trailers = metadata.MD(envelope.Trailers).Copy()
		s.responseRead = true
		s.applyOptionsLocked()
	}
	s.mu.Unlock()
	s.finish(err)
	return err
}
