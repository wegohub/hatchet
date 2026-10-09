package rpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// observeResult 保留权威结果；观察订阅失败由 backend 重连，业务失败不会重新提交
func (s *taskClientStream) observeResult(ref ports.Run) {
	defer s.background.Done()
	result, err := ref.Wait(s.ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resultDone, s.resultErr = true, err
	if err == nil {
		s.result = &result
	}
	if err == nil && !s.desc.ServerStreams {
		out, outErr := SingleOutput(result.Outputs)
		if outErr == nil {
			envelope, decodeErr := wire.AsEnvelope(out)
			if decodeErr == nil {
				s.headers = metadata.MD(envelope.Headers).Copy()
				s.headersReady = true
				s.trailers = metadata.MD(envelope.Trailers).Copy()
			}
		}
	}
	if transport := new(model.RPCError); errors.As(err, &transport) {
		if !s.headersReady {
			s.headers = metadata.MD(transport.Headers).Copy()
			s.headersReady = true
		}
		s.resultEnd = transport.Stream
		s.trailers = metadata.MD(transport.Trailers).Copy()
	}
	s.notify()
}

// observeOutput 查询真实 task 身份后订阅持久输出，短暂断网从内部扫描游标重连
func (s *taskClientStream) observeOutput(runID string) {
	defer s.background.Done()
	// 实时订阅必须先建立，不能为查询读模型扩大开始发布与订阅之间的间隙
	if s.options.Mode == spec.Realtime {
		err := s.engine.Backend.Stream(s.ctx, runID, func(value string) error {
			if len(value) > base64.StdEncoding.EncodedLen(s.options.MaxEncodedMessageBytes) {
				return status.Error(codes.ResourceExhausted, "wego: realtime encoded frame exceeds limit")
			}
			data, err := base64.StdEncoding.DecodeString(value)
			if err != nil {
				return status.Error(codes.DataLoss, "wego: invalid realtime frame")
			}
			return s.accept(ports.DurableEntry{Payload: data})
		})
		if s.ctx.Err() == nil {
			s.mu.Lock()
			// 已收齐结束帧时仍需等待引擎确认，实时订阅先结束不能覆盖已完整的结果
			complete := s.state.End != nil && s.state.End.OutputSeq == s.state.LastOutput
			s.disconnected = true
			s.notify()
			s.mu.Unlock()
			if !complete {
				s.fail(err)
			}
		}
		return
	}
	lookup, ok := s.engine.Backend.(ports.RunLookup)
	if !ok {
		s.fail(status.Error(codes.Unimplemented, "wego: task lookup unavailable"))
		return
	}
	taskID, _, err := lookup.LookupRun(s.ctx, runID)
	if err != nil {
		s.fail(err)
		return
	}
	s.mu.Lock()
	s.state.TaskID = taskID
	s.notify()
	s.mu.Unlock()
	// cursor 只在成功解释并入队后推进，不等同交付 checkpoint
	cursor := s.initialCursor
	transport := s.engine.Backend.(ports.DurableStreams)
	for s.ctx.Err() == nil {
		err = transport.SubscribeDurable(s.ctx, ports.DurableSubscription{Namespace: s.engine.Config.Namespace, Topic: stream.Topic(taskID), Cursor: cursor}, func(entry ports.DurableEntry) error {
			if err := s.accept(entry); err != nil {
				return err
			}
			next := entry.Cursor
			cursor = &next
			return nil
		})
		if s.ctx.Err() != nil {
			return
		}
		if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded && status.Code(err) != codes.Unknown {
			s.fail(err)
			return
		}
		s.mu.Lock()
		s.disconnected = true
		s.notify()
		s.mu.Unlock()
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// accept 整帧还原后在锁内解释；有界队列满时只阻塞本次业务日志订阅
func (s *taskClientStream) accept(entry ports.DurableEntry) error {
	budget, cancel := context.WithTimeout(s.ctx, s.options.DecodeTimeout)
	frame, err := s.codec.Decode(budget, s.method, entry.Payload)
	cancel()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disconnected = false
	if entry.Cursor != "" && entry.Cursor == s.scannedCursor {
		return nil
	}
	// 实时出口没有持久起点查询，首个 CLAIM 提供 task 身份，最终清单再次核验
	if s.options.Mode == spec.Realtime && s.state.TaskID == "" && frame.Kind == "CLAIM" {
		if uuid.Validate(frame.TaskRunId) != nil {
			return status.Error(codes.DataLoss, "wego: invalid realtime task identity")
		}
		s.state.TaskID = frame.TaskRunId
	}
	old := s.state
	accepted, err := s.state.Apply(frame)
	if err != nil {
		return err
	}
	// 更高 CLAIM 的旧执行通知是退出加速手段；有效输出规则不依赖通知是否送达
	if s.notices != nil && frame.Kind == "CLAIM" && old.Claimed && s.state.Epoch > old.Epoch {
		notice := ports.WorkerCancelNotice{WorkerKey: old.WorkerKey, TaskID: old.TaskID, OldEpoch: old.Epoch, OldWriter: old.Writer, NewEpoch: s.state.Epoch}
		select {
		case s.notices <- notice:
		default:
			s.engine.Config.Logger.Warn("old execution notification queue full", "task_id", old.TaskID, "old_epoch", old.Epoch, "new_epoch", s.state.Epoch)
		}
	}
	if s.state.HeadersSeen && !s.headersReady {
		s.headers = wire.FromMetadata(s.state.Headers)
		s.headersReady = true
		s.notify()
	}
	if frame.Kind == "ATTEMPT_END" && s.state.End != nil && s.state.End.FrameId == frame.FrameId {
		s.notify()
	}
	if !accepted {
		s.scannedCursor = entry.Cursor
		s.notify()
		return nil
	}
	if len(frame.Payload) > s.options.MaxMessageBytes {
		return status.Error(codes.ResourceExhausted, "wego: output message exceeds limit")
	}
	for len(s.queue) >= s.options.PrefetchMessages || s.queueBytes+len(frame.Payload) > s.options.PrefetchBytes {
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-s.ctx.Done():
			s.mu.Lock()
			return s.ctx.Err()
		case <-changed:
			s.mu.Lock()
		}
	}
	// State 快照不复制结束帧；HEADERS 已在首次交付时固定，不随预取修改
	snapshot := s.state
	snapshot.End = nil
	s.queue = append(s.queue, bufferedOutput{data: json.RawMessage(frame.Payload), cursor: entry.Cursor, state: snapshot})
	s.queueBytes += len(frame.Payload)
	s.recordMetric("wego_output_prefetch_messages", "active", 1)
	s.recordMetric("wego_output_prefetch_bytes", "active", float64(len(frame.Payload)))
	s.scannedCursor = entry.Cursor
	s.notify()
	return nil
}

// streamCompletion 从独立任务唯一结果解码最终清单
func streamCompletion(result ports.Result) (stream.Completion, error) {
	value, err := SingleOutput(result.Outputs)
	if err != nil {
		return stream.Completion{}, err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return stream.Completion{}, err
	}
	// completion 只携带 wego 自有最终身份
	var completion stream.Completion
	if err := json.Unmarshal(data, &completion); err != nil {
		return completion, status.Error(codes.DataLoss, "wego: invalid stream completion")
	}
	return completion, nil
}

// fail 保存首个传输错误并唤醒消费者，后台由 finish 的取消屏障统一回收
func (s *taskClientStream) fail(err error) {
	if err == nil {
		err = status.Error(codes.Unavailable, "wego: output subscription ended")
	}
	s.mu.Lock()
	if s.streamErr == nil {
		s.streamErr = err
	}
	s.notify()
	s.mu.Unlock()
	go s.finish(err)
}

// finish 取消后等待 SDK 观察资源退出，再释放实例登记；正常 EOF 不取消已经完成的任务
func (s *taskClientStream) finish(err error) {
	s.closed.Do(func() {
		// 先与提交锁同步，保证 Wait 开始后不会新增后台观察者
		s.sendMu.Lock()
		s.mu.Lock()
		s.finished = true
		s.terminalErr = err
		s.recordMetric("wego_output_prefetch_messages", "active", -float64(len(s.queue)))
		s.recordMetric("wego_output_prefetch_bytes", "active", -float64(s.queueBytes))
		s.queue, s.queueBytes = nil, 0
		s.notify()
		s.mu.Unlock()
		s.cancel()
		s.sendMu.Unlock()
		s.background.Wait()
		if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
			s.mu.Lock()
			id := s.runID
			s.mu.Unlock()
			if id != "" {
				budget, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), time.Second)
				if cancelErr := s.engine.Backend.Feature(budget, ports.RunsCancel{Request: map[string]any{"externalIds": []string{id}}}, nil); cancelErr != nil {
					s.engine.Config.Logger.Warn("run cancellation request failed", "run_id", id, "error", cancelErr)
				}
				cancel()
			}
		}
		s.complete(err)
	})
}

// recordMetric 只使用此已验证 RPC 的有限标签，运行 UUID 和 cursor 留在日志及断点中
func (s *taskClientStream) recordMetric(name, outcome string, value float64) {
	s.engine.ObserveProtocol(ports.ProtocolMetric{Name: name, Method: s.method, Mode: modeName(s.options.Mode), Outcome: outcome, Value: value})
}
