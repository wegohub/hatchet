package rpc

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// taskServerStream 在单次任务中适配 gRPC；Recv 与 Send 各允许一个并发调用
type taskServerStream struct {
	// ctx 保留任务身份、输入 deadline 及 checkpoint
	ctx context.Context
	// engine 提供本实例 codec 和观测
	engine *engine.Engine
	// method 用于 protobuf 类型与双向能力校验
	method binding.Method
	// options 是本方法的有效资源预算
	options spec.StreamOptions
	// input 由唯一 feeder 填充并关闭
	input chan json.RawMessage
	// recvMu 防止并发 Recv 破坏业务顺序
	recvMu sync.Mutex
	// sendMu 同时保护 headers、trailers、响应及 producer 序号
	sendMu sync.Mutex
	// headers 暂存首次输出之前的多值响应头
	headers metadata.MD
	// trailers 保存最终状态交付的尾部
	trailers metadata.MD
	// headerSent 防止重复 SendHeader 和修改已发布的响应头
	headerSent bool
	// response 仅用于 client stream 的唯一最终响应
	response *wire.Envelope
	// producer 在 server/bidi 方向发布完整帧
	producer *stream.Producer
	// history 在重试时要求业务先恢复历史，首次执行为空
	history *stream.History
	// sendError 保留首次发布失败，即使业务忽略 Send 错误也不能伪报成功
	sendError error
}

// Context 返回包含任务身份的标准上下文
func (s *taskServerStream) Context() context.Context { return s.ctx }

// SetHeader 只在首次响应头发布前合并缓存
func (s *taskServerStream) SetHeader(md metadata.MD) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.headerSent {
		return status.Error(codes.Internal, "wego: headers already sent")
	}
	s.headers = metadata.Join(s.headers, md)
	return nil
}

// SendHeader 发布完整 HEADERS 帧；client stream 随任务结果携带
func (s *taskServerStream) SendHeader(md metadata.MD) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.headerSent {
		return status.Error(codes.Internal, "wego: headers already sent")
	}
	s.headers = metadata.Join(s.headers, md)
	return s.flushHeaders()
}

// flushHeaders 在发送锁内发布；发布失败不允许后续 DATA 越过
func (s *taskServerStream) flushHeaders() error {
	if s.headerSent {
		return s.sendError
	}
	s.headerSent = true
	if s.producer != nil {
		s.sendError = s.producer.Publish(s.ctx, &wire.LogFrame{Kind: "HEADERS", Metadata: wire.Metadata(s.headers)})
	}
	return s.sendError
}

// SetTrailer 在发送锁内复制多值尾部，调用方修改原 map 不改变结果
func (s *taskServerStream) SetTrailer(md metadata.MD) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.trailers = metadata.Join(s.trailers, md)
}

// RecvMsg 逐条还原业务 codec，channel 耗尽只半关闭输入方向
func (s *taskServerStream) RecvMsg(value any) error {
	message, ok := value.(proto.Message)
	if !ok || message == nil || !message.ProtoReflect().IsValid() || message.ProtoReflect().Descriptor().FullName() != s.method.Descriptor.Input().FullName() {
		return status.Error(codes.InvalidArgument, "wego: stream request protobuf type mismatch")
	}
	if !s.recvMu.TryLock() {
		return status.Error(codes.FailedPrecondition, "wego: concurrent stream receives")
	}
	defer s.recvMu.Unlock()
	select {
	case <-s.ctx.Done():
		return status.FromContextError(s.ctx.Err()).Err()
	case data, open := <-s.input:
		if !open {
			return io.EOF
		}
		if err := protojson.Unmarshal(data, message); err != nil {
			return status.Error(codes.DataLoss, "wego: stream request decode failed")
		}
		return nil
	}
}

// SendMsg 对 server/bidi 只编码完整 DATA 帧；client stream 最终响应独立应用一次业务 codec
func (s *taskServerStream) SendMsg(value any) (err error) {
	message, ok := value.(proto.Message)
	if !ok || message == nil || !message.ProtoReflect().IsValid() || message.ProtoReflect().Descriptor().FullName() != s.method.Descriptor.Output().FullName() {
		return status.Error(codes.InvalidArgument, "wego: stream response protobuf type mismatch")
	}
	if !s.sendMu.TryLock() {
		return status.Error(codes.FailedPrecondition, "wego: concurrent stream sends")
	}
	defer s.sendMu.Unlock()
	if s.sendError != nil {
		return s.sendError
	}
	if s.history != nil && !s.history.Restored() {
		return status.Error(codes.FailedPrecondition, "wego: checkpoint must be read to EOF before new output")
	}
	// 编码、资源或传输失败必须进入最终状态；业务忽略 Send 错误也不能伪报完整成功
	defer func() {
		if err != nil {
			s.sendError = err
		}
	}()
	plain, err := wire.MarshalMessage(message, s.options.MaxMessageBytes)
	if err != nil {
		return err
	}
	if !s.method.Stream.ServerStreams {
		if s.response != nil {
			return status.Error(codes.Internal, "wego: duplicate client stream response")
		}
		envelope, err := wire.EncodeSnapshot(s.ctx, s.method.FullName, string(message.ProtoReflect().Descriptor().FullName()), plain, nil, s.engine.Config.Middleware, s.options.MaxEncodedMessageBytes)
		if err != nil {
			return err
		}
		s.response = &envelope
		return nil
	}
	if err := s.flushHeaders(); err != nil {
		return err
	}
	if s.producer.LastOutput == ^uint64(0) {
		return status.Error(codes.ResourceExhausted, "wego: output sequence exhausted")
	}
	err = s.producer.Publish(s.ctx, &wire.LogFrame{Kind: "DATA", OutputSeq: s.producer.LastOutput + 1, Payload: plain})
	if err == nil {
		s.producer.LastOutput++
	} else {
		s.sendError = err
	}
	return err
}
