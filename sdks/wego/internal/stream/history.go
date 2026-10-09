package stream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// History 以一个消息的队列扫描 CLAIM 之前的日志，不一次性加载全部历史
type History struct {
	// mu 保护恢复完成状态；Next 的序号由单个接收方控制
	mu sync.Mutex
	// nextMu 拒绝多个并发恢复接收，避免结果次序不明确
	nextMu sync.Mutex
	// start 保证首次读取才建立历史订阅
	start sync.Once
	// ctx 与 cancel 限定此恢复扫描生命周期
	ctx context.Context
	// cancel 可中止阻塞中的订阅与队列交付
	cancel context.CancelFunc
	// transport 复用当前实例的持久流连接
	transport ports.DurableStreams
	// codec 还原整帧后才解释历史身份
	codec *wire.FrameCodec
	// request 是本次获胜执行身份
	request ClaimRequest
	// claim 是本次接管边界及此前有效输出计数
	claim ClaimResult
	// options 限制扫描和一条还原消息
	options spec.StreamOptions
	// outputType 限定历史 Next 的业务响应类型，避免兼容字段掩盖错误消息类型
	outputType protoreflect.FullName
	// queue 最多持有一条 ProtoJSON 输出
	queue chan json.RawMessage
	// done 表示扫描 goroutine 已释放
	done chan struct{}
	// err 保存扫描失败，不能当作空历史
	err error
	// complete 只有读到边界且业务取得 EOF 才为真
	complete bool
	// created 和 observed 使恢复耗时与结果只记录一次，重复 Next/Close 不重复计数
	created time.Time
	// observed 控制恢复指标的一次性提交
	observed sync.Once
}

// NewHistory 创建惰性迭代器；首次执行的空历史无需订阅
func NewHistory(ctx context.Context, transport ports.DurableStreams, codec *wire.FrameCodec, request ClaimRequest, claim ClaimResult, options spec.StreamOptions, outputType protoreflect.FullName) *History {
	ctx, cancel := context.WithTimeout(ctx, options.RecoveryTimeout)
	return &History{ctx: ctx, cancel: cancel, transport: transport, codec: codec, request: request, claim: claim, options: options, outputType: outputType, queue: make(chan json.RawMessage, 1), done: make(chan struct{}), complete: claim.Prefix.LastOutput == 0, created: time.Now()}
}

// Restored 在发送新 DATA 前确认业务已完整消化历史；空历史可以直接继续
func (h *History) Restored() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.complete }

// Next 校验消息后交付一条历史输出，读到真实边界才允许新发布
func (h *History) Next(message proto.Message) (err error) {
	defer func() {
		if err == io.EOF {
			h.observe("success")
		} else if err != nil && status.Code(err) != codes.InvalidArgument && status.Code(err) != codes.FailedPrecondition {
			h.observe("failed")
		}
	}()
	if message == nil || !message.ProtoReflect().IsValid() || message.ProtoReflect().Descriptor().FullName() != h.outputType {
		return status.Error(codes.InvalidArgument, "wego: nil checkpoint message")
	}
	if !h.nextMu.TryLock() {
		return status.Error(codes.FailedPrecondition, "wego: concurrent checkpoint reads")
	}
	defer h.nextMu.Unlock()
	if h.claim.Prefix.LastOutput == 0 {
		return io.EOF
	}
	if h.claim.Prefix.LastOutput > uint64(h.options.MaxCheckpointMessages) {
		return status.Error(codes.ResourceExhausted, "wego: checkpoint message count exceeds budget")
	}
	if err := h.ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	h.start.Do(func() { go h.scan() })
	select {
	case <-h.ctx.Done():
		return status.FromContextError(h.ctx.Err()).Err()
	case data, ok := <-h.queue:
		if ok {
			if err := protojson.Unmarshal(data, message); err != nil {
				h.cancel()
				return status.Error(codes.DataLoss, "wego: checkpoint protobuf decode failed")
			}
			return nil
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.err != nil {
			if errors.Is(h.err, context.Canceled) || errors.Is(h.err, context.DeadlineExceeded) {
				return status.FromContextError(h.err).Err()
			}
			return h.err
		}
		h.complete = true
		return io.EOF
	}
}

// observe 仅使用 method/mode/outcome 标签，历史条数作为数值而不是高基数标签
func (h *History) observe(outcome string) {
	h.observed.Do(func() {
		observeProtocol(h.transport, "wego_checkpoint_restore_total", h.request.Method, "reliable", outcome, 1)
		observeProtocol(h.transport, "wego_checkpoint_restore_duration_seconds", h.request.Method, "reliable", outcome, time.Since(h.created).Seconds())
		observeProtocol(h.transport, "wego_checkpoint_history_messages", h.request.Method, "reliable", outcome, float64(h.claim.Prefix.LastOutput))
	})
}

// Close 取消并等待已启动的扫描；未读完历史不产生完成标记
func (h *History) Close() error {
	h.cancel()
	h.start.Do(func() { close(h.done) })
	<-h.done
	return nil
}

// scan 使用与客户端相同的解释器，有限边界之外的帧不属于恢复历史
func (h *History) scan() {
	defer close(h.done)
	defer close(h.queue)
	state := State{TaskID: h.request.TaskID, RunID: h.request.RunID, Method: h.request.Method, InputDigest: h.request.InputDigest}
	frames, encoded, plain, outputBytes := 0, 0, 0, 0
	boundary := errors.New("checkpoint boundary")
	err := SubscribeResumable(h.ctx, h.transport, ports.DurableSubscription{Namespace: h.request.Namespace, Topic: Topic(h.request.TaskID)}, func(entry ports.DurableEntry) error {
		frames++
		encoded += len(entry.Payload)
		if frames > h.options.MaxReplayFrames || encoded > h.options.MaxReplayBytes {
			return status.Error(codes.ResourceExhausted, "wego: checkpoint scan exceeds budget")
		}
		frame, err := decodeBudget(h.ctx, h.options.DecodeTimeout, h.codec, h.request.Method, entry.Payload)
		if err != nil {
			return err
		}
		plain += proto.Size(frame)
		if plain > h.options.MaxReplayBytes {
			return status.Error(codes.ResourceExhausted, "wego: checkpoint restored history exceeds budget")
		}
		accepted, err := state.Apply(frame)
		if err != nil {
			return err
		}
		if entry.Cursor == h.claim.Cursor {
			if frame.Kind != "CLAIM" || state.Writer != h.request.Writer || state.LastOutput != h.claim.Prefix.LastOutput {
				return status.Error(codes.DataLoss, "wego: checkpoint boundary mismatch")
			}
			return boundary
		}
		if accepted {
			outputBytes += len(frame.Payload)
			if outputBytes > h.options.MaxCheckpointBytes {
				return status.Error(codes.ResourceExhausted, "wego: checkpoint output bytes exceed budget")
			}
			if len(frame.Payload) > h.options.MaxMessageBytes {
				return status.Error(codes.ResourceExhausted, "wego: checkpoint message exceeds limit")
			}
			select {
			case h.queue <- json.RawMessage(frame.Payload):
			case <-h.ctx.Done():
				return h.ctx.Err()
			}
		}
		return nil
	})
	if errors.Is(err, boundary) {
		err = nil
	}
	h.mu.Lock()
	h.err = err
	h.mu.Unlock()
}
