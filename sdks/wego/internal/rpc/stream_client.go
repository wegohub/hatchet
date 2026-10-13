package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// bufferedOutput 保存预取业务消息及交付位置；预取不会推进消费 checkpoint
type bufferedOutput struct {
	// data 是已经整帧 codec 还原的业务 ProtoJSON
	data json.RawMessage
	// cursor 是这条 DATA 的持久位置
	cursor string
	// state 记录此输出交付时的执行身份和序号
	state stream.State
}

// taskClientStream 将发送缓冲区在 CloseSend 时一次提交，两个方向独立串行访问
type taskClientStream struct {
	// runOptions 是构造时冻结的运行选项，与原生提交共用。
	runOptions model.RunOptions
	// ownsRun 仅新提交且确认归本调用时取消远端；恢复消费者不拥有任务。
	ownsRun bool
	// ctx 与 cancel 管理此客户端流所有 SDK goroutine
	ctx context.Context
	// cancel 在 EOF、错误或调用方取消时释放后台传输
	cancel context.CancelFunc
	// engine 复用实例后端和配置
	engine *engine.Engine
	// desc 确认三种流方向
	desc *grpc.StreamDesc
	// method 是完整生成方法名
	method string
	// inputType 和 outputType 用于拒绝错误的 protobuf 类型
	inputType, outputType protoreflect.FullName
	// options 保存有效方法预算
	options spec.StreamOptions
	// maxSend 和 maxRecv 对单条 protobuf 应用标准调用限额，不改变整批输入或相反方向的预算
	maxSend, maxRecv int
	// codec 还原完整控制和业务帧
	codec *wire.FrameCodec
	// opts 写回标准 gRPC headers 和 trailers
	opts []grpc.CallOption
	// routing 是调用创建时冻结的一份调度快照
	routing map[string]any
	// operationKey 是原始幂等键，仅在提交异常时交付调用方保存
	operationKey string
	// submission 是本次实际提交的随机标识，响应丢失时只读查找此标识
	submission string
	// key 在所有提交重试中固定
	key string
	// sendMu 串行保护输入快照与 CloseSend
	sendMu sync.Mutex
	// recvMu 拒绝并发读取导致 checkpoint 次序不明确
	recvMu sync.Mutex
	// inputs 保存编码后的业务快照，CloseSend 才对整批执行一次 codec。
	inputs []json.RawMessage
	// inputBytes 记录业务快照总量；数组标点及封包另行计费。
	inputBytes int
	// submitted 标记输入已经关闭，不允许继续 Send
	submitted bool
	// submitErr 保留 CloseSend 的唯一提交结果
	submitErr error
	// inputClosed 在一次提交完成后关闭，Recv 可等待 EOF 提交而不触发隐式半包
	inputClosed chan struct{}
	// mu 保护接收状态，网络 I/O 永远在此锁外执行
	mu sync.Mutex
	// changed 广播状态变化，等待者醒来后重新检查条件
	changed chan struct{}
	// queue 保存数量及字节均有界的预取输出
	queue []bufferedOutput
	// queueBytes 是 queue 中保留的业务字节成本
	queueBytes int
	// state 是已解析日志前缀；与已经交付的 delivered 分开
	state stream.State
	// deliveredState 是最近一条成功交付 DATA 的身份快照
	deliveredState stream.State
	// initialCursor 仅用于恢复订阅的初始位置
	initialCursor *string
	// delivered 是最后成功解码并返回给业务的输出序号
	delivered uint64
	// scannedCursor 是最后解释成功的持久位置，同一游标重投不重复解释 DATA
	scannedCursor string
	// deliveredCursor 是最后交付 DATA 的持久位置
	deliveredCursor string
	// finished 表示传输已经释放，随后 Recv 仍返回同一个终态
	finished bool
	// terminalErr 是释放时的最终错误，nil 表示成功 EOF
	terminalErr error
	// result 是引擎权威最终成功结果
	result *ports.Result
	// resultErr 是引擎权威失败，不将传输 hangup 视作 EOF
	resultErr error
	// resultEnd 是权威失败所携带的最终清单，控制台取消可没有清单
	resultEnd *model.StreamCompletion
	// resultDone 区分未完成与成功空输出
	resultDone bool
	// streamErr 保存不可恢复的订阅或协议错误
	streamErr error
	// disconnected 表示最近一次传输失败尚未收到新帧，超时不能据此断言历史已损坏
	disconnected bool
	// responseRead 只允许交付一次 client stream 响应
	responseRead bool
	// headers 和 trailers 为外部调用返回独立副本
	headers, trailers metadata.MD
	// headersReady 区分空响应头与尚未发布
	headersReady bool
	// headersDelivered 表示 Header 或成功 Recv 已把头部语义交给调用方
	headersDelivered bool
	// runID 用于调用方取消时显式取消整个运行
	runID string
	// notices 为旧执行通知提供独立有界队列，发送失败不阻塞日志交付
	notices chan ports.WorkerCancelNotice
	// background 确认结果观察和订阅资源都退出
	background sync.WaitGroup
	// closed 保证完成回调仅调用一次
	closed sync.Once
	// complete 结束实例的在途登记及 span
	complete func(error)
}

// newTaskClient 验证标准生成描述与 codec，创建流时不向 broker 提交任何任务
func newTaskClient(ctx context.Context, e *engine.Engine, desc *grpc.StreamDesc, method string, complete func(error), opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return buildTaskClient(ctx, e, desc, method, complete, false, opts...)
}

// clientMethodDescriptor 使用生成描述验证流方向，JSON 内容不能改变注册的 RPC 形态。
func clientMethodDescriptor(method string, desc *grpc.StreamDesc) (protoreflect.MethodDescriptor, error) {
	parts := strings.Split(method, "/")
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(parts[1]))
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "wego: protobuf service descriptor unavailable")
	}
	service, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "wego: invalid protobuf service descriptor")
	}
	m := service.Methods().ByName(protoreflect.Name(parts[2]))
	if m == nil || m.IsStreamingClient() != desc.ClientStreams || m.IsStreamingServer() != desc.ServerStreams {
		return nil, status.Error(codes.InvalidArgument, "wego: stream descriptor mismatch")
	}
	return m, nil
}

// clientFrameCodec 先检查流预算与所需传输能力，失败时不创建输入缓冲或订阅。
func clientFrameCodec(e *engine.Engine, desc *grpc.StreamDesc, options spec.StreamOptions) (*wire.FrameCodec, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	if desc.ServerStreams && options.Mode == spec.Reliable {
		if _, ok := e.Backend.(ports.DurableStreams); !ok {
			return nil, status.Error(codes.Unimplemented, "wego: reliable streams unavailable")
		}
	}
	return wire.NewFrameCodec(e.Config, options.MaxFrameBytes, options.MaxEncodedMessageBytes, e.FrameObserver())
}

// buildTaskClient 复用传输装配；恢复只核验已有输入，不读取当前定义或生成新的调度配置。
func buildTaskClient(ctx context.Context, e *engine.Engine, desc *grpc.StreamDesc, method string, complete func(error), resume bool, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if desc == nil {
		return nil, status.Error(codes.InvalidArgument, "wego: missing stream descriptor")
	}
	if err := binding.ValidateName(e.Config.Namespace, method); err != nil {
		return nil, err
	}
	m, err := clientMethodDescriptor(method, desc)
	if err != nil {
		return nil, err
	}
	o := e.Config.StreamOptionsFor(method)
	parsed, err := ParseCallOptions(opts, resume)
	if err != nil {
		return nil, err
	}
	maxSend, maxRecv := parsed.MaxSend, parsed.MaxRecv
	if !resume {
		ctx = ports.WithExpectedOutput(ctx, string(m.Output().FullName()))
		ctx, err = ports.PrepareTarget(ctx, e.Backend, method, streamKind(desc), modeName(o.Mode), string(m.Input().FullName()))
		if err != nil {
			return nil, err
		}
	}
	codec, err := clientFrameCodec(e, desc, o)
	if err != nil {
		return nil, err
	}
	routing := map[string]any{}
	if !resume {
		routing, err = e.Config.Routing(method, parsed.Run.Routing)
	}
	if err != nil {
		return nil, err
	}
	key := parsed.Run.StreamIdempotencyKey
	if key == "" {
		key = uuid.NewString()
	}
	originalComplete := complete
	complete = func(err error) { originalComplete(err); parsed.Finish(&err) }
	ctx, cancel := context.WithCancel(ctx)
	s := &taskClientStream{ctx: ctx, cancel: cancel, engine: e, desc: desc, method: method, inputType: m.Input().FullName(), outputType: m.Output().FullName(), options: o, codec: codec, opts: append([]grpc.CallOption(nil), opts...), routing: routing, operationKey: key, submission: uuid.NewString(), key: key, changed: make(chan struct{}), inputClosed: make(chan struct{}), complete: complete}
	s.maxSend, s.maxRecv = maxSend, maxRecv
	s.runOptions, s.ownsRun = parsed.Run, !resume
	if desc.ServerStreams && o.Mode == spec.Reliable {
		s.notices = make(chan ports.WorkerCancelNotice, 16)
	}
	s.ctx = callctx.WithConsumer(s.ctx, s.resumeToken)
	go func() { <-ctx.Done(); s.finish(status.FromContextError(ctx.Err()).Err()) }()
	return s, nil
}

// Context 返回标准调用上下文，取消后所有传输均使用同一个退出屏障
func (s *taskClientStream) Context() context.Context { return s.ctx }

// notify 在状态锁内广播，队列消费者和字节预算等待者都会重新检查条件
func (s *taskClientStream) notify() { close(s.changed); s.changed = make(chan struct{}) }

// SendMsg 编码并保存独立快照，Send 成功不代表任务已提交
func (s *taskClientStream) SendMsg(value any) (err error) {
	message, ok := value.(proto.Message)
	if !ok || message == nil || !message.ProtoReflect().IsValid() || message.ProtoReflect().Descriptor().FullName() != s.inputType {
		return status.Error(codes.InvalidArgument, "wego: stream request protobuf type mismatch")
	}
	if !s.sendMu.TryLock() {
		return status.Error(codes.FailedPrecondition, "wego: concurrent stream sends")
	}
	defer s.sendMu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if s.submitted {
		return io.EOF
	}
	// 整批输入超限后禁止提交已缓冲的前缀，例如第 65 条失败不能只执行前 64 条
	defer func() {
		if status.Code(err) == codes.ResourceExhausted {
			s.submitted, s.submitErr = true, err
			s.inputs = nil
			close(s.inputClosed)
			s.fail(err)
		}
	}()
	if s.maxSend >= 0 && proto.Size(message) > s.maxSend {
		return status.Error(codes.ResourceExhausted, "wego: protobuf request exceeds call send limit")
	}
	if len(s.inputs) >= s.options.MaxInputMessages || !s.desc.ClientStreams && len(s.inputs) > 0 {
		return status.Error(codes.ResourceExhausted, "wego: stream request count exceeds limit")
	}
	plain, err := wire.MarshalMessage(message, s.options.MaxMessageBytes)
	if err != nil {
		return err
	}
	logicalBytes := s.inputBytes + len(plain)
	if s.desc.ClientStreams {
		logicalBytes += 2 + len(s.inputs) // 数组括号与已有条目之间的逗号也计入逻辑预算
	}
	if logicalBytes > s.options.MaxInputBytes {
		return status.Error(codes.ResourceExhausted, "wego: stream input bytes exceed limit")
	}
	s.inputs = append(s.inputs, plain)
	s.inputBytes += len(plain)
	return nil
}

// CloseSend 在发送锁内只提交一次，EOF 后的缓冲数组不会拆成多个任务
func (s *taskClientStream) CloseSend() error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.submitted {
		return s.submitErr
	}
	s.submitted = true
	defer close(s.inputClosed)
	if err := s.ctx.Err(); err != nil {
		s.submitErr = status.FromContextError(err).Err()
		return s.submitErr
	}
	if !s.desc.ClientStreams && len(s.inputs) != 1 {
		s.submitErr = status.Error(codes.InvalidArgument, "wego: server stream requires one request")
		s.fail(s.submitErr)
		return s.submitErr
	}
	// 空批次仍有 [] 两个字节；不能因没有 SendMsg 绕过逻辑输入预算
	if s.desc.ClientStreams && s.inputBytes+2+max(0, len(s.inputs)-1) > s.options.MaxInputBytes {
		s.submitErr = status.Error(codes.ResourceExhausted, "wego: complete logical input array exceeds limit")
		s.inputs = nil
		s.fail(s.submitErr)
		return s.submitErr
	}
	// err 保存封包及一次提交的失败，CloseSend 不进行部分提交。
	var err error
	// 空输入流明确编码为 []，不把 nil 编码成 null
	payload := json.RawMessage("[]")
	if s.desc.ClientStreams {
		if len(s.inputs) > 0 {
			payload, err = json.Marshal(s.inputs)
		}
	} else {
		payload = s.inputs[0]
	}
	if err != nil {
		s.submitErr = err
		s.fail(err)
		return err
	}
	digest, err := wire.EventDigest(payload, s.routing)
	if err != nil {
		s.submitErr = err
		s.fail(err)
		return err
	}
	envelope := wire.NewEnvelope(s.ctx, s.method, string(s.inputType), payload, s.routing)
	envelope.Stream, envelope.StreamMode, envelope.InputDigest, envelope.IdempotencyKey = streamKind(s.desc), modeName(s.options.Mode), digest, s.key
	budget, cancel := context.WithTimeout(s.ctx, s.engine.Config.PayloadLimits.EncodeTimeout)
	envelope, err = wire.Seal(budget, envelope, s.engine.Config, s.options.MaxInputBytes, s.options.MaxInputBytes+(64<<10), s.options.MaxEncodedInputBytes)
	cancel()
	if err != nil {
		s.submitErr = err
		s.fail(err)
		return err
	}
	data, err := json.Marshal(envelope)
	if err != nil || len(data) > s.options.MaxEncodedInputBytes {
		s.submitErr = status.Error(codes.ResourceExhausted, "wego: complete stream envelope exceeds limit")
		s.fail(s.submitErr)
		return s.submitErr
	}
	// JSON 快照、输入摘要及随机 key 到此冻结；重试不能重新运行业务 codec
	ref, err := s.submit(envelope)
	s.inputs = nil
	if err != nil {
		s.submitErr = err
		s.fail(err)
		return err
	}
	s.mu.Lock()
	s.runID = ref.ID
	s.state = stream.State{RunID: ref.ID, Method: s.method, InputDigest: stream.ExecutionDigest(s.engine.Config.Namespace, s.method, digest)}
	s.mu.Unlock()
	s.startObservers(ref)
	return nil
}

// attachExistingStream 以实际运行版本核对绑定与输入，再以观察者身份消费；取消不能终止原调用。
func (s *taskClientStream) attachExistingStream(budget context.Context, runID string, envelope wire.Envelope) (ports.Run, error) {
	reader, ok := s.engine.Backend.(ports.RunInputReader)
	lookup, lookupOK := s.engine.Backend.(ports.RunLookup)
	if !ok || !lookupOK {
		return ports.Run{}, status.Error(codes.Unimplemented, "wego: run recovery unavailable")
	}
	if validator, ok := s.engine.Backend.(ports.RunTargetValidator); ok {
		if err := validator.ValidateRunTarget(budget, runID, s.method, ports.RPCContract{Shape: streamKind(s.desc), Mode: modeName(s.options.Mode), InputType: string(s.inputType), OutputType: string(s.outputType)}); err != nil {
			return ports.Run{}, err
		}
	}
	input, err := reader.RunInput(budget, runID)
	if err != nil {
		return ports.Run{}, err
	}
	if err := verifyStoredStream(budget, s.engine, input, s.method, envelope.InputDigest); err != nil {
		return ports.Run{}, err
	}
	// 同方法不能用另一种流方向或传输模式恢复，实时输出也不能假装可回放
	var previous wire.Envelope
	if err := json.Unmarshal(input, &previous); err != nil || previous.StreamMode != envelope.StreamMode {
		return ports.Run{}, status.Error(codes.FailedPrecondition, "wego: persisted stream shape or mode differs")
	}
	if s.desc.ServerStreams && s.options.Mode == spec.Realtime {
		return ports.Run{}, &model.SubmissionError{Err: status.Error(codes.FailedPrecondition, "wego: existing realtime output cannot be replayed"), RunID: runID, OperationKey: s.operationKey}
	}
	s.ownsRun = false
	_, ref, err := lookup.LookupRun(budget, runID)
	return ref, err
}

// submit 对不明确提交复用相同 envelope；幂等冲突先核对历史输入再恢复观察
func (s *taskClientStream) submit(envelope wire.Envelope) (ref ports.Run, err error) {
	uncertain := false
	defer func() {
		if err != nil && uncertain {
			ref, err = s.resolveSubmission(envelope, err)
		}
	}()
	budget, cancel := context.WithTimeout(s.ctx, s.options.PublishTimeout)
	defer cancel()
	for {
		if budget.Err() != nil {
			return ports.Run{}, status.FromContextError(budget.Err()).Err()
		}
		opts := spec.CloneRunOptions(s.runOptions)
		if opts.AdditionalMetadata == nil {
			opts.AdditionalMetadata = map[string]string{}
		}
		opts.AdditionalMetadata["wego_submission"] = s.submission
		ref, err := s.engine.Backend.Run(budget, s.method, envelope, opts)
		if err == nil {
			return ref, nil
		}
		collision := new(model.IdempotencyCollisionError)
		if errors.As(err, &collision) {
			return s.attachExistingStream(budget, collision.ExistingRunID, envelope)
		}
		code := status.Code(err)
		uncertain = uncertain || code == codes.Unavailable || code == codes.DeadlineExceeded || code == codes.Unknown || code == codes.Canceled
		if budget.Err() != nil {
			return ports.Run{}, status.FromContextError(budget.Err()).Err()
		}
		if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded && status.Code(err) != codes.Unknown {
			return ports.Run{}, err
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-budget.Done():
			timer.Stop()
			return ports.Run{}, status.FromContextError(budget.Err()).Err()
		case <-timer.C:
		}
	}
}
