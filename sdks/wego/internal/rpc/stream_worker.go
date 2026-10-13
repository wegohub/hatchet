package rpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/logging"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// admittedKey 把 CLAIM 门禁确认的 producer 传给同一次执行，不能由新 Function 猜测所有权
type admittedKey struct{}

// admittedOutput 保存获胜 writer 和有限历史边界，不跨物理执行共享
type admittedOutput struct {
	// envelope 保存 START 的封包头；确认 CLAIM 后才解码，以便获胜者报告解码失败。
	envelope wire.Envelope
	// inputError 保存封包解析错误，只有获胜执行可以将其上报为任务失败。
	inputError error
	// producer 负责冻结字节及串行发布
	producer *stream.Producer
	// claim 是本次规范 CLAIM 的持久读回结果
	claim stream.ClaimResult
}

// StreamDefinition 将三种流各自绑定为一个普通 StandaloneTask
func StreamDefinition(e *engine.Engine, method binding.Method, policy spec.Task, panicHandler func(context.Context, any)) (ports.Definition, error) {
	// 默认事件与自定义事件共同生效，后端按实际 namespace key 稳定去重。
	policy.Events = append(append([]string(nil), policy.Events...), method.FullName)
	o := e.Config.StreamOptionsFor(method.FullName)
	if err := o.Validate(); err != nil {
		return ports.Definition{}, err
	}
	if policy.Durable || policy.Batch != nil || len(policy.Cron) > 0 {
		return ports.Definition{}, fmt.Errorf("wego: streaming requires an ordinary standalone RPC without durable, batch or cron")
	}
	codec, err := wire.NewFrameCodec(e.Config, o.MaxFrameBytes, o.MaxEncodedMessageBytes, e.FrameObserver())
	if err != nil {
		return ports.Definition{}, err
	}
	reliable := method.Stream.ServerStreams && o.Mode == spec.Reliable
	if method.Stream.ServerStreams && o.Mode == spec.Realtime && policy.Retries.Set && policy.Retries.Value > 0 {
		return ports.Definition{}, fmt.Errorf("wego: realtime output cannot replay application retries")
	}
	if reliable {
		if _, ok := e.Backend.(ports.DurableStreams); !ok {
			return ports.Definition{}, status.Error(codes.Unimplemented, "wego: reliable stream transport unavailable")
		}
	}
	// 输出可恢复与提交幂等分别判断；client stream 也必须避免不明确提交产生两次执行
	if policy.IdempotencyExpression != "" || policy.IdempotencyStatus || policy.IdempotencyTTL != 0 && policy.IdempotencyTTL != o.IdempotencyTTL {
		return ports.Definition{}, fmt.Errorf("wego: stream reserves its submission idempotency policy")
	}
	// 同一事件触发多个 RPC 时分别占用幂等身份；直接 RPC 的已有 key 保持不变。
	eventPrefix := stream.IdempotencyKey(e.Config.Namespace, method.FullName, "") + ":"
	policy.IdempotencyExpression = fmt.Sprintf("input._wego.idempotency_key != '' ? %q + input._wego.idempotency_key : string(1 / size(input._wego.idempotency_key))", eventPrefix)
	policy.IdempotencyTTL = o.IdempotencyTTL
	if err := policy.Validate(); err != nil {
		return ports.Definition{}, err
	}
	definition := ports.Definition{Name: binding.Name(method.FullName), RPCMethod: method.FullName, RPCShape: streamKind(method.Stream), RPCMode: modeName(o.Mode), InputType: string(method.Descriptor.Input().FullName()), OutputType: string(method.Descriptor.Output().FullName()), Policy: policy}
	if reliable {
		definition.BeforeStart = func(ctx context.Context, start ports.StartInfo) (ports.Admission, error) {
			envelope, inputErr := wire.AsEnvelope(start.Input)
			// CLAIM 只依赖稳定任务身份与封包摘要。解码/类型错误留给获胜者报告，
			// 不能在门禁前返回后被当作落选，导致非法输入一直占据运行状态。
			digest := streamExecutionDigest(e.Config.Namespace, method.FullName, envelope)
			if inputErr != nil || envelope.InputDigest == "" {
				hash := sha256.Sum256(start.Input)
				digest = hex.EncodeToString(hash[:])
			}
			request := stream.ClaimRequest{Namespace: e.Config.Namespace, TaskID: start.Task.TaskRunID, RunID: start.Task.RunID, Method: method.FullName, InputDigest: digest, WorkerKey: start.WorkerKey, Writer: uuid.NewString(), Epoch: int32(start.Task.RetryCount), Timeout: o.ClaimTimeout, RecoveryTimeout: o.RecoveryTimeout, MaxReplayFrames: o.MaxReplayFrames, MaxReplayBytes: o.MaxReplayBytes}
			claim, err := stream.Claim(ctx, e.Backend.(ports.DurableStreams), codec, request)
			if err != nil {
				return ports.Admission{}, err
			}
			if !claim.Owned {
				return ports.Admission{}, nil
			}
			producer := &stream.Producer{Backend: e.Backend, Codec: codec, Options: o, Identity: request, Next: 1, LastOutput: claim.Prefix.LastOutput}
			return ports.Admission{Owned: true, Writer: request.Writer, Context: context.WithValue(ctx, admittedKey{}, &admittedOutput{producer: producer, claim: claim, envelope: envelope, inputError: inputErr})}, nil
		}
	}
	definition.Function = func(ctx context.Context, input any) (output any, business error) {
		envelope, err := wire.AsEnvelope(input)
		if admitted, ok := ctx.Value(admittedKey{}).(*admittedOutput); ok {
			envelope, err = admitted.envelope, admitted.inputError
		}
		if err != nil {
			return nil, err
		}
		if err := wire.ValidateRPCBinding(envelope, method.FullName); err != nil {
			return nil, err
		}
		native := envelope.Response != "wego"
		if envelope.IdempotencyKey == "" {
			return nil, status.Error(codes.InvalidArgument, "wego: stream requires idempotency_key")
		}
		if !native {
			if err := validateStreamEnvelope(envelope, method, o); err != nil {
				return nil, err
			}
		}

		// 输入校验及执行 deadline 先于 codec I/O，恢复读取单独使用观察者预算。
		ctx, cancel := wire.Incoming(ctx, envelope)
		defer cancel()
		budget, stopDecode := context.WithTimeout(ctx, o.DecodeTimeout)
		envelope, err = wire.Open(budget, method.FullName, envelope, e.Config, o.MaxInputBytes, o.MaxEncodedInputBytes)
		stopDecode()
		if err != nil {
			return nil, err
		}
		requests, err := streamRequests(envelope, method, o)
		if err != nil {
			return nil, err
		}
		ctx = e.Context(ctx)
		// 请求字段绑定到 context，普通日志和显式上报共享同一方法身份。
		ctx = logging.With(ctx, slog.String("rpc_method", method.FullName), slog.String("rpc_transport", "worker"))
		ctx, finish := e.StartSpan(ctx, method.FullName, trace.SpanKindServer)
		defer func() { finish(business) }()
		ss := &taskServerStream{ctx: ctx, engine: e, method: method, options: o, native: native, headers: metadata.MD{}, trailers: metadata.MD{}}
		if reliable && !native {
			admission, ok := ctx.Value(admittedKey{}).(*admittedOutput)
			if !ok {
				return nil, status.Error(codes.Internal, "wego: missing reliable output admission")
			}
			ss.producer = admission.producer
			trace.SpanFromContext(ctx).SetAttributes(attribute.String("wego.writer_nonce", ss.producer.Identity.Writer), attribute.Int("wego.epoch", int(ss.producer.Identity.Epoch)))
			ss.history = stream.NewHistory(ctx, e.Backend.(ports.DurableStreams), codec, ss.producer.Identity, admission.claim, o, method.Descriptor.Output().FullName())
			defer ss.history.Close()
			ss.ctx = callctx.WithCheckpoint(ctx, ss.history)
		} else if method.Stream.ServerStreams && !native {
			state, ok := callctx.Get(ctx)
			if !ok || state.Execution == nil {
				return nil, model.ErrTaskContext
			}
			info := state.Execution.Info()
			ss.producer = &stream.Producer{Backend: e.Backend, Codec: codec, Options: o, Identity: stream.ClaimRequest{Namespace: e.Config.Namespace, TaskID: info.TaskRunID, RunID: info.RunID, Method: method.FullName, InputDigest: streamExecutionDigest(e.Config.Namespace, method.FullName, envelope), WorkerKey: info.WorkerKey, Writer: uuid.NewString(), Epoch: int32(info.RetryCount)}, Next: 0}
			// 实时 CLAIM 只声明输出身份，不提供持久竞争或接管保证
			if info.RetryCount != 0 {
				return nil, status.Error(codes.FailedPrecondition, "wego: realtime attempt cannot resume")
			}
			if err := ss.producer.Publish(ctx, &wire.LogFrame{Kind: "CLAIM"}); err != nil {
				return nil, err
			}
		}
		// 有界 channel 由单个 feeder 关闭；handler 提前退出先取消，再等待 feeder 释放
		feedCtx, stopFeed := context.WithCancel(ss.ctx)
		ss.input = make(chan json.RawMessage, min(o.PrefetchMessages, len(requests)))
		feedDone := make(chan struct{})
		go func() {
			defer close(feedDone)
			defer close(ss.input)
			for _, request := range requests {
				select {
				case ss.input <- request:
				case <-feedCtx.Done():
					return
				}
			}
		}()
		defer func() { stopFeed(); <-feedDone }()
		handler := grpc.StreamHandler(func(service any, s grpc.ServerStream) error { return method.Stream.Handler(service, s) })
		for i := len(e.Config.Middleware) - 1; i >= 0; i-- {
			if current := e.Config.Middleware[i].StreamServer; current != nil {
				next := handler
				handler = func(service any, s grpc.ServerStream) error {
					return current(service, s, &grpc.StreamServerInfo{FullMethod: method.FullName, IsClientStream: method.Stream.ClientStreams, IsServerStream: method.Stream.ServerStreams}, next)
				}
			}
		}
		// 业务 panic 仍发布失败 attempt 的可诊断尾部；backend 负责最终错误状态上报
		func() {
			defer func() {
				if value := recover(); value != nil {
					if panicHandler != nil {
						invokePanicCallback(panicHandler, ss.ctx, value)
					}
					business = status.Errorf(codes.Internal, "wego: streaming handler panic: %v", value)
				}
			}()
			business = handler(method.Service, ss)
		}()
		return ss.finalOutput(business)
	}
	return definition, nil
}

// finalOutput 在发送锁内确定唯一终态；原生流只保存计数，client stream 只保存单个响应。
func (ss *taskServerStream) finalOutput(business error) (any, error) {
	ss.sendMu.Lock()
	defer ss.sendMu.Unlock()
	// 最终结果和关闭输出共享同一临界区，不允许 DATA 越过 ATTEMPT_END。
	if ss.finished {
		return nil, status.Error(codes.FailedPrecondition, "wego: server stream finished")
	}
	ss.finished = true
	if ss.sendError != nil && business == nil {
		business = ss.sendError
	}
	if ss.native && ss.method.Stream.ServerStreams {
		if business != nil {
			return nil, business
		}
		return map[string]any{"sent_messages": ss.sentMessages}, nil
	}
	if ss.method.Stream.ServerStreams {
		return ss.streamingOutput(business)
	}
	if business != nil {
		return nil, &model.RPCError{Err: business, Headers: ss.headers.Copy(), Trailers: ss.trailers.Copy()}
	}
	if ss.response == nil {
		return nil, status.Error(codes.Internal, "wego: client stream must send exactly one response")
	}
	ss.response.Headers, ss.response.Trailers = ss.headers.Copy(), ss.trailers.Copy()
	outputBudget, stopOutput := context.WithTimeout(ss.ctx, ss.engine.Config.PayloadLimits.EncodeTimeout)
	defer stopOutput()
	encoded, err := wire.Output(outputBudget, *ss.response, ss.engine.Config, ss.options.MaxMessageBytes, ss.options.MaxMessageBytes+(64<<10), ss.options.MaxEncodedMessageBytes)
	return encoded, err
}

// streamingOutput 将结束帧和引擎最终结果绑定到同一清单，失败绝不包装为成功业务值。
func (ss *taskServerStream) streamingOutput(business error) (any, error) {
	if err := ss.flushHeaders(); err != nil && business == nil {
		business = err
	}
	completion, err := ss.producer.End(ss.ctx, wire.Metadata(ss.trailers), business)
	if business != nil {
		transport := &model.RPCError{Err: business, Headers: ss.headers.Copy(), Trailers: ss.trailers.Copy()}
		if err == nil {
			transport.Stream = &completion
		}
		return nil, transport
	}
	if err != nil {
		return nil, err
	}
	// 最终清单也执行统一 codec，结果出口不会泄漏控制身份。
	plain, typeName, err := wire.MarshalValue(completion, ss.options.MaxMessageBytes)
	if err != nil {
		return nil, err
	}
	packet := wire.NewEnvelope(ss.ctx, ss.method.FullName, typeName, plain, nil)
	budget, cancel := context.WithTimeout(ss.ctx, ss.options.PublishTimeout)
	defer cancel()
	return wire.Output(budget, packet, ss.engine.Config, ss.options.MaxMessageBytes, ss.options.MaxMessageBytes+(64<<10), ss.options.MaxEncodedMessageBytes)
}

// streamKind 使用生成描述确认请求封包形状
func streamKind(desc *grpc.StreamDesc) string {
	if desc.ClientStreams && desc.ServerStreams {
		return "bidi"
	}
	if desc.ClientStreams {
		return "client"
	}
	return "server"
}

// modeName 对客户端和 Worker 使用同一配置表达
func modeName(mode spec.StreamMode) string {
	if mode == spec.Realtime {
		return "realtime"
	}
	return "reliable"
}

// validateStreamEnvelope 拒绝方法、协议和输出模式不一致，不能静默切换传输
func validateStreamEnvelope(envelope wire.Envelope, method binding.Method, o spec.StreamOptions) error {
	if envelope.Version != wire.Version || envelope.Response != "wego" || (envelope.Method != "" && envelope.Method != method.FullName) || (envelope.StreamMode != "" && envelope.StreamMode != modeName(o.Mode)) || envelope.InputDigest == "" {
		return status.Error(codes.FailedPrecondition, "wego: stream binding or mode mismatch")
	}
	return nil
}

// streamRequests 校验整批 JSON 大小和请求数量，空 client/bidi 输入必须明确为 []
func streamRequests(envelope wire.Envelope, method binding.Method, o spec.StreamOptions) ([]json.RawMessage, error) {
	if len(envelope.Payload) > o.MaxEncodedInputBytes {
		return nil, status.Error(codes.ResourceExhausted, "wego: stream input exceeds limit")
	}
	requests := []json.RawMessage{}
	if !method.Stream.ClientStreams {
		requests = append(requests, envelope.Payload)
	} else {
		decoder := json.NewDecoder(bytes.NewReader(envelope.Payload))
		token, err := decoder.Token()
		if err != nil || token != json.Delim('[') {
			return nil, status.Error(codes.DataLoss, "wego: stream input requires request array")
		}
		for decoder.More() {
			if len(requests) >= o.MaxInputMessages {
				return nil, status.Error(codes.ResourceExhausted, "wego: stream request count exceeds limit")
			}
			// request 的分配同时受整批编码预算约束，逐条计数避免先分配超量对象
			var request json.RawMessage
			if err := decoder.Decode(&request); err != nil {
				return nil, status.Error(codes.DataLoss, "wego: malformed stream request")
			}
			requests = append(requests, request)
		}
		if _, err := decoder.Token(); err != nil {
			return nil, status.Error(codes.DataLoss, "wego: malformed stream input array")
		}
		// trailing 禁止一个数组之后夹带额外 JSON
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return nil, status.Error(codes.DataLoss, "wego: trailing stream input")
		}
	}
	total := 0
	if method.Stream.ClientStreams {
		total = 2 + max(0, len(requests)-1)
	}
	if total > o.MaxInputBytes {
		return nil, status.Error(codes.ResourceExhausted, "wego: restored input array exceeds limit")
	}
	for i, request := range requests {
		plain := request
		if len(plain) > o.MaxMessageBytes {
			return nil, status.Error(codes.ResourceExhausted, "wego: request exceeds message limit")
		}
		message := dynamicpb.NewMessage(method.Descriptor.Input())
		if err := protojson.Unmarshal(plain, message); err != nil {
			return nil, status.Error(codes.DataLoss, "wego: invalid stream ProtoJSON input")
		}
		total += len(plain)
		if total > o.MaxInputBytes {
			return nil, status.Error(codes.ResourceExhausted, "wego: restored input batch exceeds limit")
		}
		requests[i] = plain
	}
	if err := verifyInputDigest(envelope); err != nil {
		return nil, err
	}

	return requests, nil
}

// verifyInputDigest 对 codec 前业务值与 routing 重算摘要；普通原生调用不宣称摘要认证。
func verifyInputDigest(envelope wire.Envelope) error {
	if envelope.Response != "wego" {
		return nil
	}
	digest, err := wire.EventDigest(envelope.Payload, envelope.Routing)
	if err != nil {
		return err
	}
	if digest != envelope.InputDigest {
		return status.Error(codes.DataLoss, "wego: stream input digest mismatch")
	}
	return nil
}

// invokePanicCallback 隔离观测回调的 panic，确保原业务错误仍能形成结束清单
func invokePanicCallback(callback func(context.Context, any), ctx context.Context, value any) {
	defer func() { _ = recover() }()
	callback(ctx, fmt.Sprint(value))
}

// verifyStoredStream 同键恢复必须还原 codec 内的输入，并重算指纹；不能只信任明文 digest。
func verifyStoredStream(ctx context.Context, e *engine.Engine, data json.RawMessage, method, digest string) error {

	packet, err := wire.AsEnvelope(string(data))
	if err != nil {
		return err
	}
	if packet.Native || packet.Response != "wego" || (packet.InputDigest != digest && stream.ExecutionDigest(e.Config.Namespace, method, packet.InputDigest) != digest) {
		return status.Error(codes.FailedPrecondition, "wego: stored stream identity differs")
	}
	// 描述符来自生成客户端的固定方法，历史 payload 不能改写目标或输入形态。
	if err := binding.ValidateName(e.Config.Namespace, method); err != nil {
		return err
	}
	parts := strings.Split(method, "/")
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(parts[1]))
	if err != nil {
		return status.Error(codes.FailedPrecondition, "wego: RPC descriptor missing")
	}
	service, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return status.Error(codes.InvalidArgument, "wego: invalid service descriptor")
	}
	md := service.Methods().ByName(protoreflect.Name(parts[2]))
	if md == nil {
		return status.Error(codes.InvalidArgument, "wego: method descriptor missing")
	}
	options := e.Config.StreamOptionsFor(method)
	budget, cancel := context.WithTimeout(ctx, options.DecodeTimeout)
	defer cancel()
	packet, err = wire.Open(budget, method, packet, e.Config, options.MaxInputBytes, options.MaxEncodedInputBytes)
	if err != nil {
		return err
	}
	boundMethod := binding.Method{FullName: method, Descriptor: md, Stream: &grpc.StreamDesc{ClientStreams: md.IsStreamingClient(), ServerStreams: md.IsStreamingServer()}}
	if err := validateStreamEnvelope(packet, boundMethod, options); err != nil {
		return err
	}
	_, err = streamRequests(packet, boundMethod, options)
	return err
}

// streamExecutionDigest 为事件的一对多触发绑定接收方法，不改写事件的认证封包。
func streamExecutionDigest(namespace, method string, envelope wire.Envelope) string {
	return stream.ExecutionDigest(namespace, method, envelope.InputDigest)
}
