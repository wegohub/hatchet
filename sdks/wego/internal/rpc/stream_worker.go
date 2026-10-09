package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/logging"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// admittedKey 把 CLAIM 门禁确认的 producer 传给同一次执行，不能由新 Function 猜测所有权
type admittedKey struct{}

// admittedOutput 保存获胜 writer 和有限历史边界，不跨物理执行共享
type admittedOutput struct {
	// producer 负责冻结字节及串行发布
	producer *stream.Producer
	// claim 是本次规范 CLAIM 的持久读回结果
	claim stream.ClaimResult
}

// StreamDefinition 将三种流各自绑定为一个普通 StandaloneTask
func StreamDefinition(e *engine.Engine, method binding.Method, policy spec.Task, panicHandler func(context.Context, any)) (ports.Definition, error) {
	o := e.Config.StreamOptionsFor(method.FullName)
	if err := o.Validate(); err != nil {
		return ports.Definition{}, err
	}
	if policy.Durable || policy.Batch != nil || len(policy.Cron) > 0 || len(policy.Events) > 0 {
		return ports.Definition{}, fmt.Errorf("wego: streaming requires an ordinary standalone RPC without trigger input")
	}
	codec, err := wire.NewFrameCodec(e.Config.Middleware, o.MaxFrameBytes, o.MaxEncodedMessageBytes, e.FrameObserver())
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
	policy.IdempotencyExpression, policy.IdempotencyTTL = "input.idempotency_key", o.IdempotencyTTL
	if err := policy.Validate(); err != nil {
		return ports.Definition{}, err
	}
	definition := ports.Definition{Name: binding.Name(method.FullName), RPCMethod: method.FullName, RPCShape: streamKind(method.Stream), RPCMode: modeName(o.Mode), Policy: policy}
	if reliable {
		definition.BeforeStart = func(ctx context.Context, start ports.StartInfo) (ports.Admission, error) {
			envelope, err := wire.AsEnvelope(start.Input)
			if err != nil {
				return ports.Admission{}, err
			}
			if err := validateStreamEnvelope(envelope, method, o); err != nil {
				return ports.Admission{}, err
			}
			request := stream.ClaimRequest{Namespace: e.Config.Namespace, TaskID: start.Task.TaskRunID, RunID: start.Task.RunID, Method: method.FullName, InputDigest: envelope.InputDigest, WorkerKey: start.WorkerKey, Writer: uuid.NewString(), Epoch: int32(start.Task.RetryCount), Timeout: o.ClaimTimeout, RecoveryTimeout: o.RecoveryTimeout, MaxReplayFrames: o.MaxReplayFrames, MaxReplayBytes: o.MaxReplayBytes}
			claim, err := stream.Claim(ctx, e.Backend.(ports.DurableStreams), codec, request)
			if err != nil {
				return ports.Admission{}, err
			}
			if !claim.Owned {
				return ports.Admission{}, nil
			}
			producer := &stream.Producer{Backend: e.Backend, Codec: codec, Options: o, Identity: request, Next: 1, LastOutput: claim.Prefix.LastOutput}
			return ports.Admission{Owned: true, Writer: request.Writer, Context: context.WithValue(ctx, admittedKey{}, &admittedOutput{producer: producer, claim: claim})}, nil
		}
	}
	definition.Function = func(ctx context.Context, input any) (output any, business error) {
		envelope, err := wire.AsEnvelope(input)
		if err != nil {
			return nil, err
		}
		if err := validateStreamEnvelope(envelope, method, o); err != nil {
			return nil, err
		}
		ctx, cancel := wire.Incoming(ctx, envelope)
		defer cancel()
		ctx = e.Context(ctx)
		// 请求字段绑定到 context，普通日志和显式上报共享同一方法身份。
		ctx = logging.With(ctx, slog.String("rpc_method", method.FullName), slog.String("rpc_transport", "worker"))
		ctx, finish := e.StartSpan(ctx, method.FullName, trace.SpanKindServer)
		defer func() { finish(business) }()
		ss := &taskServerStream{ctx: ctx, engine: e, method: method, options: o, headers: metadata.MD{}, trailers: metadata.MD{}}
		if reliable {
			admission, ok := ctx.Value(admittedKey{}).(*admittedOutput)
			if !ok {
				return nil, status.Error(codes.Internal, "wego: missing reliable output admission")
			}
			ss.producer = admission.producer
			trace.SpanFromContext(ctx).SetAttributes(attribute.String("wego.writer_nonce", ss.producer.Identity.Writer), attribute.Int("wego.epoch", int(ss.producer.Identity.Epoch)))
			ss.history = stream.NewHistory(ctx, e.Backend.(ports.DurableStreams), codec, ss.producer.Identity, admission.claim, o, method.Descriptor.Output().FullName())
			defer ss.history.Close()
			ss.ctx = callctx.WithCheckpoint(ctx, ss.history)
		} else if method.Stream.ServerStreams {
			state, ok := callctx.Get(ctx)
			if !ok || state.Execution == nil {
				return nil, model.ErrTaskContext
			}
			info := state.Execution.Info()
			ss.producer = &stream.Producer{Backend: e.Backend, Codec: codec, Options: o, Identity: stream.ClaimRequest{Namespace: e.Config.Namespace, TaskID: info.TaskRunID, RunID: info.RunID, Method: method.FullName, InputDigest: envelope.InputDigest, WorkerKey: info.WorkerKey, Writer: uuid.NewString(), Epoch: int32(info.RetryCount)}, Next: 0}
			// 实时 CLAIM 只声明输出身份，不提供持久竞争或接管保证
			if info.RetryCount != 0 {
				return nil, status.Error(codes.FailedPrecondition, "wego: realtime attempt cannot resume")
			}
			if err := ss.producer.Publish(ctx, &wire.LogFrame{Kind: "CLAIM"}); err != nil {
				return nil, err
			}
		}
		requests, err := streamRequests(ss.ctx, envelope, method, o, e.Config.Middleware)
		if err != nil {
			return nil, err
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
		ss.sendMu.Lock()
		defer ss.sendMu.Unlock()
		if ss.sendError != nil && business == nil {
			business = ss.sendError
		}
		if method.Stream.ServerStreams {
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
			return completion, nil
		}
		if business != nil {
			return nil, &model.RPCError{Err: business, Headers: ss.headers.Copy(), Trailers: ss.trailers.Copy()}
		}
		if ss.response == nil {
			return nil, status.Error(codes.Internal, "wego: client stream must send exactly one response")
		}
		ss.response.Headers, ss.response.Trailers = ss.headers.Copy(), ss.trailers.Copy()
		return ss.response, nil
	}
	return definition, nil
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
	if envelope.Version != wire.Version || envelope.Method != method.FullName || envelope.Stream != streamKind(method.Stream) || envelope.StreamMode != modeName(o.Mode) || envelope.InputDigest == "" || envelope.Type != string(method.Descriptor.Input().FullName()) {
		return status.Error(codes.FailedPrecondition, "wego: stream binding or mode mismatch")
	}
	return nil
}

// streamRequests 校验整批 JSON 大小和请求数量，空 client/bidi 输入必须明确为 []
func streamRequests(ctx context.Context, envelope wire.Envelope, method binding.Method, o spec.StreamOptions, chain []middleware.Option) ([]json.RawMessage, error) {
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
		budget, cancel := context.WithTimeout(ctx, o.DecodeTimeout)
		plain, err := wire.RestorePayload(budget, method.FullName, request, chain, o.MaxMessageBytes, o.MaxEncodedMessageBytes)
		cancel()
		if err != nil {
			return nil, err
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
	digest, err := stream.InputDigest(method.FullName, requests, envelope.Routing)
	if err != nil {
		return nil, err
	}
	if digest != envelope.InputDigest {
		return nil, status.Error(codes.DataLoss, "wego: stream input digest mismatch")
	}
	return requests, nil
}

// invokePanicCallback 隔离观测回调的 panic，确保原业务错误仍能形成结束清单
func invokePanicCallback(callback func(context.Context, any), ctx context.Context, value any) {
	defer func() { _ = recover() }()
	callback(ctx, fmt.Sprint(value))
}
