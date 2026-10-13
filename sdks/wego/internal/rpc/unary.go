package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/logging"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// Input 为所有任务准备统一封包；地址语法决定绑定，消息类型只决定序列化。
func Input(ctx context.Context, config spec.Runtime, name string, input any, options ...model.RunOptions) (string, any, error) {
	opts := model.RunOptions{}
	if len(options) > 0 {
		opts = options[0]
	}
	if opts.ValidationError != nil {
		return "", nil, opts.ValidationError
	}
	input, defaults, err := wire.InputValue(input)
	if err != nil {
		return "", nil, err
	}
	override := map[string]any{}
	for key, value := range defaults {
		override[key] = value
	}
	for key, value := range opts.Routing {
		override[key] = value
	}
	method := ""
	if strings.HasPrefix(name, "/") {
		if err := binding.ValidateName(config.Namespace, name); err != nil {
			return "", nil, err
		}
		if _, ok := input.(proto.Message); !ok {
			return "", nil, status.Error(codes.InvalidArgument, "wego: RPC input must be protobuf")
		}
		method = name
	}
	if opts.StreamIdempotencyKey != "" {
		return "", nil, status.Error(codes.InvalidArgument, "wego: stream idempotency key requires a streaming RPC")
	}
	routing, err := config.Routing(name, override)
	if err != nil {
		return "", nil, status.Errorf(codes.InvalidArgument, "wego: routing: %v", err)
	}
	limits := config.PayloadLimits
	plain, typeName, err := wire.MarshalValue(input, limits.MaxMessageBytes)
	if err != nil {
		return "", nil, err
	}
	envelope := wire.NewEnvelope(ctx, method, typeName, plain, routing)
	budget, cancel := context.WithTimeout(ctx, limits.EncodeTimeout)
	defer cancel()
	envelope, err = wire.Seal(budget, envelope, config, limits.MaxMessageBytes, limits.MaxMessageBytes+(64<<10), limits.MaxEncodedBytes)
	return name, envelope, err
}

// validMessage 排除 nil 与 typed nil protobuf，避免访问无效反射实例。
func validMessage(value proto.Message) bool { return value != nil && value.ProtoReflect().IsValid() }

// matchesMessage 校验具体消息类型，拦截器返回值也必须符合注册描述。
func matchesMessage(value any, name protoreflect.FullName) bool {
	message, ok := value.(proto.Message)
	return ok && validMessage(message) && message.ProtoReflect().Descriptor().FullName() == name
}

// Invoke 实现 grpc.ClientConnInterface 的 unary 调用，将 protobuf 请求提交为任务并把结果解码到 reply；调用上下文控制等待预算
func Invoke(ctx context.Context, e *engine.Engine, method string, args, reply any, opts ...grpc.CallOption) (err error) {
	// 初始选项也覆盖拦截器短路；完成回调在实例登记释放之后执行。
	completion, err := ParseCallOptions(opts, false)
	defer func() { completion.Finish(&err) }()
	if err != nil {
		return err
	}
	// ctx, done, err 登记本次调用并取得结束回调，返回前释放登记以允许实例排空
	ctx, done, err := e.BeginIO(ctx, "rpc.unary")
	if err != nil {
		return err
	}

	defer done()

	// message, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值
	message, ok := args.(proto.Message)
	// 检查 !ok；不满足协议或配置约束时返回 InvalidArgument（wego: request must be protobuf）
	if !ok || !validMessage(message) {
		return status.Error(codes.InvalidArgument, "wego: request must be protobuf")
	}

	// response, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值
	response, ok := reply.(proto.Message)
	// 检查 !ok；不满足协议或配置约束时返回 InvalidArgument（wego: response must be protobuf）
	if !ok || !validMessage(response) {
		return status.Error(codes.InvalidArgument, "wego: response must be protobuf")
	}

	// invoke 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据
	invoke := func(ctx context.Context, method string, req, reply any, _ *grpc.ClientConn, callOptions ...grpc.CallOption) (err error) {
		// ctx, finish 开始属于当前实例的 span，结束时按实际执行错误标记状态
		ctx, finish := e.StartSpan(ctx, method, trace.SpanKindClient)
		defer func() {
			finish(err)
		}()

		// name, input, err 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值
		parsed, err := ParseCallOptions(callOptions, false)
		if err != nil {
			return err
		}
		completion = parsed
		request, ok := req.(proto.Message)
		if !ok || !validMessage(request) {
			return status.Error(codes.InvalidArgument, "wego: request must be protobuf")
		}
		// 拦截器可替换 invoker 的参数；替换后的响应仍须符合 protobuf 契约，不能触发类型断言 panic。
		response, _ := reply.(proto.Message)
		if !validMessage(response) {
			return status.Error(codes.InvalidArgument, "wego: response must be protobuf")
		}
		if parsed.MaxSend >= 0 && proto.Size(request) > parsed.MaxSend {
			return status.Error(codes.ResourceExhausted, "wego: request exceeds call send limit")
		}
		ctx = ports.WithExpectedOutput(ctx, wire.ValueType(reply))
		ctx, err = ports.PrepareTarget(ctx, e.Backend, method, "unary", "", wire.ValueType(req))
		if err != nil {
			return err
		}
		name, input, err := Input(ctx, e.Config, method, req, parsed.Run)
		if err != nil {
			return err
		}

		// ref, err 接收 e.Backend.Run 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		ref, err := e.Backend.Run(ctx, name, input, parsed.Run)
		if err != nil {
			if ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}
			return err
		}

		result, err := ref.Wait(ctx)
		if err != nil {
			return unaryWaitError(ctx, e, ref.ID, callOptions, err)
		}
		return decodeUnaryResult(ctx, e, method, reply, parsed, callOptions, result)
	}
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
	for i := len(e.Config.Middleware) - 1; i >= 0; i-- {
		// interceptor 本层 unary 客户端拦截器，捕获内层 next 后组成调用链，保持配置的执行顺序
		interceptor := e.Config.Middleware[i].UnaryClient
		if interceptor != nil {
			// next 构造当前层之前的内层调用闭包，捕获此值避免所有拦截器递归引用同一最终变量
			next := invoke
			invoke = func(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, opts ...grpc.CallOption) error {
				return interceptor(ctx, method, req, reply, conn, next, opts...)
			}
		}
	}
	return invoke(ctx, method, message, response, nil, opts...)
}

// unaryWaitError 保留响应 metadata，并仅取消当前调用拥有的运行；durable 子运行必须可重放。
func unaryWaitError(ctx context.Context, e *engine.Engine, runID string, options []grpc.CallOption, err error) error {
	// transport 保存带响应 metadata 的失败，字段与业务 details 分离。
	var transport *model.RPCError
	if errors.As(err, &transport) {
		applyMetadata(options, transport.Headers, transport.Trailers)
	}
	state, _ := callctx.Get(ctx)
	durableChild := state != nil && state.Execution != nil && state.Execution.Info().Durable
	if ctx.Err() == nil {
		return err
	}
	if !durableChild {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		if cancelErr := e.Backend.Feature(cleanup, ports.RunsCancel{Request: map[string]any{"externalIds": []string{runID}}}, nil); cancelErr != nil {
			e.Config.Logger.Warn("RPC cancellation request failed", "run_id", runID, "error", cancelErr)
		}
	}
	return status.FromContextError(ctx.Err()).Err()
}

// decodeUnaryResult 先完整验证和解码，再提交 reply；超限或损坏响应不留下半更新消息。
func decodeUnaryResult(ctx context.Context, e *engine.Engine, method string, reply any, parsed CallOptions, options []grpc.CallOption, result ports.Result) error {
	out, err := SingleOutput(result.Outputs)
	if err != nil {
		return err
	}
	envelope, err := wire.AsEnvelope(out)
	if err != nil {
		return err
	}
	if envelope.Native || envelope.Response != "wego" {
		return status.Error(codes.DataLoss, "wego: unexpected RPC response contract")
	}
	response, ok := reply.(proto.Message)
	if !ok || !validMessage(response) {
		return status.Error(codes.InvalidArgument, "wego: response must be protobuf")
	}
	budget, cancel := context.WithTimeout(ctx, e.Config.PayloadLimits.DecodeTimeout)
	defer cancel()
	decoded := response.ProtoReflect().New().Interface()
	if err := wire.DecodeValue(budget, method, envelope, decoded, e.Config, e.Config.PayloadLimits.MaxMessageBytes, e.Config.PayloadLimits.MaxEncodedBytes); err != nil {
		return err
	}
	applyMetadata(options, envelope.Headers, envelope.Trailers)
	if parsed.MaxRecv >= 0 && proto.Size(decoded) > parsed.MaxRecv {
		return status.Error(codes.ResourceExhausted, "wego: response exceeds call receive limit")
	}
	proto.Reset(response)
	proto.Merge(response, decoded)
	return nil
}

// SingleOutput 读取独立任务唯一输出，多输出或缺失输出返回明确错误
// 例如 {"task-a":value} 只含一个结果时返回 value；两个键不能猜测哪个是业务响应
func SingleOutput(outputs map[string]any) (any, error) {
	if len(outputs) != 1 {
		return nil, fmt.Errorf("wego: expected standalone output, got %d tasks", len(outputs))
	}

	// 逐项处理 outputs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, value := range outputs {
		return value, nil
	}
	return nil, fmt.Errorf("wego: missing output")
}

// UnaryDefinition 将 gRPC unary 描述绑定成独立任务定义，恢复标准 handler 的上下文和响应
func UnaryDefinition(ctx context.Context, e *engine.Engine, method binding.Method, policy spec.Task) (ports.Definition, error) {
	// 默认事件不能被方法覆盖清除；复制切片避免改变其他方法共享的策略。
	policy.Events = append(append([]string(nil), policy.Events...), method.FullName)
	if len(policy.Cron) > 0 && policy.CronInput == nil {
		policy.CronInput = dynamicpb.NewMessage(method.Descriptor.Input())
	}
	if policy.CronInput != nil {
		// Cron 编码保留启动取消与 I/O 预算，不把启动 deadline 写入未来触发的请求
		_, input, err := Input(wire.ForTrigger(ctx), e.Config, method.FullName, policy.CronInput)
		if err != nil {
			return ports.Definition{}, err
		}

		policy.CronInput = input
	}
	return ports.Definition{
		Name:       binding.Name(method.FullName),
		RPCMethod:  method.FullName,
		InputType:  string(method.Descriptor.Input().FullName()),
		OutputType: string(method.Descriptor.Output().FullName()),
		Policy:     policy,
		Function: func(ctx context.Context, input any) (output any, err error) {
			// envelope, err 接收 wire.AsEnvelope 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
			envelope, err := wire.AsEnvelope(input)
			if err != nil {
				return nil, err
			}

			if err := wire.ValidateRPCBinding(envelope, method.FullName); err != nil {
				return nil, err
			}
			// 执行预算包含排队和解码；历史读取不继承原请求的 deadline。
			ctx, cancel := wire.Incoming(ctx, envelope)
			defer cancel()
			decodeBudget, stopDecode := context.WithTimeout(ctx, e.Config.PayloadLimits.DecodeTimeout)
			envelope, err = wire.Open(decodeBudget, method.FullName, envelope, e.Config, e.Config.PayloadLimits.MaxMessageBytes, e.Config.PayloadLimits.MaxEncodedBytes)
			stopDecode()
			if err != nil {
				return nil, err
			}

			ctx = e.Context(ctx)
			// 请求字段绑定到 context，普通日志和显式上报共享同一方法身份。
			ctx = logging.With(ctx, slog.String("rpc_method", method.FullName), slog.String("rpc_transport", "worker"))
			// ctx, finish 开始属于当前实例的 span，结束时按实际执行错误标记状态
			ctx, finish := e.StartSpan(ctx, method.FullName, trace.SpanKindServer)
			defer func() {
				finish(err)
			}()

			// transport 为 Worker unary 响应收集方法名、headers 和 trailers 的 gRPC 传输适配器
			transport := &unaryTransport{method: method.FullName}
			defer func() {
				if err != nil {
					transport.mu.Lock()
					err = &model.RPCError{Err: err, Headers: transport.headers.Copy(), Trailers: transport.trailers.Copy()}
					transport.mu.Unlock()
				}
			}()

			ctx = grpc.NewContextWithServerTransportStream(ctx, transport)
			// interceptor 建立 Worker 服务端调用链，默认直接执行 handler，后续按注册顺序包裹
			interceptor := grpc.UnaryServerInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
				return next(ctx, req)
			})
			// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
			for i := len(e.Config.Middleware) - 1; i >= 0; i-- {
				// current 当前层的拦截器，反向构造调用链以保持配置中外层到内层的执行顺序
				current := e.Config.Middleware[i].UnaryServer
				if current != nil {
					// next 构造当前层之前的内层调用闭包，捕获此值避免所有拦截器递归引用同一最终变量
					next := interceptor
					interceptor = func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
						return current(ctx, req, info, func(ctx context.Context, req any) (any, error) {
							return next(ctx, req, info, handler)
						})
					}
				}
			}
			// response, err 接收 method.Unary.Handler 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
			response, err := method.Unary.Handler(method.Service, ctx, func(target any) error {
				// message, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值
				message, ok := target.(proto.Message)
				// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值
				if !ok {
					return fmt.Errorf("wego: generated decoder must use protobuf")
				}

				return wire.DecodeValue(ctx, method.FullName, envelope, message, e.Config, e.Config.PayloadLimits.MaxMessageBytes, e.Config.PayloadLimits.MaxEncodedBytes)
			}, interceptor)
			if err != nil {
				return nil, err
			}

			// 响应类型由注册描述决定，拦截器不能返回恰好具有相同 JSON 字段的其他消息。
			if !matchesMessage(response, method.Descriptor.Output().FullName()) {
				return nil, status.Error(codes.Internal, "wego: handler response does not match registered protobuf type")
			}

			// out, err 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷
			plain, typeName, err := wire.MarshalValue(response, e.Config.PayloadLimits.MaxMessageBytes)
			if err != nil {
				return nil, err
			}
			out := wire.NewEnvelope(ctx, method.FullName, typeName, plain, nil)
			transport.mu.Lock()
			out.Headers, out.Trailers = transport.headers.Copy(), transport.trailers.Copy()
			transport.mu.Unlock()
			budget, cancel := context.WithTimeout(ctx, e.Config.PayloadLimits.EncodeTimeout)
			defer cancel()
			out, err = wire.Output(budget, out, e.Config, e.Config.PayloadLimits.MaxMessageBytes, e.Config.PayloadLimits.MaxMessageBytes+(64<<10), e.Config.PayloadLimits.MaxEncodedBytes)
			return out, err
		},
	}, nil
}

// unaryTransport 为 Worker unary handler 收集 headers、trailers 和方法信息
type unaryTransport struct {
	// method 当前 RPC 方法或绑定信息；完整方法名用于查询投影、handler 与任务名称
	method string
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁
	mu sync.Mutex
	// headers 响应头缓存；调用方读取时返回副本
	headers metadata.MD
	// trailers 响应尾部 metadata，随最终状态交付
	trailers metadata.MD
}

// Method 返回当前完整 RPC 方法名，供拦截器和消息变换识别调用
func (t *unaryTransport) Method() string {
	return t.method
}

// SetHeader 合并响应头缓存，不立即发送
func (t *unaryTransport) SetHeader(md metadata.MD) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.headers = metadata.Join(t.headers, md)
	return nil
}

// SendHeader 合并并发布响应头；重复发送由当前传输的状态规则处理
func (t *unaryTransport) SendHeader(md metadata.MD) error {
	return t.SetHeader(md)
}

// SetTrailer 合并响应尾部 metadata，随最终状态返回
func (t *unaryTransport) SetTrailer(md metadata.MD) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.trailers = metadata.Join(t.trailers, md)
	return nil
}

// DecodeJSON 把普通 JSON 结果复制到调用方目标；protobuf 结果通过独立的 envelope 路径处理
func DecodeJSON(value, target any) error {
	// data 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
	var data []byte
	// err 当前操作产生的错误；nil 表示该步骤成功
	var err error
	// JSON 原文使用 json.RawMessage 显式表达；字符串 "7" 始终保留为字符串
	data, err = json.Marshal(value)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, target)
}

// applyMetadata 把响应 metadata 写入 grpc.Header / Trailer 的调用方目标，不改变业务 status details
func applyMetadata(opts []grpc.CallOption, headers, trailers map[string][]string) {
	// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序
	for _, option := range opts {
		// v 保存类型分派后的准确值，各分支只按该类型读取字段或调用方法
		switch v := option.(type) {
		case grpc.HeaderCallOption:
			*v.HeaderAddr = metadata.MD(headers).Copy()
		case grpc.TrailerCallOption:
			*v.TrailerAddr = metadata.MD(trailers).Copy()
		}
	}
}
