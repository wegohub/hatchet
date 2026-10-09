package wire

import (
	"context"
	"encoding/json"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// Version 是任务 RPC envelope 与流协议版本；v4 使用 ProtoJSON 业务输入
const Version = 4

// Envelope 保持调度信息为可读 JSON；业务 payload 使用 ProtoJSON，配置 codec 后保存变换字节的 base64
// Metadata 按独立的 bytes/base64 规则传输，-bin 值不能被 UTF-8 替换
// 协议值本身实现 RPC 查询和 JSON 编码接口；只有回填字段的 UnmarshalJSON 使用指针接收者。
type Envelope struct {
	// Version 拒绝客户端与 Worker 混用不同协议
	Version int `json:"version"`
	// Method 明确记录完整 RPC 绑定，不用 workflow 名称前缀猜测执行类型
	Method string `json:"method,omitempty"`
	// Payload 无 codec 时为 JSON 对象或 well-known JSON 值；有 codec 时为 base64 JSON 字符串
	Payload json.RawMessage `json:"payload"`
	// Stream 表示 client、server 或 bidi；unary 为空
	Stream string `json:"stream,omitempty"`
	// StreamMode 明确输出传输，防止两端配置不同导致永久等待
	StreamMode string `json:"stream_mode,omitempty"`
	// IdempotencyKey 是作用域内稳定的提交键，不由输入 hash 自动生成
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	// Routing 是调用方提供的调度字段，独立于加密/压缩后的业务数据
	Routing map[string]any `json:"routing,omitempty"`
	// Type 校验 protobuf 请求和响应类型
	Type string `json:"type,omitempty"`
	// InputDigest 校验幂等恢复的输入身份，使用变换之前的规范请求和 routing
	InputDigest string `json:"input_digest,omitempty"`
	// Metadata 保留请求多值和二进制 metadata
	Metadata map[string][]string `json:"metadata,omitempty"`
	// Trace 保存实例 trace 传播载体
	Trace map[string]string `json:"trace,omitempty"`
	// Deadline 保存绝对 Unix 纳秒截止时间，包含引擎排队时间
	Deadline int64 `json:"deadline,omitempty"`
	// Headers 在响应和错误路径中交付业务 headers
	Headers map[string][]string `json:"headers,omitempty"`
	// Trailers 随最终状态交付
	Trailers map[string][]string `json:"trailers,omitempty"`
}

// RPCMethod 提供显式的内部 RPC 输入能力，原生 JSON map 即使有 method 键也不实现此接口
func (e Envelope) RPCMethod() string { return e.Method }

// RPCShape 明确区分单请求与请求数组，unary 的空字段对应固定名称
func (e Envelope) RPCShape() string {
	if e.Stream == "" {
		return "unary"
	}
	return e.Stream
}

// RPCMode 为注册预检提供传输模式，不能由 workflow 名称推断
func (e Envelope) RPCMode() string { return e.StreamMode }

// MarshalMessage 采用 protobuf JSON 规则，保留 oneof、枚举、bytes 和 64 位整数语义
func MarshalMessage(message proto.Message, maxBytes int) (json.RawMessage, error) {
	if message == nil || !message.ProtoReflect().IsValid() {
		return nil, status.Error(codes.InvalidArgument, "wego: nil protobuf message")
	}
	// proto.Size 先限制明显过大的源对象，再检查 JSON 膨胀后的大小
	if proto.Size(message) > maxBytes {
		return nil, status.Error(codes.ResourceExhausted, "wego: protobuf message exceeds limit")
	}
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(message)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "wego: invalid protobuf message: %v", err)
	}
	if len(data) > maxBytes {
		return nil, status.Error(codes.ResourceExhausted, "wego: ProtoJSON message exceeds limit")
	}
	return data, nil
}

// Encode 保存 ProtoJSON 快照，按配置变换业务 payload；routing 不依赖 protobuf 字段标签
func Encode(ctx context.Context, method string, message proto.Message, routing map[string]any, chain []middleware.Option, maxBytes int) (Envelope, error) {
	data, err := MarshalMessage(message, maxBytes)
	if err != nil {
		return Envelope{}, err
	}
	return EncodeSnapshot(ctx, method, string(message.ProtoReflect().Descriptor().FullName()), data, routing, chain, maxBytes)
}

// EncodeSnapshot 只变换已经冻结的 ProtoJSON，摘要和传输使用同一份业务快照
func EncodeSnapshot(ctx context.Context, method, typeName string, data json.RawMessage, routing map[string]any, chain []middleware.Option, maxBytes int) (Envelope, error) {
	// err 是当前 codec 错误，失败不提交半成品
	var err error
	// encoded 在第一次变换后成为任意字节；最终只包一层 base64，不逐层重复转码
	encoded, transformed := []byte(data), false
	for _, m := range chain {
		if err := ctx.Err(); err != nil {
			return Envelope{}, status.FromContextError(err).Err()
		}
		if m.Payload != nil {
			transformed = true
			encoded, err = m.Payload.Encode(ctx, method, encoded)
			if err != nil {
				return Envelope{}, err
			}
			if len(encoded) > maxBytes {
				return Envelope{}, status.Error(codes.ResourceExhausted, "wego: transformed payload exceeds limit")
			}
		}
	}
	if transformed {
		data, err = json.Marshal(encoded)
		if err != nil {
			return Envelope{}, err
		}
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	out := Envelope{Version: Version, Method: method, Payload: data, Routing: routing,
		Type: typeName, Metadata: md.Copy(), Trace: carrier}
	if deadline, ok := ctx.Deadline(); ok && ctx.Value(triggerContextKey{}) == nil {
		out.Deadline = deadline.UnixNano()
	}
	return out, nil
}

// Decode 按反向 codec 链还原 ProtoJSON；版本、绑定、类型和各层大小均先校验
func Decode(ctx context.Context, method string, envelope Envelope, message proto.Message, chain []middleware.Option, maxBytes int) error {
	return DecodeWithLimits(ctx, method, envelope, message, chain, maxBytes, maxBytes)
}

// DecodeWithLimits 分开限制编码字节和最终原文，防止压缩或引用绕过业务消息上限
func DecodeWithLimits(ctx context.Context, method string, envelope Envelope, message proto.Message, chain []middleware.Option, maxPlain, maxBytes int) error {
	if envelope.Version != Version {
		return status.Error(codes.FailedPrecondition, "wego: unsupported protocol version")
	}
	if envelope.Method != "" && envelope.Method != method {
		return status.Error(codes.FailedPrecondition, "wego: RPC method mismatch")
	}
	if message == nil || !message.ProtoReflect().IsValid() {
		return status.Error(codes.InvalidArgument, "wego: nil protobuf message")
	}
	if envelope.Type != "" && envelope.Type != string(message.ProtoReflect().Descriptor().FullName()) {
		return status.Error(codes.InvalidArgument, "wego: protobuf message type mismatch")
	}
	data, err := RestorePayload(ctx, method, envelope.Payload, chain, maxPlain, maxBytes)
	if err != nil {
		return err
	}
	if err := protojson.Unmarshal(data, message); err != nil {
		return status.Errorf(codes.DataLoss, "wego: invalid ProtoJSON payload: %v", err)
	}
	return nil
}

// RestorePayload 对一条冻结消息只逆向执行一次 codec，返回受最终业务预算约束的 ProtoJSON
func RestorePayload(ctx context.Context, method string, payload json.RawMessage, chain []middleware.Option, maxPlain, maxBytes int) ([]byte, error) {
	data := []byte(payload)
	// 是否采用 base64 由实例 codec 链确定，不根据 payload 的 JSON 形状猜测
	for _, m := range chain {
		if m.Payload != nil {
			if len(data) > (maxBytes+2)/3*4+2 {
				return nil, status.Error(codes.ResourceExhausted, "wego: encoded payload exceeds limit")
			}
			if err := json.Unmarshal(data, &data); err != nil {
				return nil, status.Error(codes.DataLoss, "wego: codec payload requires base64 JSON")
			}
			break
		}
	}
	if len(data) > maxBytes {
		return nil, status.Error(codes.ResourceExhausted, "wego: payload exceeds limit")
	}
	for i := len(chain) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		if chain[i].Payload != nil {
			// err 是当前层还原的错误，失败不得继续解释业务消息
			var err error
			if bounded, ok := chain[i].Payload.(middleware.FramedPayload); ok {
				data, err = bounded.DecodeLimit(ctx, method, data, maxBytes)
			} else {
				data, err = chain[i].Payload.Decode(ctx, method, data)
			}
			if err != nil {
				return nil, err
			}
			if len(data) > maxBytes {
				return nil, status.Error(codes.ResourceExhausted, "wego: restored payload exceeds limit")
			}
		}
	}
	if len(data) > maxPlain {
		return nil, status.Error(codes.ResourceExhausted, "wego: restored business message exceeds limit")
	}
	return data, nil
}

// AsEnvelope 把字符串、字符串指针或 JSON 值统一解码为 envelope
func AsEnvelope(value any) (Envelope, error) {
	// data 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
	var data []byte
	// err 当前操作产生的错误；nil 表示该步骤成功
	// err 是当前层还原的错误，失败不得继续解释业务消息
	var err error
	// v 保存类型分派后的准确值，各分支只按该类型读取字段或调用方法
	switch v := value.(type) {
	case string:
		data = []byte(v)
	case *string:
		if v != nil {
			data = []byte(*v)
		}
	default:
		data, err = json.Marshal(value)
	}
	if err != nil {
		return Envelope{}, err
	}

	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
	var out Envelope
	err = json.Unmarshal(data, &out)
	return out, err
}

// Incoming 恢复请求 metadata、trace 和绝对截止时间；任务排队时间同样计入 RPC 预算
func Incoming(ctx context.Context, envelope Envelope) (context.Context, context.CancelFunc) {
	ctx = metadata.NewIncomingContext(ctx, metadata.MD(envelope.Metadata))
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier(envelope.Trace))
	// 恢复绝对 deadline 而非重新分配时长；例如排队已用 2 秒的 3 秒请求只剩约 1 秒
	if envelope.Deadline != 0 {
		return context.WithDeadline(ctx, time.Unix(0, envelope.Deadline))
	}

	return context.WithCancel(ctx)
}
