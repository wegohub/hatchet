package wire

import (
	"context"
	"encoding/json"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// Version 是任务 RPC envelope 与流协议版本；协议 6 使用 _wego 标记，注册绑定提供目标和类型
const Version = 6

// Envelope 保持调度信息为可读 JSON；业务 payload 使用 ProtoJSON，配置 codec 后保存变换字节的 base64
// Metadata 按独立的 bytes/base64 规则传输，-bin 值不能被 UTF-8 替换
// 协议值本身实现 RPC 查询和 JSON 编码接口；只有回填字段的 UnmarshalJSON 使用指针接收者。
type Envelope struct {
	// Native 表示未标记的原始值；response 契约独立于输入是否经过 codec。
	Native bool
	// Response 仅显式 wego 表示协议响应；缺省使用原生业务结果。
	Response string
	// Encoding 标识 JSON 或 base64 字节承载，后者不一定包含 codec。
	Encoding string
	// CodecIDs 按消息发送顺序记录格式，读取时逆序执行。
	CodecIDs []string
	// sealed 仅在内存中标记已经执行 codec，避免重复上传。
	sealed bool
	// opened 标记已还原的内部消息，后续业务反序列化不重复执行 codec。
	opened bool
	// Version 拒绝客户端与 Worker 混用不同协议
	Version int
	// Method 仅供本地提交绑定，不进入消息 JSON；接收端使用注册信息
	Method string
	// Payload 保存 JSON value；仅配置变换或数值不安全时使用 base64
	Payload json.RawMessage
	// Stream 表示 client、server 或 bidi；unary 为空
	Stream string
	// StreamMode 明确输出传输，防止两端配置不同导致永久等待
	StreamMode string
	// IdempotencyKey 是作用域内稳定的提交键，不由输入 hash 自动生成
	IdempotencyKey string
	// Routing 是调用方提供的调度字段，独立于加密/压缩后的业务数据
	Routing map[string]any
	// Type 仅用于本地类型检查，不随消息传输
	Type string
	// InputDigest 校验幂等恢复的输入身份，使用变换之前的规范请求和 routing
	InputDigest string
	// Metadata 保留请求多值和二进制 metadata
	Metadata map[string][]string
	// Trace 保存实例 trace 传播载体
	Trace map[string]string
	// Deadline 保存绝对 Unix 纳秒截止时间，包含引擎排队时间
	Deadline int64
	// Headers 在响应和错误路径中交付业务 headers
	Headers map[string][]string
	// Trailers 随最终状态交付
	Trailers map[string][]string
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
func Encode(ctx context.Context, method string, message proto.Message, routing map[string]any, config spec.Runtime, maxBytes int) (Envelope, error) {
	data, err := MarshalMessage(message, maxBytes)
	if err != nil {
		return Envelope{}, err
	}
	return EncodeSnapshot(ctx, method, string(message.ProtoReflect().Descriptor().FullName()), data, routing, config, maxBytes)
}

// EncodeSnapshot 只变换已经冻结的 ProtoJSON，摘要和传输使用同一份业务快照
func EncodeSnapshot(ctx context.Context, method, typeName string, data json.RawMessage, routing map[string]any, config spec.Runtime, maxBytes int) (Envelope, error) {
	out := NewEnvelope(ctx, method, typeName, data, routing)
	return Seal(ctx, out, config, maxBytes, maxBytes+(64<<10), 4<<20)
}

// NewEnvelope 冻结业务与请求信息，调用方补齐流形态或响应头后必须 Seal 再提交。
func NewEnvelope(ctx context.Context, method, typeName string, data json.RawMessage, routing map[string]any) Envelope {
	md, _ := metadata.FromOutgoingContext(ctx)
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	out := Envelope{Response: "wego", Version: Version, Method: method, Type: typeName, Payload: append(json.RawMessage(nil), data...), Routing: routing, Metadata: md.Copy(), Trace: carrier}
	if deadline, ok := ctx.Deadline(); ok && ctx.Value(triggerContextKey{}) == nil {
		out.Deadline = deadline.UnixNano()
	}
	return out
}

// Decode 按反向 codec 链还原 ProtoJSON；版本、绑定、类型和各层大小均先校验
func Decode(ctx context.Context, method string, envelope Envelope, message proto.Message, config spec.Runtime, maxBytes int) error {
	return DecodeWithLimits(ctx, method, envelope, message, config, maxBytes, 4<<20)
}

// DecodeWithLimits 分开限制编码字节和最终原文，防止压缩或引用绕过业务消息上限
func DecodeWithLimits(ctx context.Context, method string, envelope Envelope, message proto.Message, config spec.Runtime, maxPlain, maxBytes int) error {
	return DecodeValue(ctx, method, envelope, message, config, maxPlain, maxBytes)
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
	case Envelope:
		return v, nil
	case *Envelope:
		if v != nil {
			return *v, nil
		}
	case json.RawMessage:
		data = v
	case []byte:
		data = v
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
	ctx = WithResponse(ctx, envelope.Response == "wego")
	ctx = metadata.NewIncomingContext(ctx, metadata.MD(envelope.Metadata))
	// 已绑定的执行 trace 优先；请求头仅在没有有效父上下文时提供后备链路。
	// 例如引擎给出的排队/重试 trace 不应被最初提交时的父 span 替换。
	if !trace.SpanContextFromContext(ctx).IsValid() {
		ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier(envelope.Trace))
	}
	// 恢复绝对 deadline 而非重新分配时长；例如排队已用 2 秒的 3 秒请求只剩约 1 秒
	if envelope.Deadline != 0 {
		return context.WithDeadline(ctx, time.Unix(0, envelope.Deadline))
	}

	return context.WithCancel(ctx)
}
