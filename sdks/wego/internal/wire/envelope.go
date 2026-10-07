package wire

import (
	"context"
	"encoding/json"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// Version 是 envelope 和流帧共用的协议版本；当前为 3，流终态显式记录响应存在性。
const Version = 3

// Envelope 使用 JSON 传递协议元数据；Payload 由 encoding/json 编为 base64，内容仍是 protobuf 二进制。
// Routing 只包含显式配置的字段，供调度 CEL 表达式读取。
type Envelope struct {
	// Version SDK、服务或协议版本；各自用于报告或兼容性校验。
	Version int `json:"wego_version"`
	// Payload 业务载荷；wire 中为 protobuf 字节，JSON 编码时自动转为 base64。
	Payload []byte `json:"payload"`
	// Routing 显式投影的调度字段，例如 {"group":"group-a"}，不受载荷加密影响。
	Routing map[string]any `json:"routing,omitempty"`
	// Type 事件或 protobuf 类型名称，用于分派及类型校验。
	Type string `json:"type,omitempty"`
	// Metadata 请求或运行 metadata；保留键值和多值语义。
	Metadata map[string][]string `json:"metadata,omitempty"`
	// Trace 追踪传播字段或实例追踪配置。
	Trace map[string]string `json:"trace,omitempty"`
	// Deadline 请求绝对截止时间，单位为 Unix 纳秒；排队时间计入预算。
	Deadline int64 `json:"deadline,omitempty"`
	// Headers 响应头 metadata，可在业务响应之前交付。
	Headers map[string][]string `json:"headers,omitempty"`
	// Trailers 响应尾部 metadata，与最终状态一起读取。
	Trailers map[string][]string `json:"trailers,omitempty"`
}

// Encode 把 protobuf 消息封装为版本化 envelope；先生成 routing，再执行载荷变换和大小检查。
// 例如 Request{Message:"hello", GroupKey:"group-a"} 的业务字节放进 payload，group 投影单独保存在 routing。
func Encode(
	ctx context.Context,
	method string,
	message proto.Message,
	fields map[string]string,
	chain []middleware.Option,
	maxBytes int,
) (Envelope, error) {
	// 检查 message == nil || !message.ProtoReflect().IsValid()；不满足协议或配置约束时返回 InvalidArgument（wego: nil protobuf message）。
	if message == nil || !message.ProtoReflect().IsValid() {
		return Envelope{}, status.Error(codes.InvalidArgument, "wego: nil protobuf message")
	}

	// data, err 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
	data, err := proto.Marshal(message)
	if err != nil {
		return Envelope{}, err
	}
	// 检查 len(data) > maxBytes；不满足协议或配置约束时返回 ResourceExhausted（wego: message exceeds limit）。
	if len(data) > maxBytes {
		return Envelope{}, status.Error(codes.ResourceExhausted, "wego: message exceeds limit")
	}
	// 投影必须早于压缩、加密或卸载，调度器才能直接读取 input.routing。
	routing := map[string]any{}
	// 按显式投影映射提取字段，例如 group→group_key 写入 routing.group，不自动暴露其他业务字段。
	for key, path := range fields {
		// value, err 接收 message.ProtoReflect 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
		value, err := project(message.ProtoReflect(), path)
		if err != nil {
			return Envelope{}, err
		}

		routing[key] = value
	}
	// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序。
	for _, m := range chain {
		// 只对业务 protobuf 字节执行载荷变换；routing 已先生成，控制帧不进入此变换。
		if m.Payload != nil {
			data, err = m.Payload.Encode(ctx, method, data)
			if err != nil {
				return Envelope{}, err
			}
		}
	}
	// 检查 len(data) > maxBytes；不满足协议或配置约束时返回 ResourceExhausted（wego: transformed payload exceeds limit）。
	if len(data) > maxBytes {
		return Envelope{}, status.Error(codes.ResourceExhausted, "wego: transformed payload exceeds limit")
	}

	// md 保存调用方多值请求 metadata，wire v2 按字节传输，包含 -bin 值也不损坏。
	md, _ := metadata.FromOutgoingContext(ctx)
	// carrier 构造当前步骤的数据对象，字段在提交、编码或断言之前一次性初始化。
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	// out 复制 metadata，保持多值信息并隔离后续修改。
	out := Envelope{
		Version:  Version,
		Payload:  data,
		Routing:  routing,
		Type:     string(message.ProtoReflect().Descriptor().FullName()),
		Metadata: md.Copy(),
		Trace:    carrier,
	}
	// deadline 只在调用确有预算时写入；远期触发另行省略发布请求的截止时间。
	if deadline, ok := ctx.Deadline(); ok && ctx.Value(triggerContextKey{}) == nil {
		out.Deadline = deadline.UnixNano()
	}
	return out, nil
}

// Decode 检查版本、消息类型和大小，反向还原载荷变换，再解码 protobuf 响应。
// 例如压缩后再加密的 payload，先解密再解压，最后恢复相同的 Request.Message。
func Decode(ctx context.Context, method string, envelope Envelope, message proto.Message, chain []middleware.Option, maxBytes int) error {
	// 检查 envelope.Version != Version；不满足协议或配置约束时返回 FailedPrecondition（wego: unsupported protocol version）。
	if envelope.Version != Version {
		return status.Error(codes.FailedPrecondition, "wego: unsupported protocol version")
	}
	// 检查 len(envelope.Payload) > maxBytes；不满足协议或配置约束时返回 ResourceExhausted（wego: payload exceeds limit）。
	if len(envelope.Payload) > maxBytes {
		return status.Error(codes.ResourceExhausted, "wego: payload exceeds limit")
	}
	// 检查 envelope.Type != "" && envelope.Type != string(message.ProtoReflect().Descriptor().FullName())；不满足协议或配置约束时返回 InvalidArgument（wego: protobuf message type mismatch）。
	if envelope.Type != "" && envelope.Type != string(message.ProtoReflect().Descriptor().FullName()) {
		return status.Error(codes.InvalidArgument, "wego: protobuf message type mismatch")
	}

	// data envelope 保存的业务字节，反向还原各层变换后才执行 protobuf 解码。
	data := envelope.Payload
	// err 当前操作产生的错误；nil 表示该步骤成功。
	var err error
	// 解码顺序与编码相反，每层还原后都检查大小，限制解压后的内存占用。
	for i := len(chain) - 1; i >= 0; i-- {
		// 检查 chain[i].Payload != nil；不满足协议或配置约束时返回 ResourceExhausted（wego: restored payload exceeds limit）。
		if chain[i].Payload != nil {
			data, err = chain[i].Payload.Decode(ctx, method, data)
			if err != nil {
				return err
			}
			// 检查 len(data) > maxBytes；不满足协议或配置约束时返回 ResourceExhausted（wego: restored payload exceeds limit）。
			if len(data) > maxBytes {
				return status.Error(codes.ResourceExhausted, "wego: restored payload exceeds limit")
			}
		}
	}
	return proto.Unmarshal(data, message)
}

// AsEnvelope 把字符串、字符串指针或 JSON 值统一解码为 envelope。
func AsEnvelope(value any) (Envelope, error) {
	// data 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
	var data []byte
	// err 当前操作产生的错误；nil 表示该步骤成功。
	var err error
	// v 保存类型分派后的准确值，各分支只按该类型读取字段或调用方法。
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

	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
	var out Envelope
	err = json.Unmarshal(data, &out)
	return out, err
}

// Incoming 恢复请求 metadata、trace 和绝对截止时间；任务排队时间同样计入 RPC 预算。
func Incoming(ctx context.Context, envelope Envelope) (context.Context, context.CancelFunc) {
	ctx = metadata.NewIncomingContext(ctx, metadata.MD(envelope.Metadata))
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier(envelope.Trace))
	// 恢复绝对 deadline 而非重新分配时长；例如排队已用 2 秒的 3 秒请求只剩约 1 秒。
	if envelope.Deadline != 0 {
		return context.WithDeadline(ctx, time.Unix(0, envelope.Deadline))
	}

	return context.WithCancel(ctx)
}
