package binding

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Method 服务描述与 protobuf 描述之间的绑定，决定方法名、请求和响应类型。
type Method struct {
	// FullName 完整名称；RPC 绑定时必须与生成客户端调用的方法名一致。
	FullName string
	// Service 业务服务实例，网络与 Worker 入口共享同一 handler。
	Service any
	// Unary unary 方法描述；stream 方法时为 nil。
	Unary *grpc.MethodDesc
	// Stream 流方法描述或流配置，由所属类型决定。
	Stream *grpc.StreamDesc
	// Descriptor protobuf 方法描述，用于创建准确的请求和响应类型。
	Descriptor protoreflect.MethodDescriptor
}

// Name 将完整方法名转换为后端可注册名称，确保提交和注册使用相同规则。
func Name(fullMethod string) string {
	// hash 对实际数据计算稳定摘要，用于关联内容或显式断言身份。
	hash := sha256.Sum256([]byte(fullMethod))
	// parts 解析结构化名称或参数，后续分派只接受明确注册的操作。
	parts := strings.Split(strings.TrimPrefix(fullMethod, "/"), "/")
	// readable 将服务与方法拼成可读名称，例如 /Greeter/Hello 变成 Greeter-Hello。
	readable := strings.Join(parts, "-")
	return "wego-rpc-" + strings.ToLower(readable) + "-" + hex.EncodeToString(hash[:6])
}

// Methods 展开服务描述，绑定 unary 和流方法到 protobuf 描述。
func Methods(description *grpc.ServiceDesc, service any) ([]Method, error) {
	// descriptor, err 接收 protoregistry.GlobalFiles.FindDescriptorByName 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(description.ServiceName))
	// 当前步骤失败时终止处理：wego: protobuf service descriptor missing for %s；不把无效结果交给下一步。
	if err != nil {
		return nil, fmt.Errorf("wego: protobuf service descriptor missing for %s", description.ServiceName)
	}

	// d, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值。
	d, ok := descriptor.(protoreflect.ServiceDescriptor)
	// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值。
	if !ok {
		return nil, fmt.Errorf("wego: invalid service descriptor")
	}

	// out 逐项收集的定义或配置列表，保留注册顺序及每项独立策略。
	out := []Method{}
	// 逐项处理 description.Methods，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for i := range description.Methods {
		// m 当前方法的服务描述，包含 handler 或流方向，绑定时与 protobuf 描述进行一致性检查。
		m := &description.Methods[i]
		// method 解析生成代码的服务描述，绑定稳定方法名与 protobuf 请求类型。
		method := d.Methods().ByName(protoreflect.Name(m.MethodName))
		if method == nil || method.IsStreamingClient() || method.IsStreamingServer() {
			return nil, fmt.Errorf("wego: unary method descriptor mismatch: %s", m.MethodName)
		}
		out = append(out, Method{
			FullName:   "/" + description.ServiceName + "/" + m.MethodName,
			Service:    service,
			Unary:      m,
			Descriptor: method,
		})
	}
	// 逐项处理 description.Streams，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for i := range description.Streams {
		// m 当前方法的服务描述，包含 handler 或流方向，绑定时与 protobuf 描述进行一致性检查。
		m := &description.Streams[i]
		// method 解析生成代码的服务描述，绑定稳定方法名与 protobuf 请求类型。
		method := d.Methods().ByName(protoreflect.Name(m.StreamName))
		if method == nil || method.IsStreamingClient() != m.ClientStreams || method.IsStreamingServer() != m.ServerStreams {
			return nil, fmt.Errorf("wego: streaming method descriptor mismatch: %s", m.StreamName)
		}
		out = append(out, Method{
			FullName:   "/" + description.ServiceName + "/" + m.StreamName,
			Service:    service,
			Stream:     m,
			Descriptor: method,
		})
	}
	return out, nil
}
