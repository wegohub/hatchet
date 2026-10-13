package spec

import (
	"reflect"
	"slices"
	"unicode/utf8"

	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PayloadCodecs 是冻结后的实例注册表；组件由应用保证并发安全，表和编码顺序不可变。
type PayloadCodecs struct {
	// registry 按持久格式 ID 寻址，不使用接收端注册顺序解释消息。
	registry map[string]middleware.Payload
	// thresholds 保存每个发送步骤的有效阈值，解码不读取此配置。
	thresholds map[string]int
	// encoding 只决定本实例发送消息的变换顺序，允许同一 ID 出现多次。
	encoding []string
}

// prepareCodecs 在配置生效时拒绝歧义；例如两个不同密钥的组件不能注册为同一个 ID。
func (r *Runtime) prepareCodecs() (*PayloadCodecs, error) {
	prepared := &PayloadCodecs{registry: make(map[string]middleware.Payload), thresholds: make(map[string]int)}
	for _, option := range r.Middleware {
		if option.Payload == nil {
			if option.UnaryServer == nil && option.UnaryClient == nil && option.StreamServer == nil && option.StreamClient == nil {
				return nil, status.Error(codes.InvalidArgument, "wego: nil payload codec or empty middleware")
			}
			continue
		}
		value := reflect.ValueOf(option.Payload)
		switch value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if value.IsNil() {
				return nil, status.Error(codes.InvalidArgument, "wego: nil payload codec")
			}
		}
		id := option.Payload.CodecID()
		if id == "" || !utf8.ValidString(id) {
			return nil, status.Error(codes.FailedPrecondition, "wego: codec requires a nonempty UTF-8 CodecID")
		}
		if _, exists := prepared.registry[id]; exists {
			return nil, status.Errorf(codes.FailedPrecondition, "wego: duplicate CodecID %q", id)
		}
		prepared.registry[id] = option.Payload
		if component, ok := option.Payload.(middleware.ThresholdPayload); ok {
			prepared.thresholds[id] = component.EncodingThreshold()
		}
		prepared.encoding = append(prepared.encoding, id)
	}
	for _, threshold := range prepared.thresholds {
		if threshold < 0 {
			return nil, status.Error(codes.InvalidArgument, "wego: encoding threshold cannot be negative")
		}
	}
	if err := r.prepareEncoding(prepared); err != nil {
		return nil, err
	}
	return prepared, nil
}

// Codecs 返回已验证实例的只读注册表；未构造 Engine 的内部测试也使用相同校验。
// 配置修改后必须重新 Validate，不能在运行中的实例上追加或替换组件。
func (r Runtime) Codecs() (*PayloadCodecs, error) {
	if r.codecs != nil {
		return r.codecs, nil
	}
	return r.prepareCodecs()
}

// EncodingIDs 返回发送链的独立快照；调用方不能改写下一条消息的编码顺序。
func (c *PayloadCodecs) EncodingIDs() []string { return slices.Clone(c.encoding) }

// Lookup 返回指定格式的处理组件；是否存在由调用方在任何解码 I/O 前统一检查。
func (c *PayloadCodecs) Lookup(id string) (middleware.Payload, bool) {
	component, ok := c.registry[id]
	return component, ok
}

// HasDecoders 判断结果还原是否可能进行 codec I/O；显式空发送链仍可能下载历史对象。
func (c *PayloadCodecs) HasDecoders() bool { return len(c.registry) != 0 }

// prepareEncoding 冻结发送步骤；允许重复步骤，但不允许指向不存在的组件。
func (r *Runtime) prepareEncoding(prepared *PayloadCodecs) error {
	if r.EncodingCodecsSet {
		prepared.encoding = slices.Clone(r.EncodingCodecs)
	}
	if len(prepared.encoding) > 16 {
		return status.Error(codes.ResourceExhausted, "wego: codec chain exceeds 16 steps")
	}
	for _, id := range prepared.encoding {
		if _, exists := prepared.registry[id]; !exists {
			return status.Errorf(codes.FailedPrecondition, "wego: encoding CodecID %q is not registered", id)
		}
	}

	return nil
}

// ShouldEncode 按当前步骤输入长度判断；达到阈值即执行，不影响历史解码。
func (c *PayloadCodecs) ShouldEncode(id string, size int) bool { return size >= c.thresholds[id] }
