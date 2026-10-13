package rpc

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// RunCallOption 携带一份已经冻结的运行配置；原生网络连接不会执行任务调度。
type RunCallOption struct {
	// EmptyCallOption 保留标准接口身份，原生 gRPC 不执行任务提交选项。
	grpc.EmptyCallOption
	// Options 只供内部适配器读取，不引用业务的可变配置。
	Options model.RunOptions
}

// NewRunCallOption 构造生成客户端可接受的选项，错误在实际调用前交付。
func NewRunCallOption(options ...spec.RunOption) grpc.CallOption {
	out, err := spec.ApplyRunOptions(options...)
	if err != nil {
		out.ValidationError = err
	}
	return RunCallOption{Options: out}
}

// CallOptions 为每次 gRPC 调用保存独立配置；-1 表示没有设置消息大小覆盖。
type CallOptions struct {
	// Run 是与原生 Run 共用的执行选项。
	Run model.RunOptions
	// MaxSend、MaxRecv 限制 protobuf 二进制长度，零允许空消息。
	MaxSend int
	// MaxRecv 是独立接收限制，最后一次配置生效。
	MaxRecv int
	// callbacks 不在状态锁内执行，调用终态确定后最多执行一次。
	callbacks []func(error)
}

// ParseCallOptions 明确拒绝没有任务传输语义的网络选项，不静默忽略。
func ParseCallOptions(options []grpc.CallOption, resume bool) (CallOptions, error) {
	out := CallOptions{MaxSend: -1, MaxRecv: -1}
	// 即使后续参数校验失败，完成通知也属于本次调用的终态。
	for _, option := range options {
		if value, ok := option.(grpc.OnFinishCallOption); ok && value.OnFinish != nil {
			out.callbacks = append(out.callbacks, value.OnFinish)
		}
	}
	for _, option := range options {
		if err := out.apply(option, resume); err != nil {
			return out, err
		}
	}
	return out, nil
}

// apply 对单个选项应用标准调用语义；完成回调已在解析前收集，因此参数错误也会通知终态。
func (o *CallOptions) apply(option grpc.CallOption, resume bool) error {
	switch value := option.(type) {
	case RunCallOption:
		if resume {
			return status.Error(codes.InvalidArgument, "wego: resumed stream cannot accept submission options")
		}
		if err := mergeRunCallOptions(&o.Run, value.Options); err != nil {
			return err
		}
	case grpc.MaxSendMsgSizeCallOption:
		if err := validateCallSize(value.MaxSendMsgSize, resume); err != nil {
			return err
		}
		o.MaxSend = value.MaxSendMsgSize
	case grpc.MaxRecvMsgSizeCallOption:
		if err := validateCallSize(value.MaxRecvMsgSize, false); err != nil {
			return err
		}
		o.MaxRecv = value.MaxRecvMsgSize
	case grpc.HeaderCallOption:
		if value.HeaderAddr == nil {
			return status.Error(codes.InvalidArgument, "wego: nil header target")
		}
	case grpc.TrailerCallOption:
		if value.TrailerAddr == nil {
			return status.Error(codes.InvalidArgument, "wego: nil trailer target")
		}
	case grpc.OnFinishCallOption:
	case grpc.StaticMethodCallOption:
	default:
		return status.Error(codes.Unimplemented, "wego: unsupported gRPC CallOption")
	}
	return nil
}

// validateCallSize 零允许空 protobuf；恢复流禁止新增发送配置，负数不表示取消限制。
func validateCallSize(size int, closedInput bool) error {
	if closedInput {
		return status.Error(codes.InvalidArgument, "wego: resumed stream input is closed")
	}
	if size < 0 {
		return status.Error(codes.InvalidArgument, "wego: negative message limit")
	}
	return nil
}

// mergeRunCallOptions 按调用顺序覆盖显式字段，routing 按顶层键合并；快照不借用调用方容器。
func mergeRunCallOptions(out *model.RunOptions, options model.RunOptions) error {
	next := spec.CloneRunOptions(options)
	if next.ValidationError != nil {
		return status.Errorf(codes.InvalidArgument, "wego: %v", next.ValidationError)
	}
	if next.Key != nil {
		out.Key = next.Key
	}
	if next.Sticky != nil {
		out.Sticky = next.Sticky
	}
	if next.Priority != nil {
		out.Priority = next.Priority
	}
	if next.AdditionalMetadata != nil {
		out.AdditionalMetadata = next.AdditionalMetadata
	}
	if next.DesiredWorkerLabels != nil {
		out.DesiredWorkerLabels = next.DesiredWorkerLabels
	}
	if next.StreamIdempotencyKey != "" {
		out.StreamIdempotencyKey = next.StreamIdempotencyKey
	}
	if out.Routing == nil {
		out.Routing = map[string]any{}
	}
	for key, item := range next.Routing {
		out.Routing[key] = item
	}
	return nil
}

// Finish 在调用方已经释放传输登记后通知完成，不持有 SDK 锁。
func (o *CallOptions) Finish(err *error) {
	for _, callback := range o.callbacks {
		callback(*err)
	}
}
