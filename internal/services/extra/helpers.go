package extra

import (
	"context"
	"errors"
	"maps"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"

	ns "github.com/nats-io/nats-server/v2/server"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// ptr、mapPtr、mapSlice 仅复制边界值；例如修改请求的 labels map 不会修改原生配置。
func ptr[T any](v T) *T {
	return &v
}

// mapPtr 保留 nil 的未配置语义，仅对存在的值执行转换。
func mapPtr[T, U any](p *T, f func(T) U) U {
	var z U
	if p == nil {
		return z
	}
	return f(*p)
}

// mapSlice 创建独立结果切片，避免请求与原生配置共享可修改元素。
func mapSlice[T, U any](a []T, f func(T) U) []U {
	if a == nil {
		return nil
	}
	b := make([]U, len(a))
	for i, v := range a {
		b[i] = f(v)
	}
	return b
}

// stamp 把原生非零时间转换为 Timestamp，零时间表示字段缺省。
func stamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// unstampPtr 保留时间字段的未指定状态；非 nil 值须已通过 Timestamp 校验。
func unstampPtr(t *timestamppb.Timestamp) *time.Time {
	if t == nil {
		return nil
	}
	v := t.AsTime()
	return &v
}

// encodeHeaders 复制多值 header 切片，返回的响应不与原生消息共享可变数据。
func encodeHeaders(h nats.Header) map[string]*pb.StringValues {
	if h == nil {
		return nil
	}
	out := make(map[string]*pb.StringValues, len(h))
	for k, v := range h {
		out[k] = &pb.StringValues{Values: slices.Clone(v)}
	}
	return out
}

// decodeHeaders 冻结 protobuf header 值，发送期间调用方修改不会影响消息。
func decodeHeaders(h map[string]*pb.StringValues) nats.Header {
	if h == nil {
		return nil
	}
	out := make(nats.Header, len(h))
	for k, v := range h {
		out[k] = slices.Clone(v.GetValues())
	}
	return out
}

// invalid 用统一的 InvalidArgument 状态报告本地参数错误。
func invalid(msg string) error {
	return status.Error(codes.InvalidArgument, msg)
}

// rpcError 保留官方错误码，屏蔽内部资源名称；RPC 取消不会被转换为成功 EOF。
func rpcError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	code := codes.Internal
	switch {
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
		code = codes.DeadlineExceeded
	case errors.Is(err, nats.ErrMaxPayload):
		code = codes.ResourceExhausted
	case errors.Is(err, jetstream.ErrInvalidOption), errors.Is(err, nats.ErrBadSubject):
		code = codes.InvalidArgument
	case errors.Is(err, jetstream.ErrStreamNotFound), errors.Is(err, jetstream.ErrConsumerNotFound), errors.Is(err, jetstream.ErrBucketNotFound), errors.Is(err, jetstream.ErrKeyNotFound), errors.Is(err, jetstream.ErrKeyDeleted), errors.Is(err, jetstream.ErrMsgNotFound):
		code = codes.NotFound
	case errors.Is(err, jetstream.ErrKeyExists):
		code = codes.AlreadyExists
	case errors.Is(err, nats.ErrConnectionClosed), errors.Is(err, nats.ErrNoServers), errors.Is(err, nats.ErrNoResponders), errors.Is(err, jetstream.ErrNoStreamResponse), errors.Is(err, nats.ErrDisconnected):
		code = codes.Unavailable
	}
	var api *jetstream.APIError
	detail := &pb.NatsError{
		GrpcCode: int32(code),
		Message:  code.String(),
	}
	if errors.As(err, &api) {
		switch api.Code {
		case 400:
			code = codes.InvalidArgument
		case 404:
			code = codes.NotFound
		case 409:
			code = codes.FailedPrecondition
		case 429:
			code = codes.ResourceExhausted
		case 503:
			code = codes.Unavailable
		}
		// 期望序号/版本冲突不能当作普通写入成功。
		if errors.Is(err, jetstream.ErrKeyExists) {
			code = codes.AlreadyExists
		}
		if api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant {
			code = codes.FailedPrecondition
		}
		switch ns.ErrorIdentifier(api.ErrorCode) {
		case ns.JSStreamStoreFailedF:
			// 原生存储错误共用编号，仅按固定存储哨兵识别容量失败，不能把 IO 故障当过载。
			switch api.Description {
			case ns.ErrMaxBytes.Error(), ns.ErrMaxMsgs.Error(), ns.ErrMaxMsgsPerSubject.Error():
				code, detail.Reason = codes.ResourceExhausted, ReasonStoreExhausted
			case ns.ErrMsgTooLarge.Error():
				code, detail.Reason = codes.ResourceExhausted, ReasonMessageTooLarge
			}
		case ns.JSStreamMessageExceedsMaximumErr:
			code, detail.Reason = codes.ResourceExhausted, ReasonMessageTooLarge
		case ns.JSAtomicPublishTooManyInflight, ns.JSBatchPublishTooManyInflight:
			code, detail.Reason = codes.ResourceExhausted, ReasonBatchCapacity
		case ns.JSMaximumConsumersLimitErr:
			code, detail.Reason = codes.ResourceExhausted, ReasonConsumerLimit
		case ns.JSInsufficientResourcesErr, ns.JSAccountResourcesExceededErr, ns.JSMaximumStreamsLimitErr, ns.JSMemoryResourcesExceededErr, ns.JSStorageResourcesExceededErr, ns.JSStreamMaxStreamBytesExceeded:
			code, detail.Reason = codes.ResourceExhausted, ReasonStoreExhausted
		}
		detail.ApiCode = apiCode(api.Code)
		detail.NatsErrorCode = ptr(uint32(api.ErrorCode))
	}
	if code == codes.ResourceExhausted && detail.Reason == "" {
		detail.Reason = ReasonStoreExhausted
		if errors.Is(err, nats.ErrMaxPayload) {
			detail.Reason = ReasonMessageTooLarge
		}
	}
	detail.GrpcCode = int32(code)
	detail.Message = code.String()
	if api != nil {
		detail.Message = publicNATSError(api.Description)
	}
	msg := detail.Message
	if msg == "" {
		msg = "NATS operation failed"
	}
	st := status.New(code, msg)
	if d, e := st.WithDetails(detail); e == nil {
		return d.Err()
	}
	return st.Err()
}

// asItemError 为流水线单项失败保存 gRPC 与原生 NATS 错误信息。
func asItemError(err error) *pb.NatsError {
	st := status.Convert(rpcError(err))
	for _, d := range st.Details() {
		if e, ok := d.(*pb.NatsError); ok {
			return e
		}
	}
	code := st.Code()
	if code > codes.Unauthenticated {
		code = codes.Unknown
	}
	return &pb.NatsError{
		GrpcCode: int32(code),
		Message:  st.Message(),
	}
}

// kvStatus 复制桶配置和元数据，将存储状态转换为协议响应。
func kvStatus(s jetstream.KeyValueStatus) *pb.KeyValueStatus {
	c := s.Config()
	c.Metadata = maps.Clone(c.Metadata)
	return &pb.KeyValueStatus{
		Bucket: s.Bucket(),
		Values: s.Values(),
		// #nosec G115 -- NATS 将 History 限定在 1..64，不能出现负值或溢出。
		History:          uint64(s.History()),
		TtlNs:            int64(s.TTL()),
		BackingStore:     s.BackingStore(),
		Bytes:            s.Bytes(),
		Compressed:       s.IsCompressed(),
		LimitMarkerTtlNs: int64(s.LimitMarkerTTL()),
		Metadata:         maps.Clone(s.Metadata()),
		Config:           encodeKeyValueConfig(&c),
	}
}

// kvEntry 复制业务字节，保留 revision、创建时间和删除操作标记。
func kvEntry(e jetstream.KeyValueEntry) *pb.KeyValueEntry {
	op := pb.KeyValueEntry_PUT
	switch e.Operation() {
	case jetstream.KeyValueDelete:
		op = pb.KeyValueEntry_DELETE
	case jetstream.KeyValuePurge:
		op = pb.KeyValueEntry_PURGE
	}
	return &pb.KeyValueEntry{
		Bucket:    e.Bucket(),
		Key:       e.Key(),
		Value:     slices.Clone(e.Value()),
		Revision:  e.Revision(),
		Created:   stamp(e.Created()),
		Delta:     e.Delta(),
		Operation: op,
	}
}

// validName 只接受有界的 ASCII 逻辑名称，禁止路径和 subject 分隔符。
func validName(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	for _, c := range s {
		valid := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-'
		if !valid {
			return false
		}
	}
	return true
}

// validSubject 验证点分隔 subject；通配符只能占据完整 token，尾部 > 不能放在中间。
func validSubject(s string, wild bool) bool {
	if len(s) == 0 || len(s) > 512 || strings.HasPrefix(s, "$") {
		return false
	}
	tokens := strings.Split(s, ".")
	for i, t := range tokens {
		if t == "" {
			return false
		}
		if wild && (t == "*" || t == ">" && i == len(tokens)-1) {
			continue
		}
		if strings.ContainsAny(t, "*> \t\r\n\x00") {
			return false
		}
		for _, c := range t {
			if c < 33 || c == 127 {
				return false
			}
		}
	}
	return true
}

// statusTooLarge 用于拒绝业务数据与头部超限，不能依赖 gRPC 封包大小代替业务预算。
func statusTooLarge() error {
	return exhausted(ReasonMessageTooLarge, "message exceeds configured limit")
}

// casError 把 KV 版本冲突映射为 Aborted，同时保留原生错误码。
func casError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, jetstream.ErrKeyExists) {
		st := status.New(codes.Aborted, "KV revision conflict")
		var api *jetstream.APIError
		if errors.As(err, &api) {
			if with, e := st.WithDetails(&pb.NatsError{
				GrpcCode:      int32(codes.Aborted),
				Message:       "KV revision conflict",
				ApiCode:       apiCode(api.Code),
				NatsErrorCode: ptr(uint32(api.ErrorCode)),
			}); e == nil {
				return with.Err()
			}
		}
		return st.Err()
	}
	return rpcError(err)
}

// apiCode 只在 HTTP 状态码能无损表示时携带它，禁止整数转换造成伪造错误码。
func apiCode(code int) *int32 {
	if code < 0 || code > math.MaxInt32 {
		return nil
	}
	return ptr(int32(code))
}

// statusUnavailable 将订阅意外结束报告为故障，不能伪装为正常 EOF。
func statusUnavailable() error {
	return status.Error(codes.Unavailable, "watch ended before cancellation")
}

// 原生错误描述保留诊断信息，展示名称去除内部租户/namespace 前缀。
var resourcePrefix = regexp.MustCompile("X_[a-f0-9]{32}_(?:[A-Z2-7]+|0)_")

var subjectPrefix = regexp.MustCompile("x[.][a-f0-9]{32}[.](?:[A-Z2-7]+|0)[.]")

// publicNATSError 移除诊断文本中的内部租户寻址前缀，保留业务资源名称。
func publicNATSError(s string) string {
	s = resourcePrefix.ReplaceAllString(s, "")
	return subjectPrefix.ReplaceAllString(s, "")
}
