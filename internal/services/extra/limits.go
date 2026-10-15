package extra

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// PublishBatchPerMessageOverhead 是批次预算中每条消息额外预留的原生控制头与回复地址开销。
const PublishBatchPerMessageOverhead = 256

const (
	ReasonBatchCapacity       = "BATCH_CAPACITY"
	ReasonBatchMessageLimit   = "BATCH_MESSAGE_LIMIT"
	ReasonBatchByteLimit      = "BATCH_BYTE_LIMIT"
	ReasonAckBatchLimit       = "ACK_BATCH_LIMIT"
	ReasonMessageTooLarge     = "MESSAGE_TOO_LARGE"
	ReasonRPCCapacity         = "RPC_CAPACITY"
	ReasonStreamCapacity      = "STREAM_CAPACITY"
	ReasonReadCapacity        = "READ_CAPACITY"
	ReasonFetchBuffer         = "FETCH_BUFFER"
	ReasonStoreExhausted      = "STORE_EXHAUSTED"
	ReasonConsumerLimit       = "CONSUMER_LIMIT"
	ReasonReplicasUnsupported = "REPLICAS_UNSUPPORTED"
	ReasonPublishPrecondition = "PUBLISH_PRECONDITION"
	ReasonBatchInFlight       = "BATCH_IN_FLIGHT"
)

func reasonError(code codes.Code, reason, message string) error {
	s := status.New(code, message)
	with, err := s.WithDetails(&pb.NatsError{GrpcCode: int32(code), Message: message, Reason: reason})
	if err != nil {
		return s.Err()
	}
	return with.Err()
}

func exhausted(reason, message string) error {
	return reasonError(codes.ResourceExhausted, reason, message)
}

// GetLimits 返回构造时校验并冻结的配置，鉴权由统一 unary 拦截器执行。
func (s *Service) GetLimits(context.Context, *emptypb.Empty) (*pb.ServiceLimits, error) {
	c := s.cfg
	// #nosec G115 -- 配置数值已通过 validateLimits 的正值和上界校验。
	return &pb.ServiceLimits{
		MaxMessageBytes: uint64(c.MaxMessageBytes), MaxFetchMessages: uint32(c.MaxFetchMessages),
		MaxFetchBytes: uint64(c.MaxFetchBytes), MaxPublishBatchMessages: uint32(c.MaxPublishBatchMessages),
		MaxPublishBatchBytes: uint64(c.MaxPublishBatchBytes), PublishBatchPerMessageOverhead: PublishBatchPerMessageOverhead,
		MaxPublishBatchesInflight: uint32(c.MaxPublishBatchesInFlight), MaxAckBatchMessages: uint32(c.MaxAckBatchMessages),
		MaxConcurrentUnary: uint32(c.MaxConcurrentUnary), MaxConcurrentStreams: uint32(c.MaxConcurrentStreams),
		MaxConcurrentReads: uint32(c.MaxConcurrentReads), MaxStoreBytes: c.MaxStoreBytes, MaxMemoryBytes: c.MaxMemoryBytes,
		BatchCommitTimeoutNs: int64(c.BatchCommitTimeout), BatchAbandonTimeoutNs: int64(c.BatchAbandonTimeout),
		PublishBatchFlowControl: uint32(c.PublishBatchFlowControl), MaxConsumers: 0, SingleNode: true,
	}, nil
}
