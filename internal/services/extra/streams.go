package extra

import (
	"context"
	"errors"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// CreateOrUpdateStream 校验并映射当前租户的 stream 配置，创建或更新后返回逻辑名称。
func (s *Service) CreateOrUpdateStream(ctx context.Context, r *pb.StreamConfigRequest) (*pb.NatsStreamInfo, error) {
	if r == nil || r.Config == nil {
		return nil, invalid("stream config required")
	}
	if err := checkTimes(r.Config.ProtoReflect()); err != nil {
		return nil, err
	}
	if r.Config.GetCompression() > 1 {
		return nil, invalid("invalid compression policy")
	}
	a, err := addressFor(ctx, r.Scope)
	if err != nil {
		return nil, err
	}
	c := decodeStreamConfig(r.Config)
	if c.Replicas > 1 {
		return nil, reasonError(codes.FailedPrecondition, ReasonReplicasUnsupported, "single node supports one replica")
	}
	budget := s.cfg.MaxStoreBytes
	if c.Storage == jetstream.MemoryStorage {
		budget = s.cfg.MaxMemoryBytes
	}
	if c.MaxBytes > budget {
		return nil, exhausted(ReasonStoreExhausted, "stream byte limit exceeds account budget")
	}

	if err = a.mapStreamConfig(&c); err != nil {
		return nil, err
	}
	if r.Config.MaxMsgs == nil {
		c.MaxMsgs = -1
	}
	if r.Config.MaxBytes == nil {
		c.MaxBytes = -1
	}
	if r.Config.MaxConsumers == nil {
		c.MaxConsumers = -1
	}
	if r.Config.MaxMsgsPerSubject == nil {
		c.MaxMsgsPerSubject = -1
	}
	if c.MaxMsgSize == 0 || int64(c.MaxMsgSize) > int64(s.cfg.MaxMessageBytes) {
		// #nosec G115 -- New 将 MaxMessageBytes 限定在 1..64 MiB，能无损转为 int32。
		c.MaxMsgSize = int32(s.cfg.MaxMessageBytes)
	}
	release, err := s.configurationWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	var stream jetstream.Stream
	if r.CreateOnly {
		if _, err := s.js.Stream(ctx, c.Name); err == nil {
			return nil, status.Error(codes.AlreadyExists, "stream already exists")
		} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, rpcError(err)
		}
		stream, err = s.js.CreateStream(ctx, c)
	} else {
		stream, err = s.js.CreateOrUpdateStream(ctx, c)
	}
	if err != nil {
		return nil, rpcError(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	return a.streamInfo(info), nil
}

// GetStream 查询当前租户和 namespace 的 stream，不接受物理资源地址。
func (s *Service) GetStream(ctx context.Context, r *pb.StreamInfoRequest) (*pb.NatsStreamInfo, error) {
	if r == nil {
		return nil, invalid("request required")
	}
	a, n, err := s.stream(ctx, r.Stream)
	if err != nil {
		return nil, err
	}
	var opts []jetstream.StreamInfoOpt
	if r.DeletedDetails != nil {
		opts = append(opts, jetstream.WithDeletedDetails(*r.DeletedDetails))
	}
	if r.SubjectFilter != nil {
		sub, e := a.subject(*r.SubjectFilter, true)
		if e != nil {
			return nil, e
		}
		opts = append(opts, jetstream.WithSubjectFilter(sub))
	}
	stream, err := s.js.Stream(ctx, n)
	if err != nil {
		return nil, rpcError(err)
	}
	info, err := stream.Info(ctx, opts...)
	if err != nil {
		return nil, rpcError(err)
	}
	return a.streamInfo(info), nil
}

// ListStreams 逐页列出当前租户和 namespace 的 stream，取消时停止后台枚举。
func (s *Service) ListStreams(r *pb.StreamListRequest, out grpc.ServerStreamingServer[pb.NatsStreamInfo]) error {
	if r == nil {
		return invalid("request required")
	}
	ctx := out.Context()
	a, err := addressFor(ctx, r.Scope)
	if err != nil {
		return err
	}
	var opts []jetstream.StreamListOpt
	if r.SubjectFilter != nil {
		sub, e := a.subject(*r.SubjectFilter, true)
		if e != nil {
			return e
		}
		opts = append(opts, jetstream.WithStreamListSubject(sub))
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	list := s.js.ListStreams(ctx, opts...)
	for info := range list.Info() {
		if !strings.HasPrefix(info.Config.Name, a.names) {
			continue
		}
		if err := out.Send(a.streamInfo(info)); err != nil {
			return rpcError(err)
		}
	}
	return rpcError(list.Err())
}

// DeleteStream 删除当前作用域的 stream；删除包含其消息和消费者状态。
func (s *Service) DeleteStream(ctx context.Context, r *pb.StreamRef) (*emptypb.Empty, error) {
	_, n, err := s.stream(ctx, r)
	if err != nil {
		return nil, err
	}
	err = s.js.DeleteStream(ctx, n)
	return &emptypb.Empty{}, rpcError(err)
}

// GetMessage 先冻结选择器，再查询后端；缺少选择器不能产生额外的网络请求。
func (s *Service) GetMessage(ctx context.Context, r *pb.GetMessageRequest) (*pb.NatsRawStreamMsg, error) {
	if r == nil {
		return nil, invalid("request required")
	}
	a, n, err := s.stream(ctx, r.Stream)
	if err != nil {
		return nil, err
	}
	if r.GetDirect() || r.NextSubject != nil {
		return nil, invalid("legacy direct-get options are not supported")
	}
	var sequence uint64
	var last string
	var opts []jetstream.GetMsgOpt
	switch v := r.Selector.(type) {
	case *pb.GetMessageRequest_Sequence:
		if v.Sequence == 0 {
			return nil, invalid("sequence must be positive")
		}
		sequence = v.Sequence
		if r.Subject != nil {
			sub, e := a.subject(*r.Subject, true)
			if e != nil {
				return nil, e
			}
			opts = append(opts, jetstream.WithGetMsgSubject(sub))
		}
	case *pb.GetMessageRequest_LastSubject:
		if r.Subject != nil {
			return nil, invalid("subject selectors conflict")
		}
		last, err = a.subject(v.LastSubject, true)
		if err != nil {
			return nil, err
		}
	default:
		return nil, invalid("message selector required")
	}
	stream, err := s.js.Stream(ctx, n)
	if err != nil {
		return nil, rpcError(err)
	}
	var m *jetstream.RawStreamMsg
	if sequence != 0 {
		m, err = stream.GetMsg(ctx, sequence, opts...)
	} else {
		m, err = stream.GetLastMsgForSubject(ctx, last)
	}
	if err != nil {
		return nil, rpcError(err)
	}
	m.Subject = a.logicalSubject(m.Subject)
	return encodeRawStreamMsg(m), nil
}
