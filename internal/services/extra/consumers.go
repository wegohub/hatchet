package extra

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/hatchet-dev/hatchet/internal/services/extra/proto/v1"
)

// consumer 校验 stream 和 consumer 引用，消费者名称只在所属 stream 内寻址。
func (s *Service) consumer(ctx context.Context, r *pb.ConsumerRef) (address, string, string, error) {
	if r == nil {
		return address{}, "", "", invalid("consumer required")
	}
	a, n, err := s.stream(ctx, r.Stream)
	if err != nil {
		return a, "", "", err
	}
	if !validName(r.Consumer) {
		return a, "", "", invalid("invalid consumer name")
	}
	return a, n, r.Consumer, nil
}

// CreateOrUpdateConsumer 创建或更新具名 durable pull consumer，过滤 subject 始终限定在租户内。
func (s *Service) CreateOrUpdateConsumer(ctx context.Context, r *pb.ConsumerConfigRequest) (*pb.NatsConsumerInfo, error) {
	if r == nil || r.Config == nil {
		return nil, invalid("consumer config required")
	}
	if r.Push {
		return nil, invalid("push consumer is not enabled")
	}
	if err := checkTimes(r.Config.ProtoReflect()); err != nil {
		return nil, err
	}
	a, n, err := s.stream(ctx, r.Stream)
	if err != nil {
		return nil, err
	}
	c := decodeConsumerConfig(r.Config)
	if err = a.consumerConfig(&c); err != nil {
		return nil, err
	}
	cons, err := s.js.CreateOrUpdateConsumer(ctx, n, c)
	if err != nil {
		return nil, rpcError(err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	return a.consumerInfo(info), nil
}

// GetConsumer 读取持久消费者的投递与确认进度，资源地址由认证租户生成。
func (s *Service) GetConsumer(ctx context.Context, r *pb.ConsumerRef) (*pb.NatsConsumerInfo, error) {
	a, n, c, err := s.consumer(ctx, r)
	if err != nil {
		return nil, err
	}
	cons, err := s.js.Consumer(ctx, n, c)
	if err != nil {
		return nil, rpcError(err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	return a.consumerInfo(info), nil
}

// ListConsumers 列出指定租户 stream 的消费者，保持原生确认和积压信息。
func (s *Service) ListConsumers(r *pb.StreamRef, out grpc.ServerStreamingServer[pb.NatsConsumerInfo]) error {
	ctx, cancel := context.WithCancel(out.Context())
	defer cancel()
	a, n, err := s.stream(ctx, r)
	if err != nil {
		return err
	}
	stream, err := s.js.Stream(ctx, n)
	if err != nil {
		return rpcError(err)
	}
	list := stream.ListConsumers(ctx)
	for info := range list.Info() {
		if err := out.Send(a.consumerInfo(info)); err != nil {
			return rpcError(err)
		}
	}
	return rpcError(list.Err())
}

// DeleteConsumer 删除指定持久消费者，不删除 stream 中的业务消息。
func (s *Service) DeleteConsumer(ctx context.Context, r *pb.ConsumerRef) (*emptypb.Empty, error) {
	_, n, c, err := s.consumer(ctx, r)
	if err != nil {
		return nil, err
	}
	err = s.js.DeleteConsumer(ctx, n, c)
	return &emptypb.Empty{}, rpcError(err)
}

// pullRequest 字段与官方 JetStream pull 协议一致；同时限制消息数与字节数，避免 FetchBytes 的大缓冲。
type pullRequest struct {
	Batch         int    `json:"batch"`
	MaxBytes      int    `json:"max_bytes"`
	Expires       int64  `json:"expires,omitempty"`
	NoWait        bool   `json:"no_wait,omitempty"`
	Heartbeat     int64  `json:"idle_heartbeat,omitempty"`
	Group         string `json:"group,omitempty"`
	MinPending    int64  `json:"min_pending,omitempty"`
	MinAckPending int64  `json:"min_ack_pending,omitempty"`
	Priority      uint32 `json:"priority,omitempty"`
}

// Fetch 拉取有消息数和字节上限的投递，不自动 ACK；控制帧不作为业务消息发送。
func (s *Service) Fetch(r *pb.FetchRequest, out grpc.ServerStreamingServer[pb.Delivery]) error {
	if r == nil {
		return invalid("fetch request required")
	}
	ctx := out.Context()
	a, n, c, err := s.consumer(ctx, r.Consumer)
	if err != nil {
		return err
	}
	req := pullRequest{
		Batch:    s.cfg.MaxFetchMessages,
		MaxBytes: s.cfg.MaxFetchBytes,
		NoWait:   r.NoWait,
	}
	switch v := r.Batch.(type) {
	case *pb.FetchRequest_MaxMessages:
		if v.MaxMessages == 0 || v.MaxMessages > 4096 || int(v.MaxMessages) > s.cfg.MaxFetchMessages {
			return invalid("invalid batch size")
		}
		req.Batch = int(v.MaxMessages)
	case *pb.FetchRequest_MaxBytes:
		if v.MaxBytes == 0 || v.MaxBytes > 256<<20 || int(v.MaxBytes) > s.cfg.MaxFetchBytes {
			return invalid("invalid fetch byte limit")
		}
		req.MaxBytes = int(v.MaxBytes)
	default:
		return invalid("fetch batch required")
	}
	if r.MaxBytesLimit != nil {
		if *r.MaxBytesLimit == 0 || *r.MaxBytesLimit > 256<<20 || int(*r.MaxBytesLimit) > req.MaxBytes {
			return invalid("invalid additional byte limit")
		}
		req.MaxBytes = int(*r.MaxBytesLimit)
	}
	wait := maxFetchWait
	if o := r.Options; o != nil {
		if o.MaxWaitNs != nil {
			wait = time.Duration(*o.MaxWaitNs)
			if wait <= 0 || wait > maxFetchWait {
				return invalid("fetch wait must be between zero and 30 seconds")
			}
		}
		req.Heartbeat = o.GetHeartbeatNs()
		req.Group = o.GetPriorityGroup()
		req.MinPending = o.GetMinPending()
		req.MinAckPending = o.GetMinAckPending()
		req.Priority = o.GetPriority()
		if req.Priority > 9 || req.MinPending < 0 || req.MinAckPending < 0 {
			return invalid("invalid fetch priority")
		}
		if (o.MinPending != nil || o.MinAckPending != nil || o.Priority != nil) && req.Group == "" {
			return invalid("priority group required")
		}
		if req.Group != "" && !validName(req.Group) {
			return invalid("invalid priority group")
		}
	}
	if d, ok := ctx.Deadline(); ok && time.Until(d) < wait {
		wait = time.Until(d)
	}
	if wait <= 0 {
		return rpcError(context.DeadlineExceeded)
	}
	if req.Heartbeat < 0 || req.Heartbeat > 0 && req.Heartbeat >= int64(wait)/2 {
		return invalid("heartbeat must be less than half the wait")
	}
	if req.NoWait {
		if r.Options != nil {
			return invalid("no_wait does not accept fetch options")
		}
	} else {
		req.Expires = int64(wait)
	}
	cons, err := s.js.Consumer(ctx, n, c)
	if err != nil {
		return rpcError(err)
	}
	// pinned consumer 需要跨请求固定客户端身份，首版未开放 Unpin/会话 API。
	if cons.CachedInfo().Config.PriorityPolicy == jetstream.PriorityPolicyPinned {
		return invalid("pinned consumers are not enabled")
	}
	inbox := s.nc.NewInbox()
	// 使用有界原始投递队列，不能让 NextMsgWithContext 将空业务消息的 Status=503
	// 转换成 NoResponders。是否为控制帧由 inbox 和回复地址共同决定。
	messages := make(chan *nats.Msg, req.Batch+8)
	overflow := make(chan struct{}, 1)
	sub, err := s.nc.Subscribe(inbox, func(message *nats.Msg) {
		select {
		case messages <- message:
		case <-ctx.Done():
		default:
			select {
			case overflow <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		return rpcError(err)
	}
	defer func() {
		// 关闭连接也会使订阅失效，清理错误不覆盖本次 pull 的结果。
		_ = sub.Unsubscribe()
	}()
	if err = sub.SetPendingLimits(req.Batch+8, req.MaxBytes+65536); err != nil {
		return rpcError(err)
	}
	// Flush 保证服务器先建立回复订阅，再接收 pull；不会共享业务 ACK 状态。
	flushCtx, flushCancel := context.WithTimeout(ctx, 5*time.Second)
	err = s.nc.FlushWithContext(flushCtx)
	flushCancel()
	if err != nil {
		return rpcError(err)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return rpcError(err)
	}
	if err = s.nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT."+n+"."+c, inbox, body); err != nil {
		return rpcError(err)
	}
	// 额外时间只用于收到服务端的批次结束控制帧，不能超过调用方 deadline。
	readCtx, cancel := context.WithTimeout(ctx, wait+time.Second)
	defer cancel()
	bytesRead := 0
	count := 0
	for count < req.Batch {
		if dropped, err := sub.Dropped(); err != nil {
			return rpcError(err)
		} else if dropped > 0 {
			return status.Error(codes.ResourceExhausted, "fetch subscription buffer exhausted")
		}
		nextCtx := readCtx
		var nextCancel context.CancelFunc
		if req.Heartbeat > 0 {
			nextCtx, nextCancel = context.WithTimeout(readCtx, 2*time.Duration(req.Heartbeat))
		}
		var m *nats.Msg
		select {
		case <-overflow:
			if nextCancel != nil {
				nextCancel()
			}
			return status.Error(codes.ResourceExhausted, "fetch receive buffer exhausted")
		case <-nextCtx.Done():
			if nextCancel != nil {
				nextCancel()
			}
			return rpcError(nextCtx.Err())
		case m = <-messages:
		}
		if nextCancel != nil {
			nextCancel()
		}
		// 控制帧发往本次 inbox；业务消息即使含 Status header 也必须正常投递。
		if control := m.Header.Get("Status"); control != "" && m.Subject == inbox && m.Reply == "" {
			switch control {
			case "100":
				continue
			case "404", "408":
				if ctx.Err() != nil {
					return rpcError(ctx.Err())
				}
				return nil
			case "409":
				desc := m.Header.Get("Description")
				if strings.Contains(desc, "Batch Completed") || strings.Contains(desc, "Message Size Exceeds MaxBytes") {
					return nil
				}
				return status.Error(codes.FailedPrecondition, "consumer pull rejected")
			default:
				return status.Error(codes.Unavailable, "consumer pull unavailable")
			}
		}
		bytesRead += m.Size()
		if bytesRead > req.MaxBytes || len(m.Data) > s.cfg.MaxMessageBytes {
			return statusTooLarge()
		}
		meta, e := m.Metadata()
		if e != nil {
			return status.Error(codes.DataLoss, "invalid NATS delivery metadata")
		}
		if meta.Stream != n || meta.Consumer != c || !strings.HasPrefix(m.Subject, a.subjects) {
			return status.Error(codes.DataLoss, "delivery identity mismatch")
		}
		token, e := s.signACK(ctx, n, c, m.Reply)
		if e != nil {
			return rpcError(e)
		}
		md := &pb.NatsMsgMetadata{
			Stream:   ptr(a.logicalName(n)),
			Consumer: ptr(c),
			Domain:   ptr(meta.Domain),
			Sequence: &pb.NatsSequencePair{
				Consumer: ptr(meta.Sequence.Consumer),
				Stream:   ptr(meta.Sequence.Stream),
			},
			NumDelivered: ptr(meta.NumDelivered),
			NumPending:   ptr(meta.NumPending),
			Timestamp:    stamp(meta.Timestamp),
		}
		delivery := &pb.Delivery{
			Message: &pb.Message{
				Subject: a.logicalSubject(m.Subject),
				Data:    m.Data,
				Headers: encodeHeaders(m.Header),
			},
			Metadata: md,
			AckToken: token,
		}
		if err = out.Send(delivery); err != nil {
			return rpcError(err)
		}
		count++
	}
	return nil
}
