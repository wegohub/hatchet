package extra

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// preparePublish 在任何网络 I/O 前完成参数验证，控制 headers 只允许从 typed options 生成。
func (s *Service) preparePublish(ctx context.Context, r *pb.PublishRequest) (address, *nats.Msg, []jetstream.PublishOpt, error) {
	if r == nil || r.Message == nil {
		return address{}, nil, nil, invalid("message required")
	}
	a, err := addressFor(ctx, r.Scope)
	if err != nil {
		return a, nil, nil, err
	}
	sub, err := a.subject(r.Message.Subject, false)
	if err != nil {
		return a, nil, nil, err
	}
	if len(r.Message.Data) > s.cfg.MaxMessageBytes {
		return a, nil, nil, statusTooLarge()
	}
	if r.Message.Reply != "" || len(r.Message.ReplyToken) > 0 {
		return a, nil, nil, invalid("reply is not supported for JetStream publish")
	}
	headerSize := len("NATS/1.0\r\n\r\n")
	for k, v := range r.Message.Headers {
		if k == "" || strings.ContainsAny(k, " \t\r\n:") || strings.HasPrefix(strings.ToLower(k), "nats-") {
			return a, nil, nil, invalid("invalid or reserved header")
		}
		for _, str := range v.GetValues() {
			if strings.ContainsAny(str, "\r\n\x00") {
				return a, nil, nil, invalid("invalid header value")
			}
			headerSize += len(k) + len(": ") + len(str) + len("\r\n")
		}
	}
	if headerSize > 65536 {
		return a, nil, nil, statusTooLarge()
	}
	m := &nats.Msg{
		Subject: sub,
		Data:    slices.Clone(r.Message.Data),
		Header:  decodeHeaders(r.Message.Headers),
	}
	o := r.Options
	var opts []jetstream.PublishOpt
	if o == nil {
		return a, m, opts, nil
	}
	if o.MessageId != nil {
		if len(*o.MessageId) == 0 || !validHeaderOption(*o.MessageId) {
			return a, nil, nil, invalid("invalid message ID")
		}
		opts = append(opts, jetstream.WithMsgID(*o.MessageId))
	}
	if o.ExpectedStream != nil {
		n, e := a.name(*o.ExpectedStream)
		if e != nil {
			return a, nil, nil, e
		}
		opts = append(opts, jetstream.WithExpectStream(n))
	}
	if o.ExpectedLastMessageId != nil {
		if !validHeaderOption(*o.ExpectedLastMessageId) {
			return a, nil, nil, invalid("invalid expected message ID")
		}
		opts = append(opts, jetstream.WithExpectLastMsgID(*o.ExpectedLastMessageId))
	}
	if o.ExpectedLastSequence != nil {
		opts = append(opts, jetstream.WithExpectLastSequence(*o.ExpectedLastSequence))
	}
	if o.ExpectedLastSubject != nil {
		if o.ExpectedLastSubjectSequence == nil {
			return a, nil, nil, invalid("expected subject sequence required")
		}
		sub, e := a.subject(*o.ExpectedLastSubject, true)
		if e != nil {
			return a, nil, nil, e
		}
		opts = append(opts, jetstream.WithExpectLastSequenceForSubject(*o.ExpectedLastSubjectSequence, sub))
	} else if o.ExpectedLastSubjectSequence != nil {
		opts = append(opts, jetstream.WithExpectLastSequencePerSubject(*o.ExpectedLastSubjectSequence))
	}
	if o.MessageTtlNs != nil {
		if *o.MessageTtlNs <= 0 {
			return a, nil, nil, invalid("message TTL must be positive")
		}
		opts = append(opts, jetstream.WithMsgTTL(time.Duration(*o.MessageTtlNs)))
	}
	if o.RetryAttempts != nil {
		if *o.RetryAttempts > 10 {
			return a, nil, nil, invalid("retry attempts exceed 10")
		}
		opts = append(opts, jetstream.WithRetryAttempts(int(*o.RetryAttempts)))
	}
	if o.RetryWaitNs != nil {
		if *o.RetryWaitNs <= 0 || *o.RetryWaitNs > int64(maxFetchWait) {
			return a, nil, nil, invalid("invalid retry wait")
		}
		opts = append(opts, jetstream.WithRetryWait(time.Duration(*o.RetryWaitNs)))
	}
	if o.StallWaitNs != nil {
		return a, nil, nil, invalid("native asynchronous stall wait is not supported by this pipeline")
	}
	if x := o.Schedule; x != nil {
		target, e := a.subject(x.TargetSubject, false)
		if e != nil {
			return a, nil, nil, e
		}
		opts = append(opts, jetstream.WithScheduleTarget(target))
		switch v := x.Schedule.(type) {
		case *pb.ScheduleOptions_At:
			if v.At == nil || v.At.CheckValid() != nil {
				return a, nil, nil, invalid("invalid schedule time")
			}
			opts = append(opts, jetstream.WithScheduleAt(v.At.AsTime()))
		case *pb.ScheduleOptions_Cron:
			if v.Cron == "" || !validHeaderOption(v.Cron) {
				return a, nil, nil, invalid("cron required")
			}
			opts = append(opts, jetstream.WithScheduleCron(v.Cron))
		case *pb.ScheduleOptions_EveryNs:
			if v.EveryNs < int64(time.Second) {
				return a, nil, nil, invalid("schedule interval must be at least one second")
			}
			opts = append(opts, jetstream.WithScheduleEvery(time.Duration(v.EveryNs)))
		default:
			return a, nil, nil, invalid("schedule required")
		}
		if x.SourceSubject != nil {
			sub, e := a.subject(*x.SourceSubject, false)
			if e != nil {
				return a, nil, nil, e
			}
			opts = append(opts, jetstream.WithScheduleSource(sub))
		}
		if x.TimeZone != nil {
			if _, ok := x.Schedule.(*pb.ScheduleOptions_Cron); !ok {
				return a, nil, nil, invalid("time zone requires cron")
			}
			if _, e := time.LoadLocation(*x.TimeZone); e != nil {
				return a, nil, nil, invalid("invalid time zone")
			}
			opts = append(opts, jetstream.WithScheduleTimeZone(*x.TimeZone))
		}
		if x.GetRollup() {
			opts = append(opts, jetstream.WithScheduleRollup())
		}
		switch v := x.Expiry.(type) {
		case *pb.ScheduleOptions_TtlNs:
			if v.TtlNs <= 0 {
				return a, nil, nil, invalid("schedule TTL must be positive")
			}
			opts = append(opts, jetstream.WithScheduleTTL(time.Duration(v.TtlNs)))
		case *pb.ScheduleOptions_TtlNever:
			if !v.TtlNever {
				return a, nil, nil, invalid("ttl_never must be true")
			}
			opts = append(opts, jetstream.WithScheduleTTLNever())
		}
	}
	return a, m, opts, nil
}

// validHeaderOption 禁止 typed options 注入协议头；值允许空格，但不能包含控制字符。
func validHeaderOption(value string) bool {
	if len(value) > 256 {
		return false
	}
	for _, c := range value {
		if c < 32 || c == 127 {
			return false
		}
	}
	return true
}

// Publish 校验租户地址和发布选项，返回 JetStream 持久发布确认。
func (s *Service) Publish(ctx context.Context, r *pb.PublishRequest) (*pb.NatsPubAck, error) {
	a, m, opts, err := s.preparePublish(ctx, r)
	if err != nil {
		return nil, err
	}
	ack, err := s.js.PublishMsg(ctx, m, opts...)
	if err != nil {
		var api *jetstream.APIError
		if errors.As(err, &api) && (api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
			api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant) {
			st := status.Convert(rpcError(err))
			converted := status.New(codes.FailedPrecondition, st.Message())
			for _, detail := range st.Details() {
				if d, ok := detail.(*pb.NatsError); ok {
					d.GrpcCode = int32(codes.FailedPrecondition)
					d.Message = "publish precondition failed"
					if with, e := converted.WithDetails(d); e == nil {
						converted = with
					}
				}
			}
			return nil, converted.Err()
		}
		return nil, rpcError(err)
	}
	ack.Stream = a.logicalName(ack.Stream)
	return encodePubAck(ack), nil
}

// PublishAsync 限制在途数量；只有主循环写 gRPC 流，工作 goroutine 不持有发送锁。
func (s *Service) PublishAsync(out grpc.BidiStreamingServer[pb.PublishRequest, pb.PublishResult]) error {
	ctx, cancel := context.WithCancel(out.Context())
	defer cancel()
	type received struct {
		r   *pb.PublishRequest
		err error
	}
	requests := make(chan received, 1)
	go func() {
		for {
			r, err := out.Recv()
			select {
			case requests <- received{r, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	completed := make(chan *pb.PublishResult, s.cfg.MaxAsyncInFlight)
	var workers sync.WaitGroup
	reserved := 0
	defer func() {
		cancel()
		workers.Wait()
		for reserved > 0 {
			<-s.asyncSlots
			reserved--
		}
	}()
	inFlight := make(map[string]struct{}, s.cfg.MaxAsyncInFlight)
	reading := true
	var waiting *pb.PublishRequest
	for reading || len(inFlight) > 0 {
		var input <-chan received
		if reading && len(inFlight) < s.cfg.MaxAsyncInFlight && waiting == nil {
			input = requests
		}
		var reserve chan<- struct{}
		if waiting != nil {
			reserve = s.asyncSlots
		}
		select {
		case <-ctx.Done():
			return rpcError(ctx.Err())
		case v := <-input:
			if v.err == io.EOF {
				reading = false
				continue
			}
			if v.err != nil {
				return rpcError(v.err)
			}
			id := v.r.GetCorrelationId()
			if id == "" || len(id) > 256 {
				if err := out.Send(&pb.PublishResult{
					CorrelationId: id,
					Result:        &pb.PublishResult_Error{Error: asItemError(invalid("correlation ID required, maximum 256 bytes"))},
				}); err != nil {
					return rpcError(err)
				}
				continue
			}
			if _, ok := inFlight[id]; ok {
				return invalid("duplicate in-flight correlation ID")
			}
			inFlight[id] = struct{}{}
			waiting = v.r
		case reserve <- struct{}{}:
			reserved++
			r := waiting
			waiting = nil
			workers.Add(1)
			go func(r *pb.PublishRequest) {
				defer workers.Done()
				ack, err := s.Publish(ctx, r)
				res := &pb.PublishResult{CorrelationId: r.CorrelationId}
				if err != nil {
					res.Result = &pb.PublishResult_Error{Error: asItemError(err)}
				} else {
					res.Result = &pb.PublishResult_Ack{Ack: ack}
				}
				select {
				case completed <- res:
				case <-ctx.Done():
				}
			}(r)
		case r := <-completed:
			delete(inFlight, r.CorrelationId)
			<-s.asyncSlots
			reserved--
			if err := out.Send(r); err != nil {
				return rpcError(err)
			}
		}
	}
	return nil
}
