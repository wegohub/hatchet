package extra

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

var errReadBoundary = errors.New("read snapshot boundary reached")

// Read 冻结当前尾部，临时消费者只读取本次有限快照；取消也使用独立预算清理资源。
func (s *Service) Read(r *pb.ReadRequest, out grpc.ServerStreamingServer[pb.NatsRawStreamMsg]) (result error) {
	if r == nil {
		return invalid("read request required")
	}
	ctx := out.Context()
	a, n, err := s.stream(ctx, r.Stream)
	if err != nil {
		return err
	}
	subject, err := a.subject(r.Subject, false)
	if err != nil {
		return err
	}
	start := max(uint64(1), r.StartSequence)
	if r.EndSequence != 0 && start > r.EndSequence {
		return invalid("invalid read sequence range")
	}
	count, bytes := s.cfg.MaxFetchMessages, s.cfg.MaxFetchBytes
	if r.MaxMessages != 0 {
		if uint64(r.MaxMessages) > uint64(count) {
			return invalid("read message limit exceeds service limit")
		}
		count = int(r.MaxMessages)
	}
	if r.MaxBytes != 0 {
		if r.MaxBytes > uint64(bytes) {
			return invalid("read byte limit exceeds service limit")
		}
		bytes = int(r.MaxBytes)
	}
	stream, err := s.js.Stream(ctx, n)
	if err != nil {
		return rpcError(err)
	}
	tail, err := stream.GetLastMsgForSubject(ctx, subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil
	}
	if err != nil {
		return rpcError(err)
	}
	end := tail.Sequence
	if r.EndSequence != 0 {
		end = min(end, r.EndSequence)
	}
	if start > end {
		return nil
	}
	name := "read_" + uuid.NewString()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := s.js.DeleteConsumer(cleanup, n, name); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) && !errors.Is(err, jetstream.ErrStreamNotFound) {
			slog.Error("extra Read consumer cleanup failed", "stream", a.logicalName(n), "consumer", name)
			if result == nil {
				result = rpcError(err)
			}
		}
	}()
	_, err = stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name: name, FilterSubject: subject, DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq: start, AckPolicy: jetstream.AckNonePolicy, ReplayPolicy: jetstream.ReplayInstantPolicy,
		InactiveThreshold: 30 * time.Second,
	})
	if err != nil {
		return rpcError(err)
	}
	result = s.pullMessages(ctx, a, n, name, pullRequest{Batch: count, MaxBytes: bytes, NoWait: true}, maxFetchWait,
		func(m *nats.Msg, meta *nats.MsgMetadata) error {
			if m.Subject != subject || meta.Sequence.Stream < start {
				return status.Error(codes.DataLoss, "read identity mismatch")
			}
			if meta.Sequence.Stream > end {
				return errReadBoundary
			}
			raw := &jetstream.RawStreamMsg{Subject: r.Subject, Sequence: meta.Sequence.Stream, Header: m.Header, Data: m.Data, Time: meta.Timestamp}
			if err := out.Send(encodeRawStreamMsg(raw)); err != nil {
				return rpcError(err)
			}
			if meta.Sequence.Stream == end {
				return errReadBoundary
			}
			return nil
		})
	if errors.Is(result, errReadBoundary) {
		return nil
	}
	return result
}
