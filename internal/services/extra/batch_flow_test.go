package extra

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc/codes"
)

type measuredBatchConnection struct {
	*nats.Conn
	requests     int
	dropSequence string
	stallCommit  bool
}

func (c *measuredBatchConnection) PublishMsg(m *nats.Msg) error {
	if m.Header.Get("Nats-Batch-Sequence") == c.dropSequence {
		return nil
	}
	return c.Conn.PublishMsg(m)
}

func (c *measuredBatchConnection) RequestMsgWithContext(ctx context.Context, m *nats.Msg) (*nats.Msg, error) {
	c.requests++
	if c.stallCommit && m.Header.Get("Nats-Batch-Commit") == "1" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c.Conn.RequestMsgWithContext(ctx, m)
}

func preparedFlowBatch(t *testing.T, f *fixture, count int) (address, []*nats.Msg) {
	t.Helper()
	ctx := contextWithTenant(f.ctx, f.tenant)
	var a address
	var messages []*nats.Msg
	id := uuid.NewString()
	for i := range count {
		var err error
		var m *nats.Msg
		a, m, err = f.server.service.prepareBatchMessage(ctx, f.batchRequest(id, i, 0), i)
		requireOK(t, err)
		messages = append(messages, m)
	}
	return a, messages
}

func TestAtomicBatchFlowControlAndNativeIdentity(t *testing.T) {
	for _, flow := range []int{1, 16, 100} {
		t.Run(fmt.Sprint(flow), func(t *testing.T) {
			f := openFixture(t, Config{PublishBatchFlowControl: flow})
			f.createAtomicStream(t, -1)
			a, messages := preparedFlowBatch(t, f, 100)
			conn := &measuredBatchConnection{Conn: f.server.service.nc}
			ack, err := f.server.service.commitBatchOn(f.ctx, a, uuid.NewString(), messages, conn)
			requireOK(t, err)
			want := (98 / flow) + 2 // 首条和最终 commit 各一次，首条之后每 flow 条一次。
			if ack.GetSequence() != 100 || conn.requests != want || f.messageCount(t) != 100 {
				t.Fatalf("sequence=%d requests=%d want=%d", ack.GetSequence(), conn.requests, want)
			}
			puback, err := f.sendBatch(f.ctx, 1, 100)
			requireOK(t, err)
			m, err := f.streams.GetMessage(f.ctx, &pb.GetMessageRequest{Stream: f.streamRef(), Selector: &pb.GetMessageRequest_Sequence{Sequence: 101}})
			requireOK(t, err)
			if id := decodeHeaders(m.Header).Get("Nats-Batch-Id"); id == "" || id != puback.NativeBatchId {
				t.Fatalf("native batch identity mismatch: %q != %q", id, puback.NativeBatchId)
			}
		})
	}
}

func TestAtomicBatchMissingIntermediateAndDeadline(t *testing.T) {
	t.Run("missing_intermediate", func(t *testing.T) {
		f := openFixture(t, Config{})
		f.createAtomicStream(t, -1)
		a, messages := preparedFlowBatch(t, f, 100)
		_, err := f.server.service.commitBatchOn(f.ctx, a, uuid.NewString(), messages, &measuredBatchConnection{Conn: f.server.service.nc, dropSequence: "2"})
		if err == nil || f.messageCount(t) != 0 {
			t.Fatalf("missing intermediate committed a prefix: %v", err)
		}
	})
	for _, parentShorter := range []bool{false, true} {
		t.Run(fmt.Sprint(parentShorter), func(t *testing.T) {
			f := openFixture(t, Config{BatchCommitTimeout: 100 * time.Millisecond, BatchAbandonTimeout: time.Second})
			f.createAtomicStream(t, -1)
			a, messages := preparedFlowBatch(t, f, 3)
			ctx := f.ctx
			if parentShorter {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
			}
			started := time.Now()
			_, err := f.server.service.commitBatchOn(ctx, a, uuid.NewString(), messages, &measuredBatchConnection{Conn: f.server.service.nc, stallCommit: true})
			requireCode(t, err, codes.DeadlineExceeded)
			if time.Since(started) > time.Second || f.messageCount(t) != 0 {
				t.Fatal("commit exceeded budget or stored a prefix")
			}
		})
	}
}

func TestNativeBatchAbandonTimeout(t *testing.T) {
	f := openFixture(t, Config{BatchCommitTimeout: 20 * time.Millisecond, BatchAbandonTimeout: 80 * time.Millisecond})
	f.createAtomicStream(t, -1)
	a, messages := preparedFlowBatch(t, f, 2)
	advisories := make(chan *nats.Msg, 1)
	sub, err := f.server.service.nc.ChanSubscribe("$JS.EVENT.ADVISORY.STREAM.BATCH_ABANDONED."+a.names+"messages", advisories)
	requireOK(t, err)
	defer sub.Unsubscribe()
	requireOK(t, f.server.service.nc.Flush())
	id := uuid.NewString()
	m := messages[0]
	m.Header.Set("Nats-Batch-Id", id)
	m.Header.Set("Nats-Batch-Sequence", "1")
	_, err = f.server.service.nc.RequestMsgWithContext(f.ctx, m)
	requireOK(t, err)
	select {
	case <-advisories:
	case <-time.After(2 * time.Second):
		t.Fatal("configured native abandon timeout did not take effect")
	}
	m = messages[1]
	m.Header.Set("Nats-Batch-Id", id)
	m.Header.Set("Nats-Batch-Sequence", "2")
	m.Header.Set("Nats-Batch-Commit", "1")
	response, err := f.server.service.nc.RequestMsgWithContext(f.ctx, m)
	requireOK(t, err)
	var ack atomicAck
	requireOK(t, json.Unmarshal(response.Data, &ack))
	if ack.Error == nil || f.messageCount(t) != 0 {
		t.Fatal("abandoned batch was committed")
	}
}

func TestBatchInFlightReason(t *testing.T) {
	f := openFixture(t, Config{})
	f.createAtomicStream(t, -1)
	attempt := uuid.NewString()
	ack, err := sendIdentifiedBatch(f.ctx, f.publisher, f.identifiedBatch(attempt, uuid.NewString(), 1, 0))
	requireOK(t, err)
	f.server.service.batchIDs.Store(ack.NativeBatchId, struct{}{})
	defer f.server.service.batchIDs.Delete(ack.NativeBatchId)
	_, err = sendIdentifiedBatch(f.ctx, f.publisher, f.identifiedBatch(attempt, uuid.NewString(), 1, 1))
	requireReason(t, err, codes.Aborted, ReasonBatchInFlight)
}
