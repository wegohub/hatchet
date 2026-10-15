package extra

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	ns "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

func requireReason(t *testing.T, err error, code codes.Code, reason string) {
	t.Helper()
	requireCode(t, err, code)
	if detail := asItemError(err); detail.Reason != reason {
		t.Fatalf("reason=%q want=%q detail=%v", detail.Reason, reason, detail)
	}
}

func TestGetLimitsAndAuthentication(t *testing.T) {
	f := openFixture(t, Config{
		MaxMessageBytes: 1024, MaxFetchMessages: 9, MaxFetchBytes: 8192,
		MaxPublishBatchMessages: 11, MaxPublishBatchBytes: 32768, MaxPublishBatchesInFlight: 5,
		MaxAckBatchMessages: 7, MaxConcurrentUnary: 2, MaxConcurrentStreams: 3, MaxConcurrentReads: 4,
		MaxStoreBytes: 32 << 20, MaxMemoryBytes: 16 << 20,
		BatchCommitTimeout: time.Second, BatchAbandonTimeout: 2 * time.Second, PublishBatchFlowControl: 4, MaxConsumers: 7,
	})
	limits, err := f.streams.GetLimits(f.ctx, &emptypb.Empty{})
	requireOK(t, err)
	want := &pb.ServiceLimits{
		MaxMessageBytes: 1024, MaxFetchMessages: 9, MaxFetchBytes: 8192,
		MaxPublishBatchMessages: 11, MaxPublishBatchBytes: 32768, PublishBatchPerMessageOverhead: 256,
		MaxPublishBatchesInflight: 5, MaxAckBatchMessages: 7, MaxConcurrentUnary: 2,
		MaxConcurrentStreams: 3, MaxConcurrentReads: 4, MaxStoreBytes: 32 << 20, MaxMemoryBytes: 16 << 20,
		BatchCommitTimeoutNs: int64(time.Second), BatchAbandonTimeoutNs: int64(2 * time.Second), PublishBatchFlowControl: 4,
		MaxConsumers: 0, SingleNode: true,
	}
	if !proto.Equal(limits, want) {
		t.Fatalf("limits=%v want=%v", limits, want)
	}
	_, err = f.streams.GetLimits(context.Background(), &emptypb.Empty{})
	requireCode(t, err, codes.Unauthenticated)
}

func TestResourceReasonsAndSingleNodeConstraints(t *testing.T) {
	f := openFixture(t, Config{MaxMessageBytes: 1024})
	f.createAtomicStream(t, 1)
	_, err := f.sendBatch(f.ctx, 2, 0)
	requireReason(t, err, codes.ResourceExhausted, ReasonStoreExhausted)
	_, err = f.publisher.Publish(f.ctx, &pb.PublishRequest{Scope: f.scope, Message: &pb.Message{Subject: "assets.one", Data: make([]byte, 1025)}})
	requireReason(t, err, codes.ResourceExhausted, ReasonMessageTooLarge)
	_, err = f.streams.CreateOrUpdateStream(f.ctx, &pb.StreamConfigRequest{Scope: f.scope, Config: &pb.NatsStreamConfig{Name: ptr("replicated"), Replicas: ptr(int64(3))}})
	requireReason(t, err, codes.FailedPrecondition, ReasonReplicasUnsupported)
	_, err = f.consumers.CreateOrUpdateConsumer(f.ctx, &pb.ConsumerConfigRequest{Stream: f.streamRef(), Config: &pb.NatsConsumerConfig{Durable: ptr("replicated"), Replicas: ptr(int64(3))}})
	requireReason(t, err, codes.FailedPrecondition, ReasonReplicasUnsupported)
	_, err = f.streams.CreateOrUpdateStream(f.ctx, &pb.StreamConfigRequest{Scope: f.scope, Config: &pb.NatsStreamConfig{Name: ptr("oversized"), MaxBytes: ptr(int64(2 << 30))}})
	requireReason(t, err, codes.ResourceExhausted, ReasonStoreExhausted)
	_, err = f.streams.CreateOrUpdateStream(f.ctx, &pb.StreamConfigRequest{Scope: f.scope, Config: &pb.NatsStreamConfig{Name: ptr("invalid"), MaxBytes: ptr(int64(-2))}})
	requireCode(t, err, codes.InvalidArgument)
	for _, test := range []struct {
		err    error
		reason string
	}{
		{nats.ErrMaxPayload, ReasonMessageTooLarge},
		{&jetstream.APIError{Code: 429, ErrorCode: 10210}, ReasonBatchCapacity},
		{&jetstream.APIError{Code: 400, ErrorCode: jetstream.ErrorCode(ns.JSMaximumConsumersLimitErr)}, ReasonConsumerLimit},
		{&jetstream.APIError{Code: 500, ErrorCode: jetstream.ErrorCode(ns.JSStorageResourcesExceededErr)}, ReasonStoreExhausted},
		{&jetstream.APIError{Code: 400, ErrorCode: jetstream.ErrorCode(ns.JSStreamMaxStreamBytesExceeded)}, ReasonStoreExhausted},
	} {
		requireReason(t, rpcError(test.err), codes.ResourceExhausted, test.reason)
	}
}

// admissionStream 只提供拦截器需要的 context，handler 用屏障模拟正在服务的请求。
type admissionStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s admissionStream) Context() context.Context { return s.ctx }

func TestAdmissionReasonsAndReadIsolation(t *testing.T) {
	ctx := context.WithValue(context.Background(), rpcStateKey{}, &rpcState{admitted: true})
	slots, reads := make(chan struct{}, 1), make(chan struct{}, 1)
	admit := streamAdmission(slots, reads)
	stream := admissionStream{ctx: ctx}
	slots <- struct{}{}
	requireReason(t, admit(nil, stream, &grpc.StreamServerInfo{FullMethod: pb.ConsumerService_Fetch_FullMethodName}, func(any, grpc.ServerStream) error { return nil }), codes.ResourceExhausted, ReasonStreamCapacity)
	requireOK(t, admit(nil, stream, &grpc.StreamServerInfo{FullMethod: pb.StreamService_Read_FullMethodName}, func(any, grpc.ServerStream) error { return nil }))
	reads <- struct{}{}
	requireReason(t, admit(nil, stream, &grpc.StreamServerInfo{FullMethod: pb.StreamService_Read_FullMethodName}, func(any, grpc.ServerStream) error { return nil }), codes.ResourceExhausted, ReasonReadCapacity)
	<-slots
	requireOK(t, admit(nil, stream, &grpc.StreamServerInfo{FullMethod: pb.ConsumerService_Fetch_FullMethodName}, func(any, grpc.ServerStream) error { return nil }))
	unary := make(chan struct{}, 1)
	unary <- struct{}{}
	_, err := unaryAdmission(unary)(ctx, nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) { return nil, nil })
	requireReason(t, err, codes.ResourceExhausted, ReasonRPCCapacity)
}

func TestFetchBufferReason(t *testing.T) {
	f := openFixture(t, Config{})
	f.createStream(t)
	f.createConsumer(t, time.Minute)
	f.publish(t, "first", uuid.NewString())
	a, err := addressFor(contextWithTenant(f.ctx, f.tenant), f.scope)
	requireOK(t, err)
	entered := make(chan string, 1)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- f.server.service.pullMessages(f.ctx, a, a.names+"messages", "reader",
			pullRequest{Batch: 2, MaxBytes: 65536, Expires: int64(2 * time.Second)}, 2*time.Second,
			func(m *nats.Msg, _ *nats.MsgMetadata) error { entered <- m.Sub.Subject; <-release; return nil })
	}()
	var inbox string
	select {
	case inbox = <-entered:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	for range 100 {
		requireOK(t, f.server.service.nc.PublishMsg(&nats.Msg{Subject: inbox, Header: nats.Header{"Status": {"100"}}}))
	}
	requireOK(t, f.server.service.nc.Flush())
	processed := make(chan struct{})
	requireOK(t, f.server.service.nc.Barrier(func() { close(processed) }))
	<-processed
	close(release)
	requireReason(t, <-done, codes.ResourceExhausted, ReasonFetchBuffer)
}

func readAll(t *testing.T, f *fixture, r *pb.ReadRequest) []*pb.NatsRawStreamMsg {
	t.Helper()
	stream, err := f.streams.Read(f.ctx, r)
	requireOK(t, err)
	var messages []*pb.NatsRawStreamMsg
	for {
		m, err := stream.Recv()
		if err == io.EOF {
			break
		}
		requireOK(t, err)
		messages = append(messages, m)
	}
	return messages
}

func requireNoReadConsumers(t *testing.T, f *fixture) {
	t.Helper()
	list, err := f.consumers.ListConsumers(f.ctx, f.streamRef())
	requireOK(t, err)
	for {
		info, err := list.Recv()
		if err == io.EOF {
			return
		}
		requireOK(t, err)
		if info.GetName() != "reader" {
			t.Fatalf("Read consumer leaked: %v", info)
		}
	}
}

func TestReadRangeBudgetsAndIsolation(t *testing.T) {
	f := openFixture(t, Config{})
	f.createStream(t)
	for i := range 5 {
		f.publish(t, fmt.Sprintf("%0512d", i), uuid.NewString())
	}
	f.createConsumer(t, time.Minute)
	for _, test := range []struct {
		name       string
		start, end uint64
		count      uint32
		bytes      uint64
		want       int
	}{
		{"closed_interval", 2, 4, 0, 0, 3}, {"snapshot", 0, 0, 0, 0, 5},
		{"count_limit", 0, 0, 2, 0, 2}, {"byte_limit", 0, 0, 0, 1024, 1}, {"empty_budget", 0, 0, 0, 1, 0},
		{"past_tail", 6, 0, 0, 0, 0}, {"finite_future_end", 1, 100, 0, 0, 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			messages := readAll(t, f, &pb.ReadRequest{Stream: f.streamRef(), Subject: "assets.one", StartSequence: test.start, EndSequence: test.end, MaxMessages: test.count, MaxBytes: test.bytes})
			if len(messages) != test.want {
				t.Fatalf("got %d messages want %d", len(messages), test.want)
			}
			for _, m := range messages {
				if m.GetSubject() != "assets.one" || test.start > 0 && m.GetSequence() < test.start || test.end > 0 && m.GetSequence() > test.end {
					t.Fatalf("record outside range: %v", m)
				}
			}
			requireNoReadConsumers(t, f)
		})
	}
	for _, invalidRequest := range []*pb.ReadRequest{
		{Stream: f.streamRef(), Subject: "assets.one", StartSequence: 4, EndSequence: 2},
		{Stream: f.streamRef(), Subject: "assets.one", MaxMessages: 101},
		{Stream: f.streamRef(), Subject: "assets.one", MaxBytes: 5 << 20},
	} {
		call, err := f.streams.Read(f.ctx, invalidRequest)
		requireOK(t, err)
		_, err = call.Recv()
		requireCode(t, err, codes.InvalidArgument)
	}
	foreign := metadata.NewOutgoingContext(f.ctx, metadata.Pairs("authorization", "Bearer "+uuid.NewString()))
	call, err := f.streams.Read(foreign, &pb.ReadRequest{Stream: f.streamRef(), Subject: "assets.one"})
	requireOK(t, err)
	_, err = call.Recv()
	requireCode(t, err, codes.NotFound)
	state, err := f.consumers.GetConsumer(f.ctx, f.consumerRef())
	requireOK(t, err)
	if state.AckFloor.GetStream() != 0 || state.GetNumPending() != 5 || state.GetNumAckPending() != 0 {
		t.Fatalf("Read changed durable state: %v", state)
	}
	if messages := readAll(t, f, &pb.ReadRequest{Stream: f.streamRef(), Subject: "assets.empty"}); len(messages) != 0 {
		t.Fatal("missing subject returned data")
	}
	for _, subject := range []string{"", "assets.*", "assets.>", "x.foreign.scope.assets.one"} {
		stream, err := f.streams.Read(f.ctx, &pb.ReadRequest{Stream: f.streamRef(), Subject: subject})
		requireOK(t, err)
		_, err = stream.Recv()
		if subject == "x.foreign.scope.assets.one" {
			if err != io.EOF {
				t.Fatalf("foreign subject visible: %v", err)
			}
		} else {
			requireCode(t, err, codes.InvalidArgument)
		}
	}
}

// blockingRead 在首条发送处设置屏障，使取消和快照测试不依赖 gRPC 窗口大小或固定等待。
type blockingRead struct {
	grpc.ServerStream
	ctx      context.Context
	entered  chan struct{}
	release  chan struct{}
	messages []*pb.NatsRawStreamMsg
}

func (s *blockingRead) Context() context.Context { return s.ctx }
func (s *blockingRead) Send(m *pb.NatsRawStreamMsg) error {
	if len(s.messages) == 0 {
		close(s.entered)
		select {
		case <-s.release:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
	s.messages = append(s.messages, m)
	return nil
}

func TestReadCancellationAndFrozenTail(t *testing.T) {
	for _, cancelRead := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelRead), func(t *testing.T) {
			f := openFixture(t, Config{})
			f.createStream(t)
			for i := range 3 {
				f.publish(t, fmt.Sprint(i), uuid.NewString())
			}
			ctx, cancel := context.WithCancel(contextWithTenant(f.ctx, f.tenant))
			defer cancel()
			out := &blockingRead{ctx: ctx, entered: make(chan struct{}), release: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				done <- f.server.service.Read(&pb.ReadRequest{Stream: f.streamRef(), Subject: "assets.one"}, out)
			}()
			select {
			case <-out.entered:
			case <-f.ctx.Done():
				t.Fatal(f.ctx.Err())
			}
			if cancelRead {
				cancel()
			} else {
				f.publish(t, "after_snapshot", uuid.NewString())
				close(out.release)
			}
			err := <-done
			if cancelRead {
				requireCode(t, err, codes.Canceled)
			} else {
				requireOK(t, err)
				if len(out.messages) != 3 {
					t.Fatalf("snapshot followed new messages: %d", len(out.messages))
				}
			}
			requireNoReadConsumers(t, f)
		})
	}
}
