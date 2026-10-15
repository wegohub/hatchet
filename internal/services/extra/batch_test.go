package extra

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// createAtomicStream 建立真实原子发布 stream，容量限制由场景提供。
func (f *fixture) createAtomicStream(t *testing.T, maxMessages int64) {
	t.Helper()
	_, err := f.streams.CreateOrUpdateStream(f.ctx, &pb.StreamConfigRequest{
		Scope: f.scope,
		Config: &pb.NatsStreamConfig{
			Name: ptr("messages"), Subjects: []string{"assets.>"},
			AllowAtomicPublish: ptr(true), MaxMsgs: ptr(maxMessages), Discard: ptr(int64(1)),
		},
	})
	requireOK(t, err)
}

// batchRequest 构造只在首条带条件的冻结批次记录。
func (f *fixture) batchRequest(id string, index int, expected uint64) *pb.PublishBatchRequest {
	r := &pb.PublishBatchRequest{
		Scope: f.scope, BatchId: id,
		Message: &pb.Message{Subject: "assets.one", Data: []byte(fmt.Sprintf("message-%d", index))},
	}
	if index == 0 {
		r.Options = &pb.PublishOptions{ExpectedLastSubjectSequence: ptr(expected)}
	}
	return r
}

// sendBatch 通过 TCP gRPC 发送完整批次，不自动重试。
func (f *fixture) sendBatch(ctx context.Context, count int, expected uint64) (*pb.PublishBatchAck, error) {
	stream, err := f.publisher.PublishBatch(ctx)
	if err != nil {
		return nil, err
	}
	id := uuid.NewString()
	for i := range count {
		if err := stream.Send(f.batchRequest(id, i, expected)); err != nil && err != io.EOF {
			return nil, err
		}
	}
	return stream.CloseAndRecv()
}

// messageCount 读取服务端已提交消息数，不能把临时批次缓冲计入。
func (f *fixture) messageCount(t *testing.T) uint64 {
	t.Helper()
	info, err := f.streams.GetStream(f.ctx, &pb.StreamInfoRequest{Stream: f.streamRef()})
	requireOK(t, err)
	return info.State.GetMsgs()
}

// TestAtomicBatchCommitAndCAS 验证 N 条独立消息、顺序、最终确认及提交时条件竞争。
func TestAtomicBatchCommitAndCAS(t *testing.T) {
	f := openFixture(t, Config{})
	f.createAtomicStream(t, -1)
	ack, err := f.sendBatch(f.ctx, 100, 0)
	requireOK(t, err)
	if ack.BatchSize != 100 || ack.Ack.GetSequence() != 100 || ack.Ack.GetStream() != "messages" {
		t.Fatalf("unexpected batch acknowledgement: %v", ack)
	}
	for i := uint64(1); i <= 100; i++ {
		m, err := f.streams.GetMessage(f.ctx, &pb.GetMessageRequest{Stream: f.streamRef(), Selector: &pb.GetMessageRequest_Sequence{Sequence: i}})
		requireOK(t, err)
		if string(m.Data) != fmt.Sprintf("message-%d", i-1) || m.GetSequence() != i {
			t.Fatalf("message %d: %v", i, m)
		}
	}
	_, err = f.sendBatch(f.ctx, 3, 0)
	requireReason(t, err, codes.FailedPrecondition, ReasonPublishPrecondition)
	if count := f.messageCount(t); count != 100 {
		t.Fatalf("failed batch left a committed prefix: %d", count)
	}
	ack, err = f.sendBatch(f.ctx, 1, 100)
	requireOK(t, err)
	if ack.BatchSize != 1 || ack.Ack.GetSequence() != 101 {
		t.Fatalf("single message batch: %v", ack)
	}
}

// TestAtomicBatchCapacityRollback 验证服务器提交失败不留下部分业务消息。
func TestAtomicBatchCapacityRollback(t *testing.T) {
	f := openFixture(t, Config{})
	f.createAtomicStream(t, 2)
	_, err := f.sendBatch(f.ctx, 3, 0)
	if err == nil {
		t.Fatal("over-capacity batch succeeded")
	}
	if count := f.messageCount(t); count != 0 {
		t.Fatalf("capacity rejection left %d messages", count)
	}
}

// TestAtomicBatchConcurrentWriters 验证多个独立网络写者只允许一个条件批次提交。
func TestAtomicBatchConcurrentWriters(t *testing.T) {
	f := openFixture(t, Config{})
	f.createAtomicStream(t, -1)
	start := make(chan struct{})
	errors := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			_, err := f.sendBatch(f.ctx, 3, 0)
			errors <- err
		}()
	}
	close(start)
	success := 0
	for range 2 {
		if err := <-errors; err == nil {
			success++
		} else {
			requireCode(t, err, codes.FailedPrecondition)
		}
	}
	if success != 1 || f.messageCount(t) != 3 {
		t.Fatalf("successes=%d messages=%d", success, f.messageCount(t))
	}
}

// TestAtomicBatchValidationNoPrefix 验证非法后续记录与批次预算在持久发布前被拒绝。
func TestAtomicBatchValidationNoPrefix(t *testing.T) {
	cases := []struct {
		name   string
		config Config
		mutate func(*pb.PublishBatchRequest)
		code   codes.Code
	}{
		{"subject", Config{}, func(r *pb.PublishBatchRequest) { r.Message.Subject = "assets.two" }, codes.InvalidArgument},
		{"scope", Config{}, func(r *pb.PublishBatchRequest) { r.Scope = &pb.Scope{Namespace: "another"} }, codes.InvalidArgument},
		{"batch_id", Config{}, func(r *pb.PublishBatchRequest) { r.BatchId = uuid.NewString() }, codes.InvalidArgument},
		{"late_condition", Config{}, func(r *pb.PublishBatchRequest) {
			r.Options = &pb.PublishOptions{ExpectedLastSubjectSequence: ptr(uint64(0))}
		}, codes.InvalidArgument},
		{"reserved_header", Config{}, func(r *pb.PublishBatchRequest) {
			r.Message.Headers = map[string]*pb.StringValues{"Nats-Batch-Commit": {Values: []string{"1"}}}
		}, codes.InvalidArgument},
		{"schedule", Config{}, func(r *pb.PublishBatchRequest) { r.Options = &pb.PublishOptions{Schedule: &pb.ScheduleOptions{}} }, codes.InvalidArgument},
		{"count", Config{MaxPublishBatchMessages: 1}, func(*pb.PublishBatchRequest) {}, codes.ResourceExhausted},
		{"bytes", Config{MaxMessageBytes: 128, MaxPublishBatchBytes: 500}, func(*pb.PublishBatchRequest) {}, codes.ResourceExhausted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := openFixture(t, tc.config)
			f.createAtomicStream(t, -1)
			stream, err := f.publisher.PublishBatch(f.ctx)
			requireOK(t, err)
			id := uuid.NewString()
			requireOK(t, stream.Send(f.batchRequest(id, 0, 0)))
			if f.messageCount(t) != 0 {
				t.Fatal("unclosed batch was published")
			}
			r := f.batchRequest(id, 1, 0)
			tc.mutate(r)
			if err := stream.Send(r); err != nil && err != io.EOF {
				t.Fatal(err)
			}
			_, err = stream.CloseAndRecv()
			requireCode(t, err, tc.code)
			if tc.name == "count" {
				requireReason(t, err, tc.code, ReasonBatchMessageLimit)
			}
			if tc.name == "bytes" {
				requireReason(t, err, tc.code, ReasonBatchByteLimit)
			}
			if f.messageCount(t) != 0 {
				t.Fatal("invalid batch published a prefix")
			}
		})
	}
}

// TestAtomicBatchCancellationAndAdmission 验证缓冲名额在取消后释放，不影响后续合法批次。
func TestAtomicBatchCancellationAndAdmission(t *testing.T) {
	f := openFixture(t, Config{MaxPublishBatchesInFlight: 1})
	f.createAtomicStream(t, -1)
	ctx, cancel := context.WithCancel(f.ctx)
	stream, err := f.publisher.PublishBatch(ctx)
	requireOK(t, err)
	requireOK(t, stream.Send(f.batchRequest(uuid.NewString(), 0, 0)))
	waitBatchSlots(t, f, 1)
	_, err = f.sendBatch(f.ctx, 1, 0)
	requireReason(t, err, codes.ResourceExhausted, ReasonBatchCapacity)
	cancel()
	_, err = stream.CloseAndRecv()
	requireCode(t, err, codes.Canceled)
	waitBatchSlots(t, f, 0)
	if f.messageCount(t) != 0 {
		t.Fatal("cancelled receive committed data")
	}
	_, err = f.sendBatch(f.ctx, 1, 0)
	requireOK(t, err)
}

// waitBatchSlots 等待真实 handler 的准入或回收，避免用固定延时推测网络状态。
func waitBatchSlots(t *testing.T, f *fixture, count int) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for len(f.server.service.batchSlots) != count {
		select {
		case <-timer.C:
			t.Fatalf("batch slots did not reach %d", count)
		case <-ticker.C:
		}
	}
}

// TestAckBatchPartialResults 验证跨租户 token 拒绝、逐项结果与未被隐式确认的缺口。
func TestAckBatchPartialResults(t *testing.T) {
	f := openFixture(t, Config{})
	f.createStream(t)
	f.createConsumer(t, time.Minute)
	for i := range 3 {
		f.publish(t, fmt.Sprint(i), fmt.Sprint(i))
	}
	deliveries := f.fetch(t, 3, true)
	if len(deliveries) != 3 {
		t.Fatalf("deliveries=%d", len(deliveries))
	}
	foreign := metadata.NewOutgoingContext(f.ctx, metadata.Pairs("authorization", "Bearer "+uuid.NewString()))
	denied, err := f.consumers.AckBatch(foreign, &pb.AckBatchRequest{Items: []*pb.AckRequest{{AckToken: deliveries[0].AckToken}}})
	requireOK(t, err)
	if denied.Results[0].Error.GetGrpcCode() != int32(codes.PermissionDenied) {
		t.Fatal("cross-tenant ACK token accepted")
	}
	items := []*pb.AckRequest{
		{AckToken: deliveries[0].AckToken, Confirm: true},
		{AckToken: []byte("invalid")},
		{AckToken: deliveries[2].AckToken, Confirm: true},
	}
	response, err := f.consumers.AckBatch(f.ctx, &pb.AckBatchRequest{Items: items})
	requireOK(t, err)
	if len(response.Results) != 3 {
		t.Fatalf("results=%d", len(response.Results))
	}
	for i, r := range response.Results {
		if r.Index != uint32(i) {
			t.Fatal("ACK indexes do not match input")
		}
		if i == 1 {
			if r.Error.GetGrpcCode() != int32(codes.PermissionDenied) {
				t.Fatalf("invalid result: %v", r)
			}
		} else if r.Error != nil {
			t.Fatalf("valid ACK failed: %v", r)
		}
	}
	info, err := f.consumers.GetConsumer(f.ctx, f.consumerRef())
	requireOK(t, err)
	if info.GetNumAckPending() != 1 {
		t.Fatalf("unconfirmed gap=%d", info.GetNumAckPending())
	}
	_, err = f.consumers.AckBatch(f.ctx, &pb.AckBatchRequest{Items: []*pb.AckRequest{{AckToken: deliveries[1].AckToken, Confirm: true}}})
	requireOK(t, err)
	info, err = f.consumers.GetConsumer(f.ctx, f.consumerRef())
	requireOK(t, err)
	if info.GetNumAckPending() != 0 {
		t.Fatalf("pending=%d", info.GetNumAckPending())
	}
}

// TestBatchRequestLimits 验证空批次、重复消息 ID 与 ACK 请求上限不能产生副作用。
func TestBatchRequestLimits(t *testing.T) {
	f := openFixture(t, Config{MaxAckBatchMessages: 1})
	f.createAtomicStream(t, -1)
	empty, err := f.publisher.PublishBatch(f.ctx)
	requireOK(t, err)
	_, err = empty.CloseAndRecv()
	requireCode(t, err, codes.InvalidArgument)
	stream, err := f.publisher.PublishBatch(f.ctx)
	requireOK(t, err)
	id := uuid.NewString()
	for i := range 2 {
		r := f.batchRequest(id, i, 0)
		if r.Options == nil {
			r.Options = &pb.PublishOptions{}
		}
		r.Options.MessageId = ptr("same-message")
		if err := stream.Send(r); err != nil && err != io.EOF {
			t.Fatal(err)
		}
	}
	_, err = stream.CloseAndRecv()
	requireCode(t, err, codes.InvalidArgument)
	if f.messageCount(t) != 0 {
		t.Fatal("duplicate IDs committed data")
	}
	_, err = f.consumers.AckBatch(f.ctx, &pb.AckBatchRequest{})
	requireCode(t, err, codes.InvalidArgument)
	_, err = f.consumers.AckBatch(f.ctx, &pb.AckBatchRequest{Items: []*pb.AckRequest{{}, {}}})
	requireReason(t, err, codes.ResourceExhausted, ReasonAckBatchLimit)
}

// TestHistoryConsumerIsolation 验证 AckNone 临时扫描不推进正常 durable consumer 的消费位置。
func TestHistoryConsumerIsolation(t *testing.T) {
	f := openFixture(t, Config{})
	f.createAtomicStream(t, -1)
	_, err := f.sendBatch(f.ctx, 4, 0)
	requireOK(t, err)
	f.createConsumer(t, time.Minute)
	_, err = f.consumers.CreateOrUpdateConsumer(f.ctx, &pb.ConsumerConfigRequest{
		Stream: f.streamRef(), CreateOnly: true,
		Config: &pb.NatsConsumerConfig{Durable: ptr("history"), FilterSubject: ptr("assets.one"), AckPolicy: ptr(int64(1)), InactiveThreshold: ptr(int64(time.Minute))},
	})
	requireOK(t, err)
	historyRef := &pb.ConsumerRef{Stream: f.streamRef(), Consumer: "history"}
	history, err := f.consumers.Fetch(f.ctx, &pb.FetchRequest{Consumer: historyRef, Batch: &pb.FetchRequest_MaxMessages{MaxMessages: 4}, MaxBytesLimit: ptr(uint64(4096)), NoWait: true})
	requireOK(t, err)
	count := 0
	for {
		_, err := history.Recv()
		if err == io.EOF {
			break
		}
		requireOK(t, err)
		count++
	}
	if count != 4 {
		t.Fatalf("history count=%d", count)
	}
	normal, err := f.consumers.GetConsumer(f.ctx, f.consumerRef())
	requireOK(t, err)
	if normal.GetNumPending() != 4 || normal.Delivered.GetStream() != 0 {
		t.Fatal("history advanced normal consumer")
	}
	_, err = f.consumers.DeleteConsumer(f.ctx, historyRef)
	requireOK(t, err)
}

// TestCreateOnlyConcurrentInitialization 验证同配置竞争也只有一个创建成功，且不覆盖资源配置。
func TestCreateOnlyConcurrentInitialization(t *testing.T) {
	f := openFixture(t, Config{})
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			_, err := f.streams.CreateOrUpdateStream(f.ctx, &pb.StreamConfigRequest{Scope: f.scope, CreateOnly: true, Config: &pb.NatsStreamConfig{Name: ptr("messages"), Subjects: []string{"assets.>"}, MaxAge: ptr(int64(time.Hour))}})
			errors <- err
		})
	}
	wg.Wait()
	close(errors)
	success := 0
	for err := range errors {
		if err == nil {
			success++
		} else {
			requireCode(t, err, codes.AlreadyExists)
		}
	}
	if success != 1 {
		t.Fatalf("creators=%d", success)
	}
	_, err := f.streams.CreateOrUpdateStream(f.ctx, &pb.StreamConfigRequest{Scope: f.scope, CreateOnly: true, Config: &pb.NatsStreamConfig{Name: ptr("messages"), Subjects: []string{"assets.>"}, MaxAge: ptr(int64(2 * time.Hour))}})
	requireCode(t, err, codes.AlreadyExists)
	info, err := f.streams.GetStream(f.ctx, &pb.StreamInfoRequest{Stream: f.streamRef()})
	requireOK(t, err)
	if info.Config.GetMaxAge() != int64(time.Hour) {
		t.Fatal("create-only updated stream")
	}
	for i := range 2 {
		_, err = f.consumers.CreateOrUpdateConsumer(f.ctx, &pb.ConsumerConfigRequest{Stream: f.streamRef(), CreateOnly: true, Config: &pb.NatsConsumerConfig{Durable: ptr("reader"), FilterSubject: ptr("assets.>"), AckWait: ptr(int64(time.Minute))}})
		if i == 0 {
			requireOK(t, err)
		} else {
			requireCode(t, err, codes.AlreadyExists)
		}
		_, err = f.kv.CreateOrUpdateBucket(f.ctx, &pb.KeyValueConfigRequest{Scope: f.scope, CreateOnly: true, Config: &pb.NatsKeyValueConfig{Bucket: ptr("state"), History: ptr(uint32(2))}})
		if i == 0 {
			requireOK(t, err)
		} else {
			requireCode(t, err, codes.AlreadyExists)
		}
	}
}
