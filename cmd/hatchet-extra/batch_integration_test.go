//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// verifyBatchRPCs 在已部署 TLS 服务上验证原子发布、逐项 ACK 与独立历史消费者。
func verifyBatchRPCs(t *testing.T, conn *grpc.ClientConn, ctx context.Context) {
	t.Helper()
	scope := &pb.Scope{Namespace: "batch-" + uuid.NewString()[:12]}
	s := pb.NewStreamServiceClient(conn)
	p := pb.NewPublisherServiceClient(conn)
	c := pb.NewConsumerServiceClient(conn)
	kv := pb.NewKeyValueServiceClient(conn)
	sr := &pb.StreamRef{Scope: scope, Stream: "messages"}
	limits, err := s.GetLimits(ctx, &emptypb.Empty{})
	check(t, err)
	if !limits.SingleNode || limits.PublishBatchPerMessageOverhead != 256 || limits.MaxConcurrentUnary != 128 || limits.MaxConcurrentStreams != 1024 || limits.MaxConcurrentReads != 64 || limits.MaxPublishBatchesInflight != 32 || limits.BatchCommitTimeoutNs != int64(8*time.Second) || limits.BatchAbandonTimeoutNs != int64(10*time.Second) || limits.PublishBatchFlowControl != 16 || limits.MaxConsumers != 0 {
		t.Fatalf("service limits: %v", limits)
	}
	create := &pb.StreamConfigRequest{Scope: scope, CreateOnly: true, Config: &pb.NatsStreamConfig{
		Name: point("messages"), Subjects: []string{"rpc_test_client"}, AllowAtomicPublish: point(true),
	}}
	_, err = s.CreateOrUpdateStream(ctx, create)
	check(t, err)
	defer func() { _, e := s.DeleteStream(ctx, sr); check(t, e) }()
	_, err = s.CreateOrUpdateStream(ctx, create)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("stream create-only: %v", err)
	}
	call, err := p.PublishBatch(ctx)
	check(t, err)
	batchID := uuid.NewString()
	logicalBatchID := uuid.NewString()
	for i := range 100 {
		r := &pb.PublishBatchRequest{Scope: scope, BatchId: batchID,
			Message: &pb.Message{Subject: "rpc_test_client", Data: []byte(fmt.Sprint(i))},
			Options: &pb.PublishOptions{MessageId: point(fmt.Sprintf("%s.100.%d", logicalBatchID, i))}}
		if i == 0 {
			r.Options.ExpectedLastSubjectSequence = point(uint64(0))
		}
		check(t, call.Send(r))
	}
	ack, err := call.CloseAndRecv()
	check(t, err)
	if ack.BatchId != batchID || ack.BatchSize != 100 || ack.Ack.GetSequence() != 100 {
		t.Fatalf("batch ack: %v", ack)
	}
	stored, err := s.GetMessage(ctx, &pb.GetMessageRequest{Stream: sr, Selector: &pb.GetMessageRequest_Sequence{Sequence: 100}})
	check(t, err)
	if values := stored.Header["Nats-Batch-Id"].GetValues(); len(values) != 1 || values[0] != ack.NativeBatchId {
		t.Fatal("native batch identity not returned in acknowledgement")
	}
	// SDK 无需知道物理前缀，单次首条查询即可从稳定 Msg-Id 核对并还原批尾。
	first, err := s.GetMessage(ctx, &pb.GetMessageRequest{Stream: sr,
		Selector: &pb.GetMessageRequest_Sequence{Sequence: 1}, Subject: point("rpc_test_client")})
	check(t, err)
	values := first.Header["Nats-Msg-Id"].GetValues()
	if len(values) != 1 || values[0] != logicalBatchID+".100.0" || first.GetSequence()+99 != ack.Ack.GetSequence() {
		t.Fatalf("batch reconciliation failed: %v", first)
	}
	failed, err := p.PublishBatch(ctx)
	check(t, err)
	check(t, failed.Send(&pb.PublishBatchRequest{Scope: scope, BatchId: uuid.NewString(), Message: &pb.Message{Subject: "rpc_test_client", Data: []byte("rejected")}, Options: &pb.PublishOptions{ExpectedLastSubjectSequence: point(uint64(0))}}))
	_, err = failed.CloseAndRecv()
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("conditional conflict: %v", err)
	}
	info, err := s.GetStream(ctx, &pb.StreamInfoRequest{Stream: sr})
	check(t, err)
	if info.State.GetMsgs() != 100 {
		t.Fatal("failed atomic batch changed message count")
	}

	cr := &pb.ConsumerRef{Stream: sr, Consumer: "reader"}
	consumer := &pb.ConsumerConfigRequest{Stream: sr, CreateOnly: true, Config: &pb.NatsConsumerConfig{Durable: point("reader"), FilterSubject: point("rpc_test_client"), AckWait: point(int64(time.Minute))}}
	_, err = c.CreateOrUpdateConsumer(ctx, consumer)
	check(t, err)
	_, err = c.CreateOrUpdateConsumer(ctx, consumer)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("consumer create-only: %v", err)
	}
	fetch, err := c.Fetch(ctx, &pb.FetchRequest{Consumer: cr, Batch: &pb.FetchRequest_MaxMessages{MaxMessages: 100}, MaxBytesLimit: point(uint64(4 << 20)), NoWait: true})
	check(t, err)
	items := make([]*pb.AckRequest, 0, 100)
	for i := range 100 {
		delivery, err := fetch.Recv()
		check(t, err)
		if string(delivery.Message.Data) != fmt.Sprint(i) {
			t.Fatalf("batch ordering at %d", i)
		}
		items = append(items, &pb.AckRequest{AckToken: delivery.AckToken})
	}
	_, err = fetch.Recv()
	end(t, err)
	confirmed, err := c.AckBatch(ctx, &pb.AckBatchRequest{Items: items})
	check(t, err)
	if len(confirmed.Results) != 100 {
		t.Fatalf("ACK results=%d", len(confirmed.Results))
	}
	for i, result := range confirmed.Results {
		if result.Index != uint32(i) || result.Error != nil {
			t.Fatalf("ACK result: %v", result)
		}
	}
	partial, err := c.AckBatch(ctx, &pb.AckBatchRequest{Items: []*pb.AckRequest{{AckToken: []byte("invalid")}, items[0]}})
	check(t, err)
	if partial.Results[0].Error.GetGrpcCode() != int32(codes.PermissionDenied) || partial.Results[1].Error != nil {
		t.Fatalf("partial ACK: %v", partial)
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := c.GetConsumer(ctx, cr)
		check(t, err)
		if state.GetNumAckPending() == 0 && state.AckFloor.GetStream() >= 100 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}

	// 历史消费不修改正常 consumer，AckNone 读取无需签发可确认的投递 token。
	history := &pb.ConsumerRef{Stream: sr, Consumer: "history"}
	_, err = c.CreateOrUpdateConsumer(ctx, &pb.ConsumerConfigRequest{Stream: sr, CreateOnly: true, Config: &pb.NatsConsumerConfig{Durable: point("history"), FilterSubject: point("rpc_test_client"), AckPolicy: point(int64(jetstream.AckNonePolicy)), InactiveThreshold: point(int64(time.Minute))}})
	check(t, err)
	historical, err := c.Fetch(ctx, &pb.FetchRequest{Consumer: history, Batch: &pb.FetchRequest_MaxMessages{MaxMessages: 100}, NoWait: true})
	check(t, err)
	count := 0
	for {
		_, err := historical.Recv()
		if err == io.EOF {
			break
		}
		check(t, err)
		count++
	}
	if count != 100 {
		t.Fatalf("history=%d", count)
	}
	_, err = c.DeleteConsumer(ctx, history)
	check(t, err)
	read, err := s.Read(ctx, &pb.ReadRequest{Stream: sr, Subject: "rpc_test_client", StartSequence: 2, EndSequence: 4})
	check(t, err)
	for sequence := uint64(2); sequence <= 4; sequence++ {
		message, err := read.Recv()
		check(t, err)
		if message.GetSequence() != sequence || message.GetSubject() != "rpc_test_client" {
			t.Fatalf("Read record: %v", message)
		}
	}
	_, err = read.Recv()
	end(t, err)
	consumerList, err := c.ListConsumers(ctx, sr)
	check(t, err)
	for {
		info, err := consumerList.Recv()
		if err == io.EOF {
			break
		}
		check(t, err)
		if info.GetName() != "reader" {
			t.Fatalf("Read leaked a consumer: %v", info)
		}
	}
	state, err := c.GetConsumer(ctx, cr)
	check(t, err)
	if state.GetNumPending() != 0 || state.GetNumAckPending() != 0 {
		t.Fatal("history changed normal consumer progress")
	}

	br := &pb.BucketRef{Scope: scope, Bucket: "state"}
	bucket := &pb.KeyValueConfigRequest{Scope: scope, CreateOnly: true, Config: &pb.NatsKeyValueConfig{Bucket: point("state")}}
	_, err = kv.CreateOrUpdateBucket(ctx, bucket)
	check(t, err)
	defer func() { _, e := kv.DeleteBucket(ctx, br); check(t, e) }()
	_, err = kv.CreateOrUpdateBucket(ctx, bucket)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("bucket create-only: %v", err)
	}
}
