package extra

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// lostBatchAck 模拟提交完成后最终确认无法交给调用方，不改变服务端提交行为。
type lostBatchAck struct{ grpc.ClientStream }

func (s lostBatchAck) RecvMsg(v any) error {
	if err := s.ClientStream.RecvMsg(v); err != nil {
		return err
	}
	return status.Error(codes.Unavailable, "final batch acknowledgement lost")
}

// identifiedBatch 给同一冻结批次的每条记录分配跨网络尝试稳定的消息 ID。
func (f *fixture) identifiedBatch(attempt, identity string, count int, expected uint64) []*pb.PublishBatchRequest {
	requests := make([]*pb.PublishBatchRequest, count)
	for i := range count {
		r := f.batchRequest(attempt, i, expected)
		if r.Options == nil {
			r.Options = &pb.PublishOptions{}
		}
		r.Options.MessageId = ptr(fmt.Sprintf("%s.%d.%d", identity, count, i))
		requests[i] = r
	}
	return requests
}

func sendIdentifiedBatch(ctx context.Context, publisher pb.PublisherServiceClient, requests []*pb.PublishBatchRequest) (*pb.PublishBatchAck, error) {
	call, err := publisher.PublishBatch(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range requests {
		if err := call.Send(r); err != nil && err != io.EOF {
			return nil, err
		}
	}
	return call.CloseAndRecv()
}

// nextBatchRecord 只使用公开的逻辑 subject 和持久位置，既不计算物理前缀也不创建 consumer。
func (f *fixture) nextBatchRecord(expected uint64) (*pb.NatsRawStreamMsg, error) {
	return f.streams.GetMessage(f.ctx, &pb.GetMessageRequest{
		Stream: f.streamRef(), Selector: &pb.GetMessageRequest_Sequence{Sequence: expected + 1},
		Subject: ptr("assets.one"),
	})
}

func TestAtomicBatchReconcileLostAck(t *testing.T) {
	f := openFixture(t, Config{})
	f.createAtomicStream(t, -1)
	// 同一 Stream 中其他 subject 的消息使第一条批内序号不等于 expected+1。
	_, err := f.publisher.Publish(f.ctx, &pb.PublishRequest{Scope: f.scope, Message: &pb.Message{Subject: "assets.other"}})
	requireOK(t, err)
	conn, err := grpc.NewClient(f.listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			call, err := streamer(ctx, desc, cc, method, opts...)
			return lostBatchAck{call}, err
		}))
	requireOK(t, err)
	defer func() { requireOK(t, conn.Close()) }()
	identity := uuid.NewString()
	requests := f.identifiedBatch(uuid.NewString(), identity, 100, 0)
	ack, err := sendIdentifiedBatch(f.ctx, pb.NewPublisherServiceClient(conn), requests)
	requireCode(t, err, codes.Unavailable)
	if ack != nil {
		t.Fatal("unknown commit returned a usable acknowledgement")
	}
	_, err = f.publisher.Publish(f.ctx, &pb.PublishRequest{Scope: f.scope, Message: &pb.Message{Subject: "assets.other"}})
	requireOK(t, err)
	first, err := f.nextBatchRecord(0)
	requireOK(t, err)
	headers := decodeHeaders(first.Header)
	if first.GetSequence() != 2 || headers.Get("Nats-Msg-Id") != requests[0].Options.GetMessageId() || headers.Get("Nats-Batch-Sequence") != "1" {
		t.Fatalf("cannot identify committed first record: %v", first)
	}
	lastSequence := first.GetSequence() + uint64(len(requests)) - 1
	// 断言批次内全局序号连续，并保留每条消息的稳定 ID；算法本身只需读首条。
	for i, r := range requests {
		m, err := f.streams.GetMessage(f.ctx, &pb.GetMessageRequest{Stream: f.streamRef(), Selector: &pb.GetMessageRequest_Sequence{Sequence: first.GetSequence() + uint64(i)}})
		requireOK(t, err)
		h := decodeHeaders(m.Header)
		if h.Get("Nats-Msg-Id") != r.Options.GetMessageId() || h.Get("Nats-Batch-Sequence") != strconv.Itoa(i+1) || string(m.Data) != string(r.Message.Data) || h.Get("Nats-Batch-Id") != headers.Get("Nats-Batch-Id") {
			t.Fatalf("batch record %d has inconsistent identity or position: %v", i, m)
		}
		if i == len(requests)-1 && h.Get("Nats-Batch-Commit") != "1" {
			t.Fatal("last record has no commit marker")
		}
	}
	_, err = f.nextBatchRecord(lastSequence)
	requireCode(t, err, codes.NotFound)
	// 同一消息身份的新会话可能返回去重错误，仍必须读回，而非当竞争失败退出。
	_, err = sendIdentifiedBatch(f.ctx, f.publisher, f.identifiedBatch(uuid.NewString(), identity, 100, 0))
	if err == nil {
		t.Fatal("repeated batch unexpectedly committed")
	}
	firstAgain, err := f.nextBatchRecord(0)
	requireOK(t, err)
	if decodeHeaders(firstAgain.Header).Get("Nats-Msg-Id") != requests[0].Options.GetMessageId() || f.messageCount(t) != 102 {
		t.Fatal("reconciliation failed after conditional or duplicate rejection")
	}
}

// TestAtomicBatchRetryWithStagedAttempt 在旧原生会话仍存在时发起新会话，不查询或等待放弃状态。
func TestAtomicBatchRetryWithStagedAttempt(t *testing.T) {
	for _, winner := range []string{"prior", "retry"} {
		t.Run(winner, func(t *testing.T) {
			f := openFixture(t, Config{})
			f.createAtomicStream(t, -1)
			identity := uuid.NewString()
			requests := f.identifiedBatch(uuid.NewString(), identity, 3, 0)
			ctx := contextWithTenant(f.ctx, f.tenant)
			// 故障注入只负责暂停旧会话；结果核对仍完全通过公开 RPC。
			priorID := uuid.NewString()
			var finalResponse func() atomicAck
			for i, r := range requests {
				_, m, err := f.server.service.prepareBatchMessage(ctx, r, i)
				requireOK(t, err)
				m.Header.Set("Nats-Required-Api-Level", "2")
				m.Header.Set("Nats-Batch-Id", priorID)
				m.Header.Set("Nats-Batch-Sequence", strconv.Itoa(i+1))
				if i == len(requests)-1 {
					m.Header.Set("Nats-Batch-Commit", "1")
					finalResponse = func() atomicAck {
						response, err := f.server.service.nc.RequestMsgWithContext(ctx, m)
						requireOK(t, err)
						var ack atomicAck
						requireOK(t, json.Unmarshal(response.Data, &ack))
						return ack
					}
					break
				}
				response, err := f.server.service.nc.RequestMsgWithContext(ctx, m)
				requireOK(t, err)
				if len(response.Data) != 0 {
					t.Fatalf("unexpected staging response: %s", response.Data)
				}
			}
			_, err := f.nextBatchRecord(0)
			requireCode(t, err, codes.NotFound)
			if code := asItemError(err).GetNatsErrorCode(); code != 10037 {
				t.Fatalf("message not found has code %d", code)
			}
			if winner == "prior" {
				if ack := finalResponse(); ack.Error != nil || ack.Sequence != 3 {
					t.Fatalf("prior commit failed: %v", ack)
				}
			}
			_, retryErr := sendIdentifiedBatch(f.ctx, f.publisher, requests)
			if winner == "retry" {
				requireOK(t, retryErr)
				if ack := finalResponse(); ack.Error == nil {
					t.Fatal("staged prior attempt also committed")
				}
			} else if retryErr == nil {
				t.Fatal("retry also committed after prior attempt won")
			}
			first, err := f.nextBatchRecord(0)
			requireOK(t, err)
			if decodeHeaders(first.Header).Get("Nats-Msg-Id") != requests[0].Options.GetMessageId() || f.messageCount(t) != 3 {
				t.Fatal("could not reconcile exactly one committed batch")
			}
		})
	}
}

func TestAtomicBatchDuplicateMessageIDRollsBackPrefix(t *testing.T) {
	f := openFixture(t, Config{})
	f.createAtomicStream(t, -1)
	duplicateID := uuid.NewString()
	_, err := f.publisher.Publish(f.ctx, &pb.PublishRequest{Scope: f.scope,
		Message: &pb.Message{Subject: "assets.other"}, Options: &pb.PublishOptions{MessageId: ptr(duplicateID)}})
	requireOK(t, err)
	requests := f.identifiedBatch(uuid.NewString(), uuid.NewString(), 3, 0)
	requests[1].Options.MessageId = ptr(duplicateID)
	_, err = sendIdentifiedBatch(f.ctx, f.publisher, requests)
	if code := asItemError(err).GetNatsErrorCode(); code != 10201 {
		t.Fatalf("duplicate batch rejection: code=%d error=%v", code, err)
	}
	_, err = f.nextBatchRecord(0)
	requireCode(t, err, codes.NotFound)
	if f.messageCount(t) != 1 {
		t.Fatal("duplicate rejection left a committed prefix")
	}
}

func TestAtomicBatchReconcileCompetingWriter(t *testing.T) {
	f := openFixture(t, Config{})
	f.createAtomicStream(t, -1)
	expected := f.publish(t, "baseline", uuid.NewString()).GetSequence()
	identity := uuid.NewString()
	requests := f.identifiedBatch(uuid.NewString(), identity, 3, expected)
	_, err := f.nextBatchRecord(expected)
	requireCode(t, err, codes.NotFound)
	competitor := f.identifiedBatch(uuid.NewString(), uuid.NewString(), 2, expected)
	ack, err := sendIdentifiedBatch(f.ctx, f.publisher, competitor)
	requireOK(t, err)
	_, err = sendIdentifiedBatch(f.ctx, f.publisher, requests)
	requireCode(t, err, codes.FailedPrecondition)
	// 竞争者后续又追加，核对目标仍是 E 后第一条，而非最新尾部。
	_, err = sendIdentifiedBatch(f.ctx, f.publisher, f.identifiedBatch(uuid.NewString(), uuid.NewString(), 1, ack.Ack.GetSequence()))
	requireOK(t, err)
	first, err := f.nextBatchRecord(expected)
	requireOK(t, err)
	messageID := decodeHeaders(first.Header).Get("Nats-Msg-Id")
	if messageID != competitor[0].Options.GetMessageId() || messageID == requests[0].Options.GetMessageId() || f.messageCount(t) != 4 {
		t.Fatal("first record did not identify the competing writer")
	}
}
