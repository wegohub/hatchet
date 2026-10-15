package extra

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// atomicAck 保留原生最终确认的批次身份；空的中间确认不能替代最终确认。
type atomicAck struct {
	Stream    string              `json:"stream"`
	Sequence  uint64              `json:"seq"`
	Domain    string              `json:"domain"`
	Duplicate bool                `json:"duplicate"`
	BatchID   string              `json:"batch"`
	Count     uint64              `json:"count"`
	Error     *jetstream.APIError `json:"error"`
}

// prepareBatchMessage 只允许原子批次支持的 typed options，拒绝单条发布的重试与调度语义。
func (s *Service) prepareBatchMessage(ctx context.Context, r *pb.PublishBatchRequest, index int) (address, *nats.Msg, error) {
	if r == nil || !validName(r.BatchId) {
		return address{}, nil, invalid("batch ID must be 1..64 ASCII letters, digits, underscores or hyphens")
	}
	o := r.Options
	if o != nil && (o.ExpectedLastMessageId != nil || o.ExpectedLastSequence != nil ||
		o.ExpectedLastSubject != nil || o.MessageTtlNs != nil || o.RetryAttempts != nil ||
		o.RetryWaitNs != nil || o.StallWaitNs != nil || o.Schedule != nil ||
		index > 0 && o.ExpectedLastSubjectSequence != nil) {
		return address{}, nil, invalid("unsupported atomic batch publish options")
	}
	a, m, _, err := s.preparePublish(ctx, &pb.PublishRequest{Scope: r.Scope, Message: r.Message, Options: o})
	if err != nil {
		return a, nil, err
	}
	if m.Header == nil {
		m.Header = make(nats.Header)
	}
	if o != nil {
		if o.MessageId != nil {
			m.Header.Set("Nats-Msg-Id", *o.MessageId)
		}
		if o.ExpectedStream != nil {
			stream, _ := a.name(*o.ExpectedStream)
			m.Header.Set("Nats-Expected-Stream", stream)
		}
		if o.ExpectedLastSubjectSequence != nil {
			m.Header.Set("Nats-Expected-Last-Subject-Sequence", strconv.FormatUint(*o.ExpectedLastSubjectSequence, 10))
		}
	}
	return a, m, nil
}

// PublishBatch 在完整请求通过校验后才访问 NATS，断流或非法后续项不会留下已提交前缀。
func (s *Service) PublishBatch(out grpc.ClientStreamingServer[pb.PublishBatchRequest, pb.PublishBatchAck]) error {
	select {
	case s.batchSlots <- struct{}{}:
		defer func() { <-s.batchSlots }()
	default:
		return exhausted(ReasonBatchCapacity, "atomic batch capacity exhausted")
	}
	ctx := out.Context()
	var messages []*nats.Msg
	var scope address
	var id string
	var size int
	seenIDs := make(map[string]struct{})
	for {
		r, err := out.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return rpcError(err)
		}
		if len(messages) >= s.cfg.MaxPublishBatchMessages {
			return exhausted(ReasonBatchMessageLimit, "atomic batch message limit exceeded")
		}
		a, m, err := s.prepareBatchMessage(ctx, r, len(messages))
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			scope, id = a, r.BatchId
		} else if a != scope || r.BatchId != id || m.Subject != messages[0].Subject {
			return invalid("atomic batch requires one scope, batch ID and subject")
		}
		if msgID := m.Header.Get("Nats-Msg-Id"); msgID != "" {
			if _, exists := seenIDs[msgID]; exists {
				return invalid("duplicate message ID within atomic batch")
			}
			seenIDs[msgID] = struct{}{}
		}
		// 预留原生批次控制头与回复地址的编码开销。
		size += m.Size() + PublishBatchPerMessageOverhead
		if size > s.cfg.MaxPublishBatchBytes {
			return exhausted(ReasonBatchByteLimit, "atomic batch byte limit exceeded")
		}
		messages = append(messages, m)
	}
	if len(messages) == 0 {
		return invalid("atomic batch requires at least one message")
	}
	// 原生批次 ID 在共享 stream 内唯一，不能被不同租户或 namespace 的同名 ID 混用。
	digest := sha256.Sum256([]byte(scope.names + id))
	nativeID := hex.EncodeToString(digest[:])
	if _, exists := s.batchIDs.LoadOrStore(nativeID, struct{}{}); exists {
		return reasonError(codes.Aborted, ReasonBatchInFlight, "atomic batch already in flight")
	}
	defer s.batchIDs.Delete(nativeID)
	ack, err := s.commitBatch(ctx, scope, nativeID, messages)
	if err != nil {
		return err
	}
	return out.SendAndClose(&pb.PublishBatchAck{
		BatchId: id, NativeBatchId: nativeID,
		// #nosec G115 -- MaxPublishBatchMessages 不超过 1000。
		BatchSize: uint32(len(messages)),
		Ack:       ack,
	})
}

// commitBatch 使用原生原子批次协议，只认提交记录的最终 PubAck，不自动重试结果未知的提交。
func (s *Service) commitBatch(ctx context.Context, scope address, id string, messages []*nats.Msg) (*pb.NatsPubAck, error) {
	return s.commitBatchOn(ctx, scope, id, messages, s.nc)
}

// batchConnection 保留原生有序连接语义，便于故障测试测量检查点与最终提交。
type batchConnection interface {
	PublishMsg(*nats.Msg) error
	RequestMsgWithContext(context.Context, *nats.Msg) (*nats.Msg, error)
}

func (s *Service) commitBatchOn(ctx context.Context, scope address, id string, messages []*nats.Msg, nc batchConnection) (*pb.NatsPubAck, error) {
	budget, cancel := context.WithTimeout(ctx, s.cfg.BatchCommitTimeout)
	defer cancel()
	for i, message := range messages {
		if err := budget.Err(); err != nil {
			return nil, rpcError(err)
		}
		// 原子批次需要 API level 2，不能把未识别的控制头当普通发布接纳。
		message.Header.Set("Nats-Required-Api-Level", "2")
		message.Header.Set("Nats-Batch-Id", id)
		message.Header.Set("Nats-Batch-Sequence", strconv.Itoa(i+1))
		final := i == len(messages)-1
		if final {
			message.Header.Set("Nats-Batch-Commit", "1")
		}
		// 首条确认能力与会话创建，之后按窗口等待空确认；最终确认才证明持久提交。
		if !final && i > 0 && i%s.cfg.PublishBatchFlowControl != 0 {
			message.Reply = ""
			if err := nc.PublishMsg(message); err != nil {
				return nil, rpcError(err)
			}
			continue
		}
		response, err := nc.RequestMsgWithContext(budget, message)
		if err != nil {
			return nil, rpcError(err)
		}
		var ack atomicAck
		if len(response.Data) != 0 {
			if err := json.Unmarshal(response.Data, &ack); err != nil {
				return nil, status.Error(codes.DataLoss, "invalid atomic batch acknowledgement")
			}
			if ack.Error != nil {
				return nil, publishError(ack.Error)
			}
		}
		if !final {
			if len(response.Data) != 0 {
				return nil, status.Error(codes.DataLoss, "unexpected atomic batch intermediate acknowledgement")
			}
			continue
		}
		if ack.Sequence == 0 || ack.BatchID != id || ack.Count != uint64(len(messages)) || !strings.HasPrefix(ack.Stream, scope.names) {
			return nil, status.Error(codes.DataLoss, "atomic batch acknowledgement identity mismatch")
		}
		return &pb.NatsPubAck{
			Stream:    ptr(scope.logicalName(ack.Stream)),
			Sequence:  ptr(ack.Sequence),
			Domain:    ptr(ack.Domain),
			Duplicate: ptr(ack.Duplicate),
		}, nil
	}
	return nil, status.Error(codes.Internal, "atomic batch has no commit")
}

// publishError 区分发布尾序号冲突与 KV key 已存在，二者可能复用原生错误编号。
func publishError(err error) error {
	var api *jetstream.APIError
	if errors.As(err, &api) && (api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence || api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant) {
		detail := asItemError(err)
		detail.GrpcCode = int32(codes.FailedPrecondition)
		detail.Reason = ReasonPublishPrecondition
		detail.Message = "publish precondition failed"
		st := status.New(codes.FailedPrecondition, detail.Message)
		if with, e := st.WithDetails(detail); e == nil {
			return with.Err()
		}
		return st.Err()
	}
	return rpcError(err)
}

// AckBatch 保持输入索引与结果一一对应；某条失败不跳过其后独立的确认。
func (s *Service) AckBatch(ctx context.Context, r *pb.AckBatchRequest) (*pb.AckBatchResponse, error) {
	if r == nil || len(r.Items) == 0 {
		return nil, invalid("ACK batch requires at least one item")
	}
	if len(r.Items) > s.cfg.MaxAckBatchMessages {
		return nil, exhausted(ReasonAckBatchLimit, "ACK batch message limit exceeded")
	}
	response := &pb.AckBatchResponse{Results: make([]*pb.AckBatchResult, 0, len(r.Items))}
	for i, item := range r.Items {
		if err := ctx.Err(); err != nil {
			return nil, rpcError(err)
		}
		_, err := s.Ack(ctx, item)
		// #nosec G115 -- MaxAckBatchMessages 不超过 4096。
		result := &pb.AckBatchResult{Index: uint32(i)}
		if err != nil {
			result.Error = asItemError(err)
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}
