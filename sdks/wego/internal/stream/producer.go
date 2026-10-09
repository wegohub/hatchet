package stream

import (
	"context"
	"encoding/base64"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// Producer 由 ServerStream 的发送锁串行调用；序号只在持久发布确认后推进
type Producer struct {
	// Backend 提供持久发布或实时 PutStream，实例连接不会被单个 producer 关闭
	Backend ports.Backend
	// Codec 对完整协议帧只编码一次
	Codec *wire.FrameCodec
	// Options 是方法的有效预算
	Options spec.StreamOptions
	// Identity 记录获胜 CLAIM 与当前实际执行
	Identity ClaimRequest
	// Next 是本代次下一条物理日志序号，CLAIM 已占用 0
	Next int64
	// failed 冻结发布失败；不明确的序号不能被不同 DATA 或 END 占用
	failed error
	// LastOutput 是跨代次有效业务序号
	LastOutput uint64
}

// Publish 补齐公共身份，并将随机 codec 输出冻结到本次调用结束
func (p *Producer) Publish(ctx context.Context, frame *wire.LogFrame) (err error) {
	if p.failed != nil {
		return p.failed
	}
	started := time.Now()
	defer func() {
		mode, outcome := "reliable", "success"
		if p.Options.Mode == spec.Realtime {
			mode = "realtime"
		}
		if err != nil {
			outcome = "rejected"
			code := status.Code(err)
			if p.failed != nil && (code == codes.Unavailable || code == codes.Unknown || code == codes.DeadlineExceeded || code == codes.Canceled) {
				outcome = "uncertain"
			}
		}
		observeProtocol(p.Backend, "wego_stream_publish_total", p.Identity.Method, mode, outcome, 1)
		observeProtocol(p.Backend, "wego_stream_publish_duration_seconds", p.Identity.Method, mode, outcome, time.Since(started).Seconds())
	}()
	if p.Next == math.MaxInt64 {
		return status.Error(codes.ResourceExhausted, "wego: producer sequence exhausted")
	}
	r := p.Identity
	frame.Version, frame.TaskRunId, frame.RunId, frame.Method = wire.LogVersion, r.TaskID, r.RunID, r.Method
	frame.Epoch, frame.Writer, frame.WorkerKey, frame.InputDigest = r.Epoch, r.Writer, r.WorkerKey, r.InputDigest
	frame.FrameId = uuid.NewString()
	budget, cancel := context.WithTimeout(ctx, p.Options.PublishTimeout)
	defer cancel()
	data, err := p.Codec.Encode(budget, r.Method, frame)
	if err != nil {
		return err
	}
	if p.Options.Mode == spec.Reliable {
		transport, ok := p.Backend.(ports.DurableStreams)
		if !ok {
			return status.Error(codes.Unimplemented, "wego: durable streams unavailable")
		}
		err = PublishFixed(budget, transport, ports.DurableMessage{Namespace: r.Namespace, Topic: Topic(r.TaskID), Producer: r.TaskID + ":" + strconv.FormatInt(int64(r.Epoch), 10), Sequence: p.Next, Payload: data})
	} else {
		// PutStream 的 string 出口只有一层 base64；不明确结果不重复发布实时 DATA
		err = p.Backend.Publish(budget, r.TaskID, []byte(base64.StdEncoding.EncodeToString(data)))
	}
	if err == nil {
		p.Next++
	} else {
		// 包括不明确 ACK：保持序号和原始诊断，后续 END 不能用新字节覆盖同一 producer_seq
		p.failed = err
	}
	return err
}

// observeProtocol 复用可选实例出口，协议 fixture 不需要伪造观测实现
func observeProtocol(observer any, name, method, mode, outcome string, value float64) {
	if sink, ok := observer.(ports.ProtocolObserver); ok {
		sink.ObserveProtocol(ports.ProtocolMetric{Name: name, Method: method, Mode: mode, Outcome: outcome, Value: value})
	}
}

// End 发布包含末条序号的结束帧；此帧仍须与引擎权威结果对账
func (p *Producer) End(ctx context.Context, trailers map[string]*wire.Values, business error) (Completion, error) {
	terminal, err := proto.Marshal(status.Convert(business).Proto())
	if err != nil {
		return Completion{}, err
	}
	frame := &wire.LogFrame{Kind: "ATTEMPT_END", OutputSeq: p.LastOutput, Trailers: trailers, Status: terminal}
	if err := p.Publish(ctx, frame); err != nil {
		return Completion{}, err
	}
	return Completion{Version: wire.LogVersion, TaskID: p.Identity.TaskID, Epoch: p.Identity.Epoch, Writer: p.Identity.Writer, LastOutput: p.LastOutput, EndID: frame.FrameId}, nil
}

// decodeBudget 只为当前帧创建还原预算，不缩短整个流的生命周期
func decodeBudget(ctx context.Context, timeout time.Duration, codec *wire.FrameCodec, method string, data []byte) (*wire.LogFrame, error) {
	budget, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return codec.Decode(budget, method, data)
}
