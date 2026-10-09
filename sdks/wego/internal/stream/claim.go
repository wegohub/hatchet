package stream

import (
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// ClaimRequest 定义一次候选执行，不允许以展示名称代替 WorkerKey
type ClaimRequest struct {
	// Namespace 隔离此输出日志的实例组
	Namespace string
	// TaskID、RunID、Method、InputDigest 表示稳定的逻辑调用身份
	TaskID, RunID, Method, InputDigest string
	// WorkerKey 和 Writer 分别标识 Worker 实例和本次物理执行
	WorkerKey, Writer string
	// Epoch 由官方 RetryCount 提供，不能自动递增
	Epoch int32
	// Timeout 与扫描预算来自此方法配置，零值采用默认值
	Timeout time.Duration
	// RecoveryTimeout 限制重投前的起点核验，独立于 CLAIM 编码和竞争预算
	RecoveryTimeout time.Duration
	// MaxReplayFrames 限制已扫描的全部帧
	MaxReplayFrames int
	// MaxReplayBytes 分别限制 codec 前后历史字节
	MaxReplayBytes int
}

// ClaimResult 只表示日志中的获胜情况，不表示引擎确认此执行完成
type ClaimResult struct {
	// Owned 仅在读回 nonce 与候选一致时为 true
	Owned bool
	// Cursor 是规范 CLAIM 的实际持久位置
	Cursor string
	// Prefix 为 CLAIM 前的有效业务输出序号及当前 writer
	Prefix State
}

// Topic 将逻辑任务定位到固定输出日志，不随重试或 Worker 身份改变
func Topic(taskID string) string { return "rpc." + taskID }

// Claim 使用同一 producer/seq 竞争，只有持久读回才能确认获胜
func Claim(ctx context.Context, transport ports.DurableStreams, codec *wire.FrameCodec, request ClaimRequest) (result ClaimResult, err error) {
	started := time.Now()
	defer func() {
		outcome := "owned"
		if err != nil {
			outcome = "uncertain"
		} else if !result.Owned {
			outcome = "lost"
			observeProtocol(transport, "wego_stream_claim_conflict_total", request.Method, "reliable", outcome, 1)
		}
		observeProtocol(transport, "wego_stream_claim_duration_seconds", request.Method, "reliable", outcome, time.Since(started).Seconds())
	}()
	if request.TaskID == "" || request.Method == "" || request.WorkerKey == "" || request.Writer == "" || request.Epoch < 0 {
		return ClaimResult{}, status.Error(codes.InvalidArgument, "wego: incomplete CLAIM identity")
	}
	if request.Timeout == 0 {
		request.Timeout = 10 * time.Second
	}
	if request.MaxReplayFrames == 0 {
		request.MaxReplayFrames = 100000
	}
	if request.MaxReplayBytes == 0 {
		request.MaxReplayBytes = 64 << 20
	}
	if request.Epoch > 0 {
		// 重试不能在初始 CLAIM 已过期时先写一个新 CLAIM，制造看似有效的新起点
		timeout := request.RecoveryTimeout
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		budget, stop := context.WithTimeout(ctx, timeout)
		err := verifyOrigin(budget, transport, codec, request)
		stop()
		if err != nil {
			return ClaimResult{}, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, request.Timeout)
	defer cancel()
	frame := &wire.LogFrame{Version: wire.LogVersion, Kind: "CLAIM", TaskRunId: request.TaskID,
		RunId: request.RunID, Method: request.Method, Epoch: request.Epoch, Writer: request.Writer,
		WorkerKey: request.WorkerKey, InputDigest: request.InputDigest, FrameId: uuid.NewString()}
	encoded, err := codec.Encode(ctx, request.Method, frame)
	if err != nil {
		return ClaimResult{}, err
	}
	message := ports.DurableMessage{Namespace: request.Namespace, Topic: Topic(request.TaskID),
		Producer: request.TaskID + ":" + strconv.FormatInt(int64(request.Epoch), 10), Sequence: 0, Payload: encoded}
	if err := PublishFixed(ctx, transport, message); err != nil {
		return ClaimResult{}, err
	}
	state := State{TaskID: request.TaskID, Method: request.Method, InputDigest: request.InputDigest}
	// result 仅在读到规范 CLAIM 后填写，发布 ACK 不产生所有权
	// 扫描预算包含被忽略的旧 writer，不能仅按有效 DATA 计费
	frames, encodedBytes, plainBytes := 0, 0, 0
	// completed 只终止本次订阅，不作为业务 EOF 或引擎完成信号
	completed := errors.New("claim readback complete")
	err = SubscribeResumable(ctx, transport, ports.DurableSubscription{Namespace: request.Namespace, Topic: message.Topic}, func(entry ports.DurableEntry) error {
		frames++
		encodedBytes += len(entry.Payload)
		if frames > request.MaxReplayFrames || encodedBytes > request.MaxReplayBytes {
			return status.Error(codes.ResourceExhausted, "wego: CLAIM history scan exceeds frame/encoded-byte budget")
		}
		decoded, err := codec.Decode(ctx, request.Method, entry.Payload)
		if err != nil {
			return err
		}
		plainBytes += proto.Size(decoded)
		if plainBytes > request.MaxReplayBytes {
			return status.Error(codes.ResourceExhausted, "wego: CLAIM history scan exceeds restored-byte budget")
		}
		if _, err := state.Apply(decoded); err != nil {
			return err
		}
		if decoded.Kind == "CLAIM" && decoded.Epoch >= request.Epoch {
			result = ClaimResult{Owned: state.Epoch == request.Epoch && state.Writer == request.Writer, Cursor: entry.Cursor, Prefix: state.Snapshot()}
			return completed
		}
		return nil
	})
	if errors.Is(err, completed) {
		return result, nil
	}
	return ClaimResult{}, err
}

// verifyOrigin 只确认最早保留帧为同一调用的 epoch=0 起点；恢复前缀仍由竞争后的完整读回确定
func verifyOrigin(ctx context.Context, transport ports.DurableStreams, codec *wire.FrameCodec, request ClaimRequest) error {
	confirmed := errors.New("initial claim confirmed")
	err := SubscribeResumable(ctx, transport, ports.DurableSubscription{Namespace: request.Namespace, Topic: Topic(request.TaskID)}, func(entry ports.DurableEntry) error {
		frame, err := codec.Decode(ctx, request.Method, entry.Payload)
		if err != nil {
			return err
		}
		state := State{TaskID: request.TaskID, RunID: request.RunID, Method: request.Method, InputDigest: request.InputDigest}
		if frame.Kind != "CLAIM" || frame.Epoch != 0 {
			return status.Error(codes.DataLoss, "wego: initial CLAIM is unavailable; recovery cannot establish origin")
		}
		if _, err := state.Apply(frame); err != nil {
			return err
		}
		return confirmed
	})
	if errors.Is(err, confirmed) {
		return nil
	}
	return err
}

// PublishFixed 在不明确结果时复用同一消息字节，明确拒绝或预算耗尽时返回错误
func PublishFixed(ctx context.Context, transport ports.DurableStreams, message ports.DurableMessage) error {
	// 调用方提供阶段预算；未提供 deadline 时才使用默认值，避免覆盖显式配置
	if _, bounded := ctx.Deadline(); !bounded {
		// cancel 仅在调用方未提供预算时由此发布函数拥有
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	delay := 100 * time.Millisecond
	for {
		err := transport.PublishDurable(ctx, message)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		code := status.Code(err)
		if code != codes.Unavailable && code != codes.DeadlineExceeded && code != codes.Unknown {
			return err
		}
		// 抖动避免多个候选在网络恢复后同步冲击同一 topic
		timer := time.NewTimer(delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
}
