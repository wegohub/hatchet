//go:build e2e

package backend

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// probeCipher 验证 CLAIM 和 DATA 在同一随机加密链中的竞争和恢复
type probeCipher struct{ aead cipher.AEAD }

// CodecID 是公开测试格式标识，不来自运行环境
func (c *probeCipher) CodecID() string { return "p0-aes-gcm-v1" }

// Encode 在真实发布前为完整帧生成随机 nonce
func (c *probeCipher) Encode(_ context.Context, method string, data []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return c.aead.Seal(nonce, nonce, data, []byte(method)), nil
}

// Decode 是消息 codec 接口，持久日志通过 DecodeLimit 调用
func (c *probeCipher) Decode(ctx context.Context, method string, data []byte) ([]byte, error) {
	return c.DecodeLimit(ctx, method, data, 4<<20)
}

// DecodeLimit 在明文分配前限制可还原长度
func (c *probeCipher) DecodeLimit(_ context.Context, method string, data []byte, limit int) ([]byte, error) {
	n := c.aead.NonceSize()
	if len(data) < n+c.aead.Overhead() {
		return nil, errors.New("truncated encrypted frame")
	}
	if len(data)-n-c.aead.Overhead() > limit {
		return nil, status.Error(codes.ResourceExhausted, "probe plaintext exceeds limit")
	}
	return c.aead.Open(nil, data[:n], data[n:], []byte(method))
}

// lostPublishResponse 在服务端已成功写入后只丢掉第一次响应，真实存储仍由官方引擎执行
type lostPublishResponse struct {
	// DurableStreams 委托真实引擎存储，仅替换一次响应
	ports.DurableStreams
	// once 为当前测试限制一个不明确响应
	once sync.Once
}

// PublishDurable 模拟响应丢失，不改变 producer、序号或字节
func (p *lostPublishResponse) PublishDurable(ctx context.Context, message ports.DurableMessage) error {
	if err := p.DurableStreams.PublishDurable(ctx, message); err != nil {
		return err
	}
	lost := false
	p.once.Do(func() { lost = true })
	if lost {
		return status.Error(codes.Unavailable, "injected stored publish response loss")
	}
	return nil
}

// TestDurableStreamsP0Probe 验证正式引擎的持久竞争、去重、跨代次过滤和显式错误
// 此探针不宣称已经验证 dispatcher 的真实执行/终态上报隔离
func TestDurableStreamsP0Probe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	config := spec.Defaults()
	config.Token = os.Getenv("HATCHET_CLIENT_TOKEN")
	if config.Token == "" {
		t.Fatal("required real-engine token missing; P0 cannot skip")
	}
	config.Address, config.ServerURL = "localhost:7077", "http://localhost:8080"
	config.TLS, config.TLSSet = nil, true
	config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	b, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	block, err := aes.NewCipher(bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := wire.NewFrameCodec([]middleware.Option{middleware.WithPayload(&probeCipher{aead})}, 4<<20, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	namespace := "p0-" + uuid.NewString()
	taskID := uuid.NewString()
	requests := []stream.ClaimRequest{
		{Namespace: namespace, TaskID: taskID, Method: "/fixture/Call", WorkerKey: uuid.NewString(), Writer: uuid.NewString(), InputDigest: "fixture-digest"},
		{Namespace: namespace, TaskID: taskID, Method: "/fixture/Call", WorkerKey: uuid.NewString(), Writer: uuid.NewString(), InputDigest: "fixture-digest"},
	}
	results := make([]stream.ClaimResult, len(requests))
	errorsByIndex := make([]error, len(requests))
	// wg 确认两个并发 CLAIM 都读回规范日志后再判断胜负
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errorsByIndex[i] = stream.Claim(ctx, &lostPublishResponse{DurableStreams: b}, codec, requests[i])
		}()
	}
	close(start)
	wg.Wait()
	winner := -1
	for i, err := range errorsByIndex {
		if err != nil {
			t.Fatalf("CLAIM candidate %d: %v", i, err)
		}
		if results[i].Owned {
			if winner != -1 {
				t.Fatal("two CLAIM candidates won")
			}
			winner = i
		}
	}
	if winner == -1 || results[0].Cursor != results[1].Cursor {
		t.Fatalf("canonical CLAIM identity mismatch: %+v", results)
	}
	publish := func(epoch int32, writer string, producerSequence int64, kind string, outputSequence uint64) {
		t.Helper()
		frame := &wire.LogFrame{Version: wire.LogVersion, Kind: kind, TaskRunId: taskID, Method: "/fixture/Call", Epoch: epoch,
			Writer: writer, WorkerKey: requests[winner].WorkerKey, OutputSeq: outputSequence, FrameId: uuid.NewString(), InputDigest: "fixture-digest"}
		encoded, err := codec.Encode(ctx, frame.Method, frame)
		if err != nil {
			t.Fatal(err)
		}
		message := ports.DurableMessage{Namespace: namespace, Topic: stream.Topic(taskID), Producer: fmt.Sprintf("%s:%d", taskID, epoch), Sequence: producerSequence, Payload: encoded}
		// 所有帧均在服务端存储成功后丢一次响应，重试仍不得增加消息
		if err := stream.PublishFixed(ctx, &lostPublishResponse{DurableStreams: b}, message); err != nil {
			t.Fatal(err)
		}
	}
	publish(0, requests[winner].Writer, 1, "HEADERS", 0)
	publish(0, requests[winner].Writer, 2, "DATA", 1)
	next := requests[winner]
	next.Epoch = 1
	next.Writer = uuid.NewString()
	takeover, err := stream.Claim(ctx, b, codec, next)
	if err != nil || !takeover.Owned || takeover.Prefix.LastOutput != 1 {
		t.Fatalf("takeover: %+v %v", takeover, err)
	}
	publish(0, requests[winner].Writer, 3, "DATA", 2)
	publish(0, requests[winner].Writer, 4, "HEADERS", 0)
	publish(0, requests[winner].Writer, 5, "ATTEMPT_END", 2)
	publish(1, next.Writer, 1, "DATA", 2)
	publish(1, next.Writer, 2, "ATTEMPT_END", 2)
	state := stream.State{TaskID: taskID, Method: "/fixture/Call", InputDigest: "fixture-digest"}
	claimCount, dataCount, physicalFrames := 0, 0, 0
	completed := errors.New("probe output complete")
	err = b.SubscribeDurable(ctx, ports.DurableSubscription{Namespace: namespace, Topic: stream.Topic(taskID)}, func(entry ports.DurableEntry) error {
		physicalFrames++
		frame, err := codec.Decode(ctx, state.Method, entry.Payload)
		if err != nil {
			return err
		}
		if frame.Kind == "CLAIM" {
			claimCount++
		}
		accepted, err := state.Apply(frame)
		if err != nil {
			return err
		}
		if accepted {
			dataCount++
		}
		if state.End != nil {
			return completed
		}
		return nil
	})
	if !errors.Is(err, completed) || claimCount != 2 || dataCount != 2 || physicalFrames != 9 || state.LastOutput != 2 {
		t.Fatalf("persistent prefix: claims=%d outputs=%d frames=%d state=%+v error=%v", claimCount, dataCount, physicalFrames, state, err)
	}
	gap := ports.DurableMessage{Namespace: namespace, Topic: stream.Topic(taskID), Producer: taskID + ":1", Sequence: 4, Payload: []byte("gap")}
	if err := b.PublishDurable(ctx, gap); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("producer gap: %v", err)
	}
	cursor := "invalid-cursor"
	err = b.SubscribeDurable(ctx, ports.DurableSubscription{Namespace: namespace, Topic: stream.Topic(taskID), Cursor: &cursor}, func(ports.DurableEntry) error { return nil })
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid cursor: %v", err)
	}
	// 过期游标使用同一真实地址；服务端必须拒绝，不能从剩余历史静默重置
	rawCursor, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(results[winner].Cursor, "v1:"))
	if err != nil {
		t.Fatal(err)
	}
	// oldCursor 仅改变测试游标的时间，保持真实 namespace/topic 身份
	var oldCursor map[string]any
	if err := json.Unmarshal(rawCursor, &oldCursor); err != nil {
		t.Fatal(err)
	}
	oldCursor["created_at"] = time.Now().Add(-400 * 24 * time.Hour).UTC().Format(time.RFC3339Nano)
	expired, err := json.Marshal(oldCursor)
	if err != nil {
		t.Fatal(err)
	}
	expiredCursor := "v1:" + base64.RawURLEncoding.EncodeToString(expired)
	err = b.SubscribeDurable(ctx, ports.DurableSubscription{Namespace: namespace, Topic: stream.Topic(taskID), Cursor: &expiredCursor}, func(ports.DurableEntry) error { return nil })
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("expired cursor: %v", err)
	}
	// JOIN 也经过相同随机加密；响应丢失后仍只认读回的屏障位置
	joinCursor, err := stream.Join(ctx, &lostPublishResponse{DurableStreams: b}, codec, namespace, "workers.broadcast", uuid.NewString())
	if err != nil || joinCursor == "" {
		t.Fatalf("JOIN: cursor=%s error=%v", joinCursor, err)
	}
	// 先发布 CLAIM 后丢失确认，不允许下一候选盗用 nonce
	uncertainTask := uuid.NewString()
	uncertain := stream.ClaimRequest{Namespace: namespace, TaskID: uncertainTask, Method: "/fixture/Call", WorkerKey: uuid.NewString(), Writer: uuid.NewString(), InputDigest: "digest"}
	_, err = stream.Claim(ctx, &failedReadback{DurableStreams: b}, codec, uncertain)
	if status.Code(err) != codes.Unavailable && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("uncertain claim: %v", err)
	}
	uncertain.Writer = uuid.NewString()
	rejected, err := stream.Claim(ctx, b, codec, uncertain)
	if err != nil || rejected.Owned {
		t.Fatalf("same epoch stolen after crash: %+v %v", rejected, err)
	}
	uncertain.Epoch = 1
	uncertain.Writer = uuid.NewString()
	recovered, err := stream.Claim(ctx, b, codec, uncertain)
	if err != nil || !recovered.Owned || recovered.Prefix.LastOutput != 0 {
		t.Fatalf("next epoch failed takeover: %+v %v", recovered, err)
	}
	// 首次 CLAIM 前丢失投递没有完整历史证明，应明确失败而非当作首次执行
	uncertain.TaskID, uncertain.Writer = uuid.NewString(), uuid.NewString()
	uncertain.RecoveryTimeout = 50 * time.Millisecond
	_, err = stream.Claim(ctx, b, codec, uncertain)
	if status.Code(err) != codes.DataLoss && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing origin proof: %v", err)
	}
	// 绕过本地预检查的受控协议请求，验证官方真实内联上限
	_, err = b.durableStreams.Publish(b.auth(ctx), &v1.PublishStreamMessageRequest{Namespace: namespace, Topic: "payload-limit", ProducerId: uuid.NewString(), Payload: make([]byte, durablePayloadLimit+1)})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("server payload limit: %v", err)
	}
	// 故障 fixture 只删除本探针地址的水位/起点，不修改生产配置或共享分区
	if os.Getenv("WEGO_P0_FAULT_FIXTURES") != "1" {
		t.Fatal("P0 retention fault acceptance requires WEGO_P0_FAULT_FIXTURES=1")
	}
	fixture := func(action string, extra ...string) {
		t.Helper()
		args := append([]string{"../../scripts/local-stream-fixture.py", action, namespace, stream.Topic(taskID)}, extra...)
		if output, err := exec.CommandContext(ctx, "python3", args...).CombinedOutput(); err != nil {
			t.Fatalf("retention fixture: %v %s", err, output)
		}
	}
	fixture("forget-producer", "--producer", taskID+":1")
	afterExpiry := ports.DurableMessage{Namespace: namespace, Topic: stream.Topic(taskID), Producer: taskID + ":1", Sequence: 3, Payload: []byte("same-producer-after-expiry")}
	if err := stream.PublishFixed(ctx, b, afterExpiry); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expired producer watermark silently reset: %v", err)
	}
	fixture("delete-origin")
	missingOrigin := next
	missingOrigin.Epoch, missingOrigin.Writer = 2, uuid.NewString()
	_, err = stream.Claim(ctx, b, codec, missingOrigin)
	if status.Code(err) != codes.DataLoss {
		t.Fatalf("partial history silently treated as fresh: %v", err)
	}
	// 报告只记录实际验证的协议探针；topic 历史由官方保留策略管理，不伪造资源删除
	report := map[string]any{"server_version": "v0.110.5", "namespace": namespace, "topic": stream.Topic(taskID),
		"canonical_claim_cursor": results[winner].Cursor, "takeover_cursor": takeover.Cursor, "effective_outputs": dataCount,
		"physical_frames": physicalFrames, "checks": []string{"encrypted_claim_competition", "stored_response_loss_dedup", "cross_epoch_late_write", "producer_gap", "invalid_cursor", "expired_cursor", "encrypted_join_response_loss", "claim_confirmation_loss", "missing_initial_claim", "server_inline_limit", "producer_watermark_expiry", "partial_history_origin_loss"},
		"cleanup": "client closed; dedicated topic retained under server policy", "dispatcher_terminal_isolation": "not tested by this probe"}
	writeP0Report(t, b, "v020-p0-streams", report)
	t.Logf("verified CLAIM cursor=%s takeover=%s outputs=%d frames=%d", results[winner].Cursor, takeover.Cursor, dataCount, physicalFrames)
}

// failedReadback 模拟 CLAIM 写入后、确认所有权之前断连；不能凭发布 ACK 进入业务
type failedReadback struct {
	// DurableStreams 保留真实持久写入路径，只中断本次读回
	ports.DurableStreams
}

// SubscribeDurable 明确返回传输故障，不伪造规范 CLAIM
func (*failedReadback) SubscribeDurable(context.Context, ports.DurableSubscription, func(ports.DurableEntry) error) error {
	return status.Error(codes.Unavailable, "injected readback disconnection")
}
