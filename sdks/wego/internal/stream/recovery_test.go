package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// recoveryLog 是独立只读日志 fixture，只改变明确的 ACK 或订阅故障，不伪造业务解码结果
type recoveryLog struct {
	// Backend 提供本测试未调用的任务接口，持久接口在下面显式实现
	ports.Backend
	// entries 是按持久位置排列的完整编码帧
	entries []ports.DurableEntry
	// writes 保存每次真实发布的冻结字节及序号
	writes []ports.DurableMessage
	// ackLost 模拟服务端已接受但响应丢失
	ackLost bool
	// disconnect 只在第一次读取首帧后断开
	disconnect bool
	// reads 记录实际订阅调用数
	reads int
}

// PublishDurable 保存独立字节，ACK 丢失不改变服务端已保存的请求
func (f *recoveryLog) PublishDurable(_ context.Context, message ports.DurableMessage) error {
	message.Payload = bytes.Clone(message.Payload)
	f.writes = append(f.writes, message)
	if f.ackLost {
		return status.Error(codes.Unavailable, "stored ACK lost")
	}
	return nil
}

// SubscribeDurable 遵守游标之后读取，全部历史交付后受 context 约束，不以空 hangup 伪造 EOF
func (f *recoveryLog) SubscribeDurable(ctx context.Context, request ports.DurableSubscription, consume func(ports.DurableEntry) error) error {
	f.reads++
	start := 0
	if request.Cursor != nil {
		start, _ = strconv.Atoi(*request.Cursor)
	}
	for index := start; index < len(f.entries); index++ {
		if err := consume(f.entries[index]); err != nil {
			return err
		}
		if f.disconnect && f.reads == 1 {
			return status.Error(codes.Unavailable, "injected hangup")
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

// recoveryCodec 创建有界协议编码器，并把明文 fixture 变为真实整帧数据
func recoveryCodec(t *testing.T, frames ...*wire.LogFrame) (*wire.FrameCodec, []ports.DurableEntry) {
	t.Helper()
	codec, err := wire.NewFrameCodec(nil, 4096, 8192)
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]ports.DurableEntry, len(frames))
	for index, frame := range frames {
		frame.RunId = "run"
		data, err := codec.Encode(context.Background(), "/fixture/Call", frame)
		if err != nil {
			t.Fatal(err)
		}
		entries[index] = ports.DurableEntry{Cursor: strconv.Itoa(index + 1), Payload: data}
	}
	return codec, entries
}

// TestProducerFreezesAmbiguousFailure 确认 DATA 的 ACK 不明确后不能以 END 或另一条 DATA 覆盖同一物理序号
func TestProducerFreezesAmbiguousFailure(t *testing.T) {
	codec, _ := recoveryCodec(t)
	fixture := &recoveryLog{ackLost: true}
	// defaults 为当前测试独立的配置，方法预算通过指针接收者读取。
	defaults := spec.Defaults()
	options := defaults.StreamOptionsFor("/fixture/Call")
	options.PublishTimeout = 20 * time.Millisecond
	producer := &Producer{Backend: fixture, Codec: codec, Options: options, Identity: ClaimRequest{TaskID: "task", RunID: "run", Method: "/fixture/Call", Writer: "a", WorkerKey: "worker-a", InputDigest: "digest"}, Next: 2}
	err := producer.Publish(context.Background(), &wire.LogFrame{Kind: "DATA", OutputSeq: 1, Payload: []byte(`{"count":0}`)})
	if !errors.Is(err, context.DeadlineExceeded) || producer.Next != 2 || len(fixture.writes) == 0 {
		t.Fatal(err, producer.Next)
	}
	count := len(fixture.writes)
	if _, endErr := producer.End(context.Background(), nil, nil); !errors.Is(endErr, err) {
		t.Fatal(endErr)
	}
	if nextErr := producer.Publish(context.Background(), &wire.LogFrame{Kind: "DATA", OutputSeq: 2}); !errors.Is(nextErr, err) {
		t.Fatal(nextErr)
	}
	if len(fixture.writes) != count {
		t.Fatal("failed sequence was reused by different bytes")
	}
	for _, message := range fixture.writes {
		if message.Sequence != 2 || message.Producer != "task:0" || !bytes.Equal(message.Payload, fixture.writes[0].Payload) {
			t.Fatal("ambiguous retry changed identity or encoding")
		}
	}
}

// TestFiniteHistoryConstraints 用真实协议数据验证 EOF、资源预算、过期起点和错误 protobuf 类型
func TestFiniteHistoryConstraints(t *testing.T) {
	for _, scenario := range []string{"success", "missing_origin", "missing_headers", "output_gap", "message_count", "output_bytes", "recovery_deadline", "wrong_message"} {
		t.Run(scenario, func(t *testing.T) {
			frames := []*wire.LogFrame{logFrame("CLAIM", 0, "a", 0), logFrame("HEADERS", 0, "a", 0), logFrame("DATA", 0, "a", 1), logFrame("CLAIM", 1, "b", 0)}
			frames[2].Payload = []byte(`{"message":"history","count":1}`)
			// defaults 隔离每个子场景的默认配置。
			defaults := spec.Defaults()
			options := defaults.StreamOptionsFor("/fixture/Call")
			prefix := State{LastOutput: 1}
			expected := codes.OK
			switch scenario {
			case "missing_origin":
				frames = frames[1:]
				expected = codes.DataLoss
			case "output_gap":
				frames[2].OutputSeq = 2
				expected = codes.DataLoss
			case "missing_headers":
				frames = append(frames[:1], frames[2:]...)
				expected = codes.DataLoss
			case "message_count":
				prefix.LastOutput = 2
				options.MaxCheckpointMessages = 1
				expected = codes.ResourceExhausted
			case "output_bytes":
				options.MaxCheckpointBytes = 1
				expected = codes.ResourceExhausted
			case "recovery_deadline":
				frames = nil
				options.RecoveryTimeout = 20 * time.Millisecond
				expected = codes.DeadlineExceeded
			case "wrong_message":
				expected = codes.InvalidArgument
			}
			codec, entries := recoveryCodec(t, frames...)
			boundary := strconv.Itoa(len(entries))
			history := NewHistory(context.Background(), &recoveryLog{entries: entries}, codec, ClaimRequest{TaskID: "task", RunID: "run", Method: "/fixture/Call", InputDigest: "digest", Writer: "b"}, ClaimResult{Cursor: boundary, Prefix: prefix}, options, (&pb.Reply{}).ProtoReflect().Descriptor().FullName())
			defer history.Close()
			if history.Restored() {
				t.Fatal("nonempty history restored before reads")
			}
			if scenario == "wrong_message" {
				if err := history.Next(&pb.Request{}); status.Code(err) != expected {
					t.Fatal(err)
				}
				return
			}
			item := &pb.Reply{}
			err := history.Next(item)
			if expected != codes.OK {
				if status.Code(err) != expected {
					t.Fatal(err)
				}
				if history.Restored() {
					t.Fatal("failed history became restored")
				}
				return
			}
			if err != nil || item.Count != 1 || item.Message != "history" {
				t.Fatal(item, err)
			}
			if history.Restored() {
				t.Fatal("history restored before business receives EOF")
			}
			if err := history.Next(&pb.Reply{}); err != io.EOF || !history.Restored() {
				t.Fatal(err)
			}
		})
	}
}

// TestSubscriptionResumesOnlyTransportErrors 验证断网续读不重复交付，业务 sentinel 和过期游标原样保留
func TestSubscriptionResumesOnlyTransportErrors(t *testing.T) {
	fixture := &recoveryLog{disconnect: true, entries: []ports.DurableEntry{{Cursor: "1"}, {Cursor: "2"}}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	completed := errors.New("finite scan complete")
	seen := []string{}
	err := SubscribeResumable(ctx, fixture, ports.DurableSubscription{}, func(entry ports.DurableEntry) error {
		seen = append(seen, entry.Cursor)
		if entry.Cursor == "2" {
			return completed
		}
		return nil
	})
	if !errors.Is(err, completed) || len(seen) != 2 || fixture.reads != 2 {
		t.Fatal(err, seen, fixture.reads)
	}
	fixture.disconnect, fixture.reads = false, 0
	err = SubscribeResumable(ctx, fixture, ports.DurableSubscription{}, func(ports.DurableEntry) error { return status.Error(codes.Unavailable, "codec object unavailable") })
	if status.Code(err) != codes.Unavailable || fixture.reads != 1 {
		t.Fatal("callback failure was retried", err)
	}
}
