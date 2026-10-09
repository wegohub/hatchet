package stream

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
)

// claimLog 保存首次发布的规范 CLAIM，竞争者的成功 ACK 不会覆盖已有字节
// 每个 fixture 由一个测试 goroutine 使用；并发竞争由真实引擎验收负责
type claimLog struct {
	// DurableStreams 的其他能力不得被本场景意外使用
	ports.DurableStreams
	// entries 是按持久位置排列的已有历史
	entries []ports.DurableEntry
	// writes 用于断言恢复起点失败时没有发布新 CLAIM
	writes int
	// reject 模拟明确拒绝，不进入发布重试
	reject error
}

// PublishDurable 只为空日志写入首个候选，已有规范历史保持不变
func (f *claimLog) PublishDurable(_ context.Context, message ports.DurableMessage) error {
	f.writes++
	if f.reject != nil {
		return f.reject
	}
	if len(f.entries) == 0 {
		f.entries = []ports.DurableEntry{{Cursor: "1", Payload: bytes.Clone(message.Payload)}}
	}
	return nil
}

// SubscribeDurable 在消费到目标帧前保持订阅；历史不足只由调用预算终止
func (f *claimLog) SubscribeDurable(ctx context.Context, _ ports.DurableSubscription, consume func(ports.DurableEntry) error) error {
	for _, entry := range f.entries {
		if err := consume(entry); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

// TestClaimReadbackBoundaries 覆盖获胜、落选、历史丢失与资源预算，ACK 不能作为所有权证据
func TestClaimReadbackBoundaries(t *testing.T) {
	for _, name := range []string{"owned", "lost", "missing_identity", "negative_epoch", "missing_origin", "corrupt_frame", "frame_budget", "byte_budget", "rejected", "canceled"} {
		t.Run(name, func(t *testing.T) {
			codec, err := wire.NewFrameCodec(nil, 4096, 8192)
			if err != nil {
				t.Fatal(err)
			}
			request := ClaimRequest{TaskID: "task", RunID: "run", Method: "/fixture/Call", InputDigest: "digest", WorkerKey: "worker-a", Writer: "a", Timeout: time.Second}
			fixture := &claimLog{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := codes.OK
			switch name {
			case "lost":
				_, fixture.entries = recoveryCodec(t, logFrame("CLAIM", 0, "b", 0))
			case "missing_identity":
				request.Writer = ""
				want = codes.InvalidArgument
			case "negative_epoch":
				request.Epoch = -1
				want = codes.InvalidArgument
			case "missing_origin":
				request.Epoch = 1
				_, fixture.entries = recoveryCodec(t, logFrame("CLAIM", 1, "b", 0))
				want = codes.DataLoss
			case "corrupt_frame":
				fixture.entries = []ports.DurableEntry{{Payload: []byte{0xff}}}
				want = codes.DataLoss
			case "frame_budget":
				request.Epoch = 1
				request.MaxReplayFrames = 1
				_, fixture.entries = recoveryCodec(t, logFrame("CLAIM", 0, "a", 0), logFrame("CLAIM", 1, "b", 0))
				want = codes.ResourceExhausted
			case "byte_budget":
				request.MaxReplayBytes = 1
				want = codes.ResourceExhausted
			case "rejected":
				fixture.reject = status.Error(codes.PermissionDenied, "fixture denied")
				want = codes.PermissionDenied
			case "canceled":
				cancel()
				want = codes.Canceled
			}
			result, err := Claim(ctx, fixture, codec, request)
			code := status.Code(err)
			if ctx.Err() != nil {
				code = status.FromContextError(ctx.Err()).Code()
			}
			if code != want {
				t.Fatalf("code=%s want=%s error=%v", status.Code(err), want, err)
			}
			if name == "owned" && (!result.Owned || result.Cursor != "1") {
				t.Fatal("publish did not read canonical CLAIM", result)
			}
			if name == "lost" && (result.Owned || result.Prefix.Writer != "b") {
				t.Fatal("ACK granted ownership to loser", result)
			}
			if name == "missing_origin" && fixture.writes != 0 {
				t.Fatal("new CLAIM published before origin verified")
			}
		})
	}
}

// BenchmarkClaimAcquire 测量 SDK 编码、发布和规范读回开销；不包含引擎网络延迟
func BenchmarkClaimAcquire(b *testing.B) {
	codec, err := wire.NewFrameCodec(nil, 4096, 8192)
	if err != nil {
		b.Fatal(err)
	}
	request := ClaimRequest{TaskID: "task", RunID: "run", Method: "/fixture/Call", InputDigest: "digest", WorkerKey: "worker-a", Writer: "a"}
	b.ReportAllocs()
	for b.Loop() {
		if result, err := Claim(context.Background(), &claimLog{}, codec, request); err != nil || !result.Owned {
			b.Fatal(result, err)
		}
	}
}

// BenchmarkLogInterpreter 测量连续 DATA 的 O(1) 路径与长历史完整扫描，帧分配不计入状态机成本
func BenchmarkLogInterpreter(b *testing.B) {
	for _, count := range []int{1, 10000} {
		b.Run(fmt.Sprintf("frames_%d", count), func(b *testing.B) {
			frames := make([]*wire.LogFrame, 0, count+2)
			frames = append(frames, logFrame("CLAIM", 0, "a", 0), logFrame("HEADERS", 0, "a", 0))
			for i := 1; i <= count; i++ {
				frames = append(frames, logFrame("DATA", 0, "a", uint64(i)))
			}
			b.ReportAllocs()
			for b.Loop() {
				state := State{TaskID: "task", Method: "/fixture/Call", InputDigest: "digest"}
				for _, frame := range frames {
					if _, err := state.Apply(frame); err != nil {
						b.Fatal(err)
					}
				}
				if state.LastOutput != uint64(count) {
					b.Fatal("history scan incomplete")
				}
			}
		})
	}
}
