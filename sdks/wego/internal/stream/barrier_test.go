package stream

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// barrierFixture 用确定性历史与阻塞模拟屏障预算，不模拟引擎执行终态
type barrierFixture struct {
	// mu 保护已经发布的字节快照
	mu sync.Mutex
	// saved 是唯一一次发布后的独立字节，用于核对重试不重新编码
	saved []byte
	// mode 选择超时或历史超限，不包含外部租户名称
	mode string
	// historical 为重复扫描使用的不可变有效历史帧
	historical []byte
	// prelude 保存本次 JOIN 之前的独立历史消息，包括已不可解码的对象引用。
	prelude [][]byte
}

// PublishDurable 保存已编码的 JOIN；fixture 不对字节作任何变换
func (f *barrierFixture) PublishDurable(ctx context.Context, message ports.DurableMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saved = append([]byte(nil), message.Payload...)
	return nil
}

// SubscribeDurable 按确定性模式等待预算或达到扫描上限，成功模式返回保存的真实屏障
func (f *barrierFixture) SubscribeDurable(ctx context.Context, _ ports.DurableSubscription, consume func(ports.DurableEntry) error) error {
	if f.mode == "timeout" {
		<-ctx.Done()
		return ctx.Err()
	}
	if f.mode == "history_limit" {
		for i := 0; i <= 100000; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := consume(ports.DurableEntry{Cursor: strconv.Itoa(i), Payload: f.historical}); err != nil {
				return err
			}
		}
	}
	for i, payload := range f.prelude {
		if err := consume(ports.DurableEntry{Cursor: strconv.Itoa(i + 1), Payload: payload}); err != nil {
			return err
		}
	}
	f.mu.Lock()
	data := append([]byte(nil), f.saved...)
	f.mu.Unlock()
	if f.mode == "empty_cursor" {
		return consume(ports.DurableEntry{Payload: data})
	}
	return consume(ports.DurableEntry{Cursor: "barrier", Payload: data})
}

// barrierObjectCodec 模拟对象引用：持久帧只保留引用，历史对象可独立过期。
type barrierObjectCodec struct {
	// objects 模拟可按引用下载的对象，删除键代表历史对象已过期。
	objects map[string][]byte
	// decodeCalls 记录实际下载次数，用于确认加入前的历史未访问存储。
	decodeCalls int
	// dropNext 让下一次编码产生无法还原的引用，模拟自己的 JOIN 对象缺失。
	dropNext bool
	// next 保证删除旧对象后生成的引用仍然唯一。
	next int
}

// CodecID 固定测试对象引用的持久格式标识。
func (*barrierObjectCodec) CodecID() string { return "barrier-object.v1" }

// Encode 将当前完整帧保存为对象并返回引用，必要时立即模拟对象过期。
func (c *barrierObjectCodec) Encode(_ context.Context, data []byte) ([]byte, error) {
	if c.objects == nil {
		c.objects = make(map[string][]byte)
	}
	c.next++
	ref := fmt.Sprintf("ref-%d", c.next)
	c.objects[ref] = append([]byte(nil), data...)
	if c.dropNext {
		delete(c.objects, ref)
		c.dropNext = false
	}
	return []byte(ref), nil
}

// Decode 使用足够大的默认上限，测试中的有界路径调用 DecodeLimit。
func (c *barrierObjectCodec) Decode(ctx context.Context, data []byte) ([]byte, error) {
	return c.DecodeLimit(ctx, data, 1<<20)
}

// DecodeLimit 只对实际读取的引用返回错误，并限制还原后的字节数。
func (c *barrierObjectCodec) DecodeLimit(_ context.Context, data []byte, limit int) ([]byte, error) {
	c.decodeCalls++
	value, ok := c.objects[string(data)]
	if !ok {
		return nil, status.Error(codes.DataLoss, "codec: historical object missing")
	}
	if len(value) > limit {
		return nil, status.Error(codes.ResourceExhausted, "codec: object exceeds limit")
	}
	return append([]byte(nil), value...), nil
}

// TestJoinSkipsExpiredHistory 验证新实例不下载加入前的对象，但会读回自己的屏障。
func TestJoinSkipsExpiredHistory(t *testing.T) {
	objects := &barrierObjectCodec{}
	codec, err := wire.NewFrameCodec(spec.Runtime{Middleware: []middleware.Option{middleware.WithPayload(objects)}}, 4096, 8192)
	if err != nil {
		t.Fatal(err)
	}
	history, err := codec.Encode(context.Background(), EventMethod, &wire.LogFrame{
		Version: wire.LogVersion, Kind: "JOIN", Method: EventMethod, WorkerKey: "old-worker", FrameId: "old-join",
	})
	if err != nil {
		t.Fatal(err)
	}
	delete(objects.objects, "ref-1")
	fixture := &barrierFixture{prelude: [][]byte{[]byte("malformed history"), history}}
	cursor, err := Join(context.Background(), fixture, codec, "fixture", "broadcast", "new-worker")
	if err != nil || cursor != "barrier" || objects.decodeCalls != 1 {
		t.Fatalf("cursor=%q err=%v decode_calls=%d", cursor, err, objects.decodeCalls)
	}
}

// TestJoinRejectsMissingOwnObject 验证自己的 JOIN 即使字节相同，也必须能还原对象。
func TestJoinRejectsMissingOwnObject(t *testing.T) {
	objects := &barrierObjectCodec{dropNext: true}
	codec, err := wire.NewFrameCodec(spec.Runtime{Middleware: []middleware.Option{middleware.WithPayload(objects)}}, 4096, 8192)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := Join(context.Background(), &barrierFixture{}, codec, "fixture", "broadcast", "worker")
	if status.Code(err) != codes.DataLoss || cursor != "" || objects.decodeCalls != 1 {
		t.Fatalf("cursor=%q err=%v decode_calls=%d", cursor, err, objects.decodeCalls)
	}
}

// TestJoinBudgetsAndReadback 验证空 Publish 响应不能代替游标，超时和扫描超限均明确失败
func TestJoinBudgetsAndReadback(t *testing.T) {
	codec, err := wire.NewFrameCodec(spec.Runtime{}, 4096, 8192)
	if err != nil {
		t.Fatal(err)
	}
	history, err := codec.Encode(context.Background(), EventMethod, &wire.LogFrame{Version: wire.LogVersion, Kind: "JOIN", FrameId: "historical"})
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"success", "timeout", "history_limit", "empty_cursor"} {
		t.Run(scenario, func(t *testing.T) {
			budget := time.Second
			if scenario == "timeout" {
				budget = 20 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			fixture := &barrierFixture{mode: scenario, historical: history}
			// defaults 为每个子场景提供独立的通知预算。
			defaults := spec.Defaults()
			options := defaults.WorkerEventOptionsFor()
			if scenario == "history_limit" {
				options.MaxReplayFrames = 3
			}
			cursor, err := Join(ctx, fixture, codec, "fixture", "broadcast", "worker", options)
			switch scenario {
			case "success":
				if err != nil || cursor != "barrier" {
					t.Fatal(cursor, err)
				}
			case "timeout":
				if err != context.DeadlineExceeded || cursor != "" {
					t.Fatal(cursor, err)
				}
			case "history_limit":
				if status.Code(err) != codes.ResourceExhausted || cursor != "" {
					t.Fatal(cursor, err)
				}
			case "empty_cursor":
				if status.Code(err) != codes.DataLoss || cursor != "" {
					t.Fatal(cursor, err)
				}
			}
		})
	}
}
