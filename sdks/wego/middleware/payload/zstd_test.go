package payload

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestZstdRoundTripAndLimits 验证空值、压缩膨胀、损坏帧及取消，不依赖外部服务。
func TestZstdRoundTripAndLimits(t *testing.T) {
	codec := &Zstd{}
	ctx := context.Background()
	for _, input := range [][]byte{nil, []byte(`{"message":"hello"}`), bytes.Repeat([]byte("a"), 2<<20)} {
		encoded, err := codec.Encode(ctx, input)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := codec.Decode(ctx, encoded)
		if err != nil || !bytes.Equal(decoded, input) {
			t.Fatalf("roundtrip: size=%d, error=%v", len(input), err)
		}
		if len(input) > 1024 {
			if _, err := codec.DecodeLimit(ctx, encoded, 1024); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("expansion bypassed budget: %v", err)
			}
		}
	}
	if _, err := codec.Decode(ctx, []byte("invalid")); status.Code(err) != codes.DataLoss {
		t.Fatalf("invalid frame accepted: %v", err)
	}
	if _, err := codec.Encode(ctx, make([]byte, maxObjectBytes+1)); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized input accepted: %v", err)
	}
	if _, err := codec.DecodeLimit(ctx, nil, 0); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("invalid limit accepted: %v", err)
	}
	// canceled 在组件调用前取消，编码和解码都不应继续处理消息。
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := codec.Encode(canceled, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("encode ignored cancellation: %v", err)
	}
	if _, err := codec.Decode(canceled, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("decode ignored cancellation: %v", err)
	}
}

// TestZstdConcurrentUse 验证同一注册组件可同时处理多个独立请求。
func TestZstdConcurrentUse(t *testing.T) {
	codec := &Zstd{}
	// pending 等待全部并行请求结束后再释放测试上下文。
	var pending sync.WaitGroup
	for range 8 {
		pending.Go(func() {
			input := bytes.Repeat([]byte("concurrent"), 1024)
			encoded, err := codec.Encode(context.Background(), input)
			if err != nil {
				t.Error(err)
				return
			}
			decoded, err := codec.Decode(context.Background(), encoded)
			if err != nil || !bytes.Equal(decoded, input) {
				t.Errorf("concurrent roundtrip: %v", err)
			}
		})
	}
	pending.Wait()
}
