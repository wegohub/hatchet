package payload

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestComponentThresholdOptions 验证组件默认值、显式零值及独立快照，发送阈值不改变 CodecID。
func TestComponentThresholdOptions(t *testing.T) {
	for _, threshold := range []int{0, 128 << 10, 256 << 10} {
		gzip, err := NewGzip(WithThreshold(threshold))
		if err != nil {
			t.Fatal(err)
		}
		zstd, err := NewZstd(WithThreshold(threshold))
		if err != nil {
			t.Fatal(err)
		}
		if gzip.EncodingThreshold() != threshold || zstd.EncodingThreshold() != threshold {
			t.Fatal("component threshold not retained")
		}
		if gzip.CodecID() != GzipCodecID || zstd.CodecID() != ZstdCodecID {
			t.Fatal("threshold changed persistent format ID")
		}
	}
	// 默认构造与零值注册具有相同规则，只有显式 WithThreshold(0) 才强制处理。
	gzip, err := NewGzip()
	if err != nil {
		t.Fatal(err)
	}
	zstd, err := NewZstd()
	if err != nil {
		t.Fatal(err)
	}
	if gzip.EncodingThreshold() != DefaultEncodingThreshold || zstd.EncodingThreshold() != DefaultEncodingThreshold || (&Gzip{}).EncodingThreshold() != DefaultEncodingThreshold || (&Zstd{}).EncodingThreshold() != DefaultEncodingThreshold {
		t.Fatal("default threshold differs")
	}
	options := []Option{WithThreshold(1024), WithThreshold(2048)}
	configured, err := NewGzip(options...)
	if err != nil {
		t.Fatal(err)
	}
	options[1] = WithThreshold(0)
	if configured.EncodingThreshold() != 2048 || gzip.EncodingThreshold() != DefaultEncodingThreshold {
		t.Fatal("component configuration aliases caller or other instance")
	}
}

// TestInvalidComponentThresholds 拒绝负数和 nil 选项，S3 在初始化连接或请求桶前失败。
func TestInvalidComponentThresholds(t *testing.T) {
	for _, option := range []Option{WithThreshold(-1), nil} {
		if _, err := NewGzip(option); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("gzip invalid option: %v", err)
		}
		if _, err := NewZstd(option); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("zstd invalid option: %v", err)
		}
		if _, err := NewS3(context.Background(), Config{}, "fixture/", option); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("S3 invalid option: %v", err)
		}
	}
}
