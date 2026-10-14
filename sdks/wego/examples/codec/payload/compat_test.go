package payload

import (
	"bytes"
	"context"
	"testing"

	core "github.com/hatchet-dev/hatchet/sdks/wego/middleware/payload"
)

// TestLegacyImportReadsCurrentFormat 验证旧导入构造的组件和正式组件互读同一持久格式。
func TestLegacyImportReadsCurrentFormat(t *testing.T) {
	old, err := NewGzip(WithThreshold(0))
	if err != nil {
		t.Fatal(err)
	}
	current, err := core.NewGzip(core.WithThreshold(0))
	if err != nil {
		t.Fatal(err)
	}
	if old.CodecID() != current.CodecID() || old.CodecID() != GzipCodecID {
		t.Fatal("gzip format identity changed")
	}
	data := []byte(`{"message":"历史消息"}`)
	encoded, err := old.Encode(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := current.DecodeLimit(context.Background(), encoded, len(data))
	if err != nil || !bytes.Equal(decoded, data) {
		t.Fatalf("stored message unreadable: %v", err)
	}
}
