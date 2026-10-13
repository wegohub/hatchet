package wire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/hatchet-dev/hatchet/sdks/wego/examples/codec/payload"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// BenchmarkMessageProtocol 对比完整业务封包；S3 I/O 单独验收，不混入本地编码成本。
func BenchmarkMessageProtocol(b *testing.B) {
	aes, err := payload.NewAESGCM(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		b.Fatal(err)
	}
	profiles := map[string][]middleware.Option{
		"plain":    nil,
		"gzip":     {middleware.WithPayload(&payload.Gzip{})},
		"aes":      {middleware.WithPayload(aes)},
		"gzip-aes": {middleware.WithPayload(&payload.Gzip{}), middleware.WithPayload(aes)},
	}
	for _, size := range []int{16, 1024, 65536, 1 << 20} {
		for name, options := range profiles {
			config := spec.Defaults()
			config.Middleware = options
			if err := config.Validate(); err != nil {
				b.Fatal(err)
			}
			plain, err := json.Marshal(string(bytes.Repeat([]byte{'x'}, size)))
			if err != nil {
				b.Fatal(err)
			}
			packet := NewEnvelope(context.Background(), "/fixture/Call", "", plain, nil)
			b.Run(fmt.Sprintf("%s/%d", name, size), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					encoded, err := Seal(context.Background(), packet, config, 2<<20, 3<<20, 4<<20)
					if err != nil {
						b.Fatal(err)
					}
					data, err := json.Marshal(encoded)
					if err != nil {
						b.Fatal(err)
					}
					b.ReportMetric(float64(len(data)), "wire-B")
				}
			})
		}
	}
}
