package runtime

import (
	"slices"
	"testing"

	"github.com/hatchet-dev/hatchet/sdks/wego/examples/codec/payload"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// TestCodecOptionsSnapshot 验证配置构造时冻结切片，后配置空链不会清除接收能力。
func TestCodecOptionsSnapshot(t *testing.T) {
	options := []middleware.Option{middleware.WithPayload(&payload.Gzip{})}
	register := WithMiddleware(options...)
	options[0] = middleware.WithPayload(nil)
	ids := []string{payload.GzipCodecID, payload.GzipCodecID}
	selectChain := WithEncodingCodecs(ids...)
	ids[0] = "missing"
	config := spec.Defaults()
	register(&config)
	selectChain(&config)
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	registry, _ := config.Codecs()
	if !slices.Equal(registry.EncodingIDs(), []string{payload.GzipCodecID, payload.GzipCodecID}) {
		t.Fatal("configuration aliases caller slices")
	}
	clone := config.Clone()
	WithEncodingCodecs()(&clone)
	if err := clone.Validate(); err != nil {
		t.Fatal(err)
	}
	decoder, _ := clone.Codecs()
	if len(decoder.EncodingIDs()) != 0 || len(registry.EncodingIDs()) != 2 {
		t.Fatal("clone changed another instance's encoding chain")
	}
	if _, ok := decoder.Lookup(payload.GzipCodecID); !ok {
		t.Fatal("empty encoding chain removed decoder")
	}
}

// TestCodecConfigurationErrors 所有无效配置都在连接和 codec I/O 之前失败。
func TestCodecConfigurationErrors(t *testing.T) {
	// typedNil 验证接口非 nil 而实际组件为 nil 的情况。
	var typedNil *payload.Gzip
	cases := [][]Option{
		{WithMiddleware(middleware.WithPayload(nil))},
		{WithMiddleware(middleware.WithPayload(typedNil))},
		{WithMiddleware(middleware.WithPayload(&payload.Gzip{}), middleware.WithPayload(&payload.Gzip{}))},
		{WithEncodingCodecs(payload.GzipCodecID)},
	}
	for i, options := range cases {
		config := spec.Defaults()
		for _, option := range options {
			option(&config)
		}
		if err := config.Validate(); err == nil {
			t.Fatalf("configuration %d accepted", i)
		}
	}
}
