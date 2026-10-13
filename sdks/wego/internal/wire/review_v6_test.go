package wire

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// reviewCodec 记录变换次数，证明非法头和过期请求在外部 I/O 前被拒绝。
type reviewCodec struct {
	// encodes 是当前串行用例实际执行的发送步骤数。
	encodes int
	// decodes 是当前串行用例实际执行的还原步骤数。
	decodes int
}

// CodecID 固定测试格式，与发送链和接收查找使用同一标识。
func (*reviewCodec) CodecID() string { return "test.review.v1" }

// Encode 不改变输入，测试只观察是否错误地越过前置校验。
func (c *reviewCodec) Encode(_ context.Context, data []byte) ([]byte, error) {
	c.encodes++
	return data, nil
}

// Decode 复用有界入口，避免 fixture 的两条解码路径产生不同计数。
func (c *reviewCodec) Decode(ctx context.Context, data []byte) ([]byte, error) {
	return c.DecodeLimit(ctx, data, len(data))
}

// DecodeLimit 故意不检查 context，SDK 必须在调用组件前拒绝已到期请求。
func (c *reviewCodec) DecodeLimit(_ context.Context, data []byte, _ int) ([]byte, error) {
	c.decodes++
	return data, nil
}

// TestReviewV6AmbiguousHeader 协议控制字段不能依赖 JSON 的最后一个键或大小写匹配规则。
func TestReviewV6AmbiguousHeader(t *testing.T) {
	for _, raw := range []string{
		`{"_wego":{"version":6,"response":null},"value":{}}`,
		`{"_wego":{"version":6,"Response":"wego"},"value":{}}`,
		`{"_wego":{"version":6,"response":"wego","response":"native"},"value":{}}`,
		`{"_wego":{"version":6,"codec_ids":null},"value":{}}`,
		`{"_wego":{"version":6},"value":1,"value":2}`,
		`{"_wego":null,"_wego":{"version":6},"value":{}}`,
	} {
		if _, err := AsEnvelope(raw); err == nil {
			t.Errorf("ambiguous protocol accepted: %s", raw)
		}
	}
	// 未知可选扩展及业务 value 内的同名字段仍然合法。
	if _, err := AsEnvelope(`{"_wego":{"version":6,"extension":null},"value":{"response":null}}`); err != nil {
		t.Fatal(err)
	}
}

// TestReviewV6HeaderValidationPrecedesCodec 无效头不能先上传对象或执行随机加密。
func TestReviewV6HeaderValidationPrecedesCodec(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		codec := &reviewCodec{}
		config := spec.Defaults()
		config.Middleware = []middleware.Option{middleware.WithPayload(codec)}
		packet := NewEnvelope(context.Background(), "", "", json.RawMessage(`{}`), nil)
		packet.Metadata = map[string][]string{"plain": {string([]byte{255})}}
		if oversized {
			packet.Metadata["plain"] = []string{strings.Repeat("x", 64<<10)}
		}
		if _, err := Seal(context.Background(), packet, config, 1024, 2048, 4<<20); err == nil || codec.encodes != 0 {
			t.Fatalf("header error=%v, external encode calls=%d", err, codec.encodes)
		}
	}
}

// TestReviewV6ExecutionDeadlineDoesNotExpireHistory 执行输入受原 deadline 限制，历史读取使用观察者预算。
func TestReviewV6ExecutionDeadlineDoesNotExpireHistory(t *testing.T) {
	codec := &reviewCodec{}
	config := spec.Defaults()
	config.Middleware = []middleware.Option{middleware.WithPayload(codec)}
	packet := NewEnvelope(context.Background(), "", "", json.RawMessage(`{}`), nil)
	packet.Deadline = time.Now().Add(-time.Hour).UnixNano()
	sealed, err := Seal(context.Background(), packet, config, 1024, 2048, 4096)
	if err != nil {
		t.Fatal(err)
	}
	execution, cancel := Incoming(context.Background(), sealed)
	defer cancel()
	if _, err := Open(execution, "", sealed, config, 1024, 4096); !errors.Is(err, context.DeadlineExceeded) || codec.decodes != 0 {
		t.Fatalf("expired request error=%v, external decode calls=%d", err, codec.decodes)
	}
	if _, err := Open(context.Background(), "", sealed, config, 1024, 4096); err != nil || codec.decodes != 1 {
		t.Fatalf("historical input error=%v, decode calls=%d", err, codec.decodes)
	}
}
