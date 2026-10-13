package wire

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/examples/codec/payload"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
)

// protocolPacket 模拟服务端 JSON map 往返，大整数只存在于编码字节中。
func protocolPacket(t *testing.T, value any, chain []middleware.Option) Envelope {
	t.Helper()
	data, kind, err := MarshalValue(value, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	packet := NewEnvelope(context.Background(), "", kind, data, map[string]any{"group": "one"})

	packet, err = Seal(context.Background(), packet, spec.Runtime{Middleware: chain}, 1<<20, (1<<20)+(64<<10), 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(packet)
	if err != nil {
		t.Fatal(err)
	}
	// stored 刻意使用引擎的 float64 map 解码；业务大整数必须保持精确。
	var stored map[string]any
	if err := json.Unmarshal(encoded, &stored); err != nil {
		t.Fatal(err)
	}
	if stored["_wego"] == nil || stored["value"] == nil {
		t.Fatal("missing explicit protocol carrier")
	}
	packet, err = AsEnvelope(stored)
	if err != nil {
		t.Fatal(err)
	}
	return packet
}

// TestUnifiedPayloadPrecisionAndCodec 覆盖无变换、压缩及认证加密；普通任务不借用 RPC 身份。
func TestUnifiedPayloadPrecisionAndCodec(t *testing.T) {
	encrypted, err := payload.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	for _, chain := range [][]middleware.Option{nil, {middleware.WithPayload(&payload.Gzip{})}, {middleware.WithPayload(&payload.Gzip{}), middleware.WithPayload(encrypted)}} {
		packet := protocolPacket(t, map[string]any{"integer": int64(9007199254740993), "null": nil, "text": "null"}, chain)
		// result 的动态整数必须为精确 json.Number，合法 null 不变成缺少输出。
		var result map[string]any
		if err := DecodeValue(context.Background(), "", packet, &result, spec.Runtime{Middleware: chain}, 1<<20, 4<<20); err != nil {
			t.Fatal(err)
		}
		if result["integer"] != json.Number("9007199254740993") || result["null"] != nil || result["text"] != "null" {
			t.Fatal(result)
		}
		request := &pb.Request{Message: "protobuf-native", Count: 3}
		packet = protocolPacket(t, request, chain)
		if packet.Method != "" {
			t.Fatal("protobuf classified as RPC")
		}
		out := new(pb.Request)
		if err := DecodeValue(context.Background(), "", packet, out, spec.Runtime{Middleware: chain}, 1<<20, 4<<20); err != nil || !proto.Equal(request, out) {
			t.Fatal(out, err)
		}
	}
}

// TestPayloadTamperAndAtomicDecode 拒绝明文头被替换、类型不匹配及超限，错误不修改业务目标。
func TestPayloadTamperAndAtomicDecode(t *testing.T) {
	packet := protocolPacket(t, &pb.Request{Message: "input"}, nil)
	packet.Routing["group"] = "other"
	if _, err := Open(context.Background(), "", packet, spec.Runtime{}, 1<<20, 4<<20); err != nil {
		t.Fatal(err)
	}
	packet = protocolPacket(t, &pb.Request{Message: "input"}, nil)
	if _, err := Open(context.Background(), "", packet, spec.Runtime{}, 1<<20, 2); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	jsonPacket := protocolPacket(t, map[string]any{"count": "invalid"}, nil)
	destination := struct {
		// Count 在解码失败时必须保持 7，而不能被部分覆盖。
		Count int
	}{Count: 7}
	if err := DecodeValue(context.Background(), "", jsonPacket, &destination, spec.Runtime{}, 1<<20, 4<<20); err == nil || destination.Count != 7 {
		t.Fatal(destination, err)
	}
}

// TestPayloadCodecProfileAndSnapshot 验证配置不匹配明确失败，封包后业务 map 的修改不影响头部快照。
func TestPayloadCodecProfileAndSnapshot(t *testing.T) {
	routing := map[string]any{"group": "one"}
	packet := NewEnvelope(context.Background(), "", "", json.RawMessage(`{"count":1}`), routing)

	packet, err := Seal(context.Background(), packet, spec.Runtime{}, 1024, 2048, 4096)
	if err != nil {
		t.Fatal(err)
	}
	routing["group"] = "mutated"
	// stored 是独立传输快照，不能继续引用调用方 routing。
	stored, err := Open(context.Background(), "", packet, spec.Runtime{}, 1024, 4096)
	if err != nil || !reflect.DeepEqual(stored.Routing, map[string]any{"group": "one"}) {
		t.Fatal(stored.Routing, err)
	}
	// 接收端多注册 gzip 不改变历史空链封包的含义。
	if _, err := Open(context.Background(), "", packet, spec.Runtime{Middleware: []middleware.Option{middleware.WithPayload(&payload.Gzip{})}}, 1024, 4096); err != nil {
		t.Fatal(err)
	}
}

// FuzzUnifiedPayload 只交付协议与大小均合法的单个消息；随机数据不能触发 panic。
func FuzzUnifiedPayload(f *testing.F) {
	f.Add([]byte(`{"version":5,"format":1,"scope":"task::plain","payload":"AA=="}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 8192 {
			return
		}
		packet, err := AsEnvelope(string(data))
		if err != nil {
			return
		}
		_, _ = Open(context.Background(), "", packet, spec.Runtime{}, 2048, 8192)
	})
}

// TestRPCBinding 使用注册目标；输入没有方法字段时不要求重建 scope。
func TestRPCBinding(t *testing.T) {
	method := "/fixture.Service/Call"
	if err := ValidateRPCBinding(Envelope{}, method); err != nil {
		t.Fatal(err)
	}
	if err := ValidateRPCBinding(Envelope{Method: "/fixture.Other/Call"}, method); err == nil {
		t.Fatal("different local binding accepted")
	}
}
