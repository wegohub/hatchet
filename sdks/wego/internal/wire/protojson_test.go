package wire

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// TestProtoJSONSpecialValues 验证 64 位整数、bytes、well-known 和 oneof 的 JSON 规则往返
func TestProtoJSONSpecialValues(t *testing.T) {
	for _, message := range []proto.Message{
		wrapperspb.Int64(9007199254740993), wrapperspb.Bytes([]byte{0, 255, 128}),
		&descriptorpb.FieldDescriptorProto{Name: proto.String("field"), Number: proto.Int32(0), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()},
		timestamppb.New(time.Unix(1700000000, 123)), structpb.NewStringValue("oneof-value"),
	} {
		envelope, err := Encode(context.Background(), "/fixture/Call", message, nil, spec.Runtime{}, 1024)
		if err != nil {
			t.Fatal(err)
		}
		out := message.ProtoReflect().New().Interface()
		if err := Decode(context.Background(), "/fixture/Call", envelope, out, spec.Runtime{}, 1024); err != nil || !proto.Equal(message, out) {
			t.Fatalf("%T roundtrip: %v", message, err)
		}
	}
	// 64 位整数按 protobuf JSON 字符串表达，不将它转换为 float64
	encoded, err := MarshalMessage(wrapperspb.Int64(9007199254740993), 1024)
	if err != nil || !strings.Contains(string(encoded), `"9007199254740993"`) {
		t.Fatalf("large integer lost: %s %v", encoded, err)
	}
	if _, err := MarshalMessage(&timestamppb.Timestamp{Seconds: 253402300800}, 1024); err == nil {
		t.Fatal("invalid timestamp accepted")
	}
}

// TestProtoJSONArrayEncoding 验证数组的 oneof 与 64 位整数仍使用 ProtoJSON，不使用生成结构体的 JSON 标签。
func TestProtoJSONArrayEncoding(t *testing.T) {
	for _, values := range []any{
		[]*structpb.Value{structpb.NewStringValue("oneof"), structpb.NewBoolValue(true)},
		[]any{structpb.NewStringValue("oneof"), structpb.NewBoolValue(true)},
		[]proto.Message{wrapperspb.Int64(9007199254740993), wrapperspb.Int64(9007199254740995)},
	} {
		data, name, err := MarshalValue(values, 1024)
		if err != nil || name == "" {
			t.Fatal(name, err)
		}
		// parts 保留实际 ProtoJSON 元素，逐项验证特殊值的标准表达。
		var parts []json.RawMessage
		if err := json.Unmarshal(data, &parts); err != nil || len(parts) != 2 {
			t.Fatal(string(data), err)
		}
		if name == "google.protobuf.Value" && (string(parts[0]) != `"oneof"` || string(parts[1]) != "true") {
			t.Fatal(string(data))
		}
		if name == "google.protobuf.Int64Value" && string(parts[0]) != `"9007199254740993"` {
			t.Fatal(string(data))
		}
	}
	for _, values := range []any{[]proto.Message{wrapperspb.Int64(1), wrapperspb.String("mixed")}, []*wrapperspb.Int64Value{nil}, []any{wrapperspb.Int64(1), 7}} {
		if _, _, err := MarshalValue(values, 1024); status.Code(err) != codes.InvalidArgument {
			t.Fatal("invalid protobuf array", err)
		}
	}
	data, name, err := MarshalValue([]*wrapperspb.Int64Value(nil), 2)
	if err != nil || string(data) != "[]" || name != "" {
		t.Fatal(string(data), name, err)
	}
	if _, _, err := MarshalValue([]*wrapperspb.Int64Value{wrapperspb.Int64(123)}, 4); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("array budget", err)
	}
}
