package wire

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestProtoJSONSpecialValues 验证 64 位整数、bytes、well-known 和 oneof 的 JSON 规则往返
func TestProtoJSONSpecialValues(t *testing.T) {
	for _, message := range []proto.Message{
		wrapperspb.Int64(9007199254740993), wrapperspb.Bytes([]byte{0, 255, 128}),
		&descriptorpb.FieldDescriptorProto{Name: proto.String("field"), Number: proto.Int32(0), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()},
		timestamppb.New(time.Unix(1700000000, 123)), structpb.NewStringValue("oneof-value"),
	} {
		envelope, err := Encode(context.Background(), "/fixture/Call", message, nil, nil, 1024)
		if err != nil {
			t.Fatal(err)
		}
		out := message.ProtoReflect().New().Interface()
		if err := Decode(context.Background(), "/fixture/Call", envelope, out, nil, 1024); err != nil || !proto.Equal(message, out) {
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
