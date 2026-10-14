package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestReadableErrorReport 检查 Hatchet 实际收到 JSON 对象，控制台可以读取错误码、消息和业务详情。
func TestReadableErrorReport(t *testing.T) {
	original, err := status.New(codes.InvalidArgument, "missing id").WithDetails(&errdetails.BadRequest{
		FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: "id", Description: "required"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	text := EncodeError(original.Err())
	if !bytes.Contains([]byte(text), []byte(`"code_name":"InvalidArgument"`)) ||
		!bytes.Contains([]byte(text), []byte(`"message":"missing id"`)) ||
		!bytes.Contains([]byte(text), []byte(`"fieldViolations"`)) ||
		!bytes.Contains([]byte(text), []byte(`"value_base64"`)) {
		t.Fatalf("错误内容不可读: %s", text)
	}
	payload := ErrorReportPayload(text)
	if _, ok := payload.(json.RawMessage); !ok {
		t.Fatalf("错误被再次编码为字符串: %T", payload)
	}
	transport, err := json.Marshal(payload)
	if err != nil || len(transport) == 0 || transport[0] != '{' {
		t.Fatalf("Hatchet 失败载荷不是对象: %s, %v", transport, err)
	}
	restored := DecodeError("task failed: " + string(transport))
	if restored == nil || !proto.Equal(original.Proto(), status.Convert(restored).Proto()) {
		t.Fatalf("gRPC 错误未还原: %v", restored)
	}
	if got := ErrorReportPayload("native failure"); got != "native failure" {
		t.Fatalf("原生错误被改写: %#v", got)
	}
}

// TestUnknownErrorDetail 保留未注册 Any 的类型和字节，空字节值也不能丢失。
func TestUnknownErrorDetail(t *testing.T) {
	original := &spb.Status{Code: int32(codes.FailedPrecondition), Message: "unknown detail", Details: []*anypb.Any{
		{TypeUrl: "type.example.invalid/private.Detail", Value: []byte{0, 1, 2}},
		{TypeUrl: "type.example.invalid/private.Empty"},
	}}
	text := EncodeError(status.FromProto(original).Err())
	if !bytes.Contains([]byte(text), []byte(`"value_base64":"AAEC"`)) ||
		!bytes.Contains([]byte(text), []byte(`"value_base64":""`)) {
		t.Fatalf("未知详情未无损保存: %s", text)
	}
	restored := DecodeError(text)
	if restored == nil || !proto.Equal(original, status.Convert(restored).Proto()) {
		t.Fatalf("未知详情未还原: %v", restored)
	}
}

// TestEmptyRPCErrorMetadata 即使失败没有响应头尾，也保留 RPCError 传输身份。
func TestEmptyRPCErrorMetadata(t *testing.T) {
	original := &model.RPCError{Err: status.Error(codes.Aborted, "handler rejected")}
	restored := DecodeError(EncodeError(original))
	// transport 验证空响应头尾不会使 RPC 错误退化为普通 status。
	var transport *model.RPCError
	if !errors.As(restored, &transport) || status.Code(restored) != codes.Aborted {
		t.Fatalf("空 metadata 丢失 RPC 错误身份: %v", restored)
	}
}

// TestErrorProtocolValidation 拒绝畸形协议，同时保留旧运行的错误读取能力。
func TestErrorProtocolValidation(t *testing.T) {
	for _, text := range []string{
		`{"_wego_error":4,"code":3,"code_name":"InvalidArgument","message":"bad"}`,
		`{"_wego_error":3,"code":0,"code_name":"OK","message":"bad"}`,
		`{"_wego_error":3,"code":3,"code_name":"Unknown","message":"bad"}`,
		`{"_wego_error":3,"code":3,"code_name":"InvalidArgument","message":"bad","details":[{"type_url":"unknown","value_base64":"!"}]}`,
	} {
		if got := status.Code(DecodeError(text)); got != codes.DataLoss {
			t.Fatalf("畸形错误未拒绝: %s => %s", text, got)
		}
	}
	if DecodeError("plain failure") != nil {
		t.Fatal("原生错误被识别成 wego 协议")
	}
	old := encodeLegacyError(errors.New("old failure"))
	if got := DecodeError(old); got == nil || status.Convert(got).Message() != "old failure" {
		t.Fatalf("历史错误无法读取: %v", got)
	}
}
