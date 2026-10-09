package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestRoutingAndProtoJSONEnvelope 把 protobuf 请求编码为 envelope，检查二进制往返和独立 routing 投影都正确
func TestRoutingAndProtoJSONEnvelope(t *testing.T) {
	// in 请求包含文本 protobuf、分组 one 和 Count=9，分别核对二进制往返与显式 routing 投影
	in := &pb.Request{Message: "protobuf", GroupKey: "one", Count: 9}
	// e, err 接收 context.Background 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	e, err := Encode(context.Background(), "method", in, map[string]any{"group": "one"}, nil, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Routing) != 1 || e.Routing["group"] != "one" {
		t.Fatal(e.Routing)
	}
	// data, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷
	data, _ := json.Marshal(e)
	// stored JSON envelope 解码结果，检查 payload 的 base64 与 routing 独立字段
	var stored map[string]any
	if err = json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	e, err = AsEnvelope(string(data))
	if err != nil {
		t.Fatal(err)
	}
	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
	var out pb.Request
	if err = Decode(context.Background(), "method", e, &out, nil, 1024); err != nil || !proto.Equal(in, &out) {
		t.Fatalf("%v: %v", &out, err)
	}
	if payload, ok := stored["payload"].(map[string]any); !ok || payload["group_key"] != "one" {
		t.Fatalf("ProtoJSON payload is not readable: %#v", stored["payload"])
	}
}

// TestStatusDetailsAndNonRetryable 编码并恢复带业务 details 的 gRPC 错误，检查 code、details 和不可重试标记
func TestStatusDetailsAndNonRetryable(t *testing.T) {
	// s, _ 构造或读取标准 gRPC 状态，保留 code 与业务 details
	s, _ := status.New(codes.InvalidArgument, "invalid").WithDetails(&errdetails.BadRequest{
		FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: "id", Description: "required"}},
	})
	// err 接收 s.Err 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	err := DecodeError("task failed: " + EncodeError(&model.NonRetryableError{Err: s.Err()}))
	// marked 禁止重试错误包装的识别目标，验证 errors.As 与状态编码均保留标记
	var marked *model.NonRetryableError
	if !errors.As(err, &marked) || status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if len(status.Convert(err).Details()) != 2 {
		t.Fatal("status details lost")
	}
}

// TestWrappedContextErrorsKeepStatus 包装 Canceled 和 DeadlineExceeded 后编码解码，确认 gRPC 状态不会退化为 Unknown
func TestWrappedContextErrorsKeepStatus(t *testing.T) {
	// 逐项处理 []error{context.Canceled, context.DeadlineExceeded}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		// err 接收 fmt.Errorf 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		err := DecodeError(EncodeError(fmt.Errorf("handler wait: %w", cause)))
		if status.Code(err) != status.FromContextError(cause).Code() {
			t.Fatalf("context code lost: %v", err)
		}
	}
}

// TestErrorMetadataDoesNotPolluteBusinessDetails 恢复 headers / trailers 的同时检查业务 status details 数量与内容不变
func TestErrorMetadataDoesNotPolluteBusinessDetails(t *testing.T) {
	// original, _ 构造或读取标准 gRPC 状态，保留 code 与业务 details
	original, _ := status.New(codes.FailedPrecondition, "handler failed").WithDetails(&errdetails.BadRequest{})
	// input 同时携带业务 status details、headers 与 trailers，错误传输必须完整恢复
	input := &model.RPCError{
		Err:      &model.NonRetryableError{Err: original.Err()},
		Headers:  map[string][]string{"worker": {"ready"}},
		Trailers: map[string][]string{"state": {"failed"}},
	}
	// err 当前操作产生的错误；nil 表示该步骤成功
	err := DecodeError(EncodeError(input))
	// transport 携带响应 metadata 的 wego RPCError 目标，headers / trailers 不混入业务 details
	var transport *model.RPCError
	// nonretry 禁止重试错误的识别目标；转换后只保留 wego 或标准错误链
	var nonretry *model.NonRetryableError
	if !errors.As(err, &transport) || !errors.As(err, &nonretry) || status.Code(err) != codes.FailedPrecondition || len(status.Convert(err).Details()) != 2 || transport.Headers["worker"][0] != "ready" || transport.Trailers["state"][0] != "failed" {
		t.Fatalf("transport/status lost: %v", err)
	}
}
