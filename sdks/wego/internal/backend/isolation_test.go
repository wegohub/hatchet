package backend

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestErrorsDoNotRetainBackendInstances 构造后端幂等错误并转换，遍历错误链确认不存在后端实例，同时保留冲突 RunID
func TestErrorsDoNotRetainBackendInstances(t *testing.T) {
	// input 保存含当前失败条件的明确错误，后续不得继续沿成功路径执行
	input := fmt.Errorf("wrapped: %w", &v0.IdempotencyViolationErr{ExistingRunExternalId: "run"})
	// err 当前操作产生的错误；nil 表示该步骤成功
	err := Normalize(input)
	// conflict errors.As 的幂等冲突目标，用于读取已存在运行身份
	var conflict *model.IdempotencyCollisionError
	if !errors.As(err, &conflict) || conflict.ExistingRunID != "run" {
		t.Fatal(err)
	}
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
	for e := err; e != nil; e = errors.Unwrap(e) {
		if reflect.TypeOf(e).String() == "*client.IdempotencyViolationErr" {
			t.Fatal("backend error leaked")
		}
	}
	// s, _ 构造或读取标准 gRPC 状态，保留 code 与业务 details
	s, _ := status.New(codes.Aborted, "backend status").WithDetails(&v1.IdempotencyCollisionError{ExistingRunExternalId: "run"})
	// normalized 复制后端错误为 wego 错误或 gRPC status，错误链不保留后端实例
	normalized := Normalize(s.Err())
	// 逐项处理 status.Convert(normalized).Details()，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, detail := range status.Convert(normalized).Details() {
		// _, ok 接收 detail. 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
		if _, ok := detail.(*v1.IdempotencyCollisionError); ok {
			t.Fatal("backend protobuf leaked through status details")
		}
	}
}
