package backend

import (
	"context"
	"errors"
	"fmt"
	"strings"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	sdk "github.com/hatchet-dev/hatchet/sdks/go"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// Normalize 将后端错误转换为 wego 错误或标准 gRPC status
// 错误链不保留 Hatchet 实例；幂等冲突保留已有 RunID，未知后端详情只保留类型标识
func Normalize(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	// e, ok 读取冲突运行身份并转换为 wego 自有错误，已有 RunID 必须保留
	if e, ok := sdk.IsIdempotencyCollisionError(err); ok {
		return &model.IdempotencyCollisionError{ExistingRunID: e.ExistingRunExternalId}
	}

	// highSingle 同时识别 SDK 错误的包装形式，errors.As 不依赖最外层动态类型
	var highSingle *sdk.IdempotencyCollisionError
	if errors.As(err, &highSingle) {
		return &model.IdempotencyCollisionError{ExistingRunID: highSingle.ExistingRunExternalId}
	}
	// conflict errors.As 的幂等冲突目标，用于读取已存在运行身份
	var conflict *v0.IdempotencyViolationErr
	if errors.As(err, &conflict) {
		return &model.IdempotencyCollisionError{ExistingRunID: conflict.ExistingRunExternalId}
	}

	// bulk 批量幂等冲突目标，保留已成功提交项及各冲突身份
	var bulk *v0.BulkIdempotencyViolationErr
	// 批量冲突不是全部提交失败；例如三项中成功两项、冲突一项，须同时返回两类身份
	if errors.As(err, &bulk) {
		// result 汇总批提交中已成功的 RunID 和冲突 RunID，完整保留部分成功信息
		result := &model.BulkIdempotencyCollisionError{
			SuccessfulRunIDs: append([]string(nil), bulk.SuccessfulRunExternalIds...),
		}
		// 逐项处理 bulk.Collisions，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for _, collision := range bulk.Collisions {
			result.Collisions = append(result.Collisions, &model.IdempotencyCollisionError{ExistingRunID: collision.ExistingRunExternalId})
		}
		return result
	}
	// highBulk 处理公开 Go SDK 返回的实际批量错误及包装形式，不能降为普通字符串
	var highBulk *sdk.BulkTriggerIdempotencyCollisionError
	if errors.As(err, &highBulk) {
		// result 当前完成结果，只有成功确认后才交付业务
		result := &model.BulkIdempotencyCollisionError{SuccessfulRunIDs: append([]string(nil), highBulk.SuccessfulRunExternalIds...)}
		// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
		for _, c := range highBulk.Collisions {
			result.Collisions = append(result.Collisions, &model.IdempotencyCollisionError{ExistingRunID: c.ExistingRunExternalId})
		}
		return result
	}
	// 自有协议提交直接收到 gRPC details；在脱敏前提取所有成功及冲突身份
	if s, ok := status.FromError(err); ok {
		// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
		for _, detail := range s.Details() {
			// switch d 取得 detail. 的结果，确认成功后才进入下一处理阶段
			switch d := detail.(type) {
			case *v1.IdempotencyCollisionError:
				return &model.IdempotencyCollisionError{ExistingRunID: d.ExistingRunExternalId}
			case *v1.BulkTriggerIdempotencyCollisionError:
				// result 当前完成结果，只有成功确认后才交付业务
				result := &model.BulkIdempotencyCollisionError{SuccessfulRunIDs: append([]string(nil), d.SuccessfulWorkflowRunExternalIds...)}
				// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
				for _, c := range d.Collisions {
					result.Collisions = append(result.Collisions, &model.IdempotencyCollisionError{ExistingRunID: c.ExistingRunExternalId})
				}
				return result
			}
		}
	}
	// 字符串包含 wego 错误协议时恢复原 gRPC code、details 和不可重试标记
	if e := wire.DecodeError(err.Error()); e != nil {
		return e
	}
	// 标准 gRPC 错误保留 code 与 Google details；未知后端 details 转成类型标记，避免泄漏后端消息类型
	if s, ok := status.FromError(err); ok {
		// result 取得 status 的 protobuf 表示，保留业务 code 和 details
		result := s.Proto()
		// 逐项处理 result.Details，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for i, detail := range result.Details {
			// 非 Google detail 不作为动态后端 protobuf 对象返回，只用 ErrorInfo 保存原 type_url
			if !strings.HasPrefix(detail.TypeUrl, "type.googleapis.com/google.") {
				// marker 自有 ErrorInfo 标记，只保存未知后端 detail 的 type_url，不返回后端 protobuf 实例
				marker := &errdetails.ErrorInfo{
					Reason:   "BACKEND_DETAIL",
					Domain:   "wego",
					Metadata: map[string]string{"type_url": detail.TypeUrl},
				}
				// sanitized, _ 构造或读取标准 gRPC 状态，保留 code 与业务 details
				sanitized, _ := status.New(codes.Code(result.Code), result.Message).WithDetails(marker)
				result.Details[i] = sanitized.Proto().Details[0]
			}
		}
		return status.FromProto(result).Err()
	}

	return fmt.Errorf("wego: %s", err.Error())
}

// toBackendError 将 status 和不可重试标记封装到引擎可传递的错误文本中
func toBackendError(err error) error {
	if err == nil {
		return nil
	}

	// message 是带协议前缀的任务错误文本，包含 gRPC 状态与 wego 标记，不携带后端错误实例
	message := wire.EncodeError(err)
	// nonretry 禁止重试错误的识别目标；转换后只保留 wego 或标准错误链
	var nonretry *model.NonRetryableError
	if errors.As(err, &nonretry) {
		return sdk.NewNonRetryableError(errors.New(message))
	}

	return errors.New(message)
}
