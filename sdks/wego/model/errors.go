package model

import (
	"errors"
	"fmt"
	"google.golang.org/grpc/status"
)

// ErrBorrowedResource 借用视图尝试关闭共享资源时返回的哨兵错误。
var ErrBorrowedResource = errors.New("wego: borrowed resource cannot be closed")

// ErrClosed 实例已关闭或正在拒绝新增调用时返回的哨兵错误。
var ErrClosed = errors.New("wego: resource is closed")

// ErrTaskContext 当前 context 不包含任务执行身份时返回；网络 handler 调用任务能力会遇到此错误。
var ErrTaskContext = errors.New("wego: task execution context is required")

// ErrDurableContext 当前 context 不具备 durable 等待或重放能力时返回。
var ErrDurableContext = errors.New("wego: durable task execution context is required")

// IdempotencyCollisionError 单次提交的幂等冲突，携带已存在的 RunID 供调用方复用结果。
type IdempotencyCollisionError struct {
	// ExistingRunID 幂等冲突时已经存在的运行身份，调用方可据此查询结果。
	ExistingRunID string
}

// BulkIdempotencyCollisionError 批量提交的部分成功与冲突记录，不能把整批简单当作全部失败。
type BulkIdempotencyCollisionError struct {
	// SuccessfulRunIDs 批量提交中已成功创建的运行身份，顺序由后端返回。
	SuccessfulRunIDs []string
	// Collisions 批量提交中的幂等冲突记录。
	Collisions []*IdempotencyCollisionError
	// Err 保留各块的其他失败原因，例如同时发生冲突与超时。
	Err error
}

// Error 返回供日志或调用方读取的错误文本，不额外暴露后端错误对象。
func (e *BulkIdempotencyCollisionError) Error() string {
	return fmt.Sprintf("wego: batch trigger contains %d idempotency conflicts", len(e.Collisions))
}

// Error 返回供日志或调用方读取的错误文本，不额外暴露后端错误对象。
func (e *IdempotencyCollisionError) Error() string {
	return fmt.Sprintf("wego: idempotency collision with run %s", e.ExistingRunID)
}

// NonRetryableError 禁止引擎重试的错误标记，保留 wego 或标准错误链。
type NonRetryableError struct {
	// Err 包装的 wego 或标准库错误；不可保存后端错误实例。
	Err error
}

// Error 返回供日志或调用方读取的错误文本，不额外暴露后端错误对象。
func (e *NonRetryableError) Error() string {
	return e.Err.Error()
}

// Unwrap 返回当前包装的 wego 或标准错误，使 errors.Is / As 可以识别原因。
func (e *NonRetryableError) Unwrap() error {
	return e.Err
}

// GRPCStatus 保留包装错误的 gRPC 状态，使 status.Code 和 details 可读取。
func (e *NonRetryableError) GRPCStatus() *status.Status {
	return status.Convert(e.Err)
}

// RPCError 在 handler 返回错误时仍携带响应 headers 和 trailers。
type RPCError struct {
	// Err 包装的 wego 或标准库错误；不可保存后端错误实例。
	Err error
	// Headers 响应头 metadata，可在业务响应之前交付。
	Headers map[string][]string
	// Trailers 响应尾部 metadata，与最终状态一起读取。
	Trailers map[string][]string
}

// Error 返回供日志或调用方读取的错误文本，不额外暴露后端错误对象。
func (e *RPCError) Error() string {
	return e.Err.Error()
}

// Unwrap 返回当前包装的 wego 或标准错误，使 errors.Is / As 可以识别原因。
func (e *RPCError) Unwrap() error {
	return e.Err
}

// GRPCStatus 保留包装错误的 gRPC 状态，使 status.Code 和 details 可读取。
func (e *RPCError) GRPCStatus() *status.Status {
	return status.Convert(e.Err)
}

// PartialSubmissionError 表示提交已有部分成功；重试必须排除这些已确认的 RunID。
type PartialSubmissionError struct {
	// SuccessfulRunIDs 例如首块已确认的 1000 个运行，即使后一块取消也保留。
	SuccessfulRunIDs []string
	// Err 仅含 wego、标准库或 gRPC 错误，不携带 Hatchet 错误实例。
	Err error
}

// Error 同时报告部分成功数量和失败原因。
func (e *PartialSubmissionError) Error() string {
	return fmt.Sprintf("wego: %d runs accepted before submission failed: %v", len(e.SuccessfulRunIDs), e.Err)
}

// Unwrap 保留取消原因，允许 errors.Is 检查 deadline。
func (e *PartialSubmissionError) Unwrap() error { return e.Err }

// Unwrap 保留批量提交的完整错误链，errors.Is 仍可识别其他块的取消或超时。
func (e *BulkIdempotencyCollisionError) Unwrap() error { return e.Err }
