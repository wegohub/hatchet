package backend

import (
	"context"
	"testing"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// resultAdminFixture 返回权威持久记录，测试不借助结果通知伪造完成
type resultAdminFixture struct {
	// AdminServiceClient 的其余管理方法不参与此结果解码测试
	v1.AdminServiceClient
	// details 是当前运行真实状态的受控快照
	details *v1.GetRunDetailsResponse
}

// GetRunDetails 保持正式方法签名，包含完成状态和单任务输出
func (f *resultAdminFixture) GetRunDetails(context.Context, *v1.GetRunDetailsRequest, ...grpc.CallOption) (*v1.GetRunDetailsResponse, error) {
	return f.details, nil
}

// TestPersistedTerminalIgnoresEmptySuccessError 验证官方成功记录的空 error 指针不会变成假失败
func TestPersistedTerminalIgnoresEmptySuccessError(t *testing.T) {
	empty := ""
	details := &v1.GetRunDetailsResponse{Done: true, Status: v1.RunStatus_COMPLETED,
		TaskRuns: map[string]*v1.TaskRunDetail{"external-id": {ReadableId: "task", Status: v1.RunStatus_COMPLETED, Error: &empty, Output: []byte(`{"value":1}`)}}}
	b := &Backend{v1admin: &resultAdminFixture{details: details}}
	result, done, err := b.persistedResult(context.Background(), "run")
	if err != nil || !done || result.Outputs["task"] == nil {
		t.Fatalf("success: result=%v done=%v error=%v", result, done, err)
	}
	details.Done, details.Status = false, v1.RunStatus_RUNNING
	_, done, err = b.persistedResult(context.Background(), "run")
	if done || err != nil {
		t.Fatalf("running notification became terminal: %v %v", done, err)
	}
	details.Done, details.Status = true, v1.RunStatus_CANCELLED
	if _, done, err := b.persistedResult(context.Background(), "run"); !done || status.Code(err) != codes.Canceled {
		t.Fatal(done, err)
	}
}

// TestLookupUsesExplicitTaskExternalID 防止正式响应的 readable_id map 键被误当作 task 身份
func TestLookupUsesExplicitTaskExternalID(t *testing.T) {
	b := &Backend{v1admin: &resultAdminFixture{details: &v1.GetRunDetailsResponse{TaskRuns: map[string]*v1.TaskRunDetail{"readable-task": {ExternalId: "actual-task-id", ReadableId: "readable-task"}}}}}
	id, ref, err := b.LookupRun(context.Background(), "actual-run-id")
	if err != nil || id != "actual-task-id" || ref.ID != "actual-run-id" {
		t.Fatal(id, ref.ID, err)
	}
}
