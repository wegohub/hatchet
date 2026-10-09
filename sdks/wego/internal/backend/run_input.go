package backend

import (
	"context"
	"encoding/json"
	"time"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/stream"
)

// RunInput 读取现有运行的持久输入；用于冲突恢复身份核验，不重新提交任务
func (b *Backend) RunInput(ctx context.Context, runID string) (json.RawMessage, error) {
	details, err := b.v1admin.GetRunDetails(b.auth(ctx), &v1.GetRunDetailsRequest{ExternalId: runID})
	if err != nil {
		return nil, Normalize(err)
	}
	if details == nil || len(details.Input) == 0 {
		return nil, status.Error(codes.DataLoss, "wego: persisted run input unavailable")
	}
	return append(json.RawMessage(nil), details.Input...), nil
}

// 编译检查保证恢复身份读取保持自有端口边界
var _ ports.RunInputReader = (*Backend)(nil)

// LookupRun 在提交后查询真实任务身份；没有唯一任务不能伪造 topic 名称
// LookupRun 查询提交后的唯一路由身份，读模型尚未可见时在阶段预算内退避
func (b *Backend) LookupRun(ctx context.Context, runID string) (string, ports.Run, error) {
	budget, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	delay := 100 * time.Millisecond
	for {
		details, err := b.v1admin.GetRunDetails(b.auth(budget), &v1.GetRunDetailsRequest{ExternalId: runID})
		if err == nil && details != nil && len(details.TaskRuns) > 0 {
			if len(details.TaskRuns) != 1 {
				return "", ports.Run{}, status.Error(codes.DataLoss, "wego: stream requires one persisted task")
			}
			for _, task := range details.TaskRuns {
				if task != nil && task.ExternalId != "" {
					return task.ExternalId, b.runRef(runID), nil
				}
			}
			return "", ports.Run{}, status.Error(codes.DataLoss, "wego: missing task identity")
		}
		if err != nil && !retryObservation(err) {
			return "", ports.Run{}, Normalize(err)
		}
		if budget.Err() != nil {
			return "", ports.Run{}, status.FromContextError(budget.Err()).Err()
		}
		if err := waitObservationRetry(budget, delay); err != nil {
			return "", ports.Run{}, status.FromContextError(err).Err()
		}
		delay = min(delay*2, 2*time.Second)
	}
}

// FindSubmission 用随机 submission 标识只读查询真实记录；不会创建取消之后才出现的新任务
func (b *Backend) FindSubmission(ctx context.Context, submission, method, digest string) (ports.Run, error) {
	pairs := []string{"wego_submission:" + submission}
	limit := int64(2)
	include := false
	rows, err := b.features.Runs().List(ctx, rest.V1WorkflowRunListParams{Since: time.Unix(0, 0), Limit: &limit, OnlyTasks: true, IncludePayloads: &include, AdditionalMetadata: &pairs})
	if err != nil {
		return ports.Run{}, Normalize(err)
	}
	if len(rows.Rows) == 0 {
		return ports.Run{}, status.Error(codes.NotFound, "wego: submitted run is not yet visible")
	}
	if len(rows.Rows) != 1 {
		return ports.Run{}, status.Error(codes.DataLoss, "wego: submission identity matched multiple runs")
	}
	row := rows.Rows[0]
	if row.WorkflowName == nil || *row.WorkflowName != binding.WorkflowName(b.config.Namespace, method) {
		return ports.Run{}, status.Error(codes.DataLoss, "wego: submission workflow identity mismatch")
	}
	id := row.WorkflowRunExternalId.String()
	input, err := b.RunInput(ctx, id)
	if err != nil {
		return ports.Run{}, err
	}
	if err := stream.VerifyRunIdentity(input, method, digest); err != nil {
		return ports.Run{}, err
	}
	return b.runRef(id), nil
}
