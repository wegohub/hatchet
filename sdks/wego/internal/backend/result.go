package backend

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"time"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

// runObservationError 将可重连的观察失败与同 code 的业务最终失败分开
// handler 返回 Unavailable 仍是业务失败，不能因此重新提交或无限重新等待
type runObservationError struct {
	// err 已规范化的连接/查询错误，不携带 Hatchet 错误实例
	err error
}

// Error 返回脱敏后的观察诊断
func (e *runObservationError) Error() string { return e.err.Error() }

// Unwrap 保留标准 context/status 判断能力
func (e *runObservationError) Unwrap() error { return e.err }

// retryObservation 只重试暂时观察失败，权限和协议错误立即交付
func retryObservation(err error) bool {
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.Unknown || code == codes.DeadlineExceeded || code == codes.NotFound
}

// persistedResult 读取引擎已接受的最终 attempt；旧通知中的 output/error 永远不作为结果
func (b *Backend) persistedResult(ctx context.Context, id string) (ports.Result, bool, error) {
	budget, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	details, err := b.v1admin.GetRunDetails(b.auth(budget), &v1.GetRunDetailsRequest{ExternalId: id})
	if err != nil {
		return ports.Result{}, false, &runObservationError{Normalize(err)}
	}
	if details == nil {
		return ports.Result{}, true, status.Error(codes.DataLoss, "wego: missing persisted run details")
	}
	if !details.Done {
		return ports.Result{}, false, nil
	}
	if details.Status == v1.RunStatus_CANCELLED {
		return ports.Result{}, true, status.Error(codes.Canceled, "wego: run was cancelled")
	}
	// 先检查所有错误，再解码输出，不让 map 顺序决定返回成功还是失败
	for _, task := range details.TaskRuns {
		if task != nil && task.Error != nil && *task.Error != "" {
			return ports.Result{}, true, Normalize(errors.New(*task.Error))
		}
	}
	if details.Status == v1.RunStatus_FAILED {
		return ports.Result{}, true, status.Error(codes.Unknown, "wego: run failed without persisted task error")
	}
	if details.Status != v1.RunStatus_COMPLETED {
		return ports.Result{}, true, status.Error(codes.DataLoss, "wego: nonterminal run marked done")
	}
	result := ports.Result{RunID: id, Outputs: map[string]any{}}
	for name, task := range details.TaskRuns {
		if task == nil {
			return ports.Result{}, true, status.Error(codes.DataLoss, "wego: nil persisted task detail")
		}
		if len(task.Output) > b.messageLimit() {
			return ports.Result{}, true, status.Error(codes.ResourceExhausted, "wego: persisted task output exceeds backend message budget")
		}
		if task.Output == nil {
			continue
		}
		// value 保留 null、数组和标量；不同任务按 readable_id 独立选择
		var value any
		if err := json.Unmarshal(task.Output, &value); err != nil {
			return ports.Result{}, true, status.Error(codes.DataLoss, "wego: malformed persisted task output")
		}
		if task.ReadableId != "" {
			name = task.ReadableId
		}
		result.Outputs[name] = value
	}
	return result, true, nil
}

// waitObservationRetry 使用有界抖动退避，网络恢复时多个运行不会同时密集重订阅
func waitObservationRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1)))
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
