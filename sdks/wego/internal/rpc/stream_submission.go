package rpc

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// resolveSubmission 在失败之后只读找回已接受的运行；不能为了找 RunID 创建新的逻辑提交
func (s *taskClientStream) resolveSubmission(input wire.Envelope, cause error) (ports.Run, error) {
	failure := &model.SubmissionError{Err: cause, OperationKey: s.operationKey}
	finder, ok := s.engine.Backend.(ports.SubmissionFinder)
	if !ok {
		return ports.Run{}, failure
	}
	// 独立短预算不阻塞取消长时间；未确认身份时由业务持有的幂等键继续恢复
	budget, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), min(time.Second, s.options.RecoveryTimeout))
	defer cancel()
	for {
		ref, err := finder.FindSubmission(budget, s.submission, s.method, input.InputDigest)
		if err == nil {
			failure.RunID = ref.ID
			s.mu.Lock()
			s.runID = ref.ID
			s.mu.Unlock()
			if err := s.engine.Backend.Feature(budget, ports.RunsCancel{Request: map[string]any{"externalIds": []string{ref.ID}}}, nil); err != nil {
				s.engine.Config.Logger.Warn("recovered submission cancellation failed", "run_id", ref.ID, "error", err)
			}
			return ports.Run{}, failure
		}
		code := status.Code(err)
		if budget.Err() != nil || code != codes.NotFound && code != codes.Unavailable && code != codes.DeadlineExceeded && code != codes.Unknown {
			s.engine.Config.Logger.Warn("submission identity remains unconfirmed", "submission", s.submission, "error", err)
			return ports.Run{}, failure
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-budget.Done():
			timer.Stop()
			return ports.Run{}, failure
		case <-timer.C:
		}
	}
}
