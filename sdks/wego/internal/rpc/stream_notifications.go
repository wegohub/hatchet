package rpc

import (
	"context"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

// startObservers 在提交锁内启动资源，finish 的等待屏障之后不得再次调用
func (s *taskClientStream) startObservers(ref ports.Run) {
	s.background.Add(1)
	go s.observeResult(ref)
	if s.desc.ServerStreams {
		s.background.Add(1)
		go s.observeOutput(ref.ID)
	}
	if s.notices != nil {
		s.background.Add(1)
		go s.sendNotifications()
	}
}

// sendNotifications 独立串行发送旧 writer 取消；缓慢对象存储不会阻塞 DATA 消费
func (s *taskClientStream) sendNotifications() {
	defer s.background.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case notice := <-s.notices:
			budget, cancel := context.WithTimeout(s.ctx, 10*time.Second)
			err := s.engine.Backend.Feature(budget, notice, nil)
			cancel()
			if err != nil && s.ctx.Err() == nil {
				s.engine.Config.Logger.Warn("old execution notification failed", "task_id", notice.TaskID, "worker_key", notice.WorkerKey, "old_epoch", notice.OldEpoch, "error", err)
			}
		}
	}
}
