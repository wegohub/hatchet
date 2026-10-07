package server

import (
	"sync"
)

// Stop 取消所有入口，并允许中断正在等待业务完成的 GracefulStop。
func (s *Server) Stop() {
	s.forceOnce.Do(func() {
		close(s.force)
		s.cancelDrain()
	})
	s.stop()
}

// GracefulStop 拒绝新请求，并等待现有业务与流会话排空；无业务等待超时。
// 例如一个 bidi handler 仍在等待消息，优雅关闭会保留它的控制通道直到终态；Stop 可主动中断。
func (s *Server) GracefulStop() {
	s.stop()
}

// stop 协调单次停止流程，先等待初始化屏障，再并行关闭拥有的入口资源。
func (s *Server) stop() {
	s.mu.Lock()
	s.stopping = true
	// started 入口是否已经启动；与停止并发时在锁内读写。
	started := s.started
	s.mu.Unlock()
	s.cancelStart()

	s.stopOnce.Do(func() {
		go func() {
			defer close(s.stopDone)
			defer s.cancelDrain()
			if started {
				// 初始化可能正在发布资源；屏障之后资源列表不会再增加。
				<-s.initialized
			}
			s.closeEngines()
		}()
	})
	<-s.stopDone
}

// closeEngines 并行关闭网络和 Worker 入口，业务完成后再释放传输与观测资源。
func (s *Server) closeEngines() {
	// wg 等待本组并发步骤结束，资源关闭前必须确认所有已启动步骤退出。
	var wg sync.WaitGroup
	if s.network != nil {
		wg.Go(func() {
			// 先关闭接收入口，再等待业务；避免原生无限排空阻止强制取消。
			_ = s.listener.Close()
			s.networkCalls.drain(s.force)
			// 等待排空状态或强制停止信号；Stop 可以打断 GracefulStop 的持续等待。
			select {
			case <-s.force:
				s.network.Stop()
			default:
				s.network.GracefulStop()
			}
		})
	}
	if s.engine != nil {
		wg.Go(func() {
			// err 在明确预算内关闭资源，超时后仍须取消执行并释放传输。
			if err := s.engine.Shutdown(s.drainCtx); err != nil && s.config.Logger != nil {
				s.config.Logger.Debug("Worker 资源关闭", "error", err)
			}
		})
	}
	wg.Wait()
}
