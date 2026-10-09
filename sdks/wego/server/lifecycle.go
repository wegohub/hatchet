package server

import (
	"context"
	"errors"
	"net"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Stop 取消所有入口，可中断正在排空的 GracefulStop；等待资源清理完成。
func (s *Server) Stop() {
	s.forceStop()
	s.beginStop()
	<-s.stopDone
}

// GracefulStop 拒绝新调用并持续等待已有业务；Stop 可强制中断。
func (s *Server) GracefulStop() {
	s.beginStop()
	<-s.stopDone
}

// forceStop 只发布一次强制信号，清理另用独立预算，不等待不合作的业务。
func (s *Server) forceStop() {
	s.forceOnce.Do(func() { close(s.force); s.cancelDrain() })
}

// beginStop 先禁止新增资源，等待初始化屏障后清理；调用方不持锁等待。
func (s *Server) beginStop() {
	s.mu.Lock()
	s.stopping = true
	started := s.started
	s.mu.Unlock()
	s.cancelStart()
	s.stopOnce.Do(func() {
		go func() {
			defer close(s.stopDone)
			defer s.cancelDrain()
			if started {
				<-s.initialized
			}
			s.closeEngines()
			close(s.entrancesDone)
			s.background.Wait()
		}()
	})
}

// closeEngines 同时排空两个入口；保留 Worker 通知、输出、上报连接直到业务排空。
func (s *Server) closeEngines() {
	// wg 确认本组自有协程全部退出，不能把启动当作完成。
	var wg sync.WaitGroup
	if s.network != nil {
		wg.Go(func() {
			if err := s.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				s.recordFault(err)
			}
			s.networkCalls.drain(s.force)
			select {
			case <-s.force:
				s.network.Stop()
			default:
				// 即使业务记录已清空，原生 GracefulStop 仍可能等待传输；Stop 必须能中断它。
				done := make(chan struct{})
				go func() { s.network.GracefulStop(); close(done) }()
				select {
				case <-done:
				case <-s.force:
					s.network.Stop()
					<-done
				}
			}
		})
	}
	if s.engine != nil {
		wg.Go(func() {
			err := s.engine.Shutdown(s.drainCtx)
			select {
			case <-s.force:
				err = withoutCanceled(err)
			default:
			}
			s.mu.Lock()
			s.closeErr = errors.Join(s.closeErr, err)
			s.mu.Unlock()
		})
	}
	wg.Wait()
}

// recordFault 在锁内保存原因后唤醒协调器；强制停止导致的取消不作为入口故障。
func (s *Server) recordFault(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	stopping := s.stopping
	s.mu.Unlock()
	if stopping {
		err = withoutCanceled(err)
	}
	if err == nil {
		return
	}
	s.mu.Lock()
	s.faultErr = errors.Join(s.faultErr, err)
	s.mu.Unlock()
	select {
	case s.errors <- struct{}{}:
	default:
	}
}

// result 只在停止完成后读取清理结果；故障不能被最后到达的取消覆盖。
func (s *Server) result() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return errors.Join(s.faultErr, s.closeErr)
}

// joinedErrors 描述可拆分的多项错误，保持真实失败的独立身份。
type joinedErrors interface {
	// Unwrap 返回各项失败，取消与清理故障分别处理。
	Unwrap() []error
}

// withoutCanceled 逐项剔除预期取消，避免 errors.Is 对混合 Join 吞掉真实清理失败。
func withoutCanceled(err error) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(joinedErrors); ok {
		// kept 保留非取消错误，混合失败不能整体忽略。
		var kept []error
		for _, child := range joined.Unwrap() {
			kept = append(kept, withoutCanceled(child))
		}
		return errors.Join(kept...)
	}
	if errors.Is(err, context.Canceled) {
		if wrapped, ok := err.(interface {
			// Unwrap 读取被包装的原因，混合错误不能整体丢弃。
			Unwrap() error
		}); ok {
			return withoutCanceled(wrapped.Unwrap())
		}
		return nil
	}
	if status.Code(err) == codes.Canceled {
		return nil
	}
	return err
}
