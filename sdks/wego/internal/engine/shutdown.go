package engine

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// ownedOperation 描述 SDK 自有 I/O，仅保存类别和开始时间，不保存输入或身份凭证
type ownedOperation struct {
	// name 是调用点的稳定类别，例如 rpc.stream 或 result.decode
	name string
	// started 用于区分新登记与长期未退出的操作
	started time.Time
}

// drainCalls 等待所有调用结束；预算耗尽只发出取消，不能把它当成传输已经释放
func (e *Engine) drainCalls(ctx context.Context) error {
	for {
		e.mu.Lock()
		if len(e.calls) == 0 {
			e.mu.Unlock()
			return nil
		}
		// changed 为当前状态通知，醒来后必须重新检查实际数量
		changed := e.changed
		e.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			e.mu.Lock()
			// cancel 只撤销当前登记，业务 handler 的实际退出由其自身响应预算
			for _, cancel := range e.calls {
				cancel()
			}
			e.mu.Unlock()
			return ctx.Err()
		}
	}
}

// drainOwnedIO 确认 SDK 传输和下载退出，并原子设置释放屏障以拒绝后续登记
func (e *Engine) drainOwnedIO(ctx context.Context) error {
	for {
		e.mu.Lock()
		if len(e.ownedIO) == 0 {
			e.releasing = true
			e.mu.Unlock()
			return nil
		}
		// changed 为自有 I/O 退出通知，普通调用退出也会唤醒但不能视为 I/O 完成
		changed := e.changed
		e.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			e.mu.Lock()
			e.releasing = true
			// operations 在锁内取得稳定快照；logger 可能执行用户代码，必须在锁外调用
			operations := make([]string, 0, len(e.ownedIO))
			// id 与 operation 用于定位未退出的登记，不读取业务参数和 token
			for id, operation := range e.ownedIO {
				operations = append(operations, fmt.Sprintf("id=%d kind=%s age=%s", id, operation.name, time.Since(operation.started)))
			}
			e.mu.Unlock()
			slices.Sort(operations)
			if len(operations) == 0 {
				return nil
			}
			if e.Config.Logger != nil {
				e.Config.Logger.Warn("SDK I/O 清理预算耗尽", "count", len(operations), "operations", operations)
			}
			return fmt.Errorf("wego: %d unfinished operations during cleanup %v: %w", len(operations), operations, ctx.Err())
		}
	}
}
