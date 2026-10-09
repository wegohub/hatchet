package backend

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

// TestCompletedCandidatesReleaseExecutionIndexes 让 1024 个独立候选落选，验证真实执行索引回收
// 检查 latest、pending、cancels，而非以 race 或全进程 goroutine 数推断没有泄漏
func TestCompletedCandidatesReleaseExecutionIndexes(t *testing.T) {
	runner, reporter, ctx := gateFixture(t, ports.Definition{BeforeStart: func(context.Context, ports.StartInfo) (ports.Admission, error) {
		return ports.Admission{Owned: false}, nil
	}})
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for batch := 0; batch < 32; batch++ {
		for item := 0; item < 32; item++ {
			action := gateAction(0)
			action.StepRunId = fmt.Sprintf("task-%d-%d", batch, item)
			if !runner.handle(ctx, action) {
				t.Fatal("candidate bypassed admission")
			}
		}
		for {
			runner.mu.Lock()
			active := runner.inflight + len(runner.latest)
			runner.mu.Unlock()
			owner := runner.dispatcher.owner
			owner.mu.Lock()
			retained := len(owner.pending) + len(owner.cancels)
			owner.mu.Unlock()
			if active == 0 && retained == 0 {
				break
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal("settled execution indexes retained resources", active, retained)
			}
		}
	}
	select {
	case event := <-reporter.events:
		t.Fatal("loser reported task state", event)
	default:
	}
}
