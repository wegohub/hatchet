package tidb

import (
	"context"
	"sync"
	"time"
)

type localGate struct {
	ready      chan struct{}
	references int
}
type localRunGates struct {
	mu    sync.Mutex
	gates map[string]*localGate
}

// Local callers queue in lock order before competing for database locks.
// Database NOWAIT locks still arbitrate independent controller processes.
func (g *localRunGates) acquire(ctx context.Context, keys []string) (func(), error) {
	start := time.Now()
	var acquired []string
	var slots []*localGate
	release := func() {
		for i := len(acquired) - 1; i >= 0; i-- {
			slot := slots[i]
			slot.ready <- struct{}{}
			g.mu.Lock()
			slot.references--
			if slot.references == 0 {
				delete(g.gates, acquired[i])
			}
			g.mu.Unlock()
		}
	}
	for _, key := range keys {
		g.mu.Lock()
		if g.gates == nil {
			g.gates = map[string]*localGate{}
		}
		slot := g.gates[key]
		if slot == nil {
			slot = &localGate{ready: make(chan struct{}, 1)}
			slot.ready <- struct{}{}
			g.gates[key] = slot
		}
		slot.references++
		g.mu.Unlock()
		select {
		case <-slot.ready:
			acquired = append(acquired, key)
			slots = append(slots, slot)
		case <-ctx.Done():
			g.mu.Lock()
			slot.references--
			if slot.references == 0 {
				delete(g.gates, key)
			}
			g.mu.Unlock()
			release()
			return nil, ctx.Err()
		}
	}
	observePhase("local_run_queue", start)
	return release, nil
}
