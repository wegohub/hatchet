package tidb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLocalRunGateCancellationAndCleanup(t *testing.T) {
	var gates localRunGates
	first, err := gates.acquire(context.Background(), []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err = gates.acquire(ctx, []string{"a", "b"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation %v", err)
	}
	first()
	var group sync.WaitGroup
	for range 32 {
		group.Go(func() {
			release, err := gates.acquire(context.Background(), []string{"a", "b"})
			if err != nil {
				t.Error(err)
				return
			}
			release()
		})
	}
	group.Wait()
	gates.mu.Lock()
	defer gates.mu.Unlock()
	if len(gates.gates) != 0 {
		t.Fatal("completed gates retained")
	}
}
