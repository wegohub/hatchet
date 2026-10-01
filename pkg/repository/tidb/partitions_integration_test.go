package tidb

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestConcurrentPartitionMaintenanceIntegration(t *testing.T) {
	s, _ := integrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	now := time.Now().UTC().Truncate(24 * time.Hour)
	runTogether := func(fn func() error) {
		t.Helper()
		errors := make(chan error, 2)
		var group sync.WaitGroup
		for range 2 {
			group.Go(func() { errors <- fn() })
		}
		group.Wait()
		close(errors)
		for err := range errors {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	runTogether(func() error { return ensureDailyPartitions(ctx, s.db, "v1_log_line", now, 31*24*time.Hour) })
	runTogether(func() error {
		return ensureDailyPartitions(ctx, s.db, "v1_log_line", now.AddDate(0, 0, 1), 31*24*time.Hour)
	})
	runTogether(func() error { return dropExpiredPartitions(ctx, s.db, "v1_log_line", now.AddDate(0, 0, -30)) })
	parts, err := listPartitions(ctx, s.db, "v1_log_line")
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := parts["p"+now.AddDate(0, 0, -31).Format("20060102")]; exists {
		t.Fatal("expired partition remains")
	}
	for _, day := range []time.Time{now.AddDate(0, 0, -30), now, now.AddDate(0, 0, 3)} {
		if _, exists := parts["p"+day.Format("20060102")]; !exists {
			t.Fatal("retained or future partition is missing")
		}
	}
}
