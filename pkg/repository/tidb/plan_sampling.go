package tidb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	metrics "github.com/hatchet-dev/hatchet/pkg/integrations/metrics/prometheus"
)

type planSampler struct {
	ctx     context.Context
	mu      sync.Mutex
	last    map[string]time.Time
	busy    bool
	stopped bool
	wait    sync.WaitGroup
}

// Verification is a bounded separate sample, not an assertion about each request.
// A template without a successful sample remains unknown.
func (s *store) samplePlan(table, query string, args []any) {
	p := &s.plans
	if p.ctx == nil || p.ctx.Err() != nil {
		return
	}
	digest := sha256.Sum256([]byte(query))
	template := fmt.Sprintf("%s:%x", table, digest[:6])
	p.mu.Lock()
	if p.last == nil {
		p.last = map[string]time.Time{}
	}
	last := p.last[template]
	if p.stopped || p.ctx.Err() != nil || p.busy || time.Since(last) < time.Minute || len(p.last) >= 64 && last.IsZero() {
		p.mu.Unlock()
		return
	}
	p.last[template] = time.Now()
	p.busy = true
	p.wait.Add(1)
	p.mu.Unlock()
	for _, value := range []string{"tikv", "tiflash", "mixed", "unknown"} {
		result := float64(0)
		if value == "unknown" {
			result = 1
		}
		metrics.OLAPTiDBVerifiedPlan.WithLabelValues(template, value).Set(result)
	}
	copied := append([]any(nil), args...)
	go func() {
		defer p.wait.Done()
		defer func() { p.mu.Lock(); p.busy = false; p.mu.Unlock() }()
		ctx, cancel := context.WithTimeout(p.ctx, time.Second)
		defer cancel()
		engine, err := verifyPlan(ctx, s.db, query, copied)
		for _, value := range []string{"tikv", "tiflash", "mixed", "unknown"} {
			result := float64(0)
			if err != nil && value == "unknown" || err == nil && value == engine {
				result = 1
			}
			metrics.OLAPTiDBVerifiedPlan.WithLabelValues(template, value).Set(result)
		}
	}()
}

func (p *planSampler) stop() {
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()
	p.wait.Wait()
}

func verifyPlan(ctx context.Context, db sqlExecutor, query string, args []any) (string, error) {
	rows, err := db.QueryContext(ctx, "EXPLAIN ANALYZE "+query, args...)
	if err != nil {
		return "unknown", err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return "unknown", err
	}
	cells := make([]sql.RawBytes, len(columns))
	targets := make([]any, len(columns))
	for i := range cells {
		targets[i] = &cells[i]
	}
	tikv, tiflash := false, false
	for rows.Next() {
		if err = rows.Scan(targets...); err != nil {
			return "unknown", err
		}
		for _, cell := range cells {
			text := strings.ToLower(string(cell))
			tikv = tikv || strings.Contains(text, "tikv")
			tiflash = tiflash || strings.Contains(text, "tiflash")
		}
	}
	if err = rows.Err(); err != nil {
		return "unknown", err
	}
	if tikv && tiflash {
		return "mixed", nil
	}
	if tiflash {
		return "tiflash", nil
	}
	if tikv {
		return "tikv", nil
	}
	return "unknown", nil
}
