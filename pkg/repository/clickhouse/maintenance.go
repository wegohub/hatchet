package clickhouse

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func (r *Repository) writeDiagnostics(ctx context.Context, tenant uuid.UUID, kind string, values []any) error {
	rows := make([]entity, 0, len(values))
	for _, v := range values {
		id := uuid.New()
		row, err := makeEntity(tenant, kind, id.String(), id, id, 0, time.Now(), v)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}
	return r.store.publish(ctx, rows)
}
func (r *Repository) CreateIncomingWebhookValidationFailureLogs(ctx context.Context, tenant uuid.UUID, opts []repository.CreateIncomingWebhookFailureLogOpts) error {
	values := make([]any, len(opts))
	for i := range opts {
		values[i] = opts[i]
	}
	return r.writeDiagnostics(ctx, tenant, "webhook_failure", values)
}
func (r *Repository) StoreCELEvaluationFailures(ctx context.Context, tenant uuid.UUID, opts []repository.CELEvaluationFailure) error {
	values := make([]any, len(opts))
	for i := range opts {
		values[i] = opts[i]
	}
	return r.writeDiagnostics(ctx, tenant, "cel_failure", values)
}
func (r *Repository) UpdateTablePartitions(ctx context.Context) error {
	lock, err := r.store.keeper.lock(ctx, "publication")
	if err != nil {
		return err
	}
	defer lock.release()
	h, _, err := r.store.recover(ctx, lock)
	if err != nil {
		return err
	}
	cutoff := time.Now().UTC().Add(-r.retention).Truncate(24 * time.Hour)
	// Only published batches are eligible; concurrent staging remains recoverable.
	batches, err := r.store.conn.Query(ctx, "SELECT DISTINCT e.batch_id FROM entities e INNER JOIN commits c ON e.batch_id=c.batch_id WHERE e.inserted_at < toDateTime64(?,6,'UTC') AND c.sequence <= ?", dateArg(cutoff), h.Published)
	if err != nil {
		return err
	}
	var ids []uuid.UUID
	for batches.Next() {
		var id uuid.UUID
		if err = batches.Scan(&id); err != nil {
			_ = batches.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = batches.Err()
	_ = batches.Close()
	if err != nil {
		return err
	}
	for start := 0; start < len(ids); start += 1000 {
		end := min(start+1000, len(ids))
		if err = r.store.conn.Exec(ctx, "ALTER TABLE entities DELETE WHERE inserted_at < toDateTime64(?,6,'UTC') AND batch_id IN (?)", dateArg(cutoff), ids[start:end]); err != nil {
			return err
		}
	}
	return r.store.conn.Exec(ctx, "ALTER TABLE log_lines DELETE WHERE created_at < toDateTime64(?,6,'UTC')", dateArg(time.Now().UTC().Add(-r.coreRetention).Truncate(24*time.Hour)))
}
func (r *Repository) ListYesterdayRunCountsByStatus(ctx context.Context) (map[sqlcv1.V1ReadableStatusOlap]int64, error) {
	ctx, err := r.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	end := time.Now().UTC().Truncate(24 * time.Hour)
	start := end.Add(-24 * time.Hour)
	counts := make(map[sqlcv1.V1ReadableStatusOlap]int64)
	tasks, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Kind: "task", After: &start})
	if err != nil {
		return nil, err
	}
	for _, t := range tasks {
		if !t.DagID.Valid && t.InsertedAt.Time.Before(end) {
			counts[t.ReadableStatus]++
		}
	}
	dags, err := scanValues[sqlcv1.V1DagsOlap](ctx, r, entityFilter{Kind: "dag", After: &start})
	for _, d := range dags {
		if d.InsertedAt.Time.Before(end) {
			counts[d.ReadableStatus]++
		}
	}
	return counts, err
}
func (r *Repository) GetTaskPointMetrics(ctx context.Context, tenant uuid.UUID, start, end *time.Time, interval time.Duration) ([]*sqlcv1.GetTaskPointMetricsRow, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("bucket interval must be positive")
	}
	if start == nil || end == nil {
		return []*sqlcv1.GetTaskPointMetricsRow{}, nil
	}
	ctx, err := r.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	buckets := make(map[int64]*sqlcv1.GetTaskPointMetricsRow)
	add := func(at time.Time, status sqlcv1.V1ReadableStatusOlap) {
		if at.Before(*start) || at.After(*end) {
			return
		}
		n := at.UnixNano()
		width := int64(interval)
		b := n / width
		if n < 0 && n%width != 0 {
			b--
		}
		b *= width
		row := buckets[b]
		if row == nil {
			row = &sqlcv1.GetTaskPointMetricsRow{MinuteBucket: timestamp(time.Unix(0, b))}
			buckets[b] = row
		}
		if status == "COMPLETED" {
			row.CompletedCount++
		}
		if status == "FAILED" {
			row.FailedCount++
		}
	}
	tasks, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "task", After: start})
	if err != nil {
		return nil, err
	}
	for _, t := range tasks {
		if !t.DagID.Valid {
			add(t.InsertedAt.Time, t.ReadableStatus)
		}
	}
	dags, err := scanValues[sqlcv1.V1DagsOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "dag", After: start})
	if err != nil {
		return nil, err
	}
	for _, d := range dags {
		add(d.InsertedAt.Time, d.ReadableStatus)
	}
	rows := make([]*sqlcv1.GetTaskPointMetricsRow, 0, len(buckets))
	for _, row := range buckets {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].MinuteBucket.Time.Before(rows[j].MinuteBucket.Time) })
	return rows, nil
}
