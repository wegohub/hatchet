package tidb

import (
	"context"
	"database/sql"
	"fmt"
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
	now := time.Now().UTC()
	cutoff := dateArg(now.Add(-r.retention).Truncate(24 * time.Hour))
	logCutoff := dateArg(now.Add(-r.coreRetention).Truncate(24 * time.Hour))
	for table, column := range partitionTables {
		keep := r.retention
		limit := cutoff
		if table == "v1_log_line" {
			keep = r.coreRetention
			limit = logCutoff
		}
		if err := ensureDailyPartitions(ctx, r.store.db, table, now, keep); err != nil {
			return err
		}
		if err := dropExpiredPartitions(ctx, r.store.db, table, limit); err != nil {
			return err
		}
		if err := deleteBatches(ctx, r.store.db, table, column, limit); err != nil {
			return err
		}
	}
	for table, column := range map[string]string{"v1_lookup_table_olap": "inserted_at", "v1_olap_metadata": "inserted_at", "v1_olap_pending_updates": "task_inserted_at", "v1_olap_write_receipts": "committed_at", "v1_payloads_olap_offloaded_block_index": "inserted_at", "v1_payloads_olap_cutover_job_offset": "created_at"} {
		if err := deleteBatches(ctx, r.store.db, table, column, cutoff); err != nil {
			return err
		}
	}
	// Initialization is proof of an empty run only while its lock identity has
	// never been removed ahead of retained state or an out-of-order projection.
	for {
		res, err := r.store.db.ExecContext(ctx, "DELETE FROM v1_olap_run_locks WHERE touched_at<? AND NOT EXISTS(SELECT 1 FROM v1_runs_olap u WHERE u.tenant_id=v1_olap_run_locks.tenant_id AND u.workflow_run_id=v1_olap_run_locks.run_id) AND NOT EXISTS(SELECT 1 FROM v1_task_attempts_olap a WHERE a.tenant_id=v1_olap_run_locks.tenant_id AND a.run_id=v1_olap_run_locks.run_id) AND NOT EXISTS(SELECT 1 FROM v1_olap_pending_updates p WHERE p.tenant_id=v1_olap_run_locks.tenant_id AND p.run_id=v1_olap_run_locks.run_id) LIMIT 1000", cutoff)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n < 1000 {
			break
		}
	}
	return nil
}
func deleteBatches(ctx context.Context, db *sql.DB, table, column string, cutoff time.Time) error {
	for {
		res, err := db.ExecContext(ctx, "DELETE FROM "+table+" WHERE "+column+" < ? LIMIT 1000", cutoff)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n < 1000 {
			return nil
		}
	}
}

func (r *Repository) ListYesterdayRunCountsByStatus(ctx context.Context) (map[sqlcv1.V1ReadableStatusOlap]int64, error) {
	end := time.Now().UTC().Truncate(24 * time.Hour)
	start := end.Add(-24 * time.Hour)
	query := "SELECT readable_status,COUNT(*) FROM v1_runs_olap WHERE inserted_at>=? AND inserted_at<? AND is_placeholder=FALSE AND (kind='dag' OR is_dag_child=0) GROUP BY readable_status"
	return readAnalytics(ctx, r.store, "v1_runs_olap", query, []any{start, end}, func(rows *sql.Rows) (map[sqlcv1.V1ReadableStatusOlap]int64, error) {
		counts := make(map[sqlcv1.V1ReadableStatusOlap]int64)
		for rows.Next() {
			var status string
			var count int64
			if err := rows.Scan(&status, &count); err != nil {
				return nil, err
			}
			counts[sqlcv1.V1ReadableStatusOlap(status)] = count
		}
		return counts, rows.Err()
	})
}
func (r *Repository) GetTaskPointMetrics(ctx context.Context, tenant uuid.UUID, start, end *time.Time, interval time.Duration) ([]*sqlcv1.GetTaskPointMetricsRow, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("bucket interval must be positive")
	}
	if start == nil || end == nil {
		return []*sqlcv1.GetTaskPointMetricsRow{}, nil
	}
	micros := interval.Microseconds()
	if micros == 0 {
		return nil, fmt.Errorf("bucket interval must be at least one microsecond")
	}
	query := "SELECT TIMESTAMPADD(MICROSECOND,FLOOR(TIMESTAMPDIFF(MICROSECOND,'1970-01-01 00:00:00',inserted_at)/?)*?,'1970-01-01 00:00:00'),SUM(readable_status='COMPLETED'),SUM(readable_status='FAILED') FROM v1_runs_olap WHERE tenant_id=? AND inserted_at>=? AND inserted_at<=? AND is_placeholder=FALSE AND (kind='dag' OR is_dag_child=0) GROUP BY 1 ORDER BY 1"
	return readAnalytics(ctx, r.store, "v1_runs_olap", query, []any{micros, micros, uuidArg(tenant), dateArg(*start), dateArg(*end)}, func(rows *sql.Rows) ([]*sqlcv1.GetTaskPointMetricsRow, error) {
		result := make([]*sqlcv1.GetTaskPointMetricsRow, 0)
		for rows.Next() {
			var rawTime []byte
			var completed, failed int64
			if err := rows.Scan(&rawTime, &completed, &failed); err != nil {
				return nil, err
			}
			bucket, err := time.ParseInLocation("2006-01-02 15:04:05.999999", string(rawTime), time.UTC)
			if err != nil {
				return nil, fmt.Errorf("parse TiDB task metric bucket: %w", err)
			}
			result = append(result, &sqlcv1.GetTaskPointMetricsRow{MinuteBucket: timestamp(bucket), CompletedCount: completed, FailedCount: failed})
		}
		return result, rows.Err()
	})
}
