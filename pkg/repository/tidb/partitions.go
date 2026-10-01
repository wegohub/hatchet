package tidb

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

var dayPartition = regexp.MustCompile(`^p[0-9]{8}$`)
var partitionTables = map[string]string{
	"v1_tasks_olap": "inserted_at", "v1_dags_olap": "inserted_at", "v1_runs_olap": "inserted_at", "v1_task_events_olap": "inserted_at", "v1_payloads_olap": "inserted_at", "v1_events_olap": "inserted_at", "v1_event_to_run_olap": "inserted_at", "v1_otel_trace_olap": "inserted_at", "v1_otel_trace_lookup_olap": "inserted_at", "v1_incoming_webhook_validation_failures_olap": "inserted_at", "v1_cel_evaluation_failures_olap": "inserted_at", "v1_task_attempts_olap": "task_inserted_at", "v1_log_line": "task_inserted_at",
}

func partitionDefinition(day time.Time) string {
	return fmt.Sprintf("PARTITION p%s VALUES LESS THAN ('%s')", day.Format("20060102"), day.AddDate(0, 0, 1).Format("2006-01-02"))
}
func initialPartitions(now time.Time, retention time.Duration) string {
	today := now.UTC().Truncate(24 * time.Hour)
	start := today.Add(-retention).Truncate(24 * time.Hour)
	var parts []string
	for day := start; !day.After(today.AddDate(0, 0, 2)); day = day.AddDate(0, 0, 1) {
		parts = append(parts, partitionDefinition(day))
	}
	parts = append(parts, "PARTITION pmax VALUES LESS THAN (MAXVALUE)")
	return strings.Join(parts, ",")
}
func listPartitions(ctx context.Context, db *sql.DB, table string) (map[string]struct{}, error) {
	if _, ok := partitionTables[table]; !ok {
		return nil, fmt.Errorf("unsupported TiDB partition table %q", table)
	}
	rows, err := db.QueryContext(ctx, "SELECT PARTITION_NAME FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND PARTITION_NAME IS NOT NULL", table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = struct{}{}
	}
	return out, rows.Err()
}
func ensureDailyPartitions(ctx context.Context, db *sql.DB, table string, now time.Time, retention time.Duration) error {
	existing, err := listPartitions(ctx, db, table)
	if err != nil {
		return err
	}
	var names []string
	for name := range existing {
		if dayPartition.MatchString(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	today := now.UTC().Truncate(24 * time.Hour)
	start := today.Add(-retention).Truncate(24 * time.Hour)
	end := today.AddDate(0, 0, 2)
	if len(names) > 0 {
		first, err := time.Parse("20060102", names[0][1:])
		if err != nil {
			return err
		}
		if start.Before(first) {
			var defs []string
			for day := start; day.Before(first); day = day.AddDate(0, 0, 1) {
				defs = append(defs, partitionDefinition(day))
			}
			defs = append(defs, partitionDefinition(first))
			if _, err = db.ExecContext(ctx, "ALTER TABLE "+table+" REORGANIZE PARTITION "+names[0]+" INTO ("+strings.Join(defs, ",")+")"); err != nil {
				latest, checkErr := listPartitions(ctx, db, table)
				complete := checkErr == nil
				for day := start; !day.After(first); day = day.AddDate(0, 0, 1) {
					_, ok := latest["p"+day.Format("20060102")]
					complete = complete && ok
				}
				if !complete {
					return fmt.Errorf("extend TiDB partition window %s: %w", table, err)
				}
			}
		}
		last, _ := time.Parse("20060102", names[len(names)-1][1:])
		start = last.AddDate(0, 0, 1)
	}
	if start.After(end) {
		return nil
	}
	var defs []string
	for day := start; !day.After(end); day = day.AddDate(0, 0, 1) {
		defs = append(defs, partitionDefinition(day))
	}
	defs = append(defs, "PARTITION pmax VALUES LESS THAN (MAXVALUE)")
	if _, err = db.ExecContext(ctx, "ALTER TABLE "+table+" REORGANIZE PARTITION pmax INTO ("+strings.Join(defs, ",")+")"); err != nil {
		latest, checkErr := listPartitions(ctx, db, table)
		if checkErr == nil {
			complete := true
			for day := start; !day.After(end); day = day.AddDate(0, 0, 1) {
				_, ok := latest["p"+day.Format("20060102")]
				complete = complete && ok
			}
			if complete {
				return nil
			}
		}
		return fmt.Errorf("create TiDB daily partitions %s: %w", table, err)
	}
	return nil
}
func ensureMonthlyPartitions(ctx context.Context, db *sql.DB, table string, now time.Time) error {
	return ensureDailyPartitions(ctx, db, table, now, 30*24*time.Hour)
}
func dropExpiredPartitions(ctx context.Context, db *sql.DB, table string, cutoff time.Time) error {
	existing, err := listPartitions(ctx, db, table)
	if err != nil {
		return err
	}
	var expired []string
	for name := range existing {
		if !dayPartition.MatchString(name) {
			continue
		}
		day, err := time.ParseInLocation("20060102", name[1:], time.UTC)
		if err != nil {
			return err
		}
		if !day.AddDate(0, 0, 1).After(cutoff) {
			expired = append(expired, name)
		}
	}
	if len(expired) == 0 {
		return nil
	}
	sort.Strings(expired)
	_, err = db.ExecContext(ctx, "ALTER TABLE "+table+" DROP PARTITION "+strings.Join(expired, ","))
	if err != nil {
		latest, checkErr := listPartitions(ctx, db, table)
		if checkErr == nil {
			complete := true
			for _, name := range expired {
				_, exists := latest[name]
				complete = complete && !exists
			}
			if complete {
				return nil
			}
		}
	}
	return err
}
