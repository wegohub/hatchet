package tidb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	metrics "github.com/hatchet-dev/hatchet/pkg/integrations/metrics/prometheus"
)

func readAnalytics[T any](ctx context.Context, s *store, table, query string, args []any, decode func(*sql.Rows) (T, error)) (T, error) {
	var zero T
	if table != "v1_runs_olap" && table != "v1_log_line" {
		return zero, fmt.Errorf("unsupported TiDB analytics table %q", table)
	}
	timeout := s.queryTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var own *sql.Tx
	if _, ok := ctx.Value(transactionKey{}).(*sql.Tx); !ok {
		var err error
		own, err = s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			return zero, err
		}
		defer own.Rollback()
		ctx = context.WithValue(ctx, transactionKey{}, own)
	}
	budget := s.tiFlashTimeout
	if budget <= 0 {
		budget = 500 * time.Millisecond
	}
	first := strings.Replace(query, "SELECT ", fmt.Sprintf("SELECT /*+ MAX_EXECUTION_TIME(%d) */ ", max(int64(1), budget.Milliseconds())), 1)
	start := time.Now()
	result, err := runAnalyticalQuery(ctx, executor(ctx, s.db), first, args, decode)
	observePhase("query_analytics", start)
	resultLabel := "ok"
	if err != nil {
		resultLabel = "error"
	}
	metrics.OLAPTiDBReadRoute.WithLabelValues(table, "optimizer", resultLabel).Inc()
	s.samplePlan(table, query, args)
	if err == nil || ctx.Err() != nil {
		return result, err
	}
	reason := ""
	var mysqlError *mysql.MySQLError
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &mysqlError) && mysqlError.Number == 3024 || strings.Contains(strings.ToLower(err.Error()), "maximum statement execution time") || strings.Contains(strings.ToLower(err.Error()), "max_execution_time") {
		reason = "timeout"
	} else if strings.Contains(strings.ToLower(err.Error()), "tiflash") || strings.Contains(strings.ToLower(err.Error()), "mpp") {
		reason = "tiflash_error"
	}
	if reason == "" {
		return zero, err
	}
	metrics.OLAPTiFlashFallbacks.WithLabelValues(table, reason).Inc()
	alias := table
	if strings.Contains(query, "FROM "+table+" u ") {
		alias = "u"
	}
	selectBlock := "SELECT "
	if strings.Contains(query, "(SELECT 1 FROM "+table+" ") {
		// A storage hint belongs to the query block that defines the table alias.
		selectBlock = "SELECT 1 FROM " + table + " "
	}
	fallback := strings.Replace(query, selectBlock, "SELECT /*+ READ_FROM_STORAGE(TIKV["+alias+"]) */ "+strings.TrimPrefix(selectBlock, "SELECT "), 1)
	if fallback == query {
		return zero, err
	}
	result, retryErr := runAnalyticalQuery(ctx, executor(ctx, s.db), fallback, args, decode)
	if retryErr != nil {
		return zero, errors.Join(err, retryErr)
	}
	metrics.OLAPTiDBReadRoute.WithLabelValues(table, "tikv_fallback", "ok").Inc()
	return result, nil
}

func runAnalyticalQuery[T any](ctx context.Context, db sqlExecutor, query string, args []any, decode func(*sql.Rows) (T, error)) (T, error) {
	var zero T
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return zero, err
	}
	result, err := decode(rows)
	closeErr := rows.Close()
	if err != nil {
		return zero, err
	}
	if closeErr != nil {
		return zero, closeErr
	}
	return result, nil
}
