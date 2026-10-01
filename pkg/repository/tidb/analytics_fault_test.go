package tidb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

type fallbackDriver struct{ state *fallbackScenario }
type fallbackScenario struct {
	mu                  sync.Mutex
	connections, begins int
	queries             []string
	deadlines           []time.Time
	primaryError        error
	cancelPrimary       context.CancelFunc
}
type fallbackConnection struct{ state *fallbackScenario }
type fallbackTransaction struct{}
type fallbackRows struct{ done bool }

func (d fallbackDriver) Open(string) (driver.Conn, error) {
	d.state.connections++
	return &fallbackConnection{d.state}, nil
}
func (c *fallbackConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepared query unsupported")
}
func (c *fallbackConnection) Close() error { return nil }
func (c *fallbackConnection) Begin() (driver.Tx, error) {
	c.state.begins++
	return fallbackTransaction{}, nil
}
func (c *fallbackConnection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Begin()
}
func (fallbackTransaction) Commit() error   { return nil }
func (fallbackTransaction) Rollback() error { return nil }
func (c *fallbackConnection) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	s := c.state
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries = append(s.queries, query)
	deadline, _ := ctx.Deadline()
	s.deadlines = append(s.deadlines, deadline)
	if len(s.queries) == 1 {
		if s.cancelPrimary != nil {
			s.cancelPrimary()
		}
		return nil, s.primaryError
	}
	return &fallbackRows{}, nil
}
func (*fallbackRows) Columns() []string { return []string{"count"} }
func (*fallbackRows) Close() error      { return nil }
func (r *fallbackRows) Next(dst []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dst[0] = int64(7)
	return nil
}

func TestAnalyticsFallbackKeepsSnapshotAndRemainingBudget(t *testing.T) {
	for _, scenario := range []struct {
		name        string
		primary     error
		cancel      bool
		wantQueries int
		nested      bool
	}{
		{"statement_timeout", &mysql.MySQLError{Number: 3024, Message: "interrupted"}, false, 2, false},
		{"replica_error", errors.New("TiFlash MPP connection failed"), false, 2, false},
		{"capped_count_timeout", &mysql.MySQLError{Number: 3024}, false, 2, true},
		{"invalid_query", errors.New("unknown column"), false, 1, false},
		{"request_canceled", &mysql.MySQLError{Number: 3024}, true, 1, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			state := &fallbackScenario{primaryError: scenario.primary}
			name := "tidb_fallback_" + scenario.name + "_" + uuid.NewString()
			sql.Register(name, fallbackDriver{state})
			db, err := sql.Open(name, "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if scenario.cancel {
				state.cancelPrimary = cancel
			}
			query := "SELECT COUNT(*) FROM v1_runs_olap u WHERE u.tenant_id=?"
			if scenario.nested {
				query = "SELECT COUNT(*) FROM (SELECT 1 FROM v1_runs_olap u WHERE u.tenant_id=? LIMIT 20000) included"
			}
			result, err := readAnalytics(ctx, &store{db: db, queryTimeout: time.Second, tiFlashTimeout: 10 * time.Millisecond}, "v1_runs_olap", query, []any{[]byte{1}}, func(rows *sql.Rows) (int64, error) {
				var n int64
				if !rows.Next() {
					return 0, rows.Err()
				}
				err := rows.Scan(&n)
				return n, err
			})
			if scenario.wantQueries == 2 && (err != nil || result != 7) {
				t.Fatalf("fallback %d %v", result, err)
			}
			if scenario.wantQueries == 1 && err == nil {
				t.Fatal("error suppressed")
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			if len(state.queries) != scenario.wantQueries || state.connections != 1 || state.begins != 1 {
				t.Fatalf("queries=%d connections=%d snapshots=%d", len(state.queries), state.connections, state.begins)
			}
			if scenario.wantQueries == 2 {
				if !state.deadlines[0].Equal(state.deadlines[1]) {
					t.Fatal("fallback extended the request budget")
				}
				if !strings.Contains(state.queries[1], "READ_FROM_STORAGE(TIKV[u])") || !strings.Contains(state.queries[1], "u.tenant_id=?") {
					t.Fatal("fallback lost route or predicate")
				}
				if scenario.nested && !strings.Contains(state.queries[1], "(SELECT /*+ READ_FROM_STORAGE(TIKV[u]) */ 1 FROM v1_runs_olap u") {
					t.Fatal("fallback hint does not target the counted table")
				}
			}
		})
	}
}
