//go:build ignore

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/config/loader"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
)

type distribution struct{ P50, P95, P99, Max float64 }
type measurement struct {
	Rows           int
	Operation      string
	Milliseconds   distribution
	AllocatedBytes uint64
	HeapBytes      uint64
}
type result struct {
	Backend        string
	DatasetRows    int
	ListTotal      int
	FillSeconds    float64
	Measurements   []measurement
	ExpectedStates map[string]int
	HistoryWrites  map[string]distribution
	Plans          map[string][][]string
	EngineCounts   map[string]distribution
	EngineErrors   map[string]string
}

var namespace = uuid.MustParse("cd38f52e-c85a-4f71-b37f-fb2ff26a0760")

func identity(prefix string, n int) uuid.UUID {
	return uuid.NewSHA1(namespace, []byte(prefix+strconv.Itoa(n)))
}
func stamp(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t.UTC().Truncate(time.Microsecond), Valid: true}
}
func summary(values []float64) distribution {
	sort.Float64s(values)
	at := func(p float64) float64 { return values[max(0, int(math.Ceil(float64(len(values))*p))-1)] }
	return distribution{at(.5), at(.95), at(.99), values[len(values)-1]}
}
func fail(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func main() { fail(run()) }

func compareAnalyticalEngines(ctx context.Context, tenant uuid.UUID, size, samples int, item *result) error {
	parsed, err := mysql.ParseDSN(os.Getenv("DATABASE_TIDB_DSN"))
	if err != nil {
		return fmt.Errorf("invalid benchmark DSN")
	}
	parsed.ParseTime = true
	parsed.Loc = time.UTC
	parsed.InterpolateParams = true
	if parsed.Params == nil {
		parsed.Params = map[string]string{}
	}
	parsed.Params["time_zone"] = "'+00:00'"
	db, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, "ANALYZE TABLE v1_runs_olap"); err != nil {
		return err
	}
	until := time.Now().Add(2 * time.Minute)
	for time.Now().Before(until) {
		var available int
		if err = db.QueryRowContext(ctx, "SELECT COALESCE(MIN(AVAILABLE),0) FROM information_schema.tiflash_replica WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='v1_runs_olap'").Scan(&available); err != nil {
			return err
		}
		if available == 1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	item.Plans = map[string][][]string{}
	item.EngineCounts = map[string]distribution{}
	item.EngineErrors = map[string]string{}
	for _, engine := range []string{"optimizer", "tikv", "tiflash"} {
		hint := ""
		if engine != "optimizer" {
			hint = " /*+ READ_FROM_STORAGE(" + strings.ToUpper(engine) + "[u]) MAX_EXECUTION_TIME(500) */"
		}
		query := "SELECT" + hint + " u.readable_status,COUNT(*) FROM v1_runs_olap u WHERE u.tenant_id=? AND u.is_placeholder=FALSE AND (u.kind='dag' OR u.is_dag_child=FALSE) GROUP BY u.readable_status"
		read := func(explain bool) error {
			current := query
			if explain {
				current = "EXPLAIN ANALYZE " + current
			}
			bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			rows, err := db.QueryContext(bounded, current, tenant[:])
			if err != nil {
				return err
			}
			defer rows.Close()
			if explain {
				columns, err := rows.Columns()
				if err != nil {
					return err
				}
				values := make([]sql.RawBytes, len(columns))
				targets := make([]any, len(columns))
				for i := range values {
					targets[i] = &values[i]
				}
				for rows.Next() {
					if err = rows.Scan(targets...); err != nil {
						return err
					}
					line := make([]string, len(values))
					for i, v := range values {
						line[i] = string(v)
					}
					item.Plans[engine] = append(item.Plans[engine], line)
				}
			} else {
				total := 0
				for rows.Next() {
					var status string
					var count int
					if err = rows.Scan(&status, &count); err != nil {
						return err
					}
					if count != item.ExpectedStates[status] {
						return fmt.Errorf("engine state count mismatch")
					}
					total += count
				}
				if total != size {
					return fmt.Errorf("engine exact count differs")
				}
			}
			return rows.Err()
		}
		if err := read(true); err != nil {
			item.EngineErrors[engine] = err.Error()
			continue
		}
		var values []float64
		for range samples {
			start := time.Now()
			if err := read(false); err != nil {
				item.EngineErrors[engine] = err.Error()
				break
			}
			values = append(values, float64(time.Since(start))/float64(time.Millisecond))
		}
		if len(values) == samples {
			item.EngineCounts[engine] = summary(values)
		}
	}
	return nil
}
func run() error {
	sizesRaw := flag.String("sizes", "10000,100000,1000000", "cumulative fixture sizes")
	samples := flag.Int("samples", 50, "formal query samples")
	batchSize := flag.Int("batch", 1000, "rows per atomic write")
	output := flag.String("output", "", "result JSON")
	label := flag.String("backend", "", "result backend label")
	config := flag.String("config", "./generated", "config directory")
	flag.Parse()
	if *samples < 1 || *batchSize < 1 || *batchSize > 10000 || *output == "" {
		return fmt.Errorf("invalid benchmark options")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	layer, err := loader.NewConfigLoader(*config, loader.WithLogWriter(io.Discard)).InitDataLayer()
	if err != nil {
		return err
	}
	defer layer.Disconnect()
	repo := layer.V1.OLAP()
	now := time.Now().UTC().Truncate(24 * time.Hour).Add(12 * time.Hour)
	tenant := identity("tenant", 0)
	states := []sqlcv1.V1TaskInitialState{"QUEUED", "FAILED", "CANCELLED", "SKIPPED"}
	if os.Getenv("DATABASE_OLAP_BACKEND") == "postgres" {
		for day := 0; day < 28; day++ {
			_, err = sqlcv1.New().CreateOLAPPartitions(ctx, layer.Pool, sqlcv1.CreateOLAPPartitionsParams{Date: pgtype.Date{Time: now.AddDate(0, 0, -day), Valid: true}, Partitions: repository.NUM_PARTITIONS})
			if err != nil {
				return err
			}
		}
	}
	var results []result
	filled := 0
	for _, raw := range strings.Split(*sizesRaw, ",") {
		size, err := strconv.Atoi(raw)
		if err != nil || size <= filled {
			return fmt.Errorf("sizes must strictly increase")
		}
		begin := time.Now()
		for filled < size {
			count := min(*batchSize, size-filled)
			tasks := make([]*repository.V1TaskWithPayload, 0, count)
			for i := filled; i < filled+count; i++ {
				id := identity("run", i)
				task := &sqlcv1.V1Task{TenantID: tenant, ID: int64(i + 1), ExternalID: id, WorkflowRunID: id, WorkflowID: identity("workflow", i%8), WorkflowVersionID: identity("version", i%8), StepID: identity("step", i%8), InsertedAt: stamp(now.AddDate(0, 0, -(i % 28))), InitialState: states[i%4], Sticky: sqlcv1.V1StickyStrategyNONE, ScheduleTimeout: "1m", Queue: "default", ActionID: "action", DisplayName: "run", AdditionalMetadata: []byte(`{}`)}
				tasks = append(tasks, &repository.V1TaskWithPayload{V1Task: task})
			}
			_, blocked, err := repo.CreateTasks(ctx, tenant, tasks)
			if err != nil {
				return err
			}
			if len(blocked) != 0 {
				return fmt.Errorf("fixture run locks blocked")
			}
			filled += count
			if filled%10000 == 0 {
				fmt.Fprintf(os.Stderr, "%s fixture rows=%d elapsed=%s\n", *label, filled, time.Since(begin).Round(time.Millisecond))
			}
			if err = ctx.Err(); err != nil {
				return err
			}
		}
		item := result{Backend: *label, DatasetRows: size, FillSeconds: time.Since(begin).Seconds(), ExpectedStates: map[string]int{}, HistoryWrites: map[string]distribution{}}
		for i := 0; i < size; i++ {
			name := string(states[i%4])
			if name == "SKIPPED" {
				name = "COMPLETED"
			}
			item.ExpectedStates[name]++
		}
		measure := func(operation string, fn func() error) error {
			for range 3 {
				if err := fn(); err != nil {
					return err
				}
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			times := make([]float64, 0, *samples)
			for range *samples {
				start := time.Now()
				if err := fn(); err != nil {
					return err
				}
				times = append(times, float64(time.Since(start))/float64(time.Millisecond))
			}
			runtime.ReadMemStats(&after)
			item.Measurements = append(item.Measurements, measurement{size, operation, summary(times), after.TotalAlloc - before.TotalAlloc, after.HeapAlloc})
			return nil
		}
		start := now.AddDate(0, 0, -28)
		expectedListTotal := min(size, 20000)
		if *label == "tidb-v1" {
			// The archived backend reports uncapped totals; record that incompatibility explicitly.
			expectedListTotal = size
		}
		item.ListTotal = expectedListTotal
		err = measure("run_list_page_50_contract_count", func() error {
			rows, total, err := repo.ListWorkflowRuns(ctx, tenant, repository.ListWorkflowRunOpts{CreatedAfter: start, Limit: 50})
			if err != nil {
				return err
			}
			if total != expectedListTotal || len(rows) != 50 {
				return fmt.Errorf("run list count=%d page=%d expected=%d", total, len(rows), expectedListTotal)
			}
			return nil
		})
		if err != nil {
			return err
		}
		err = measure("state_group_exact_count", func() error {
			rows, err := repo.ReadTaskRunMetrics(ctx, tenant, repository.ReadTaskRunMetricsOpts{CreatedAfter: start})
			if err != nil {
				return err
			}
			var total int
			for _, row := range rows {
				total += int(row.Count)
				if int(row.Count) != item.ExpectedStates[string(row.Status)] {
					return fmt.Errorf("state count mismatch %s", row.Status)
				}
			}
			if total != size {
				return fmt.Errorf("state total %d expected %d", total, size)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if *label == "tidb" {
			if err = compareAnalyticalEngines(ctx, tenant, size, *samples, &item); err != nil {
				return err
			}
		}
		results = append(results, item)
		bytes, _ := json.MarshalIndent(results, "", "  ")
		if err = os.WriteFile(*output, append(bytes, '\n'), 0600); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "%s measured %d rows\n", *label, size)
	}
	// The same hot task receives increasing history while the measured operation stays equivalent.
	at := stamp(now)
	runID := identity("run", 0)
	workflow := identity("workflow", 0)
	seen := 0
	write := func(count int, measured bool) (distribution, error) {
		var times []float64
		chunk := 200
		if measured {
			chunk = 1
		}
		for from := 0; from < count; from += chunk {
			n := min(chunk, count-from)
			events := make([]sqlcv1.CreateTaskEventsOLAPParams, 0, n)
			mapping := map[uuid.UUID]uuid.UUID{}
			for range n {
				seen++
				id := identity("history", seen)
				events = append(events, sqlcv1.CreateTaskEventsOLAPParams{TenantID: tenant, TaskID: 1, TaskInsertedAt: at, ExternalID: id, WorkflowID: workflow, ReadableStatus: "RUNNING", EventType: "STARTED", DurableInvocationCount: int32(seen), EventTimestamp: stamp(now.Add(time.Duration(seen) * time.Microsecond))})
				mapping[id] = runID
			}
			begin := time.Now()
			_, blocked, err := repo.CreateTaskEvents(ctx, tenant, events, mapping, nil, nil)
			if err != nil {
				return distribution{}, err
			}
			if len(blocked) != 0 {
				return distribution{}, fmt.Errorf("history run lock blocked")
			}
			times = append(times, float64(time.Since(begin))/float64(time.Millisecond))
		}
		return summary(times), nil
	}
	_, err = write(10, false)
	if err != nil {
		return err
	}
	short, err := write(50, true)
	if err != nil {
		return err
	}
	_, err = write(10000, false)
	if err != nil {
		return err
	}
	long, err := write(50, true)
	if err != nil {
		return err
	}
	last := &results[len(results)-1]
	last.HistoryWrites["history_10_to_59"] = short
	last.HistoryWrites["history_10060_to_10109"] = long
	eventLimit := int64(20000)
	stored, err := repo.ListTaskRunEvents(ctx, tenant, 1, at, &eventLimit, nil)
	if err != nil {
		return err
	}
	var eventCount int64
	for _, row := range stored {
		eventCount += row.Count
	}
	if eventCount != int64(seen) {
		return fmt.Errorf("historical fixture persisted events=%d expected=%d", eventCount, seen)
	}
	bytes, _ := json.MarshalIndent(results, "", "  ")
	return os.WriteFile(*output, append(bytes, '\n'), 0600)
}
