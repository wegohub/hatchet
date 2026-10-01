package tidb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
)

func isolatedDatabase(t *testing.T) Config {
	if os.Getenv("TIDB_TEST_REUSE_SCHEMA") == "1" && !strings.Contains(t.Name(), "Migration") {
		return reusableTestDatabase(t)
	}
	return freshTestDatabase(t)
}

func freshTestDatabase(t *testing.T) Config {
	t.Helper()
	dsn := os.Getenv("TIDB_TEST_DSN")
	if dsn == "" {
		t.Skip("TIDB_TEST_DSN is required for TiDB integration tests")
	}
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	prefix := os.Getenv("TIDB_TEST_DATABASE_PREFIX")
	if prefix == "" {
		prefix = "hatchet_olap_test_"
	}
	name := prefix + uuid.NewString()[:8]
	parsed.DBName = ""
	admin, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err = admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP DATABASE "+name); err != nil {
			t.Errorf("drop isolated database: %v", err)
		}
		admin.Close()
	})
	parsed.DBName = name
	return Config{DSN: parsed.FormatDSN(), QueryTimeout: 5 * time.Second}
}

func integrationStore(t *testing.T) (*store, context.Context) {
	t.Helper()
	cfg := isolatedDatabase(t)
	ctx := context.Background()
	if err := Migrate(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	db, err := open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &store{db: db, coordinator: &coordinator{db: db}}, ctx
}

func TestTiDBStoreCommitAndLock(t *testing.T) {
	cfg := isolatedDatabase(t)
	ctx := context.Background()
	if err := Migrate(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, cfg); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
	db, err := open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &store{db: db, coordinator: &coordinator{db: db}}
	tenant, run := uuid.New(), uuid.New()
	row, err := makeEntity(tenant, "task", run.String(), run, run, 7, time.Now(), map[string]any{"readable_status": "QUEUED"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.publish(ctx, []entity{row}); err != nil {
		t.Fatal(err)
	}
	if !s.confirmPublished(ctx, []entity{row}) {
		t.Fatal("committed publication could not be confirmed")
	}
	got, err := s.read(ctx, 0, entityFilter{Tenant: &tenant, Kind: "task", ExternalIDs: []uuid.UUID{run}})
	if err != nil || len(got) != 1 {
		t.Fatalf("committed row read: rows=%d err=%v", len(got), err)
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(got[0].Body), &body); err != nil {
		t.Fatal(err)
	}
	if body["readable_status"] != "QUEUED" {
		t.Fatalf("status = %v", body["readable_status"])
	}
	first, err := s.coordinator.lock(ctx, "run-"+run.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.coordinator.tryLock(ctx, "run-"+run.String()); err != errLockBusy {
		t.Fatalf("concurrent run lock: %v", err)
	}
	if err := first.release(); err != nil {
		t.Fatal(err)
	}
	second, err := s.coordinator.tryLock(ctx, "run-"+run.String())
	if err != nil {
		t.Fatal(err)
	}
	_ = second.release()
}

func TestTiDBLogCommitAndMetrics(t *testing.T) {
	cfg := isolatedDatabase(t)
	ctx := context.Background()
	if err := Migrate(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	db, err := open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &store{db: db, coordinator: &coordinator{db: db}}
	l := &Logs{store: s, retention: 24 * time.Hour, validate: nil}
	tenant := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	log := sqlcv1.V1LogLine{TenantID: tenant, ID: 1, CreatedAt: pgtype.Timestamptz{Time: now, Valid: true}, TaskID: 42, TaskInsertedAt: pgtype.Timestamptz{Time: now, Valid: true}, Message: "Unicode 世界", Level: sqlcv1.V1LogLineLevelINFO}
	workflow, step := uuid.New(), uuid.New()
	log.WorkflowID, log.StepID = &workflow, &step
	if err := l.insertBatch(ctx, uuid.New(), []sqlcv1.V1LogLine{log}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM v1_log_line WHERE tenant_id=?", uuidArg(tenant)).Scan(&count); err != nil || count != 1 {
		t.Fatalf("committed log count=%d err=%v", count, err)
	}
	query := "SELECT TIMESTAMPADD(MICROSECOND,FLOOR(TIMESTAMPDIFF(MICROSECOND,'1970-01-01 00:00:00',created_at)/?)*?,'1970-01-01 00:00:00'),SUM(level='INFO') FROM v1_log_line WHERE tenant_id=? GROUP BY 1"
	var rawBucket []byte
	var info int64
	if err := db.QueryRowContext(ctx, query, int64(time.Minute.Microseconds()), int64(time.Minute.Microseconds()), uuidArg(tenant)).Scan(&rawBucket, &info); err != nil {
		t.Fatal(fmt.Errorf("log metric SQL: %w", err))
	}
	bucket, err := time.ParseInLocation("2006-01-02 15:04:05.999999", string(rawBucket), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if info != 1 || bucket.After(now) || now.Sub(bucket) >= time.Minute {
		t.Fatalf("metric bucket=%v info=%d", bucket, info)
	}
}

func TestTiDBOutOfOrderEventAndReplay(t *testing.T) {
	cfg := isolatedDatabase(t)
	ctx := context.Background()
	if err := Migrate(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	r, _, err := New(ctx, cfg, Options{OLAPRetention: 24 * time.Hour, CoreRetention: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	tenant, run, workflow, taskExternal := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	at := time.Now().UTC().Truncate(time.Microsecond)
	stamp := pgtype.Timestamptz{Time: at, Valid: true}
	eventExternal := uuid.New()
	event := sqlcv1.CreateTaskEventsOLAPParams{TenantID: tenant, TaskID: 5, TaskInsertedAt: stamp, EventType: sqlcv1.V1EventTypeOlap("FINISHED"), WorkflowID: workflow, EventTimestamp: stamp, ReadableStatus: sqlcv1.V1ReadableStatusOlapCOMPLETED, ExternalID: eventExternal}
	if _, blocked, err := r.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{event}, map[uuid.UUID]uuid.UUID{eventExternal: run}, nil, nil); err != nil || len(blocked) != 0 {
		t.Fatalf("event before task: blocked=%d err=%v", len(blocked), err)
	}
	task := &repository.V1TaskWithPayload{V1Task: &sqlcv1.V1Task{ID: 5, InsertedAt: stamp, TenantID: tenant, ExternalID: taskExternal, WorkflowRunID: run, WorkflowID: workflow, InitialState: sqlcv1.V1TaskInitialState("QUEUED")}}
	if _, blocked, err := r.CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{task}); err != nil || len(blocked) != 0 {
		t.Fatalf("task creation: blocked=%d err=%v", len(blocked), err)
	}
	state, err := r.store.read(ctx, 0, entityFilter{Tenant: &tenant, Kind: "task", Keys: []string{keyFor(task.ID, at)}})
	if err != nil || len(state) != 1 {
		t.Fatalf("task state: rows=%d err=%v", len(state), err)
	}
	decoded, err := decodeEntity[sqlcv1.V1TasksOlap](state[0])
	if err != nil || decoded.ReadableStatus != sqlcv1.V1ReadableStatusOlapCOMPLETED {
		t.Fatalf("out-of-order status: %v err=%v", decoded.ReadableStatus, err)
	}
	runs, count, err := r.ListWorkflowRuns(ctx, tenant, repository.ListWorkflowRunOpts{CreatedAfter: at.Add(-time.Minute), Statuses: []sqlcv1.V1ReadableStatusOlap{sqlcv1.V1ReadableStatusOlapCOMPLETED}, WorkflowIds: []uuid.UUID{workflow}, Limit: 10})
	if err != nil || count != 1 || len(runs) != 1 {
		t.Fatalf("filtered run list: count=%d rows=%d err=%v", count, len(runs), err)
	}
	metrics, err := r.ReadTaskRunMetrics(ctx, tenant, repository.ReadTaskRunMetricsOpts{CreatedAfter: at.Add(-time.Minute), WorkflowIds: []uuid.UUID{workflow}})
	if err != nil || len(metrics) != 5 || metrics[2].Count != 1 {
		t.Fatalf("run summary metrics=%v err=%v", metrics, err)
	}
	start, end := at.Add(-time.Minute), at.Add(time.Minute)
	points, err := r.GetTaskPointMetrics(ctx, tenant, &start, &end, time.Minute)
	if err != nil || len(points) != 1 || points[0].CompletedCount != 1 {
		t.Fatalf("task metric points=%v err=%v", points, err)
	}
	if _, _, err := r.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{event}, map[uuid.UUID]uuid.UUID{eventExternal: run}, nil, nil); err != nil {
		t.Fatal(err)
	}
	rows, err := r.store.read(ctx, 0, entityFilter{Tenant: &tenant, Kind: "task_event", TaskIDs: []int64{5}})
	if err != nil || len(rows) != 1 {
		t.Fatalf("replayed event rows=%d err=%v", len(rows), err)
	}
	event.ExternalID = uuid.New()
	if _, _, err := r.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{event}, map[uuid.UUID]uuid.UUID{event.ExternalID: run}, nil, nil); err != nil {
		t.Fatal(err)
	}
	rows, err = r.store.read(ctx, 0, entityFilter{Tenant: &tenant, Kind: "task_event", TaskIDs: []int64{5}})
	if err != nil || len(rows) != 2 {
		t.Fatalf("distinct event rows=%d err=%v", len(rows), err)
	}
}

func TestTiDBTiFlashReplicaAndPlan(t *testing.T) {
	if os.Getenv("TIDB_TEST_TIFLASH") != "1" {
		t.Skip("TIDB_TEST_TIFLASH=1 is required")
	}
	cfg := isolatedDatabase(t)
	ctx := context.Background()
	if err := Migrate(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if err := EnableTiFlash(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(45 * time.Second)
	for {
		status, err := TiFlashStatus(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(status) == 2 && status[0].Available && status[1].Available {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("TiFlash replicas unavailable: %+v", status)
		}
		time.Sleep(time.Second)
	}
	db, err := open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, "EXPLAIN SELECT /*+ READ_FROM_STORAGE(TIFLASH[v1_runs_olap]) */ readable_status,COUNT(*) FROM v1_runs_olap GROUP BY readable_status")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	plan := strings.Builder{}
	for rows.Next() {
		var id, estRows, task, access, operator string
		if err := rows.Scan(&id, &estRows, &task, &access, &operator); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(operator)
		plan.WriteString(task)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(plan.String()), "tiflash") {
		t.Fatalf("TiFlash absent from query plan: %s", plan.String())
	}
}
