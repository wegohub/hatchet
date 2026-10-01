package tidb

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func projectionTask(t *testing.T, tenant, run uuid.UUID, id int64, at time.Time) *repository.V1TaskWithPayload {
	t.Helper()
	return &repository.V1TaskWithPayload{V1Task: &sqlcv1.V1Task{TenantID: tenant, ID: id, ExternalID: run, WorkflowRunID: run, InsertedAt: timestamp(at), WorkflowID: uuid.New(), InitialState: "QUEUED", AdditionalMetadata: []byte(`{}`)}, Payload: []byte(`{"v":1}`)}
}
func TestV2CurrentReadAfterEarlierSnapshot(t *testing.T) {
	s, ctx := integrationStore(t)
	w := newStateWriter(s)
	tenant, run := uuid.New(), uuid.New()
	at := time.Now().UTC().Truncate(time.Microsecond)
	task := projectionTask(t, tenant, run, 1, at)
	if _, _, err := w.CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{task}); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var n int
	if err = tx.QueryRowContext(ctx, "SELECT /*+ READ_FROM_STORAGE(TIKV[v1_runs_olap]) */ COUNT(*) FROM v1_runs_olap").Scan(&n); err != nil {
		t.Fatal(err)
	}
	event := sqlcv1.CreateTaskEventsOLAPParams{TenantID: tenant, TaskID: task.ID, TaskInsertedAt: task.InsertedAt, ExternalID: uuid.New(), WorkflowID: task.WorkflowID, ReadableStatus: "RUNNING", EventType: "STARTED", EventTimestamp: timestamp(at.Add(time.Second))}
	if _, _, err = w.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{event}, map[uuid.UUID]uuid.UUID{event.ExternalID: run}, nil, nil); err != nil {
		t.Fatal(err)
	}
	var key []byte
	if err = tx.QueryRowContext(ctx, "SELECT lock_key FROM v1_olap_run_locks WHERE lock_key=? FOR UPDATE", []byte("run-"+tenant.String()+"-"+run.String())).Scan(&key); err != nil {
		t.Fatal(err)
	}
	state, err := w.loadCurrent(context.WithValue(ctx, transactionKey{}, tx), tenant, []uuid.UUID{run}, []int64{task.ID}, at)
	if err != nil {
		t.Fatal(err)
	}
	if state.tasks[keyFor(task.ID, at)].ReadableStatus != "RUNNING" {
		t.Fatal("current read retained an earlier snapshot")
	}
}
func TestV2PendingCountsAndLostCommitResponse(t *testing.T) {
	s, ctx := integrationStore(t)
	r := &Repository{stateWriter: newStateWriter(s), retention: 30 * 24 * time.Hour}
	tenant, run := uuid.New(), uuid.New()
	at := time.Now().UTC().Truncate(time.Microsecond)
	task := projectionTask(t, tenant, run, 2, at)
	event := sqlcv1.CreateTaskEventsOLAPParams{TenantID: tenant, TaskID: task.ID, TaskInsertedAt: task.InsertedAt, ExternalID: uuid.New(), WorkflowID: task.WorkflowID, ReadableStatus: "COMPLETED", EventType: "FINISHED", EventTimestamp: timestamp(at), Output: []byte(`{"done":true}`)}
	for range 2 {
		if _, _, err := r.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{event}, map[uuid.UUID]uuid.UUID{event.ExternalID: run}, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := r.CountOLAPTempTableSizeForTaskStatusUpdates(ctx); err != nil || n != 1 {
		t.Fatalf("pending count %d %v", n, err)
	}
	more, updates, err := r.UpdateTaskStatuses(ctx, []uuid.UUID{tenant})
	if err != nil || more || len(updates) != 0 {
		t.Fatalf("missing-task compensation %v %v %v", more, updates, err)
	}
	s.commitHook = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		return errors.New("commit acknowledgement lost")
	}
	if _, _, err = r.CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{task}); err != nil {
		t.Fatal(err)
	}
	s.commitHook = nil
	if n, err := r.CountOLAPTempTableSizeForTaskStatusUpdates(ctx); err != nil || n != 0 {
		t.Fatalf("resolved count %d %v", n, err)
	}
	state, err := r.loadCurrent(ctx, tenant, []uuid.UUID{run}, []int64{task.ID}, at)
	if err != nil {
		t.Fatal(err)
	}
	if state.tasks[keyFor(task.ID, at)].ReadableStatus != "COMPLETED" {
		t.Fatal("out-of-order event was not applied")
	}
	var count int
	if err = s.db.QueryRowContext(ctx, "SELECT event_count FROM v1_task_attempts_olap WHERE tenant_id=? AND task_id=? AND task_inserted_at=?", uuidArg(tenant), task.ID, dateArg(at)).Scan(&count); err != nil || count != 1 {
		t.Fatalf("attempt event replay count=%d err=%v", count, err)
	}
}
func TestV2MigrationRejectsLegacyAndSchemaDrift(t *testing.T) {
	cfg := isolatedDatabase(t)
	ctx := context.Background()
	db, err := open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, "CREATE TABLE entity(id BIGINT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if err = Migrate(ctx, cfg); err == nil {
		t.Fatal("legacy database upgraded without an empty schema")
	}
	if _, err = db.ExecContext(ctx, "DROP TABLE entity"); err != nil {
		t.Fatal(err)
	}
	if err = Migrate(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, "ALTER TABLE v1_log_line MODIFY message LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL"); err != nil {
		t.Fatal(err)
	}
	if err = validateSchema(ctx, db); err == nil {
		t.Fatal("incompatible text collation accepted")
	}
	if _, err = db.ExecContext(ctx, "ALTER TABLE v1_log_line MODIFY message LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL"); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, "ALTER TABLE v1_runs_olap DROP INDEX run_status"); err != nil {
		t.Fatal(err)
	}
	if err = validateSchema(ctx, db); err == nil {
		t.Fatal("missing index accepted")
	}
}

func TestV2RemoteControllerLockAndExpiredCache(t *testing.T) {
	s, ctx := integrationStore(t)
	tenant, a, b := uuid.New(), uuid.New(), uuid.New()
	first, blocked, err := s.runLocks(ctx, tenant, []uuid.UUID{a})
	if err != nil || len(blocked) != 0 {
		t.Fatalf("first lock %v %v", blocked, err)
	}
	remote := &store{db: s.db, coordinator: &coordinator{db: s.db}}
	second, blocked, err := remote.runLocks(ctx, tenant, []uuid.UUID{b, a})
	if err != nil || len(blocked) != 1 {
		releaseLocks(first)
		t.Fatalf("remote lock %v %v", blocked, err)
	}
	if _, ok := blocked[a]; !ok {
		t.Fatal("remote contention not attributed to run")
	}
	releaseLocks(second)
	releaseLocks(first)
	key := "run-" + tenant.String() + "-" + a.String()
	if _, err = s.db.ExecContext(ctx, "DELETE FROM v1_olap_run_locks WHERE lock_key=?", []byte(key)); err != nil {
		t.Fatal(err)
	}
	third, blocked, err := s.runLocks(ctx, tenant, []uuid.UUID{a})
	if err != nil || len(blocked) != 1 {
		t.Fatalf("missing cached lock %v %v", blocked, err)
	}
	releaseLocks(third)
	fourth, blocked, err := s.runLocks(ctx, tenant, []uuid.UUID{a})
	if err != nil || len(blocked) != 0 {
		t.Fatalf("recreated lock %v %v", blocked, err)
	}
	releaseLocks(fourth)
}

func TestV2LostCommitResponseAfterSubsequentState(t *testing.T) {
	s, ctx := integrationStore(t)
	tenant, run := uuid.New(), uuid.New()
	task := projectionTask(t, tenant, run, 72, time.Now().UTC().Truncate(time.Microsecond))
	remoteStore := &store{db: s.db, coordinator: &coordinator{db: s.db}}
	remote := newStateWriter(remoteStore)
	s.commitHook = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		event := sqlcv1.CreateTaskEventsOLAPParams{TenantID: tenant, TaskID: task.ID, TaskInsertedAt: task.InsertedAt, ExternalID: uuid.New(), WorkflowID: task.WorkflowID, ReadableStatus: "RUNNING", EventType: "STARTED", EventTimestamp: task.InsertedAt}
		if _, _, err := remote.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{event}, map[uuid.UUID]uuid.UUID{event.ExternalID: run}, nil, nil); err != nil {
			return err
		}
		return errors.New("commit response lost after subsequent update")
	}
	if _, _, err := newStateWriter(s).CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{task}); err != nil {
		t.Fatal(err)
	}
	rows, err := s.read(ctx, 0, entityFilter{Tenant: &tenant, Kind: "task", ExternalIDs: []uuid.UUID{run}})
	if err != nil || len(rows) != 1 {
		t.Fatalf("committed task %v %v", rows, err)
	}
	row, err := decodeEntity[sqlcv1.V1TasksOlap](rows[0])
	if err != nil || row.ReadableStatus != "RUNNING" {
		t.Fatalf("subsequent state %v %v", row, err)
	}
}
