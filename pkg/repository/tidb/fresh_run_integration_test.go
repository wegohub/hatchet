package tidb

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func TestInitializedRunSurvivesLockRetentionIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	r := &Repository{stateWriter: newStateWriter(s), retention: 30 * 24 * time.Hour, coreRetention: 7 * 24 * time.Hour}
	tenant, run := uuid.New(), uuid.New()
	at := time.Now().UTC().Truncate(time.Microsecond)
	task := projectionTask(t, tenant, run, 1, at)
	if _, _, err := r.CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{task}); err != nil {
		t.Fatal(err)
	}
	e := sqlcv1.CreateTaskEventsOLAPParams{TenantID: tenant, TaskID: task.ID, TaskInsertedAt: task.InsertedAt, WorkflowID: task.WorkflowID, ExternalID: uuid.New(), ReadableStatus: "COMPLETED", EventType: "FINISHED", EventTimestamp: task.InsertedAt}
	if _, _, err := r.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{e}, map[uuid.UUID]uuid.UUID{e.ExternalID: run}, nil, nil); err != nil {
		t.Fatal(err)
	}
	key := []byte("run-" + tenant.String() + "-" + run.String())
	if _, err := s.db.ExecContext(ctx, "UPDATE v1_olap_run_locks SET touched_at=? WHERE lock_key=?", at.AddDate(0, 0, -31), key); err != nil {
		t.Fatal(err)
	}
	if err := r.UpdateTablePartitions(ctx); err != nil {
		t.Fatal(err)
	}
	var initialized bool
	if err := s.db.QueryRowContext(ctx, "SELECT initialized FROM v1_olap_run_locks WHERE lock_key=?", key).Scan(&initialized); err != nil || !initialized {
		t.Fatalf("retained run lock initialization %t %v", initialized, err)
	}
	if _, _, err := r.CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{task}); err != nil {
		t.Fatal(err)
	}
	row, err := r.ReadTaskRun(ctx, run)
	if err != nil || row.ReadableStatus != "COMPLETED" {
		t.Fatalf("replayed creation %v %v", row, err)
	}
}
