package tidb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
)

type logTasks struct {
	repository.TaskRepository
	task *sqlcv1.V1Task
}

func (f logTasks) ListTasks(context.Context, uuid.UUID, []int64) ([]*sqlcv1.V1Task, error) {
	return []*sqlcv1.V1Task{f.task}, nil
}

func (f logTasks) FlattenExternalIds(context.Context, uuid.UUID, []uuid.UUID) ([]*sqlcv1.FlattenExternalIdsRow, error) {
	return []*sqlcv1.FlattenExternalIdsRow{{ID: f.task.ID, ExternalID: f.task.ExternalID, InsertedAt: f.task.InsertedAt}}, nil
}

func TestLogsIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	tenant, external, workflow, step := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	task := &sqlcv1.V1Task{ID: 42, TenantID: tenant, ExternalID: external, DisplayName: "task", InsertedAt: pgtype.Timestamptz{Time: time.Now().UTC().Truncate(time.Microsecond), Valid: true}}
	l := newLogs(s, logTasks{task: task}, 24*time.Hour, 20*time.Second)
	t.Cleanup(func() { _ = l.Close() })
	before := time.Now().UTC()
	opts := &repository.CreateLogLineOpts{TaskExternalId: external, TaskId: 42, TaskInsertedAt: task.InsertedAt, WorkflowId: workflow, StepId: step, Message: "same 猫 log", RetryCount: 1}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if err := l.PutLog(ctx, tenant, opts); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	rows, err := l.ListLogLines(ctx, tenant, &repository.ListLogsOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 20 {
		t.Fatalf("duplicate messages lost: %d", len(rows))
	}
	ids := make(map[int64]bool)
	for _, row := range rows {
		if row == nil || row.TaskExternalId != external || row.TaskDisplayName != "task" {
			t.Fatalf("task metadata %#v", row)
		}
		if ids[row.ID] {
			t.Fatalf("duplicate log identifier %d", row.ID)
		}
		ids[row.ID] = true
	}
	query := "same _ log"
	attempt := int32(2)
	rows, err = l.ListLogLines(ctx, tenant, &repository.ListLogsOpts{Search: &query, Attempt: &attempt, TaskExternalIds: []uuid.UUID{external}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 20 {
		t.Fatalf("Unicode LIKE search returned %d rows", len(rows))
	}
	metrics, err := l.GetLogLinePointMetrics(ctx, tenant, &repository.GetLogLinePointMetricsOpts{StartTimestamp: before, EndTimestamp: time.Now().UTC(), BucketInterval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, row := range metrics {
		total += row.InfoCount
	}
	if total != 20 {
		t.Fatalf("metrics count %d", total)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	if err = l.PutLog(ctx, tenant, opts); err == nil {
		t.Fatal("closed log writer accepted a write")
	}
}
