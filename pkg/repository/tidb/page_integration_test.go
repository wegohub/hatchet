package tidb

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func TestLargePageHydrationIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	r := &Repository{stateWriter: newStateWriter(s), retention: 30 * 24 * time.Hour}
	s.queryTimeout = 5 * time.Second
	tenant, workflow := uuid.New(), uuid.New()
	start := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Minute)
	tasks := make([]*repository.V1TaskWithPayload, 1010)
	for i := range tasks {
		id := uuid.New()
		tasks[i] = &repository.V1TaskWithPayload{V1Task: &sqlcv1.V1Task{
			TenantID: tenant, ID: int64(i + 1), ExternalID: id, WorkflowRunID: id,
			WorkflowID: workflow, WorkflowVersionID: uuid.New(), StepID: uuid.New(),
			InsertedAt:   timestamp(start.Add(time.Duration(i) * time.Microsecond)),
			InitialState: "QUEUED", Sticky: "NONE", Queue: "default", ActionID: "action",
			ScheduleTimeout: "1m", AdditionalMetadata: []byte(`{}`),
		}, Payload: []byte(`{"value":1}`)}
	}
	if _, blocked, err := r.CreateTasks(ctx, tenant, tasks); err != nil || len(blocked) != 0 {
		t.Fatalf("create page fixture: blocked=%d err=%v", len(blocked), err)
	}
	for _, include := range []bool{false, true} {
		rows, count, err := r.ListWorkflowRuns(ctx, tenant, repository.ListWorkflowRunOpts{CreatedAfter: start.Add(-time.Second), Limit: 1000, IncludePayloads: include})
		if err != nil || count != len(tasks) || len(rows) != 1000 {
			t.Fatalf("large page include=%t count=%d rows=%d err=%v", include, count, len(rows), err)
		}
		for i, row := range rows {
			if row.ExternalID != tasks[len(tasks)-1-i].ExternalID {
				t.Fatal("page order differs from SQL order")
			}
			if include && !equalJSON(row.Input, tasks[0].Payload) {
				t.Fatal("batched payload is missing")
			}
		}
	}
	rows, count, err := r.ListWorkflowRuns(ctx, tenant, repository.ListWorkflowRunOpts{CreatedAfter: start.Add(-time.Second), Limit: 1000, Offset: 1000})
	if err != nil || count != len(tasks) || len(rows) != 10 {
		t.Fatalf("last page count=%d rows=%d err=%v", count, len(rows), err)
	}
}
