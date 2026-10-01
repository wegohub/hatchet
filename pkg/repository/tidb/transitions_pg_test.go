package tidb

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
)

func compareRetryAndOperatorContracts(t *testing.T, ctx context.Context, pg repository.OLAPRepository, td *Repository, tenant uuid.UUID, source *repository.V1TaskWithPayload) {
	t.Helper()
	copy := *source.V1Task
	copy.ID -= 100
	copy.ExternalID = uuid.New()
	copy.WorkflowRunID = copy.ExternalID
	copy.IsDurable = pgtype.Bool{Bool: true, Valid: true}
	task := &repository.V1TaskWithPayload{V1Task: &copy, Payload: source.Payload}
	backends := []repository.OLAPRepository{pg, td}
	for _, backend := range backends {
		if _, blocked, err := backend.CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{task}); err != nil || len(blocked) != 0 {
			t.Fatalf("durable create %v %v", blocked, err)
		}
	}
	cases := []struct {
		status sqlcv1.V1ReadableStatusOlap
		typ    sqlcv1.V1EventTypeOlap
		retry  int32
	}{
		{"QUEUED", "QUEUED", 0}, {"RUNNING", "STARTED", 0}, {"EVICTED", "DURABLE_EVICTED", 0}, {"RUNNING", "STARTED", 0},
		{"FAILED", "FAILED", 0}, {"QUEUED", "QUEUED", 1}, {"RUNNING", "STARTED", 1}, {"COMPLETED", "FINISHED", 1}, {"FAILED", "FAILED", 0},
	}
	for i, c := range cases {
		e := sqlcv1.CreateTaskEventsOLAPParams{TenantID: tenant, TaskID: copy.ID, TaskInsertedAt: copy.InsertedAt, ExternalID: uuid.New(), WorkflowID: copy.WorkflowID, ReadableStatus: c.status, EventType: c.typ, RetryCount: c.retry, EventTimestamp: timestamp(copy.InsertedAt.Time.Add(time.Duration(i+1) * time.Second))}
		if i >= 3 {
			e.DurableInvocationCount = 1
		}
		if c.status == "COMPLETED" {
			e.Output = []byte(`{"result":2}`)
		}
		for _, backend := range backends {
			if _, blocked, err := backend.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{e}, map[uuid.UUID]uuid.UUID{e.ExternalID: copy.ExternalID}, nil, nil); err != nil || len(blocked) != 0 {
				t.Fatalf("durable transition %v %v", blocked, err)
			}
		}
		a, err := pg.ReadTaskRun(ctx, copy.ExternalID)
		if err != nil {
			t.Fatal(err)
		}
		b, err := td.ReadTaskRun(ctx, copy.ExternalID)
		if err != nil {
			t.Fatal(err)
		}
		if a.ReadableStatus != b.ReadableStatus || a.LatestRetryCount != b.LatestRetryCount {
			t.Fatalf("transition %d PG=%s/%d TiDB=%s/%d", i, a.ReadableStatus, a.LatestRetryCount, b.ReadableStatus, b.LatestRetryCount)
		}
		pa, _, err := pg.ReadTaskRunData(ctx, tenant, copy.ID, copy.InsertedAt, nil)
		if err != nil {
			t.Fatal(err)
		}
		pb, _, err := td.ReadTaskRunData(ctx, tenant, copy.ID, copy.InsertedAt, nil)
		if err != nil {
			t.Fatal(err)
		}
		if pa.Status != pb.Status || pa.RetryCount != pb.RetryCount {
			t.Fatalf("retry projection %d PG=%s/%d TiDB=%s/%d", i, pa.Status, pa.RetryCount, pb.Status, pb.RetryCount)
		}
	}
	dag := &repository.DAGWithData{V1Dag: &sqlcv1.V1Dag{TenantID: tenant, ID: copy.ID - 100, ExternalID: uuid.New(), InsertedAt: copy.InsertedAt, WorkflowID: copy.WorkflowID, WorkflowVersionID: copy.WorkflowVersionID}, IsOperatorRun: true, Input: source.Payload, AdditionalMetadata: []byte(`{}`)}
	for _, backend := range backends {
		if _, err := backend.CreateDAGs(ctx, tenant, []*repository.DAGWithData{dag}); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		status sqlcv1.V1ReadableStatusOlap
		retry  int32
	}{{"RUNNING", 0}, {"FAILED", 0}, {"RUNNING", 1}, {"COMPLETED", 1}, {"FAILED", 0}} {
		update := repository.OrchestratorDAGStatusUpdateOpt{DagId: dag.ID, DagInsertedAt: dag.InsertedAt, ExternalId: dag.ExternalID, WorkflowId: dag.WorkflowID, WorkflowVersionId: dag.WorkflowVersionID, ReadableStatus: c.status, RetryCount: c.retry, AdditionalMetadata: []byte(`{"phase":"operator"}`)}
		for _, backend := range backends {
			if _, blocked, err := backend.CreateTaskEvents(ctx, tenant, nil, nil, []repository.OrchestratorDAGStatusUpdateOpt{update}, map[uuid.UUID]struct{}{dag.ExternalID: {}}); err != nil || len(blocked) != 0 {
				t.Fatalf("operator update %v %v", blocked, err)
			}
		}
		a, err := pg.ReadDAG(ctx, dag.ExternalID)
		if err != nil {
			t.Fatal(err)
		}
		b, err := td.ReadDAG(ctx, dag.ExternalID)
		if err != nil {
			t.Fatal(err)
		}
		if a.ReadableStatus != b.ReadableStatus || a.LatestRetryCount != b.LatestRetryCount || !equalJSON(a.AdditionalMetadata, b.AdditionalMetadata) {
			t.Fatalf("operator state PG=%s/%d TiDB=%s/%d", a.ReadableStatus, a.LatestRetryCount, b.ReadableStatus, b.LatestRetryCount)
		}
	}
}
