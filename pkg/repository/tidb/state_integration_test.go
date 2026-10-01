package tidb

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestStateWriterIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	w := newStateWriter(s)
	tenant, run, workflow := uuid.New(), uuid.New(), uuid.New()
	at := pgtype.Timestamptz{Time: time.Now().UTC().Truncate(time.Microsecond), Valid: true}
	first := &repository.V1TaskWithPayload{V1Task: &sqlcv1.V1Task{ID: 101, TenantID: tenant, ExternalID: uuid.New(), WorkflowRunID: run, InsertedAt: at, WorkflowID: workflow, DagID: pgtype.Int8{Int64: 100, Valid: true}, DagInsertedAt: at, InitialState: sqlcv1.V1TaskInitialStateQUEUED, AdditionalMetadata: []byte("{}")}, Payload: []byte(`{"value":1}`)}
	second := &repository.V1TaskWithPayload{V1Task: &sqlcv1.V1Task{ID: 102, TenantID: tenant, ExternalID: uuid.New(), WorkflowRunID: run, InsertedAt: at, WorkflowID: workflow, DagID: first.DagID, DagInsertedAt: at, InitialState: sqlcv1.V1TaskInitialStateQUEUED, AdditionalMetadata: []byte("{}")}}
	dag := &repository.DAGWithData{V1Dag: &sqlcv1.V1Dag{ID: 100, TenantID: tenant, ExternalID: run, InsertedAt: at, WorkflowID: workflow}, TotalTasks: 2, Input: []byte(`{"value":0}`), AdditionalMetadata: []byte("{}")}
	if _, err := w.CreateDAGs(ctx, tenant, []*repository.DAGWithData{dag}); err != nil {
		t.Fatal(err)
	}
	event := sqlcv1.CreateTaskEventsOLAPParams{TenantID: tenant, TaskID: first.ID, TaskInsertedAt: at, ExternalID: uuid.New(), WorkflowID: workflow, ReadableStatus: sqlcv1.V1ReadableStatusOlapCOMPLETED, EventType: sqlcv1.V1EventTypeOlapFINISHED, EventTimestamp: at, RetryCount: 2}
	result, _, err := w.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{event}, map[uuid.UUID]uuid.UUID{event.ExternalID: run}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.TaskRows) != 0 {
		t.Fatal("event for missing task produced a task notification")
	}
	result, _, err = w.CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.TaskRows) != 1 || result.TaskRows[0].ReadableStatus != "COMPLETED" {
		t.Fatalf("reconciliation outcome %#v", result)
	}
	state, err := w.load(ctx, tenant, []uuid.UUID{run})
	if err != nil {
		t.Fatal(err)
	}
	if state.dags[keyFor(100, at.Time)].ReadableStatus != "RUNNING" {
		t.Fatal("partial DAG completion is not RUNNING")
	}
	event.TaskID = second.ID
	event.ExternalID = uuid.New()
	event.RetryCount = 0
	result, _, err = w.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{event}, map[uuid.UUID]uuid.UUID{event.ExternalID: run}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.DAGRows) != 1 || result.DAGRows[0].ReadableStatus != "COMPLETED" {
		t.Fatalf("DAG completion %#v", result)
	}
	event.TaskID = first.ID
	event.ExternalID = uuid.New()
	event.RetryCount = 2
	event.EventType = sqlcv1.V1EventTypeOlapQUEUED
	event.ReadableStatus = sqlcv1.V1ReadableStatusOlapQUEUED
	result, _, err = w.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{event}, map[uuid.UUID]uuid.UUID{event.ExternalID: run}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.TaskRows) != 0 || len(result.DAGRows) != 0 {
		t.Fatal("late queued event regressed completed state")
	}
	first.Payload = []byte(`{"value":999}`)
	if _, _, err = w.CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{first}); err != nil {
		t.Fatal(err)
	}
	state, err = w.load(ctx, tenant, []uuid.UUID{run})
	if err != nil {
		t.Fatal(err)
	}
	if got := state.tasks[keyFor(first.ID, at.Time)]; got.ReadableStatus != "COMPLETED" || got.LatestRetryCount != 2 {
		t.Fatalf("duplicate creation overwrote outcome %#v", got)
	}
	if got := string(state.payloads[first.ExternalID.String()].Inline); got != `{"value":1}` {
		t.Fatalf("duplicate creation overwrote payload %s", got)
	}
}

func TestConcurrentDAGCompletionIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	w := newStateWriter(s)
	tenant, run, workflow := uuid.New(), uuid.New(), uuid.New()
	at := pgtype.Timestamptz{Time: time.Now().UTC().Truncate(time.Microsecond), Valid: true}
	dag := &repository.DAGWithData{V1Dag: &sqlcv1.V1Dag{ID: 200, TenantID: tenant, ExternalID: run, InsertedAt: at, WorkflowID: workflow}, TotalTasks: 2, AdditionalMetadata: []byte("{}")}
	if _, err := w.CreateDAGs(ctx, tenant, []*repository.DAGWithData{dag}); err != nil {
		t.Fatal(err)
	}
	var tasks []*repository.V1TaskWithPayload
	for id := int64(201); id <= 202; id++ {
		tasks = append(tasks, &repository.V1TaskWithPayload{V1Task: &sqlcv1.V1Task{ID: id, TenantID: tenant, ExternalID: uuid.New(), WorkflowRunID: run, InsertedAt: at, WorkflowID: workflow, DagID: pgtype.Int8{Int64: 200, Valid: true}, DagInsertedAt: at, InitialState: sqlcv1.V1TaskInitialStateQUEUED, AdditionalMetadata: []byte("{}")}})
	}
	if _, _, err := w.CreateTasks(ctx, tenant, tasks); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan *repository.StatusUpdateResult, 2)
	for _, task := range tasks {
		wg.Go(func() {
			event := sqlcv1.CreateTaskEventsOLAPParams{TenantID: tenant, TaskID: task.ID, TaskInsertedAt: at, ExternalID: uuid.New(), WorkflowID: workflow, ReadableStatus: sqlcv1.V1ReadableStatusOlapCOMPLETED, EventType: sqlcv1.V1EventTypeOlapFINISHED, EventTimestamp: at}
			for {
				result, blocked, err := w.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{event}, map[uuid.UUID]uuid.UUID{event.ExternalID: run}, nil, nil)
				if err != nil {
					t.Error(err)
					return
				}
				if len(blocked) == 0 {
					results <- result
					return
				}
				if ctx.Err() != nil {
					t.Error(ctx.Err())
					return
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
	wg.Wait()
	close(results)
	terminal := 0
	for result := range results {
		for _, dag := range result.DAGRows {
			if dag.ReadableStatus == "COMPLETED" {
				terminal++
			}
		}
	}
	if terminal != 1 {
		t.Fatalf("terminal DAG notifications %d, want 1", terminal)
	}
	state, err := w.load(ctx, tenant, []uuid.UUID{run})
	if err != nil {
		t.Fatal(err)
	}
	if state.dags[keyFor(200, at.Time)].ReadableStatus != "COMPLETED" {
		t.Fatal("concurrent final tasks left DAG incomplete")
	}
}
