package clickhouse

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/hatchet-dev/hatchet/pkg/validator"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestRepositoryReadsIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	r := &Repository{stateWriter: newStateWriter(s), retention: 30 * 24 * time.Hour, coreRetention: 7 * 24 * time.Hour, validate: validator.NewDefaultValidator()}
	tenant, run, workflow := uuid.New(), uuid.New(), uuid.New()
	at := timestamp(time.Now().UTC().Truncate(time.Microsecond))
	task := &repository.V1TaskWithPayload{V1Task: &sqlcv1.V1Task{ID: 1, TenantID: tenant, ExternalID: run, WorkflowRunID: run, WorkflowID: workflow, InsertedAt: at, InitialState: "QUEUED", AdditionalMetadata: []byte(`{"nested":{"number":1}}`)}, Payload: []byte(`{"input":42}`)}
	if _, _, err := r.CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{task}); err != nil {
		t.Fatal(err)
	}
	e := sqlcv1.CreateTaskEventsOLAPParams{TenantID: tenant, TaskID: 1, TaskInsertedAt: at, ExternalID: uuid.New(), WorkflowID: workflow, ReadableStatus: "COMPLETED", EventType: "FINISHED", EventTimestamp: at, Output: []byte(`{"output":42}`)}
	if _, _, err := r.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{e}, map[uuid.UUID]uuid.UUID{e.ExternalID: run}, nil, nil); err != nil {
		t.Fatal(err)
	}
	rows, count, err := r.ListWorkflowRuns(ctx, tenant, repository.ListWorkflowRunOpts{CreatedAfter: at.Time.Add(-time.Second), Limit: 100, IncludePayloads: true})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(rows) != 1 || rows[0].ReadableStatus != "COMPLETED" || string(rows[0].Output) != `{"output":42}` {
		t.Fatalf("workflow output: count=%d rows=%#v", count, rows)
	}
	data, _, err := r.ReadTaskRunData(ctx, tenant, 1, at, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(data.InputPayload) != `{"input":42}` || string(data.OutputPayload) != `{"output":42}` {
		t.Fatalf("task payloads %s %s", data.InputPayload, data.OutputPayload)
	}
	limit := int64(100)
	events, err := r.ListTaskRunEvents(ctx, tenant, 1, at, &limit, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].EventExternalID != e.ExternalID {
		t.Fatalf("task events %#v", events)
	}
}

func TestEventTraceRetentionReadsIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	r := &Repository{stateWriter: newStateWriter(s), retention: 30 * 24 * time.Hour, coreRetention: 7 * 24 * time.Hour, validate: validator.NewDefaultValidator()}
	tenant, event, run, workflow := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	at := timestamp(time.Now().UTC().Truncate(time.Microsecond))
	task := &repository.V1TaskWithPayload{V1Task: &sqlcv1.V1Task{ID: 5, TenantID: tenant, ExternalID: run, WorkflowRunID: run, WorkflowID: workflow, InsertedAt: at, InitialState: "QUEUED", AdditionalMetadata: []byte("{}")}}
	if _, _, err := r.CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{task}); err != nil {
		t.Fatal(err)
	}
	opts := repository.BulkCreateEventsAndTriggersParams{BulkCreateEventsOLAPParams: &sqlcv1.BulkCreateEventsOLAPParams{Tenantids: []uuid.UUID{tenant}, Externalids: []uuid.UUID{event}, Seenats: []pgtype.Timestamptz{at}, Keys: []string{"event-key"}, Additionalmetadatas: [][]byte{[]byte(`{"number":1.0}`)}, Scopes: []pgtype.Text{{String: "scope", Valid: true}}, TriggeringWebhookNames: []pgtype.Text{{}}}, Payloads: [][]byte{[]byte(`{"payload":42}`)}}
	triggers := []repository.EventTriggersFromExternalId{{RunID: 5, RunInsertedAt: at, EventExternalId: event, EventSeenAt: at}}
	for range 2 {
		if err := r.BulkCreateEventsAndTriggers(ctx, opts, triggers); err != nil {
			t.Fatal(err)
		}
	}
	rows, count, err := r.ListEvents(ctx, sqlcv1.ListEventsParams{Tenantid: tenant, Since: timestamp(at.Time.Add(-time.Second)), WorkflowIds: []uuid.UUID{workflow}, Statuses: []string{"QUEUED"}, AdditionalMetadata: []byte(`{"number":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if *count != 1 || len(rows) != 1 || rows[0].QueuedCount != 1 || string(rows[0].Payload) != `{"payload":42}` {
		t.Fatalf("event data %#v count %d", rows, *count)
	}
	runs, total, err := r.ListWorkflowRuns(ctx, tenant, repository.ListWorkflowRunOpts{TriggeringEventExternalId: &event, Limit: 10})
	if err != nil || total != 1 || len(runs) != 1 {
		t.Fatalf("event filter %v %d %v", runs, total, err)
	}
	trace := []byte{1, 2, 3}
	span := &repository.SpanData{TaskRunExternalID: &run, WorkflowRunID: &run, Name: "span", TraceID: trace, SpanID: []byte{4}, StartTimeUnixNano: uint64(at.Time.UnixNano()), EndTimeUnixNano: uint64(at.Time.Add(time.Second).UnixNano()), ResourceAttributes: []byte(`{"service.name":"test-service"}`), Attributes: []byte(`{"key":42}`)}
	spans := &repository.CreateSpansOpts{TenantID: tenant, Spans: []*repository.SpanData{span}}
	for range 2 {
		if err = r.CreateSpans(ctx, tenant, spans); err != nil {
			t.Fatal(err)
		}
		if err = r.CreateSpanLookupTableEntries(ctx, tenant, spans); err != nil {
			t.Fatal(err)
		}
	}
	result, err := r.ListSpansByTraceId(ctx, tenant, trace, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 || result.Rows[0].ServiceName != "test-service" {
		t.Fatalf("spans %#v", result)
	}
	found, err := r.LookUpTraceId(ctx, tenant, run)
	if err != nil || !bytes.Equal(found, trace) {
		t.Fatalf("trace %x %v", found, err)
	}
	if err = r.UpdateTablePartitions(ctx); err != nil {
		t.Fatal(err)
	}
	p, err := r.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: event, InsertedAt: at})
	if err != nil || string(p) != `{"payload":42}` {
		t.Fatalf("retention deleted live payload %s %v", p, err)
	}
}

func TestRepositoryReplicaAddressValidationIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	cfg := s.keeper.config
	cfg.Addresses = append(cfg.Addresses, cfg.Addresses[0])
	r, _, err := New(ctx, cfg, Options{OLAPRetention: 24 * time.Hour, CoreRetention: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDetailReadsPinnedRetryAndChildrenIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	r := &Repository{stateWriter: newStateWriter(s), retention: 30 * 24 * time.Hour}
	tenant, run, workflow, dagRun := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	at := timestamp(time.Now().UTC().Truncate(time.Microsecond))
	parent := &repository.V1TaskWithPayload{V1Task: &sqlcv1.V1Task{ID: 101, TenantID: tenant, ExternalID: run, WorkflowRunID: run, WorkflowID: workflow, InsertedAt: at, InitialState: "QUEUED"}, Payload: []byte(`{"input":1}`)}
	if _, _, err := r.CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{parent}); err != nil {
		t.Fatal(err)
	}
	e := sqlcv1.CreateTaskEventsOLAPParams{TenantID: tenant, TaskID: 101, TaskInsertedAt: at, ExternalID: uuid.New(), WorkflowID: workflow, ReadableStatus: "COMPLETED", EventType: "FINISHED", EventTimestamp: at, Output: []byte(`{"attempt":0}`)}
	if _, _, err := r.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{e}, map[uuid.UUID]uuid.UUID{e.ExternalID: run}, nil, nil); err != nil {
		t.Fatal(err)
	}
	pinned, err := r.snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dag := &repository.DAGWithData{V1Dag: &sqlcv1.V1Dag{ID: 201, TenantID: tenant, ExternalID: dagRun, InsertedAt: at, WorkflowID: workflow}, ParentTaskExternalID: &run, TotalTasks: 1, Input: []byte(`{"dag":1}`)}
	if _, err = r.CreateDAGs(ctx, tenant, []*repository.DAGWithData{dag}); err != nil {
		t.Fatal(err)
	}
	child := &repository.V1TaskWithPayload{V1Task: &sqlcv1.V1Task{ID: 102, TenantID: tenant, ExternalID: uuid.New(), InsertedAt: at, WorkflowID: workflow, InitialState: "QUEUED", ParentTaskExternalID: &run}}
	child.WorkflowRunID = child.ExternalID
	dagStep := &repository.V1TaskWithPayload{V1Task: &sqlcv1.V1Task{ID: 202, TenantID: tenant, ExternalID: uuid.New(), WorkflowRunID: dagRun, InsertedAt: at, WorkflowID: workflow, InitialState: "QUEUED", ParentTaskExternalID: &run, DagID: pgtype.Int8{Int64: 201, Valid: true}, DagInsertedAt: at}}
	if _, _, err = r.CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{child, dagStep}); err != nil {
		t.Fatal(err)
	}
	e.ExternalID, e.RetryCount, e.Output = uuid.New(), 1, []byte(`{"attempt":1}`)
	e.EventTimestamp = timestamp(at.Time.Add(time.Second))
	if _, _, err = r.CreateTaskEvents(ctx, tenant, []sqlcv1.CreateTaskEventsOLAPParams{e}, map[uuid.UUID]uuid.UUID{e.ExternalID: run}, nil, nil); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		pinned    bool
		retry     *int
		wantRetry int32
		children  int64
		output    string
	}{
		{name: "pinned", pinned: true, wantRetry: 0, children: 0, output: `{"attempt":0}`},
		{name: "latest", wantRetry: 1, children: 2, output: `{"attempt":1}`},
		{name: "explicit_first_attempt", retry: new(int), wantRetry: 0, children: 2, output: `{"attempt":0}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readCtx := ctx
			if tc.pinned {
				readCtx = pinned
			}
			data, returnedRun, err := r.ReadTaskRunData(readCtx, tenant, 101, at, tc.retry)
			if err != nil {
				t.Fatal(err)
			}
			if returnedRun != run || data.RetryCount != tc.wantRetry || data.NumSpawnedChildren != tc.children || data.Status != "COMPLETED" || string(data.InputPayload) != `{"input":1}` || string(data.OutputPayload) != tc.output {
				t.Fatalf("detail mismatch retry=%d children=%d status=%s input=%s output=%s", data.RetryCount, data.NumSpawnedChildren, data.Status, data.InputPayload, data.OutputPayload)
			}
			if tc.retry == nil {
				workflow, err := r.ReadWorkflowRun(readCtx, run)
				if err != nil {
					t.Fatal(err)
				}
				if string(workflow.WorkflowRun.Input) != `{"input":1}` || string(workflow.WorkflowRun.Output) != tc.output {
					t.Fatalf("workflow payload mismatch %#v", workflow.WorkflowRun)
				}
				raw, err := r.ReadTaskRun(readCtx, run)
				if err != nil {
					t.Fatal(err)
				}
				if raw.LatestRetryCount != tc.wantRetry {
					t.Fatalf("raw task retry %d", raw.LatestRetryCount)
				}
			}
		})
	}
	d, err := r.ReadWorkflowRun(ctx, dagRun)
	if err != nil {
		t.Fatal(err)
	}
	if d.WorkflowRun.Kind != sqlcv1.V1RunKindDAG || string(d.WorkflowRun.Input) != `{"dag":1}` {
		t.Fatalf("DAG detail mismatch %#v", d.WorkflowRun)
	}
}
