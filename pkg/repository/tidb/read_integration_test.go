package tidb

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
