package tidb

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func compareExpandedContracts(t *testing.T, ctx context.Context, pg repository.OLAPRepository, td *Repository, pool *pgxpool.Pool, tenant uuid.UUID, task *repository.V1TaskWithPayload) {
	t.Helper()
	backends := []repository.OLAPRepository{pg, td}
	td.limits = pg.StatusUpdateBatchSizeLimits()
	for _, backend := range backends {
		row, err := backend.ReadTaskRun(ctx, task.ExternalID)
		if err != nil || row.ReadableStatus != "COMPLETED" {
			t.Fatalf("ReadTaskRun status %v %v", row, err)
		}
		run, err := backend.ReadWorkflowRun(ctx, task.ExternalID)
		if err != nil || run.WorkflowRun.ReadableStatus != "COMPLETED" {
			t.Fatalf("ReadWorkflowRun status %v %v", run, err)
		}
		names, err := backend.ListWorkflowRunDisplayNames(ctx, tenant, []uuid.UUID{task.ExternalID})
		if err != nil || len(names) != 1 || names[0].DisplayName != task.DisplayName {
			t.Fatalf("display names %v %v", names, err)
		}
		rows, err := backend.ListTasksByIdAndInsertedAt(ctx, tenant, []repository.TaskMetadata{{TaskID: task.ID, TaskInsertedAt: task.InsertedAt.Time}}, true)
		if err != nil || len(rows) != 1 || !equalJSON(rows[0].InputPayload, task.Payload) {
			t.Fatalf("task identity hydration %T rows=%v err=%v", backend, rows, err)
		}
		flat, err := backend.ListTasksByExternalIds(ctx, tenant, []uuid.UUID{task.ExternalID})
		if err != nil || len(flat) != 1 || flat[0].ID != task.ID {
			t.Fatalf("flattened task %v %v", flat, err)
		}
		times, err := backend.GetTaskStartedTimestamps(ctx, tenant, []int64{task.ID}, []time.Time{task.InsertedAt.Time}, []int32{0})
		if err != nil || len(times) != 1 || !times[0].StartedAt.Time.Equal(task.InsertedAt.Time.Add(time.Second)) {
			t.Fatalf("started timestamp %v %v", times, err)
		}
		durations, err := backend.GetTaskDurationsByTaskIds(ctx, tenant, []int64{task.ID}, []pgtype.Timestamptz{task.InsertedAt}, []sqlcv1.V1ReadableStatusOlap{"COMPLETED"})
		if err != nil || len(durations) != 1 || !durations[task.ID].FinishedAt.Time.Equal(task.InsertedAt.Time.Add(2*time.Second)) {
			t.Fatalf("task durations %v %v", durations, err)
		}
		timings, depths, err := backend.GetTaskTimings(ctx, tenant, task.ExternalID, 0)
		if err != nil || len(timings) != 1 || depths[task.ExternalID] != 0 {
			t.Fatalf("task timings %v %v %v", timings, depths, err)
		}
		ids, err := backend.ListWorkflowRunExternalIds(ctx, tenant, repository.ListWorkflowRunOpts{CreatedAfter: task.InsertedAt.Time.Add(-time.Second), Limit: 0})
		if err != nil || len(ids) != 1 || ids[0] != task.ExternalID {
			t.Fatalf("external IDs ignore pagination %v %v", ids, err)
		}
		counts, err := backend.ListYesterdayRunCountsByStatus(ctx)
		if err != nil || len(counts) != 0 {
			t.Fatalf("yesterday counts %v %v", counts, err)
		}
		if err = backend.CreateIncomingWebhookValidationFailureLogs(ctx, tenant, []repository.CreateIncomingWebhookFailureLogOpts{{WebhookName: "validation", ErrorText: "invalid input"}}); err != nil {
			t.Fatal(err)
		}
		if err = backend.StoreCELEvaluationFailures(ctx, tenant, []repository.CELEvaluationFailure{{Source: "FILTER", ErrorMessage: "invalid expression"}}); err != nil {
			t.Fatal(err)
		}
		if err = backend.ProcessOLAPPayloadCutovers(ctx, false, nil, 0, 0, false); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"v1_incoming_webhook_validation_failures_olap", "v1_cel_evaluation_failures_olap"} {
		var a, b int64
		if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+table+" WHERE tenant_id=$1", tenant).Scan(&a); err != nil {
			t.Fatal(err)
		}
		if err := td.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE tenant_id=?", uuidArg(tenant)).Scan(&b); err != nil || a != 1 || b != a {
			t.Fatalf("diagnostic persistence PG=%d TiDB=%d %v", a, b, err)
		}
	}
	rich := []byte(`{"rows":[{"a":1,"b":2},{"a":3,"b":4}],"tags":["雪","ß"],"number":9007199254740993.00,"nullable":null,"zero":"","nested":{"x":[1,2]},"truth":true}`)
	dagID, dagRun := task.ID-10, uuid.New()
	at := task.InsertedAt
	dag := &repository.DAGWithData{V1Dag: &sqlcv1.V1Dag{ID: dagID, TenantID: tenant, ExternalID: dagRun, InsertedAt: at, WorkflowID: task.WorkflowID, WorkflowVersionID: task.WorkflowVersionID, DisplayName: "dag"}, TotalTasks: 2, Input: []byte(`{"dag":true}`), AdditionalMetadata: rich}
	var children []*repository.V1TaskWithPayload
	var events []sqlcv1.CreateTaskEventsOLAPParams
	mapping := map[uuid.UUID]uuid.UUID{}
	for i := int64(1); i <= 2; i++ {
		child := projectionTask(t, tenant, uuid.New(), dagID-i, at.Time)
		child.WorkflowRunID = dagRun
		child.WorkflowID = task.WorkflowID
		child.WorkflowVersionID = task.WorkflowVersionID
		child.StepID = uuid.New()
		child.Sticky = sqlcv1.V1StickyStrategyNONE
		child.ScheduleTimeout = "1m"
		child.DagID = pgtype.Int8{Int64: dagID, Valid: true}
		child.DagInsertedAt = at
		child.AdditionalMetadata = rich
		children = append(children, child)
		for _, eventType := range []sqlcv1.V1EventTypeOlap{"QUEUED", "STARTED", "FINISHED"} {
			status := sqlcv1.V1ReadableStatusOlapQUEUED
			if eventType == "STARTED" {
				status = "RUNNING"
			}
			if eventType == "FINISHED" {
				status = "COMPLETED"
			}
			event := sqlcv1.CreateTaskEventsOLAPParams{TenantID: tenant, TaskID: child.ID, TaskInsertedAt: at, ExternalID: uuid.New(), WorkflowID: task.WorkflowID, EventTimestamp: at, EventType: eventType, ReadableStatus: status, Output: []byte(`{"result":1}`)}
			events = append(events, event)
			mapping[event.ExternalID] = dagRun
		}
	}
	for _, backend := range backends {
		if _, err := backend.CreateDAGs(ctx, tenant, []*repository.DAGWithData{dag}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := backend.CreateTasks(ctx, tenant, children); err != nil {
			t.Fatal(err)
		}
		if _, _, err := backend.CreateTaskEvents(ctx, tenant, events, mapping, nil, nil); err != nil {
			t.Fatal(err)
		}
		if _, _, err := backend.UpdateTaskStatuses(ctx, []uuid.UUID{tenant}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := backend.UpdateDAGStatuses(ctx, []uuid.UUID{tenant}); err != nil {
			t.Fatal(err)
		}
		row, err := backend.ReadDAG(ctx, dagRun)
		if err != nil || row.ReadableStatus != "COMPLETED" {
			t.Fatalf("DAG state %v %v", row, err)
		}
		rows, mapping, err := backend.ListTasksByDAGId(ctx, tenant, []uuid.UUID{dagRun}, true)
		if err != nil || len(rows) != 2 || len(mapping) != 2 {
			t.Fatalf("DAG tasks %d %v %v", len(rows), mapping, err)
		}
		events, err := backend.ListTaskRunEventsByWorkflowRunId(ctx, tenant, dagRun, false)
		if err != nil || len(events) != 6 {
			t.Fatalf("DAG event groups %d %v", len(events), err)
		}
		spans := &repository.CreateSpansOpts{TenantID: tenant, Spans: []*repository.SpanData{{TenantID: tenant, TaskRunExternalID: &children[0].ExternalID, WorkflowRunID: &dagRun, Name: "trace", TraceID: make([]byte, 16), SpanID: []byte{1, 2, 3, 4, 5, 6, 7, 8}, StartTimeUnixNano: uint64(at.Time.UnixNano()), EndTimeUnixNano: uint64(at.Time.Add(time.Millisecond).UnixNano()), ResourceAttributes: []byte(`{"service.name":"Unicode 雪"}`), Attributes: []byte(`{"key":"value"}`)}}}
		if err = backend.CreateSpans(ctx, tenant, spans); err != nil {
			t.Fatal(err)
		}
		if err = backend.CreateSpanLookupTableEntries(ctx, tenant, spans); err != nil {
			t.Fatal(err)
		}
		trace, err := backend.LookUpTraceId(ctx, tenant, dagRun)
		if err != nil || !reflect.DeepEqual(trace, make([]byte, 16)) {
			t.Fatalf("trace lookup %v %v", trace, err)
		}
		page, err := backend.ListSpansByTraceId(ctx, tenant, trace, 0, 1)
		if err != nil || len(page.Rows) != 1 || page.Total != 1 || page.Rows[0].ServiceName != "Unicode 雪" {
			t.Fatalf("trace page %v %v", page, err)
		}
	}
	cases := []string{`{"rows":[{"a":1,"b":4}]}`, `{"rows":[{"a":1},{"b":4}]}`, `{"tags":["ß","雪","雪"]}`, `{"number":9007199254740993}`, `{"number":9007199254740992}`, `{"nullable":null}`, `{"zero":""}`, `{"nested":{"x":[2]}}`, `{"tags":"雪"}`, `{"truth":true}`}
	for _, raw := range cases {
		value, err := parseJSON([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		opts := repository.ListWorkflowRunOpts{CreatedAfter: at.Time.Add(-time.Second), WorkflowIds: []uuid.UUID{task.WorkflowID}, AdditionalMetadata: value.(map[string]any), AdditionalMetadataOperator: repository.AdditionalMetadataOperatorAnd, Limit: 50, IncludePayloads: true}
		a, ac, err := pg.ListWorkflowRuns(ctx, tenant, opts)
		if err != nil {
			t.Fatal(err)
		}
		b, bc, err := td.ListWorkflowRuns(ctx, tenant, opts)
		if err != nil {
			t.Fatal(err)
		}
		if ac != bc || len(a) != len(b) {
			t.Fatalf("metadata %s PG=%d TiDB=%d", raw, ac, bc)
		}
	}
	for _, filter := range []map[string]any{{"number": "9007199254740993.00"}, {"truth": "true"}, {"nullable": "null"}, {"nested": `{"x": [1, 2]}`}, {"tags": `["雪", "ß"]`}, {"zero": ""}} {
		opts := repository.ListWorkflowRunOpts{CreatedAfter: at.Time.Add(-time.Second), AdditionalMetadata: filter, AdditionalMetadataOperator: repository.AdditionalMetadataOperatorOr, Limit: 50}
		_, a, err := pg.ListWorkflowRuns(ctx, tenant, opts)
		if err != nil {
			t.Fatal(err)
		}
		_, b, err := td.ListWorkflowRuns(ctx, tenant, opts)
		if err != nil || a != b {
			t.Fatalf("OR filter %v PG=%d TiDB=%d %v", filter, a, b, err)
		}
	}
	for _, filter := range []map[string]any{{"truth": "true", "absent": "never"}, {"nested": `{"x": [1, 2]}`, "absent": "never"}, {"zero": "", "absent": "never"}, {"absent": "never", "truth": "false"}} {
		opts := repository.ReadTaskRunMetricsOpts{CreatedAfter: at.Time.Add(-time.Second), AdditionalMetadata: filter}
		a, err := pg.ReadTaskRunMetrics(ctx, tenant, opts)
		if err != nil {
			t.Fatal(err)
		}
		b, err := td.ReadTaskRunMetrics(ctx, tenant, opts)
		if err != nil || !reflect.DeepEqual(a, b) {
			t.Fatalf("status metrics OR filter %v PG=%v TiDB=%v %v", filter, a, b, err)
		}
	}
	for _, backend := range backends {
		opts := repository.ListWorkflowRunOpts{CreatedAfter: at.Time.Add(-time.Second), Limit: 1, Offset: 1}
		rows, count, err := backend.ListWorkflowRuns(ctx, tenant, opts)
		if err != nil || count != 2 || len(rows) != 1 {
			t.Fatalf("run page count=%d len=%d err=%v", count, len(rows), err)
		}
		opts.Offset = 100
		rows, count, err = backend.ListWorkflowRuns(ctx, tenant, opts)
		if err != nil || count != 2 || len(rows) != 0 {
			t.Fatalf("empty page count=%d len=%d err=%v", count, len(rows), err)
		}
	}
	yesterdayA, err := pg.GetDAGDurations(ctx, tenant, []uuid.UUID{dagRun}, timestamp(at.Time.Add(-time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	yesterdayB, err := td.GetDAGDurations(ctx, tenant, []uuid.UUID{dagRun}, timestamp(at.Time.Add(-time.Second)))
	sameDurations := len(yesterdayA) == len(yesterdayB)
	for key, a := range yesterdayA {
		b := yesterdayB[key]
		sameDurations = sameDurations && b != nil && a.ExternalID == b.ExternalID && a.StartedAt.Valid == b.StartedAt.Valid && a.FinishedAt.Valid == b.FinishedAt.Valid && a.StartedAt.Time.Equal(b.StartedAt.Time) && a.FinishedAt.Time.Equal(b.FinishedAt.Time)
	}
	if err != nil || !sameDurations {
		t.Fatalf("DAG duration projections differ PG=%#v TiDB=%#v %v", yesterdayA[dagRun.String()], yesterdayB[dagRun.String()], err)
	}
	start, end := at.Time.Add(-time.Second), at.Time.Add(time.Second)
	pointA, err := pg.GetTaskPointMetrics(ctx, tenant, &start, &end, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	pointB, err := td.GetTaskPointMetrics(ctx, tenant, &start, &end, time.Hour)
	if err != nil || len(pointA) != len(pointB) {
		t.Fatalf("time bucket counts differ %v", err)
	}
	for i := range pointA {
		if !pointA[i].MinuteBucket.Time.Equal(pointB[i].MinuteBucket.Time) || pointA[i].CompletedCount != pointB[i].CompletedCount || pointA[i].FailedCount != pointB[i].FailedCount {
			t.Fatal("time bucket values differ")
		}
	}
	keysA, err := pg.ListEventKeys(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	keysB, err := td.ListEventKeys(ctx, tenant)
	sort.Strings(keysA)
	sort.Strings(keysB)
	if err != nil || !reflect.DeepEqual(keysA, keysB) {
		t.Fatalf("event keys differ %v", err)
	}
	pg.SetReadReplicaPool(pool)
	td.SetReadReplicaPool(pool)
	if !reflect.DeepEqual(pg.StatusUpdateBatchSizeLimits(), td.StatusUpdateBatchSizeLimits()) {
		t.Fatal("configured batch limits differ")
	}
	for _, backend := range backends {
		if err = backend.UpdateTablePartitions(ctx); err != nil {
			t.Fatal(err)
		}
		if err = backend.AnalyzeOLAPTables(ctx); err != nil {
			t.Fatal(err)
		}
	}
}
