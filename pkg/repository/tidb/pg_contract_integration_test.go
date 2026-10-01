package tidb

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/config/limits"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/hatchet-dev/hatchet/pkg/validator"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

func TestPostgresTiDBReadContractIntegration(t *testing.T) {
	testPostgresTiDBContract(t, false)
}

func TestPostgresTiDBCountBoundaryIntegration(t *testing.T) {
	testPostgresTiDBContract(t, true)
}

func testPostgresTiDBContract(t *testing.T, countBoundary bool) {
	t.Helper()
	url := os.Getenv("TIDB_CONTRACT_POSTGRES_URL")
	if url == "" {
		t.Skip("set TIDB_CONTRACT_POSTGRES_URL for shared backend contracts")
	}
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
	s := &store{db: db, coordinator: &coordinator{db: db}}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		for _, name := range []string{"v1_readable_status_olap", "_v1_readable_status_olap", "v1_log_line_level", "_v1_log_line_level"} {
			typ, err := conn.LoadType(ctx, name)
			if err != nil {
				return err
			}
			conn.TypeMap().RegisterType(typ)
		}
		_, err := conn.Exec(ctx, "SET TIME ZONE 'UTC'")
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, table := range []string{"v1_cel_evaluation_failures_olap", "v1_incoming_webhook_validation_failures_olap", "v1_otel_trace_olap", "v1_otel_trace_lookup_olap"} {
		if _, err := pool.Exec(ctx, "SELECT create_v1_range_partition($1::text,$2::date)", table, pgtype.Date{Time: time.Now().UTC(), Valid: true}); err != nil {
			t.Fatal(err)
		}
	}

	log := zerolog.Nop()
	pg, cleanup := repository.NewOLAPRepositoryFromPool(pool, &log, 30*24*time.Hour, limits.LimitConfigFile{}, false, false, repository.PayloadStoreRepositoryOpts{}, repository.StatusUpdateBatchSizeLimits{Task: 100, DAG: 100}, time.Second, false)
	t.Cleanup(func() { _ = cleanup() })
	if err := pg.UpdateTablePartitions(ctx); err != nil {
		t.Fatal(err)
	}
	td := &Repository{stateWriter: newStateWriter(s), retention: 30 * 24 * time.Hour, coreRetention: 7 * 24 * time.Hour, validate: validator.NewDefaultValidator()}
	if countBoundary {
		compareCappedListCounts(t, ctx, pg, td, pool)
		return
	}
	tenant, run, workflow := uuid.New(), uuid.New(), uuid.New()
	at := timestamp(time.Now().UTC().Truncate(time.Microsecond))
	id := -time.Now().UnixNano() / 1000
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DELETE FROM v1_event_to_run_olap WHERE event_id IN (SELECT event_id FROM v1_event_lookup_table_olap WHERE tenant_id=$1)", tenant); err != nil {
			t.Error(err)
		}
		for _, table := range []string{"v1_task_events_olap", "v1_tasks_olap", "v1_dags_olap", "v1_runs_olap", "v1_statuses_olap", "v1_lookup_table_olap", "v1_payloads_olap", "v1_event_lookup_table_olap", "v1_events_olap"} {
			if _, err := pool.Exec(context.Background(), "DELETE FROM "+table+" WHERE tenant_id=$1", tenant); err != nil {
				t.Error(err)
			}
		}
	})
	task := &repository.V1TaskWithPayload{V1Task: &sqlcv1.V1Task{ID: id, TenantID: tenant, ExternalID: run, WorkflowRunID: run, WorkflowID: workflow, WorkflowVersionID: uuid.New(), StepID: uuid.New(), Queue: "default", ActionID: "action", InsertedAt: at, InitialState: "QUEUED", Sticky: sqlcv1.V1StickyStrategyNONE, ScheduleTimeout: "1m", StepTimeout: pgtype.Text{String: "1m", Valid: true}, DisplayName: "display", AdditionalMetadata: []byte(`{"key":"value"}`)}, Payload: []byte(`{"input":42}`)}
	events := []sqlcv1.CreateTaskEventsOLAPParams{{TenantID: tenant, TaskID: id, TaskInsertedAt: at, ExternalID: uuid.New(), WorkflowID: workflow, ReadableStatus: "QUEUED", EventType: "QUEUED", EventTimestamp: at}, {TenantID: tenant, TaskID: id, TaskInsertedAt: at, ExternalID: uuid.New(), WorkflowID: workflow, ReadableStatus: "RUNNING", EventType: "STARTED", EventTimestamp: timestamp(at.Time.Add(time.Second))}, {TenantID: tenant, TaskID: id, TaskInsertedAt: at, ExternalID: uuid.New(), WorkflowID: workflow, ReadableStatus: "COMPLETED", EventType: "FINISHED", EventTimestamp: timestamp(at.Time.Add(2 * time.Second)), Output: []byte(`{"output":42}`)}}
	mapping := map[uuid.UUID]uuid.UUID{}
	for _, e := range events {
		mapping[e.ExternalID] = run
	}
	for _, backend := range []repository.OLAPRepository{pg, td} {
		if _, _, err = backend.CreateTasks(ctx, tenant, []*repository.V1TaskWithPayload{task}); err != nil {
			t.Fatal(err)
		}
		if _, _, err = backend.CreateTaskEvents(ctx, tenant, events, mapping, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Compare public fields whose timestamps come from the same source event.
	a, _, err := pg.ReadTaskRunData(ctx, tenant, id, at, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := td.ReadTaskRunData(ctx, tenant, id, at, nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.Status != b.Status || a.RetryCount != b.RetryCount || !a.StartedAt.Time.Equal(b.StartedAt.Time) || !a.FinishedAt.Time.Equal(b.FinishedAt.Time) || !equalJSON(a.InputPayload, b.InputPayload) || !equalJSON(a.OutputPayload, b.OutputPayload) {
		t.Fatalf("single task mismatch PG=%#v TiDB=%#v", a, b)
	}
	opts := repository.ListTaskRunOpts{CreatedAfter: at.Time.Add(-time.Second), Limit: 100, IncludePayloads: true}
	pa, pc, err := pg.ListTasks(ctx, tenant, opts)
	if err != nil {
		t.Fatal(err)
	}
	ca, cc, err := td.ListTasks(ctx, tenant, opts)
	if err != nil {
		t.Fatal(err)
	}
	if pc != cc || len(pa) != len(ca) || pa[0].Status != ca[0].Status || !equalJSON(pa[0].OutputPayload, ca[0].OutputPayload) {
		t.Fatalf("task list contract mismatch")
	}
	pm, err := pg.ReadTaskRunMetrics(ctx, tenant, repository.ReadTaskRunMetricsOpts{CreatedAfter: at.Time.Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	cm, err := td.ReadTaskRunMetrics(ctx, tenant, repository.ReadTaskRunMetricsOpts{CreatedAfter: at.Time.Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	pjson, _ := json.Marshal(pm)
	cjson, _ := json.Marshal(cm)
	if string(pjson) != string(cjson) {
		t.Fatalf("metrics PG=%s TiDB=%s", pjson, cjson)
	}
	limit := int64(100)
	pe, err := pg.ListTaskRunEvents(ctx, tenant, id, at, &limit, nil)
	if err != nil {
		t.Fatal(err)
	}
	ce, err := td.ListTaskRunEvents(ctx, tenant, id, at, &limit, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(pe) != len(ce) {
		t.Fatalf("event count PG=%d TiDB=%d", len(pe), len(ce))
	}
	for i := range pe {
		if pe[i].EventType != ce[i].EventType || pe[i].ReadableStatus != ce[i].ReadableStatus || pe[i].EventExternalID != ce[i].EventExternalID || pe[i].Count != ce[i].Count {
			t.Fatalf("event contract index %d", i)
		}
	}
	eventID := uuid.New()
	eventOpts := repository.BulkCreateEventsAndTriggersParams{BulkCreateEventsOLAPParams: &sqlcv1.BulkCreateEventsOLAPParams{Tenantids: []uuid.UUID{tenant}, Externalids: []uuid.UUID{eventID}, Seenats: []pgtype.Timestamptz{at}, Keys: []string{"contract-event"}, Additionalmetadatas: [][]byte{[]byte(`{"key":"value"}`)}, Scopes: []pgtype.Text{{}}, TriggeringWebhookNames: []pgtype.Text{{}}}, Payloads: [][]byte{[]byte(`{"value":42}`)}}
	triggers := []repository.EventTriggersFromExternalId{{RunID: id, RunInsertedAt: at, EventExternalId: eventID, EventSeenAt: at}}
	for _, backend := range []repository.OLAPRepository{pg, td} {
		if err = backend.BulkCreateEventsAndTriggers(ctx, eventOpts, triggers); err != nil {
			t.Fatal(err)
		}
	}
	ep, err := pg.GetEventWithPayload(ctx, eventID, tenant)
	if err != nil {
		t.Fatal(err)
	}
	ec, err := td.GetEventWithPayload(ctx, eventID, tenant)
	if err != nil {
		t.Fatal(err)
	}
	if !equalJSON(ep.TriggeredRuns, ec.TriggeredRuns) || ep.CompletedCount != ec.CompletedCount || !equalJSON(ep.Payload, ec.Payload) {
		t.Fatalf("event detail contract PG=%s TiDB=%s", ep.TriggeredRuns, ec.TriggeredRuns)
	}
	listOpts := sqlcv1.ListEventsParams{Tenantid: tenant, Since: timestamp(at.Time.Add(-time.Second)), WorkflowIds: []uuid.UUID{workflow}, Statuses: []string{"COMPLETED"}}
	pl, pcount, err := pg.ListEvents(ctx, listOpts)
	if err != nil {
		t.Fatal(err)
	}
	cl, ccount, err := td.ListEvents(ctx, listOpts)
	if err != nil {
		t.Fatal(err)
	}
	if *pcount != *ccount || len(pl) != len(cl) || !equalJSON(pl[0].TriggeredRuns, cl[0].TriggeredRuns) || pl[0].TriggeringWebhookName != nil || cl[0].TriggeringWebhookName != nil {
		t.Fatal("event list contract mismatch")
	}
	for _, backend := range []repository.OLAPRepository{pg, td} {
		event, err := backend.GetEvent(ctx, eventID)
		if err != nil || event.ExternalID != eventID {
			t.Fatalf("event identity %T %v %v", backend, event, err)
		}
		rows, err := backend.ListTaskRunEvents(ctx, tenant, id, at, nil, nil)
		if err != nil || len(rows) != 0 {
			t.Fatalf("nil event limit %T %v %v", backend, rows, err)
		}
	}
	compareExpandedContracts(t, ctx, pg, td, pool, tenant, task)
	compareLogsAndPayloadContracts(t, ctx, pg, td, pool, tenant, task)
	compareRetryAndOperatorContracts(t, ctx, pg, td, tenant, task)
}

func equalJSON(a, b []byte) bool {
	var av, bv any
	if json.Unmarshal(a, &av) != nil || json.Unmarshal(b, &bv) != nil {
		return false
	}
	aa, _ := json.Marshal(av)
	bb, _ := json.Marshal(bv)
	return string(aa) == string(bb)
}
