package tidb

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func compareCappedListCounts(t *testing.T, ctx context.Context, pg repository.OLAPRepository, td *Repository, pool *pgxpool.Pool) {
	t.Helper()
	const size = 20001
	tenant := uuid.New()
	workflows := []uuid.UUID{uuid.New(), uuid.New()}
	at := timestamp(time.Now().UTC().Truncate(time.Microsecond))
	baseID := -time.Now().UnixNano() / 1000
	backends := []repository.OLAPRepository{pg, td}
	t.Cleanup(func() {
		for _, table := range []string{"v1_tasks_olap", "v1_runs_olap", "v1_statuses_olap", "v1_lookup_table_olap", "v1_event_lookup_table_olap", "v1_events_olap"} {
			if _, err := pool.Exec(context.Background(), "DELETE FROM "+table+" WHERE tenant_id=$1", tenant); err != nil {
				t.Error(err)
			}
		}
	})
	for from := 0; from < size; from += 1000 {
		var tasks []*repository.V1TaskWithPayload
		params := &sqlcv1.BulkCreateEventsOLAPParams{}
		var payloads [][]byte
		for i := from; i < min(size, from+1000); i++ {
			run := uuid.New()
			tasks = append(tasks, &repository.V1TaskWithPayload{V1Task: &sqlcv1.V1Task{
				ID: baseID - int64(i), TenantID: tenant, ExternalID: run, WorkflowRunID: run,
				WorkflowID: workflows[i%2], WorkflowVersionID: uuid.New(), StepID: uuid.New(),
				InsertedAt: at, InitialState: "QUEUED", Sticky: sqlcv1.V1StickyStrategyNONE,
				Queue: "default", ActionID: "action", ScheduleTimeout: "1m", AdditionalMetadata: []byte(`{}`),
			}})
			params.Tenantids = append(params.Tenantids, tenant)
			params.Externalids = append(params.Externalids, uuid.New())
			params.Seenats = append(params.Seenats, at)
			params.Keys = append(params.Keys, "count-boundary")
			params.Additionalmetadatas = append(params.Additionalmetadatas, []byte(`{}`))
			params.Scopes = append(params.Scopes, pgtype.Text{})
			params.TriggeringWebhookNames = append(params.TriggeringWebhookNames, pgtype.Text{})
			payloads = append(payloads, []byte(`{}`))
		}
		for _, backend := range backends {
			if _, blocked, err := backend.CreateTasks(ctx, tenant, tasks); err != nil || len(blocked) != 0 {
				t.Fatalf("count fixture %T blocked=%d %v", backend, len(blocked), err)
			}
			if err := backend.BulkCreateEventsAndTriggers(ctx, repository.BulkCreateEventsAndTriggersParams{BulkCreateEventsOLAPParams: params, Payloads: payloads}, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, backend := range backends {
		start := at.Time.Add(-time.Second)
		tasks, total, err := backend.ListTasks(ctx, tenant, repository.ListTaskRunOpts{CreatedAfter: start, Limit: 50})
		if err != nil || total != 20000 || len(tasks) != 50 {
			t.Fatalf("capped task count %T total=%d page=%d %v", backend, total, len(tasks), err)
		}
		runs, total, err := backend.ListWorkflowRuns(ctx, tenant, repository.ListWorkflowRunOpts{CreatedAfter: start, Limit: 50})
		if err != nil || total != 20000 || len(runs) != 50 {
			t.Fatalf("capped run count %T total=%d page=%d %v", backend, total, len(runs), err)
		}
		_, total, err = backend.ListWorkflowRuns(ctx, tenant, repository.ListWorkflowRunOpts{CreatedAfter: start, Limit: 50, WorkflowIds: []uuid.UUID{workflows[1]}})
		if err != nil || total != 10000 {
			t.Fatalf("filtered run count %T total=%d %v", backend, total, err)
		}
		page, eventTotal, err := backend.ListEvents(ctx, sqlcv1.ListEventsParams{Tenantid: tenant, Since: timestamp(start), Limit: pgtype.Int8{Int64: 50, Valid: true}})
		if err != nil || eventTotal == nil || *eventTotal != 20000 || len(page) != 50 {
			t.Fatalf("capped event count %T total=%v page=%d %v", backend, eventTotal, len(page), err)
		}
		metrics, err := backend.ReadTaskRunMetrics(ctx, tenant, repository.ReadTaskRunMetricsOpts{CreatedAfter: start})
		if err != nil {
			t.Fatal(err)
		}
		var count uint64
		for _, metric := range metrics {
			count += metric.Count
		}
		if count != size {
			t.Fatalf("uncapped status count %T total=%d", backend, count)
		}
	}
}
