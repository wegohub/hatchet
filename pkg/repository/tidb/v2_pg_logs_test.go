package tidb

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/config/limits"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/validator"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

func compareLogsAndPayloadContracts(t *testing.T, ctx context.Context, pg repository.OLAPRepository, td *Repository, pool *pgxpool.Pool, tenant uuid.UUID, task *repository.V1TaskWithPayload) {
	t.Helper()
	log := zerolog.Nop()
	core, cleanup := repository.NewRepository(pool, pool, &log, time.Second, 7*24*time.Hour, 30*24*time.Hour, 10, repository.TaskOperationLimits{}, repository.PayloadStoreRepositoryOpts{}, repository.StatusUpdateBatchSizeLimits{Task: 100, DAG: 100}, limits.LimitConfigFile{}, false, repository.DurableEventBufferOpts{FlushInterval: time.Millisecond * 5, MaxBatchSize: 128, MaxConcurrentFlushes: 1})
	t.Cleanup(func() { _ = cleanup() })
	if _, err := pool.Exec(ctx, "SELECT create_v1_range_partition('v1_task',$1::date)", pgtype.Date{Time: task.InsertedAt.Time, Valid: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO v1_task(id,inserted_at,tenant_id,queue,action_id,step_id,step_readable_id,workflow_id,workflow_version_id,workflow_run_id,schedule_timeout,sticky,external_id,display_name,input,step_index) OVERRIDING SYSTEM VALUE VALUES($1,$2,$3,'default','action',$4,'step',$5,$6,$7,'1m','NONE',$7,'display','{}',0)`, task.ID, task.InsertedAt, tenant, task.StepID, task.WorkflowID, task.WorkflowVersionID, task.ExternalID); err != nil {
		t.Fatal(err)
	}
	tdLogs := newLogs(td.store, core.Tasks(), 7*24*time.Hour, 5*time.Second)
	t.Cleanup(func() { _ = tdLogs.Close() })
	tdLogs.validate = validator.NewDefaultValidator()
	start := time.Now().UTC().Add(-time.Second)
	for _, backend := range []repository.LogLineRepository{core.Logs(), tdLogs} {
		for i, message := range []string{"Unicode 猫", "Unicode 猫", "Unicode ß", "Unicode 雪"} {
			level := "INFO"
			if i == 3 {
				level = "WARN"
			}
			opts := &repository.CreateLogLineOpts{TaskExternalId: task.ExternalID, TaskId: task.ID, TaskInsertedAt: task.InsertedAt, WorkflowId: task.WorkflowID, StepId: task.StepID, Message: message, RetryCount: 1, Level: &level}
			if err := backend.PutLog(ctx, tenant, opts); err != nil {
				t.Fatal(err)
			}
		}
	}
	search := "unicode _"
	attempt := int32(2)
	limit, offset := 2, 1
	desc := "DESC"
	for _, opts := range []*repository.ListLogsOpts{{}, {Search: &search}, {Search: &search, Attempt: &attempt, TaskExternalIds: []uuid.UUID{task.ExternalID}, Limit: &limit, Offset: &offset, OrderByDirection: &desc}, {Levels: []string{"WARN"}}, {WorkflowIds: []uuid.UUID{uuid.New()}}, {StepIds: []uuid.UUID{task.StepID}}} {
		a, err := core.Logs().ListLogLines(ctx, tenant, opts)
		if err != nil {
			t.Fatal(err)
		}
		b, err := tdLogs.ListLogLines(ctx, tenant, opts)
		if err != nil {
			t.Fatal(err)
		}
		if len(a) != len(b) {
			t.Fatalf("log count PG=%d TiDB=%d", len(a), len(b))
		}
		for i := range a {
			if a[i] == nil || b[i] == nil || a[i].Message != b[i].Message || a[i].Level != b[i].Level || a[i].TaskExternalId != b[i].TaskExternalId {
				t.Fatalf("log row %d differs", i)
			}
		}
	}
	opts := &repository.GetLogLinePointMetricsOpts{StartTimestamp: start, EndTimestamp: time.Now().UTC().Add(time.Second), BucketInterval: time.Hour, Search: &search}
	a, err := core.Logs().GetLogLinePointMetrics(ctx, tenant, opts)
	if err != nil {
		t.Fatal(err)
	}
	b, err := tdLogs.GetLogLinePointMetrics(ctx, tenant, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != len(b) {
		t.Fatalf("log metrics buckets PG=%d TiDB=%d", len(a), len(b))
	}
	for i := range a {
		if !a[i].MinuteBucket.Time.Equal(b[i].MinuteBucket.Time) || a[i].InfoCount != b[i].InfoCount || a[i].WarnCount != b[i].WarnCount {
			t.Fatalf("log metrics differ")
		}
	}
	compareLogsBeforeOLAPCopy(t, ctx, core.Logs(), tdLogs, td, pool, tenant, task)
	id := uuid.New()
	payload := repository.StoreOLAPPayloadOpts{ExternalId: id, InsertedAt: task.InsertedAt, Payload: []byte(`{"unicode":"雪"}`)}
	for _, backend := range []repository.OLAPRepository{pg, td} {
		if err := backend.PutPayloads(ctx, nil, tenant, payload); err != nil {
			t.Fatal(err)
		}
		bytes, err := backend.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: id, InsertedAt: task.InsertedAt})
		if err != nil || !equalJSON(bytes, payload.Payload) {
			t.Fatalf("payload round trip %T %s %v", backend, bytes, err)
		}
		if err := backend.OffloadPayloads(ctx, tenant, []repository.OffloadPayloadOpts{{ExternalId: id, ExternalLocationKey: "key"}}); err != nil {
			t.Fatal(err)
		}
		if _, err = backend.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: id, InsertedAt: task.InsertedAt}); err != nil {
			t.Fatal(err)
		}
		if _, err = backend.CountOLAPTempTableSizeForTaskStatusUpdates(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err = backend.CountOLAPTempTableSizeForDAGStatusUpdates(ctx); err != nil {
			t.Fatal(err)
		}
	}
	td.payloadStore = pg.PayloadStore()
	if td.PayloadStore() != pg.PayloadStore() {
		t.Fatal("payload store configuration differs")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = td.PutPayloads(ctx, tx, tenant, payload); err == nil {
		t.Fatal("PG transaction accepted by TiDB")
	}
}

func compareLogsBeforeOLAPCopy(t *testing.T, ctx context.Context, pgLogs, tdLogs repository.LogLineRepository, td *Repository, pool *pgxpool.Pool, tenant uuid.UUID, task *repository.V1TaskWithPayload) {
	t.Helper()
	external := uuid.New()
	id := task.ID - 1
	if _, err := pool.Exec(ctx, `INSERT INTO v1_task(id,inserted_at,tenant_id,queue,action_id,step_id,step_readable_id,workflow_id,workflow_version_id,workflow_run_id,schedule_timeout,sticky,external_id,display_name,input,step_index) OVERRIDING SYSTEM VALUE VALUES($1,$2,$3,'default','action',$4,'step',$5,$6,$7,'1m','NONE',$7,'display','{}',0)`, id, task.InsertedAt, tenant, task.StepID, task.WorkflowID, task.WorkflowVersionID, external); err != nil {
		t.Fatal(err)
	}
	var copies int
	if err := td.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM v1_tasks_olap WHERE tenant_id=? AND external_id=?", uuidArg(tenant), uuidArg(external)).Scan(&copies); err != nil || copies != 0 {
		t.Fatalf("fixture requires no OLAP task copy: count=%d err=%v", copies, err)
	}
	start := time.Now().UTC().Add(-time.Second)
	for _, logs := range []repository.LogLineRepository{pgLogs, tdLogs} {
		if err := logs.PutLog(ctx, tenant, &repository.CreateLogLineOpts{TaskExternalId: external, TaskId: id, TaskInsertedAt: task.InsertedAt, WorkflowId: task.WorkflowID, StepId: task.StepID, Message: "before OLAP copy"}); err != nil {
			t.Fatal(err)
		}
		rows, err := logs.ListLogLines(ctx, tenant, &repository.ListLogsOpts{TaskExternalIds: []uuid.UUID{external}})
		if err != nil || len(rows) != 1 || rows[0] == nil || rows[0].TaskExternalId != external {
			t.Fatalf("core task log filter %T rows=%d err=%v", logs, len(rows), err)
		}
		points, err := logs.GetLogLinePointMetrics(ctx, tenant, &repository.GetLogLinePointMetricsOpts{StartTimestamp: start, EndTimestamp: time.Now().UTC().Add(time.Second), BucketInterval: time.Hour, TaskExternalIds: []uuid.UUID{external}})
		if err != nil || len(points) != 1 || points[0].InfoCount != 1 {
			t.Fatalf("core task log metric %T points=%d err=%v", logs, len(points), err)
		}
	}
}
