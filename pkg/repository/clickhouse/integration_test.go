package clickhouse

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
)

func integrationStore(t *testing.T) (*store, context.Context) {
	t.Helper()
	if os.Getenv("HATCHET_CLICKHOUSE_TEST") != "true" {
		t.Skip("set HATCHET_CLICKHOUSE_TEST=true to run against ClickHouse and Keeper")
	}
	id := strings.ReplaceAll(uuid.NewString(), "-", "")
	cfg := Config{Addresses: []string{envOr("CLICKHOUSE_TEST_ADDRESS", "127.0.0.1:9009")}, Database: "hatchet_test_" + id, Username: envOr("CLICKHOUSE_TEST_USERNAME", "default"), Password: os.Getenv("CLICKHOUSE_TEST_PASSWORD"), KeeperAddresses: []string{envOr("KEEPER_TEST_ADDRESS", "127.0.0.1:9181")}, KeeperRoot: "/hatchet/test/" + id, DialTimeout: 5 * time.Second, QueryTimeout: 20 * time.Second, KeeperSessionTimeout: 10 * time.Second, KeeperOperationTimeout: 5 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	admin, err := open(ctx, cfg, "default")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		if err := admin.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+cfg.Database+" SYNC"); err != nil {
			t.Error(err)
		}
		_ = admin.Close()
		k, err := connectKeeper(cleanupCtx, cfg)
		if err != nil {
			t.Error(err)
			return
		}
		defer k.close()
		removeKeeperTree(t, k.conn, cfg.KeeperRoot)
	})
	if err := Migrate(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	conn, err := open(ctx, cfg, cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := validateSchema(ctx, conn, cfg.KeeperRoot); err != nil {
		t.Fatal(err)
	}
	k, err := connectKeeper(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(k.close)
	return &store{conn, k}, ctx
}

func envOr(name, value string) string {
	if s := os.Getenv(name); s != "" {
		return s
	}
	return value
}

func removeKeeperTree(t *testing.T, c *zk.Conn, path string) {
	t.Helper()
	names, _, err := c.Children(path)
	if errors.Is(err, zk.ErrNoNode) {
		return
	}
	if err != nil {
		t.Error(err)
		return
	}
	for _, name := range names {
		removeKeeperTree(t, c, path+"/"+name)
	}
	if err = c.Delete(path, -1); err != nil && !errors.Is(err, zk.ErrNoNode) {
		t.Error(err)
	}
}

func TestPublicationIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	tenant, external := uuid.New(), uuid.New()
	row, err := makeEntity(tenant, "task", "one", external, external, 1, time.Now(), map[string]string{"status": "QUEUED"})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.publish(ctx, []entity{row}); err != nil {
		t.Fatal(err)
	}
	sequence, err := s.barrier(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if sequence != 1 {
		t.Fatalf("published sequence %d", sequence)
	}
	id := uuid.New()
	next := row
	next.Body = `{"status":"COMPLETED"}`
	if err = s.stage(ctx, id, []entity{next}); err != nil {
		t.Fatal(err)
	}
	if err = s.conn.Exec(ctx, "OPTIMIZE TABLE entities FINAL"); err != nil {
		t.Fatal(err)
	}
	assertVisible := func(seq uint64, want string) {
		t.Helper()
		rows, err := s.read(ctx, seq, entityFilter{Tenant: &tenant, Kind: "task", Keys: []string{"one"}})
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].Body != want {
			t.Fatalf("visible rows %#v, want %s", rows, want)
		}
	}
	assertVisible(sequence, row.Body)
	l, err := s.keeper.lock(ctx, "publication")
	if err != nil {
		t.Fatal(err)
	}
	defer l.release()
	h, version, err := s.keeper.readHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h.Pending = &pending{Batch: id, Sequence: 2, Rows: 1, Digest: digestRows([]entity{next})}
	if err = s.keeper.setHead(ctx, h, version, l); err != nil {
		t.Fatal(err)
	}
	if err = s.conn.Exec(ctx, "INSERT INTO commits SELECT ?, toUInt64(?)", id, uint64(2)); err != nil {
		t.Fatal(err)
	}
	assertVisible(sequence, row.Body)
	h, _, err = s.recover(ctx, l)
	if err != nil {
		t.Fatal(err)
	}
	if h.Published != 2 || h.Pending != nil {
		t.Fatalf("recovery head %#v", h)
	}
	assertVisible(2, next.Body)
	if _, _, err = s.recover(ctx, l); err != nil {
		t.Fatal(err)
	}
	assertVisible(1, row.Body)
}

func TestLeaseFencingIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	old, err := s.keeper.tryLock(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	if err = old.release(); err != nil {
		t.Fatal(err)
	}
	current, err := s.keeper.tryLock(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	defer current.release()
	h, version, err := s.keeper.readHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.keeper.setHead(ctx, h, version, old); !errors.Is(err, zk.ErrNoNode) {
		t.Fatalf("stale owner authorization: %v", err)
	}
	if err = old.release(); !errors.Is(err, zk.ErrNoNode) {
		t.Fatalf("stale owner release: %v", err)
	}
	if err = s.keeper.multi(ctx, current.check()); err != nil {
		t.Fatalf("current owner lost lease: %v", err)
	}
	if err = s.keeper.setHead(ctx, h, version, current); err != nil {
		t.Fatal(err)
	}
}

type logTasks struct {
	repository.TaskRepository
	task *sqlcv1.V1Task
}

func (f logTasks) ListTasks(context.Context, uuid.UUID, []int64) ([]*sqlcv1.V1Task, error) {
	return []*sqlcv1.V1Task{f.task}, nil
}

type logFlatten struct{ task *sqlcv1.V1Task }

func (f logFlatten) ListTasksByExternalIds(context.Context, uuid.UUID, []uuid.UUID) ([]*sqlcv1.FlattenTasksByExternalIdsRow, error) {
	return []*sqlcv1.FlattenTasksByExternalIdsRow{{ID: f.task.ID, ExternalID: f.task.ExternalID, InsertedAt: f.task.InsertedAt}}, nil
}

func TestLogsIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	tenant, external, workflow, step := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	task := &sqlcv1.V1Task{ID: 42, TenantID: tenant, ExternalID: external, DisplayName: "task", InsertedAt: pgtype.Timestamptz{Time: time.Now().UTC().Truncate(time.Microsecond), Valid: true}}
	l := newLogs(s, logTasks{task: task}, logFlatten{task}, 24*time.Hour, 20*time.Second)
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
