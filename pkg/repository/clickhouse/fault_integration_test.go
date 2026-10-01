package clickhouse

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-zookeeper/zk"
	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
)

type lostLogACK struct {
	driver.Conn
	mu   sync.Mutex
	lost bool
}

func (c *lostLogACK) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	batch, err := c.Conn.PrepareBatch(ctx, query, opts...)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.Contains(query, "INSERT INTO log_lines") && !c.lost {
		c.lost = true
		return &lostACKBatch{Batch: batch}, nil
	}
	return batch, nil
}

type lostACKBatch struct{ driver.Batch }

func (b *lostACKBatch) Send() error {
	if err := b.Batch.Send(); err != nil {
		return err
	}
	return errors.New("injected lost insert acknowledgement")
}

func TestLogRetryAfterLostACKIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	s.conn = &lostLogACK{Conn: s.conn}
	tenant, external := uuid.New(), uuid.New()
	at := pgtype.Timestamptz{Time: time.Now().UTC().Truncate(time.Microsecond), Valid: true}
	task := &sqlcv1.V1Task{ID: 7, ExternalID: external, InsertedAt: at, DisplayName: "task"}
	l := newLogs(s, logTasks{task: task}, logFlatten{task}, 24*time.Hour, 20*time.Second)
	t.Cleanup(func() { _ = l.Close() })
	opts := &repository.CreateLogLineOpts{TaskId: 7, TaskExternalId: external, TaskInsertedAt: at, Message: "repeat"}
	for range 2 {
		if err := l.PutLog(ctx, tenant, opts); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := l.ListLogLines(ctx, tenant, &repository.ListLogsOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("internal retry changed the two logical writes into %d rows", len(rows))
	}
	if rows[0].ID == rows[1].ID {
		t.Fatal("independent calls were deduplicated")
	}
}

func TestIncompletePendingPublicationIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	tenant, run := uuid.New(), uuid.New()
	first, _ := makeEntity(tenant, "task", "one", run, run, 1, time.Now(), map[string]string{"value": "one"})
	second, _ := makeEntity(tenant, "task", "two", run, run, 2, time.Now(), map[string]string{"value": "two"})
	id := uuid.New()
	if err := s.stage(ctx, id, []entity{first, second}); err != nil {
		t.Fatal(err)
	}
	l, err := s.keeper.lock(ctx, "publication")
	if err != nil {
		t.Fatal(err)
	}
	defer l.release()
	h, version, err := s.keeper.readHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h.Pending = &pending{Batch: id, Sequence: 1, Rows: 2, Digest: digestRows([]entity{first, second})}
	if err = s.keeper.setHead(ctx, h, version, l); err != nil {
		t.Fatal(err)
	}
	if err = s.conn.Exec(ctx, "ALTER TABLE entities DELETE WHERE batch_id = ? AND entity_key = 'two' SETTINGS mutations_sync=2", id); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.recover(ctx, l); err == nil {
		t.Fatal("incomplete pending data was published")
	}
	h, _, err = s.keeper.readHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.Published != 0 || h.Pending == nil {
		t.Fatalf("incomplete publication advanced head %#v", h)
	}
	rows, err := s.read(ctx, 0, entityFilter{Tenant: &tenant, Kind: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatal("unpublished partial data leaked into reads")
	}
}

func TestExpiredSessionCannotPublishIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	old, err := s.keeper.tryLock(ctx, "run-expiry")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{KeeperAddresses: []string{s.keeper.conn.Server()}, KeeperRoot: s.keeper.root, DialTimeout: 5 * time.Second, KeeperSessionTimeout: 10 * time.Second, KeeperOperationTimeout: 5 * time.Second}
	oldKeeper := s.keeper
	fresh, err := connectKeeper(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fresh.conn.Close)
	oldKeeper.conn.Close()
	s.keeper = fresh
	current, err := fresh.lock(ctx, "run-expiry")
	if err != nil {
		t.Fatal(err)
	}
	defer current.release()
	row, _ := makeEntity(uuid.New(), "task", "one", uuid.New(), uuid.New(), 1, time.Now(), map[string]string{"value": "stale"})
	if err = s.publish(ctx, []entity{row}, old); !errors.Is(err, zk.ErrNoNode) {
		t.Fatalf("expired session published staged data: %v", err)
	}
	h, _, err := fresh.readHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.Published != 0 || h.Pending != nil {
		t.Fatalf("stale owner changed publication head %#v", h)
	}
}

func TestQueryTimeoutIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	conn := &boundedConn{Conn: s.conn, timeout: 100 * time.Millisecond}
	start := time.Now()
	var value uint8
	if err := conn.QueryRow(ctx, "SELECT sleep(1)").Scan(&value); err == nil {
		t.Fatal("long caller deadline disabled the query timeout")
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("query timeout took %s", elapsed)
	}
	if ctx.Err() != nil {
		t.Fatal("query timeout cancelled the caller's context")
	}
}

func TestKeeperTimeoutReconnectIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	held, err := s.keeper.lock(ctx, "timeout-owner")
	if err != nil {
		t.Fatal(err)
	}
	callCtx, cancel := context.WithCancel(ctx)
	started := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		_, err := keeperCall(callCtx, s.keeper, func(conn *zk.Conn) (bool, error) { close(started); <-callCtx.Done(); return false, nil })
		finished <- err
	}()
	<-started
	cancel()
	if err = <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation %v", err)
	}
	next, err := s.keeper.lock(ctx, "timeout-owner")
	if err != nil {
		t.Fatal(err)
	}
	defer next.release()
	h, version, err := s.keeper.readHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.keeper.setHead(ctx, h, version, held); err == nil {
		t.Fatal("expired ownership proof authorized a write after reconnect")
	}
	if err = s.keeper.setHead(ctx, h, version, next); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaTypeMismatchIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	if err := s.conn.Exec(ctx, "ALTER TABLE entities MODIFY COLUMN body UInt64"); err != nil {
		t.Fatal(err)
	}
	if err := validateSchema(ctx, s.conn, s.keeper.root); err == nil {
		t.Fatal("incompatible payload column accepted")
	}
}

// publicationFault refuses recovery I/O during normal publication and can lose
// one acknowledgement after the server has durably inserted a marker.
type publicationFault struct {
	driver.Conn
	failInsert    string
	lost          bool
	allowRecovery bool
}

func (c *publicationFault) Exec(ctx context.Context, query string, args ...any) error {
	if strings.HasPrefix(query, "SYSTEM SYNC REPLICA") && !c.allowRecovery {
		return errors.New("normal publication performed recovery I/O")
	}
	if err := c.Conn.Exec(ctx, query, args...); err != nil {
		return err
	}
	if c.failInsert != "" && strings.HasPrefix(query, "INSERT INTO "+c.failInsert) && !c.lost {
		c.lost = true
		return errors.New("injected lost publication acknowledgement")
	}
	return nil
}

func TestAcknowledgedPublicationFastPathIntegration(t *testing.T) {
	s, ctx := integrationStore(t)
	s.conn = &publicationFault{Conn: s.conn}
	tenant, run := uuid.New(), uuid.New()
	row, err := makeEntity(tenant, "task", "one", run, run, 1, time.Now(), map[string]string{"value": "one"})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.publish(ctx, []entity{row}); err != nil {
		t.Fatal(err)
	}
	h, _, err := s.keeper.readHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.Published != 1 || h.Pending != nil {
		t.Fatalf("unexpected head %#v", h)
	}
	rows, err := s.read(ctx, h.Published, entityFilter{Tenant: &tenant, Kinds: []string{"task", "dag", "payload", "task_event"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Body != row.Body {
		t.Fatalf("published rows %#v", rows)
	}
}

func TestPublicationLostACKIntegration(t *testing.T) {
	for _, table := range []string{"manifests", "commits"} {
		t.Run(table, func(t *testing.T) {
			s, ctx := integrationStore(t)
			fault := &publicationFault{Conn: s.conn, failInsert: table}
			s.conn = fault
			tenant, run := uuid.New(), uuid.New()
			row, err := makeEntity(tenant, "task", "one", run, run, 1, time.Now(), map[string]string{"value": "one"})
			if err != nil {
				t.Fatal(err)
			}
			if err = s.publish(ctx, []entity{row}); err == nil {
				t.Fatal("lost acknowledgement reported success")
			}
			h, _, err := s.keeper.readHead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if h.Published != 0 || (h.Pending != nil) != (table == "commits") {
				t.Fatalf("unexpected uncertain head %#v", h)
			}
			rows, err := s.read(ctx, h.Published, entityFilter{Tenant: &tenant, Kind: "task"})
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 0 {
				t.Fatal("uncertain publication leaked into reads")
			}
			fault.allowRecovery = true
			if table == "commits" {
				l, err := s.keeper.lock(ctx, "publication")
				if err != nil {
					t.Fatal(err)
				}
				defer l.release()
				if _, _, err = s.recover(ctx, l); err != nil {
					t.Fatal(err)
				}
			} else if err = s.publish(ctx, []entity{row}); err != nil {
				t.Fatal(err)
			}
			h, _, err = s.keeper.readHead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rows, err = s.read(ctx, h.Published, entityFilter{Tenant: &tenant, Kind: "task"})
			if err != nil {
				t.Fatal(err)
			}
			if h.Published != 1 || h.Pending != nil || len(rows) != 1 {
				t.Fatalf("recovery head %#v, rows %d", h, len(rows))
			}
		})
	}
}
