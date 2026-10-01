package clickhouse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-zookeeper/zk"
	"github.com/google/uuid"
	metrics "github.com/hatchet-dev/hatchet/pkg/integrations/metrics/prometheus"
)

type entity struct {
	Tenant     uuid.UUID
	Kind       string
	Key        string
	ExternalID uuid.UUID
	RunID      uuid.UUID
	TaskID     int64
	InsertedAt time.Time
	Body       string
}

type store struct {
	conn   driver.Conn
	keeper *keeper
}

func dateArg(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000000") }

func makeEntity(tenant uuid.UUID, kind, key string, external, run uuid.UUID, taskID int64, insertedAt time.Time, body any) (entity, error) {
	encoded, err := json.Marshal(body)
	return entity{tenant, kind, key, external, run, taskID, insertedAt.UTC().Truncate(time.Microsecond), string(encoded)}, err
}

func digestRows(rows []entity) string {
	h := sha256.New()
	for _, row := range rows {
		encoded, _ := json.Marshal(row)
		_, _ = h.Write(encoded)
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// writeContext requires every configured replica to acknowledge each insert.
// Publication cannot proceed while a registered replica is unavailable.
func (s *store) writeContext(ctx context.Context, table string) (context.Context, error) {
	var total, active uint32
	if err := s.conn.QueryRow(ctx, "SELECT total_replicas, active_replicas FROM system.replicas WHERE database = currentDatabase() AND table = ?", table).Scan(&total, &active); err != nil {
		return nil, err
	}
	if total == 0 || total != active {
		return nil, fmt.Errorf("ClickHouse table %s has %d/%d active replicas", table, active, total)
	}
	quorum := total
	if quorum == 1 {
		quorum = 0
	}
	return ch.Context(ctx, ch.WithSettings(ch.Settings{"insert_quorum": quorum, "insert_quorum_parallel": 0, "async_insert": 0})), nil
}

func (s *store) stage(ctx context.Context, id uuid.UUID, rows []entity) error {
	writeCtx, err := s.writeContext(ctx, "entities")
	if err != nil {
		return err
	}
	batch, err := s.conn.PrepareBatch(writeCtx, "INSERT INTO entities (tenant,kind,entity_key,external_id,run_id,task_id,inserted_at,batch_id,ordinal,body)")
	if err != nil {
		return err
	}
	defer batch.Abort()
	for i, row := range rows {
		if err = batch.Append(row.Tenant, row.Kind, row.Key, row.ExternalID, row.RunID, row.TaskID, row.InsertedAt, id, uint64(i+1), row.Body); err != nil {
			return err
		}
	}
	if err = batch.Send(); err != nil {
		return err
	}
	writeCtx, err = s.writeContext(ctx, "manifests")
	if err != nil {
		return err
	}
	return s.conn.Exec(writeCtx, "INSERT INTO manifests SELECT ?, toUInt64(?), ?, toDateTime64(?, 6, 'UTC')", id, uint64(len(rows)), digestRows(rows), dateArg(time.Now()))
}

func (s *store) recover(ctx context.Context, lock *lease) (head, int32, error) {
	h, version, err := s.keeper.readHead(ctx)
	if err != nil || h.Pending == nil {
		return h, version, err
	}
	p := h.Pending
	// Sync includes authorized writes which might precede this process's connection.
	for _, table := range []string{"entities", "manifests", "commits"} {
		if err := s.conn.Exec(ctx, "SYSTEM SYNC REPLICA "+table); err != nil {
			return head{}, 0, err
		}
	}
	var count uint64
	var digest string
	if err := s.conn.QueryRow(ctx, "SELECT row_count, digest FROM manifests FINAL WHERE batch_id = ?", p.Batch).Scan(&count, &digest); err != nil {
		return head{}, 0, err
	}
	if count != p.Rows || digest != p.Digest {
		return head{}, 0, errors.New("pending publication manifest does not match Keeper authorization")
	}
	rows, err := s.batchRows(ctx, p.Batch)
	if err != nil {
		return head{}, 0, err
	}
	if uint64(len(rows)) != p.Rows || digestRows(rows) != p.Digest {
		return head{}, 0, errors.New("pending publication data is incomplete; publication remains blocked")
	}
	return s.commitPending(ctx, h, version, lock)
}

// commitPending publishes only a batch whose synchronous staging was acknowledged
// by every registered replica, or whose durable contents recovery has verified.
func (s *store) commitPending(ctx context.Context, h head, version int32, lock *lease) (head, int32, error) {
	p := h.Pending
	writeCtx, err := s.writeContext(ctx, "commits")
	if err != nil {
		return head{}, 0, err
	}
	if err = s.conn.Exec(writeCtx, "INSERT INTO commits SELECT ?, toUInt64(?)", p.Batch, p.Sequence); err != nil {
		return head{}, 0, err
	}
	h = head{Published: p.Sequence}
	if err = s.keeper.setHead(ctx, h, version, lock); err != nil {
		return head{}, 0, err
	}
	return h, version + 1, nil
}

func (s *store) batchRows(ctx context.Context, id uuid.UUID) ([]entity, error) {
	result, err := s.conn.Query(ctx, "SELECT tenant,kind,entity_key,external_id,run_id,task_id,inserted_at,body FROM entities FINAL WHERE batch_id = ? ORDER BY ordinal", id)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	var rows []entity
	for result.Next() {
		var row entity
		if err = result.Scan(&row.Tenant, &row.Kind, &row.Key, &row.ExternalID, &row.RunID, &row.TaskID, &row.InsertedAt, &row.Body); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, result.Err()
}

func (s *store) publish(ctx context.Context, rows []entity, runLocks ...*lease) error {
	if len(rows) == 0 {
		return nil
	}
	// One final version per key in a batch makes the manifest independent of merges.
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		key := row.Tenant.String() + "/" + row.Kind + "/" + row.Key
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate entity key in publication: %s", key)
		}
		seen[key] = struct{}{}
	}
	id := uuid.New()
	phaseStarted := time.Now()
	if err := s.stage(ctx, id, rows); err != nil {
		metrics.OLAPPublicationPhaseDuration.WithLabelValues("stage").Observe(time.Since(phaseStarted).Seconds())
		return err
	}
	metrics.OLAPPublicationPhaseDuration.WithLabelValues("stage").Observe(time.Since(phaseStarted).Seconds())
	phaseStarted = time.Now()
	l, err := s.keeper.lock(ctx, "publication")
	metrics.OLAPPublicationPhaseDuration.WithLabelValues("publication_lock").Observe(time.Since(phaseStarted).Seconds())
	if err != nil {
		return err
	}
	defer l.release()
	h, version, err := s.recover(ctx, l)
	if err != nil {
		return err
	}
	if h.Published == math.MaxUint64 {
		return errors.New("publication sequence exhausted")
	}
	h.Pending = &pending{Batch: id, Sequence: h.Published + 1, Rows: uint64(len(rows)), Digest: digestRows(rows)}
	if err = s.keeper.setHead(ctx, h, version, append(runLocks, l)...); err != nil {
		return err
	}
	phaseStarted = time.Now()
	_, _, err = s.commitPending(ctx, h, version+1, l)
	metrics.OLAPPublicationPhaseDuration.WithLabelValues("commit_publish").Observe(time.Since(phaseStarted).Seconds())
	return err
}

func (s *store) barrier(ctx context.Context) (uint64, error) {
	h, _, err := s.keeper.readHead(ctx)
	if err != nil {
		return 0, err
	}
	// A connection may reconnect to another replica between calls.
	// Sequential consistency rejects a replica missing an acknowledged quorum write.
	return h.Published, nil
}

type entityFilter struct {
	Tenant              *uuid.UUID
	Kind                string
	Kinds               []string
	ExternalIDs, RunIDs []uuid.UUID
	TaskIDs             []int64
	Keys                []string
	After               *time.Time
	Predicate           string
	Arguments           []any
}

func (s *store) read(ctx context.Context, sequence uint64, f entityFilter) ([]entity, error) {
	where := "e.kind = ?"
	args := []any{f.Kind}
	if f.Kinds != nil {
		where = "e.kind IN (?)"
		args = []any{f.Kinds}
	}
	if f.Tenant != nil {
		where += " AND e.tenant = ?"
		args = append(args, *f.Tenant)
	}
	if f.ExternalIDs != nil {
		where += " AND e.external_id IN (?)"
		args = append(args, f.ExternalIDs)
	}
	if f.RunIDs != nil {
		where += " AND e.run_id IN (?)"
		args = append(args, f.RunIDs)
	}
	if f.TaskIDs != nil {
		where += " AND e.task_id IN (?)"
		args = append(args, f.TaskIDs)
	}
	if f.Keys != nil {
		where += " AND e.entity_key IN (?)"
		args = append(args, f.Keys)
	}
	if f.After != nil {
		where += " AND e.inserted_at >= ?"
		args = append(args, dateArg(*f.After))
	}
	args = append(args, sequence)
	q := "SELECT tenant,kind,entity_key,tupleElement(v,1) AS external_id,tupleElement(v,2) AS run_id,tupleElement(v,3) AS task_id,tupleElement(v,4) AS inserted_at,tupleElement(v,5) AS body FROM (SELECT e.tenant AS tenant,e.kind AS kind,e.entity_key AS entity_key,argMax(tuple(e.external_id,e.run_id,e.task_id,e.inserted_at,e.body),tuple(c.sequence,e.ordinal)) AS v FROM entities e INNER JOIN (SELECT batch_id,max(sequence) AS sequence FROM commits GROUP BY batch_id) c ON e.batch_id=c.batch_id WHERE " + where + " AND c.sequence <= ? GROUP BY e.tenant,e.kind,e.entity_key)"
	if f.Predicate != "" {
		q += " WHERE " + f.Predicate
		args = append(args, f.Arguments...)
	}
	result, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	var rows []entity
	for result.Next() {
		var row entity
		if err = result.Scan(&row.Tenant, &row.Kind, &row.Key, &row.ExternalID, &row.RunID, &row.TaskID, &row.InsertedAt, &row.Body); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, result.Err()
}

func (s *store) runLocks(ctx context.Context, tenant uuid.UUID, runs []uuid.UUID) ([]*lease, map[uuid.UUID]struct{}, error) {
	unique := make(map[uuid.UUID]struct{}, len(runs))
	for _, id := range runs {
		unique[id] = struct{}{}
	}
	ids := make([]uuid.UUID, 0, len(unique))
	for id := range unique {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	var locks []*lease
	blocked := make(map[uuid.UUID]struct{})
	for _, id := range ids {
		l, err := s.keeper.tryLock(ctx, "run-"+tenant.String()+"-"+id.String())
		if errors.Is(err, zk.ErrNodeExists) {
			blocked[id] = struct{}{}
			continue
		}
		if err != nil {
			for _, held := range locks {
				_ = held.release()
			}
			return nil, nil, err
		}
		locks = append(locks, l)
	}
	if len(locks) > 0 {
		h, _, err := s.keeper.readHead(ctx)
		if err != nil {
			releaseLocks(locks)
			return nil, nil, err
		}
		if h.Pending == nil {
			return locks, blocked, nil
		}
		pub, err := s.keeper.lock(ctx, "publication")
		if err != nil {
			for _, held := range locks {
				_ = held.release()
			}
			return nil, nil, err
		}
		_, _, err = s.recover(ctx, pub)
		_ = pub.release()
		if err != nil {
			for _, held := range locks {
				_ = held.release()
			}
			return nil, nil, err
		}
	}
	return locks, blocked, nil
}
