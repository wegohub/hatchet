package tidb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	metrics "github.com/hatchet-dev/hatchet/pkg/integrations/metrics/prometheus"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

type entity struct {
	Tenant            uuid.UUID
	Kind, Key         string
	ExternalID, RunID uuid.UUID
	TaskID            int64
	InsertedAt        time.Time
	Body              string
	Value             any
	StateOnly         bool
	KnownFresh        bool
}
type store struct {
	db                           *sql.DB
	coordinator                  *coordinator
	queryTimeout, tiFlashTimeout time.Duration
	writeConcurrency             int
	permitsOnce                  sync.Once
	permits                      chan struct{}
	plans                        planSampler
	// commitHook is used by fault tests to model losing the commit response.
	commitHook func(*sql.Tx) error
}

func dateArg(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }
func makeEntity(tenant uuid.UUID, kind, key string, external, run uuid.UUID, taskID int64, at time.Time, body any) (entity, error) {
	encoded, err := json.Marshal(body)
	return entity{Tenant: tenant, Kind: kind, Key: key, ExternalID: external, RunID: run, TaskID: taskID, InsertedAt: dateArg(at), Body: string(encoded)}, err
}
func (s *store) barrier(context.Context) (uint64, error) { return 0, nil }
func (s *store) permit(ctx context.Context) (func(), error) {
	s.permitsOnce.Do(func() {
		n := s.writeConcurrency
		if n <= 0 {
			n = 8
		}
		s.permits = make(chan struct{}, n)
	})
	start := time.Now()
	select {
	case s.permits <- struct{}{}:
		observePhase("write_queue", start)
		return func() { <-s.permits }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func observePhase(phase string, start time.Time) {
	metrics.OLAPTiDBPhaseDuration.WithLabelValues(phase).Observe(time.Since(start).Seconds())
}
func publicationID(rows []entity) [32]byte {
	ordered := append([]entity(nil), rows...)
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		return a.Tenant.String()+a.Kind+a.Key < b.Tenant.String()+b.Kind+b.Key
	})
	h := sha256.New()
	for _, e := range ordered {
		fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00", e.Tenant, e.Kind, e.Key, e.Body)
	}
	var id [32]byte
	copy(id[:], h.Sum(nil))
	return id
}
func (s *store) publish(ctx context.Context, rows []entity, leases ...*lease) error {
	if len(rows) == 0 {
		return nil
	}
	if len(leases) > 1 {
		return errors.New("a TiDB batch must use a single transaction")
	}
	var tx *sql.Tx
	if len(leases) == 1 {
		tx = leases[0].tx
	} else {
		release, err := s.permit(ctx)
		if err != nil {
			return err
		}
		defer release()
		start := time.Now()
		tx, err = s.db.BeginTx(ctx, nil)
		observePhase("pool_begin", start)
		if err != nil {
			return err
		}
	}
	defer tx.Rollback()
	ctx = context.WithValue(ctx, transactionKey{}, tx)
	start := time.Now()
	grouped := map[string][]entity{}
	for _, row := range rows {
		grouped[row.Kind] = append(grouped[row.Kind], row)
	}
	kinds := make([]string, 0, len(grouped))
	for kind := range grouped {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		if kind == "pending_update" {
			if err := s.publishPending(ctx, tx, grouped[kind]); err != nil {
				return err
			}
			continue
		}
		if kind == "dag_counter" {
			if err := s.publishCounters(ctx, tx, grouped[kind]); err != nil {
				return err
			}
			continue
		}
		codec, err := codecFor(kind)
		if err != nil {
			return err
		}
		batch := grouped[kind]
		if kind == "task_event" {
			batch, err = s.freshTaskEvents(ctx, tx, batch)
			if err != nil {
				return err
			}
		}
		for from := 0; from < len(batch); {
			to := from
			size := 0
			for to < len(batch) && to-from < 200 {
				if to > from && size+len(batch[to].Body) > 1<<20 {
					break
				}
				size += len(batch[to].Body)
				to++
			}
			var values []string
			var args []any
			var selected []entity
			for _, row := range batch[from:to] {
				if row.StateOnly {
					continue
				}
				a, err := encodeColumns(row)
				if err != nil {
					return err
				}
				args = append(args, a...)
				values = append(values, "("+placeholders(len(a))+")")
				selected = append(selected, row)
			}
			if len(values) > 0 {
				var columns, updates []string
				for _, col := range codec.columns {
					columns = append(columns, "`"+col+"`")
					if col != "tenant_id" && col != "entity_key" && col != "external_id" && col != "inserted_at" {
						updates = append(updates, "`"+col+"`=VALUES(`"+col+"`)")
					}
				}
				// Event and span identities are immutable; replay cannot change their first ID or timestamp.
				if kind == "payload" || kind == "task_event" || kind == "span" || kind == "event" || kind == "trace_lookup" || kind == "event_trigger" {
					updates = []string{"entity_key=entity_key"}
				}
				q := "INSERT INTO " + codec.table + " (" + strings.Join(columns, ",") + ") VALUES " + strings.Join(values, ",") + " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ",")
				if _, err = tx.ExecContext(ctx, q, args...); err != nil {
					observeConflict("entity_upsert", err)
					return err
				}
				if err = s.publishLookups(ctx, tx, selected); err != nil {
					return err
				}
				if err = s.publishMetadata(ctx, tx, selected); err != nil {
					return err
				}
			}
			if err = upsertRunSummaries(ctx, tx, batch[from:to]); err != nil {
				return err
			}
			if kind == "task_event" {
				if err = s.publishAttempts(ctx, tx, batch[from:to]); err != nil {
					return err
				}
			}
			from = to
		}
	}
	observePhase("write_sql", start)
	if len(leases) == 1 && len(leases[0].freshRuns) > 0 {
		keys := make([]string, 0, len(leases[0].freshRuns))
		for _, key := range leases[0].freshRuns {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for from := 0; from < len(keys); from += 200 {
			part := keys[from:min(from+200, len(keys))]
			args := make([]any, 0, len(part))
			for _, key := range part {
				args = append(args, []byte(key))
			}
			if _, err := tx.ExecContext(ctx, "UPDATE v1_olap_run_locks SET initialized=TRUE WHERE lock_key IN ("+placeholders(len(part))+")", args...); err != nil {
				return err
			}
		}
	}
	id := publicationID(rows)
	if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO v1_olap_write_receipts(receipt_id) VALUES (?)", id[:]); err != nil {
		return err
	}
	start = time.Now()
	var err error
	if s.commitHook != nil {
		err = s.commitHook(tx)
	} else {
		err = tx.Commit()
	}
	observePhase("commit", start)
	if err != nil {
		observeConflict("commit", err)
		checkCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if s.confirmPublished(checkCtx, rows) {
			return nil
		}
		return fmt.Errorf("commit TiDB publication: %w", err)
	}
	return nil
}
func observeConflict(operation string, err error) {
	if isRetryable(err) {
		metrics.OLAPTiDBTransactionConflicts.WithLabelValues(operation).Inc()
	}
}
func (s *store) confirmPublished(ctx context.Context, rows []entity) bool {
	id := publicationID(rows)
	var found int
	return s.db.QueryRowContext(ctx, "SELECT 1 FROM v1_olap_write_receipts WHERE receipt_id=?", id[:]).Scan(&found) == nil
}
func (s *store) publishLookups(ctx context.Context, tx *sql.Tx, rows []entity) error {
	var values []string
	var args []any
	for _, r := range rows {
		if r.ExternalID == uuid.Nil {
			continue
		}
		values = append(values, "(?,?,?,?,?)")
		args = append(args, uuidArg(r.ExternalID), r.Kind, uuidArg(r.Tenant), []byte(r.Key), r.InsertedAt)
	}
	if len(values) == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, "INSERT IGNORE INTO v1_lookup_table_olap(external_id,kind,tenant_id,entity_key,inserted_at) VALUES "+strings.Join(values, ","), args...)
	return err
}
func upsertRunSummaries(ctx context.Context, tx *sql.Tx, rows []entity) error {
	var values []string
	var args []any
	for _, row := range rows {
		var workflow, workflowRun uuid.UUID
		var parent, idempotency, worker any
		var status sqlcv1.V1ReadableStatusOlap
		var child bool
		var retry int32
		var id int64
		switch row.Kind {
		case "task":
			v, err := decodeEntity[sqlcv1.V1TasksOlap](row)
			if err != nil {
				return err
			}
			workflow, workflowRun, status, child, retry, id = v.WorkflowID, v.WorkflowRunID, v.ReadableStatus, v.DagID.Valid, v.LatestRetryCount, v.ID
			parent, idempotency, worker = optionalUUID(v.ParentTaskExternalID), textArg(v.IdempotencyKey), optionalUUID(v.LatestWorkerID)
		case "dag":
			v, err := decodeEntity[sqlcv1.V1DagsOlap](row)
			if err != nil {
				return err
			}
			workflow, workflowRun, status, retry, id = v.WorkflowID, v.ExternalID, v.ReadableStatus, v.LatestRetryCount, v.ID
			parent, idempotency = optionalUUID(v.ParentTaskExternalID), textArg(v.IdempotencyKey)
		default:
			continue
		}
		values = append(values, "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		args = append(args, uuidArg(row.Tenant), row.Kind, uuidArg(row.ExternalID), []byte(row.Key), id, row.InsertedAt, uuidArg(workflow), uuidArg(workflowRun), parent, idempotency, string(status), retry, worker, child, false)
	}
	if len(values) == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO v1_runs_olap(tenant_id,kind,external_id,entity_key,id,inserted_at,workflow_id,workflow_run_id,parent_external_id,idempotency_key,readable_status,latest_retry_count,latest_worker_id,is_dag_child,is_placeholder) VALUES "+strings.Join(values, ",")+" ON DUPLICATE KEY UPDATE workflow_id=VALUES(workflow_id),workflow_run_id=VALUES(workflow_run_id),parent_external_id=VALUES(parent_external_id),idempotency_key=VALUES(idempotency_key),readable_status=VALUES(readable_status),latest_retry_count=VALUES(latest_retry_count),latest_worker_id=VALUES(latest_worker_id),is_dag_child=VALUES(is_dag_child),is_placeholder=FALSE", args...)
	return err
}

type entityFilter struct {
	Tenant              *uuid.UUID
	Kind                string
	Kinds               []string
	ExternalIDs, RunIDs []uuid.UUID
	TaskIDs             []int64
	Keys                []string
	After               *time.Time
	Where               string
	Args                []any
	Order               string
	Limit, Offset       *int64
	ForUpdate           bool
}

func placeholders(n int) string {
	if n < 1 {
		return "NULL"
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}
func (s *store) fromFilter(f entityFilter) (string, []any, error) {
	codec, err := codecFor(f.Kind)
	if err != nil {
		return "", nil, err
	}
	from := codec.table + " e"
	var where []string
	var args []any
	if f.Kind == "task" || f.Kind == "dag" {
		from += " JOIN v1_runs_olap r ON r.tenant_id=e.tenant_id AND r.external_id=e.external_id AND r.inserted_at=e.inserted_at AND r.kind='" + f.Kind + "'"
	}
	if f.Tenant != nil {
		where = append(where, "e.tenant_id=?")
		args = append(args, uuidArg(*f.Tenant))
	}
	for _, item := range []struct {
		col    string
		values []uuid.UUID
	}{{"external_id", f.ExternalIDs}, {"run_id", f.RunIDs}} {
		if item.values != nil {
			if len(item.values) == 0 {
				where = append(where, "FALSE")
				continue
			}
			where = append(where, "e."+item.col+" IN ("+placeholders(len(item.values))+")")
			for _, id := range item.values {
				args = append(args, uuidArg(id))
			}
		}
	}

	if f.TaskIDs != nil {
		if len(f.TaskIDs) == 0 {
			return from + " WHERE FALSE", nil, nil
		}
		where = append(where, "e.task_id IN ("+placeholders(len(f.TaskIDs))+")")
		for _, v := range f.TaskIDs {
			args = append(args, v)
		}
	}
	if f.Keys != nil {
		if len(f.Keys) == 0 {
			return from + " WHERE FALSE", nil, nil
		}
		where = append(where, "e.entity_key IN ("+placeholders(len(f.Keys))+")")
		for _, v := range f.Keys {
			args = append(args, []byte(v))
		}
	}
	if f.After != nil {
		where = append(where, "e.inserted_at>=?")
		args = append(args, dateArg(*f.After))
	}
	if f.Where != "" {
		where = append(where, f.Where)
		args = append(args, f.Args...)
	}
	if len(where) > 0 {
		from += " WHERE " + strings.Join(where, " AND ")
	}
	return from, args, nil
}
func (s *store) read(ctx context.Context, _ uint64, f entityFilter) ([]entity, error) {
	// Hydration consumes an already selected page. Bound predicate size without
	// reapplying pagination independently to each chunk.
	if len(f.ExternalIDs) > 200 && f.Limit == nil && f.Order == "" {
		var out []entity
		for from := 0; from < len(f.ExternalIDs); from += 200 {
			part := f
			part.ExternalIDs = f.ExternalIDs[from:min(from+200, len(f.ExternalIDs))]
			rows, err := s.read(ctx, 0, part)
			if err != nil {
				return nil, err
			}
			out = append(out, rows...)
		}
		return out, nil
	}
	kinds := f.Kinds
	if kinds == nil {
		kinds = []string{f.Kind}
	}
	var out []entity
	base := f
	for _, kind := range kinds {
		f = base
		f.Kind = kind
		located, empty, err := s.locate(ctx, f)
		if err != nil {
			return nil, err
		}
		if empty {
			continue
		}
		f = located
		codec, err := codecFor(kind)
		if err != nil {
			return nil, err
		}
		from, args, err := s.fromFilter(f)
		if err != nil {
			return nil, err
		}
		hint := "e"
		if kind == "task" || kind == "dag" {
			hint += ",r"
		}
		q := "SELECT /*+ READ_FROM_STORAGE(TIKV[" + hint + "]) */ " + codec.selectColumns + " FROM " + from
		if f.Order != "" {
			q += " ORDER BY " + f.Order
		}
		if f.Limit != nil {
			if *f.Limit < 0 {
				return nil, errors.New("LIMIT must not be negative")
			}
			q += " LIMIT ?"
			args = append(args, *f.Limit)
			if f.Offset != nil {
				if *f.Offset < 0 {
					return nil, errors.New("OFFSET must not be negative")
				}
				q += " OFFSET ?"
				args = append(args, *f.Offset)
			}
		}
		start := time.Now()
		if f.ForUpdate {
			q += " FOR UPDATE"
		}
		rows, err := executor(ctx, s.db).QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			v, err := scanEntity(rows, kind)
			if err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, v)
		}
		err = rows.Err()
		rows.Close()
		observePhase("read_sql", start)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
func withLease(ctx context.Context, leases []*lease) context.Context {
	if len(leases) == 1 {
		return context.WithValue(ctx, transactionKey{}, leases[0].tx)
	}
	return ctx
}
func expandSQL(query string, args []any) (string, []any) {
	var flat []any
	for _, arg := range args {
		switch v := arg.(type) {
		case []uuid.UUID:
			query = strings.Replace(query, "IN (?)", "IN ("+placeholders(len(v))+")", 1)
			for _, id := range v {
				flat = append(flat, uuidArg(id))
			}
		case uuid.UUID:
			flat = append(flat, uuidArg(v))
		case []int64:
			query = strings.Replace(query, "IN (?)", "IN ("+placeholders(len(v))+")", 1)
			for _, x := range v {
				flat = append(flat, x)
			}
		case []string:
			query = strings.Replace(query, "IN (?)", "IN ("+placeholders(len(v))+")", 1)
			for _, x := range v {
				flat = append(flat, x)
			}
		default:
			flat = append(flat, arg)
		}
	}
	return query, flat
}
