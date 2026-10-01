package tidb

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
)

func taskIDsOf(tasks []*repository.V1TaskWithPayload) []int64 {
	var ids []int64
	for _, t := range tasks {
		ids = append(ids, t.ID)
	}
	return ids
}
func dagIDsOf(dags []*repository.DAGWithData) []int64 {
	var ids []int64
	for _, d := range dags {
		ids = append(ids, d.ID)
	}
	return ids
}
func eventIDsOf(events []sqlcv1.CreateTaskEventsOLAPParams, updates []repository.OrchestratorDAGStatusUpdateOpt) []int64 {
	var ids []int64
	for _, e := range events {
		ids = append(ids, e.TaskID)
	}
	for _, u := range updates {
		ids = append(ids, u.DagId)
	}
	return ids
}

// Run locks serialize counter changes; only the affected task projections are loaded.
func (w *stateWriter) loadCurrent(ctx context.Context, tenant uuid.UUID, runs []uuid.UUID, ids []int64, dates ...time.Time) (*runState, error) {
	s := &runState{tasks: map[string]*sqlcv1.V1TasksOlap{}, dags: map[string]*sqlcv1.V1DagsOlap{}, events: map[string]*taskEvent{}, payloads: map[string]*storedPayload{}, changes: map[string]entity{}, attempts: map[string]*attempt{}, counters: map[string]*dagCounter{}, existing: map[string]bool{}}
	if len(runs) == 0 {
		return s, nil
	}
	pointWhere, pointArgs := datePredicate(dates)
	var rows []entity
	var err error
	if len(ids) > 0 {
		rows, err = w.store.read(ctx, 0, entityFilter{Tenant: &tenant, Kind: "task", RunIDs: runs, TaskIDs: ids, ForUpdate: true, Where: pointWhere, Args: pointArgs})
		if err != nil {
			return nil, err
		}
	}
	for _, r := range rows {
		v, err := decodeEntity[sqlcv1.V1TasksOlap](r)
		if err != nil {
			return nil, err
		}
		s.tasks[r.Key] = v
		s.existing[r.Key] = true
	}
	needsCounters, explicit := ctx.Value(counterRequestKey{}).(bool)
	matched := map[int64]bool{}
	for _, t := range s.tasks {
		matched[t.ID] = true
		needsCounters = needsCounters || t.DagID.Valid
	}
	allMatched := len(ids) > 0
	for _, id := range ids {
		allMatched = allMatched && matched[id]
	}
	needsDags := needsCounters || !explicit && !allMatched
	if needsDags {
		rows, err = w.store.read(ctx, 0, entityFilter{Tenant: &tenant, Kind: "dag", ExternalIDs: runs, ForUpdate: true})
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			v, err := decodeEntity[sqlcv1.V1DagsOlap](r)
			if err != nil {
				return nil, err
			}
			s.dags[r.Key] = v
		}
	}
	needsCounters = needsCounters || len(s.dags) > 0
	if needsCounters {
		args := []any{uuidArg(tenant)}
		for _, r := range runs {
			args = append(args, uuidArg(r))
		}
		var ats []time.Time
		for _, d := range s.dags {
			ats = append(ats, d.InsertedAt.Time)
		}
		for _, t := range s.tasks {
			if t.DagInsertedAt.Valid {
				ats = append(ats, t.DagInsertedAt.Time)
			}
		}
		ats = append(ats, dates...)
		dateSQL, dateArgs := datePredicate(ats)
		dateSQL = strings.ReplaceAll(dateSQL, "e.", "")
		args = append(args, dateArgs...)
		rs, err := executor(ctx, w.store.db).QueryContext(ctx, "SELECT external_id,id,inserted_at,workflow_id,queued_count,running_count,evicted_count,completed_count,cancelled_count,failed_count FROM v1_runs_olap WHERE tenant_id=? AND kind='dag' AND external_id IN ("+placeholders(len(runs))+") AND "+dateSQL+" FOR UPDATE", args...)
		if err != nil {
			return nil, err
		}
		for rs.Next() {
			c := new(dagCounter)
			if err = rs.Scan(&c.ExternalID, &c.ID, &c.At, &c.WorkflowID, &c.Counts[0], &c.Counts[1], &c.Counts[2], &c.Counts[3], &c.Counts[4], &c.Counts[5]); err != nil {
				rs.Close()
				return nil, err
			}
			s.counters[keyFor(c.ID, c.At)] = c
		}
		err = rs.Err()
		rs.Close()
		if err != nil {
			return nil, err
		}
	}
	if skip, _ := ctx.Value(skipAttemptsKey{}).(bool); skip {
		return s, nil
	}
	if len(ids) == 0 {
		return s, nil
	}
	args := []any{uuidArg(tenant)}
	for _, id := range ids {
		args = append(args, id)
	}
	// The primary key bounds every latest-attempt lookup by task and insertion time.
	attemptWhere, attemptArgs := datePredicate(dates)
	attemptWhere = strings.ReplaceAll(attemptWhere, "e.inserted_at", "task_inserted_at")
	args = append(args, attemptArgs...)
	rs, err := executor(ctx, w.store.db).QueryContext(ctx, "SELECT "+attemptColumns+" FROM v1_task_attempts_olap a WHERE tenant_id=? AND task_id IN ("+placeholders(len(ids))+") AND "+attemptWhere+" FOR UPDATE", args...)
	if err != nil {
		return nil, err
	}
	for rs.Next() {
		a, e := scanAttempt(rs)
		if e != nil {
			rs.Close()
			return nil, e
		}
		key := keyFor(a.TaskID, a.TaskAt.Time)
		if old := s.attempts[key]; old == nil || a.Retry > old.Retry {
			s.attempts[key] = a
		}
	}
	err = rs.Err()
	rs.Close()
	return s, err
}
func (s *runState) adjustCounter(t *sqlcv1.V1TasksOlap, old *sqlcv1.V1ReadableStatusOlap) {
	if !t.DagID.Valid {
		return
	}
	key := keyFor(t.DagID.Int64, t.DagInsertedAt.Time)
	c := s.counters[key]
	if c == nil {
		c = &dagCounter{ExternalID: t.WorkflowRunID, WorkflowID: t.WorkflowID, ID: t.DagID.Int64, At: t.DagInsertedAt.Time}
		s.counters[key] = c
	}
	if old != nil {
		if i := statusIndex(*old); i >= 0 {
			c.Counts[i]--
		}
	}
	if i := statusIndex(t.ReadableStatus); i >= 0 {
		c.Counts[i]++
	}
	_ = s.change(t.TenantID, "dag_counter", key, c.ExternalID, c.ExternalID, c.ID, c.At, c)
}

type pendingUpdate struct {
	TaskID  int64
	At      pgtype.Timestamptz
	Run     uuid.UUID
	Kind    string
	Delete  bool
	Retries int
}

func (s *runState) pending(tenant uuid.UUID, id int64, at pgtype.Timestamptz, run uuid.UUID, kind string, done bool) error {
	return s.change(tenant, "pending_update", kind+"/"+keyFor(id, at.Time), uuid.Nil, run, id, at.Time, pendingUpdate{TaskID: id, At: at, Run: run, Kind: kind, Delete: done})
}
func (s *store) publishPending(ctx context.Context, tx *sql.Tx, rows []entity) error {
	for _, r := range rows {
		p, err := decodeEntity[pendingUpdate](r)
		if err != nil {
			return err
		}
		if p.Delete {
			_, err = tx.ExecContext(ctx, "DELETE FROM v1_olap_pending_updates WHERE tenant_id=? AND task_id=? AND task_inserted_at=? AND kind=?", uuidArg(r.Tenant), p.TaskID, stampArg(p.At), p.Kind)
		} else {
			_, err = tx.ExecContext(ctx, "INSERT INTO v1_olap_pending_updates(tenant_id,task_id,task_inserted_at,run_id,kind,retry_after,retries) VALUES(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE retry_after=VALUES(retry_after),retries=VALUES(retries)", uuidArg(r.Tenant), p.TaskID, stampArg(p.At), uuidArg(p.Run), p.Kind, dateArg(time.Now().Add(time.Duration(min(p.Retries, 60))*time.Second)), p.Retries)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func (w *stateWriter) eventExists(ctx context.Context, tenant uuid.UUID, key string, at time.Time, external uuid.UUID) (bool, error) {
	var n int
	err := executor(ctx, w.store.db).QueryRowContext(ctx, "SELECT 1 FROM v1_task_events_olap WHERE tenant_id=? AND external_id=? AND inserted_at=? AND entity_key=?", uuidArg(tenant), uuidArg(external), dateArg(at), []byte(key)).Scan(&n)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (r *Repository) compensate(ctx context.Context, tenants []uuid.UUID, kind string) (bool, *repository.StatusUpdateResult, error) {
	if len(tenants) == 0 {
		return false, &repository.StatusUpdateResult{}, nil
	}
	limit := r.limits.Task
	if kind == "dag" {
		limit = r.limits.DAG
	}
	if limit <= 0 {
		limit = 1000
	}
	where := "kind=? AND retry_after<=UTC_TIMESTAMP(6)"
	args := []any{kind}
	if len(tenants) > 0 {
		where += " AND tenant_id IN (" + placeholders(len(tenants)) + ")"
		for _, t := range tenants {
			args = append(args, uuidArg(t))
		}
	}
	rows, err := r.store.db.QueryContext(ctx, "SELECT tenant_id,task_id,task_inserted_at,run_id,retries FROM v1_olap_pending_updates WHERE "+where+" ORDER BY retry_after,tenant_id LIMIT ?", append(args, limit)...)
	if err != nil {
		return false, nil, err
	}
	groups := map[uuid.UUID][]pendingUpdate{}
	for rows.Next() {
		var t uuid.UUID
		p := pendingUpdate{Kind: kind}
		if err = rows.Scan(&t, &p.TaskID, &p.At, &p.Run, &p.Retries); err != nil {
			rows.Close()
			return false, nil, err
		}
		groups[t] = append(groups[t], p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, nil, err
	}
	result := &repository.StatusUpdateResult{}
	processed := 0
	for tenant, ps := range groups {
		var runs []uuid.UUID
		var ids []int64
		for _, p := range ps {
			runs = append(runs, p.Run)
			ids = append(ids, p.TaskID)
		}
		locks, blocked, err := r.store.runLocks(ctx, tenant, runs)
		if err != nil {
			return false, nil, err
		}
		func() {
			defer releaseLocks(locks)
			txctx := withLease(ctx, locks)
			var state *runState
			var dates []time.Time
			for _, p := range ps {
				dates = append(dates, p.At.Time)
			}
			state, err = r.loadCurrent(txctx, tenant, availableRuns(runs, blocked), ids, dates...)
			if err != nil {
				return
			}
			affected := map[string]struct{}{}
			for _, p := range ps {
				if _, no := blocked[p.Run]; no {
					continue
				}
				key := keyFor(p.TaskID, p.At.Time)
				p.Delete = false
				if kind == "task" {
					if t := state.tasks[key]; t != nil {
						var changed bool
						changed, err = state.reconcile(t)
						if err != nil {
							return
						}
						if changed {
							result.TaskRows = append(result.TaskRows, taskOutcome(t))
							if t.DagID.Valid {
								affected[keyFor(t.DagID.Int64, t.DagInsertedAt.Time)] = struct{}{}
							}
						}
						p.Delete = true
					}
				} else {
					if d := state.dags[key]; d != nil {
						p.Delete = true
						affected[key] = struct{}{}
					}
				}
				processed++
				p.Retries++
				err = state.change(tenant, "pending_update", kind+"/"+key, uuid.Nil, p.Run, p.TaskID, p.At.Time, p)
				if err != nil {
					return
				}
			}
			var ds []repository.UpdateDAGStatusRow
			ds, err = state.rollup(affected)
			if err != nil {
				return
			}
			result.DAGRows = append(result.DAGRows, ds...)
			err = r.store.publish(txctx, state.rows(), locks...)
		}()
		if err != nil {
			return false, nil, err
		}
	}
	return processed >= int(limit), result, nil
}
func (r *Repository) countPending(ctx context.Context, tenants []uuid.UUID, kind string) (int64, error) {
	args := []any{kind}
	where := "kind=?"
	if len(tenants) > 0 {
		where += " AND tenant_id IN (" + placeholders(len(tenants)) + ")"
		for _, t := range tenants {
			args = append(args, uuidArg(t))
		}
	}
	var n int64
	err := r.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM v1_olap_pending_updates WHERE "+where, args...).Scan(&n)
	return n, err
}

func taskDates(tasks []*repository.V1TaskWithPayload) []time.Time {
	var ats []time.Time
	for _, t := range tasks {
		ats = append(ats, t.InsertedAt.Time)
		if t.DagInsertedAt.Valid {
			ats = append(ats, t.DagInsertedAt.Time)
		}
	}
	return ats
}
func dagDates(dags []*repository.DAGWithData) []time.Time {
	var ats []time.Time
	for _, d := range dags {
		ats = append(ats, d.InsertedAt.Time)
	}
	return ats
}
func eventDates(events []sqlcv1.CreateTaskEventsOLAPParams, updates []repository.OrchestratorDAGStatusUpdateOpt) []time.Time {
	var ats []time.Time
	for _, e := range events {
		ats = append(ats, e.TaskInsertedAt.Time)
	}
	for _, u := range updates {
		ats = append(ats, u.DagInsertedAt.Time)
	}
	return ats
}

type counterRequestKey struct{}
type skipAttemptsKey struct{}
