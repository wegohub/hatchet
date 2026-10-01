package tidb

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
)

type taskEvent struct {
	sqlcv1.CreateTaskEventsOLAPParams
	ID         int64
	InsertedAt pgtype.Timestamptz
}

type stateWriter struct {
	store *store
}

func newStateWriter(s *store) *stateWriter {
	return &stateWriter{store: s}
}

type runState struct {
	tasks    map[string]*sqlcv1.V1TasksOlap
	dags     map[string]*sqlcv1.V1DagsOlap
	events   map[string]*taskEvent
	payloads map[string]*storedPayload
	changes  map[string]entity
	attempts map[string]*attempt
	counters map[string]*dagCounter
	existing map[string]bool
}

func keyFor(id int64, ts time.Time) string { return fmt.Sprintf("%d/%d", id, ts.UnixMicro()) }

func decodeEntity[T any](row entity) (*T, error) {
	if value, ok := row.Value.(*T); ok {
		copy := *value
		return &copy, nil
	}
	var value T
	if err := json.Unmarshal([]byte(row.Body), &value); err != nil {
		return nil, fmt.Errorf("decode TiDB %s entity: %w", row.Kind, err)
	}
	return &value, nil
}

func (s *runState) change(tenant uuid.UUID, kind, key string, external, run uuid.UUID, id int64, at time.Time, value any) error {
	row, err := makeEntity(tenant, kind, key, external, run, id, at, value)
	if err != nil {
		return err
	}
	s.changes[kind+"/"+key] = row
	return nil
}

func (s *runState) rows() []entity {
	keys := make([]string, 0, len(s.changes))
	for key := range s.changes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]entity, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, s.changes[key])
	}
	return rows
}

type storedPayload struct {
	Inline      []byte
	ExternalKey string
	IndexFile   bool
}

func (s *runState) payload(tenant uuid.UUID, external, run uuid.UUID, at time.Time, payload []byte) error {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("{}")) {
		return nil
	}
	if !json.Valid(payload) {
		return fmt.Errorf("payload must be valid JSON")
	}
	if _, exists := s.payloads[external.String()]; exists {
		return nil
	}
	value := &storedPayload{Inline: append([]byte(nil), payload...)}
	s.payloads[external.String()] = value
	return s.change(tenant, "payload", external.String(), external, run, 0, at, value)
}

func taskOutcome(task *sqlcv1.V1TasksOlap) repository.UpdateTaskStatusRow {
	return repository.UpdateTaskStatusRow{TenantId: task.TenantID, TaskId: task.ID, TaskInsertedAt: task.InsertedAt, ReadableStatus: task.ReadableStatus, ExternalId: task.ExternalID, WorkflowId: task.WorkflowID, IsDAGTask: task.DagID.Valid, DagID: task.DagID, DagInsertedAt: task.DagInsertedAt}
}

func dagOutcome(dag *sqlcv1.V1DagsOlap) repository.UpdateDAGStatusRow {
	return repository.UpdateDAGStatusRow{TenantId: dag.TenantID, DagId: dag.ID, DagInsertedAt: dag.InsertedAt, ReadableStatus: dag.ReadableStatus, ExternalId: dag.ExternalID, WorkflowId: dag.WorkflowID}
}

func (s *runState) updateTask(task *sqlcv1.V1TasksOlap, status sqlcv1.V1ReadableStatusOlap, retry int32, worker *uuid.UUID) (bool, error) {
	if !shouldUpdateStatus(task.ReadableStatus, task.LatestRetryCount, status, retry) {
		return false, nil
	}
	oldStatus := task.ReadableStatus
	task.ReadableStatus = status
	task.LatestRetryCount = retry
	if worker != nil && *worker != uuid.Nil {
		id := *worker
		task.LatestWorkerID = &id
	}
	s.adjustCounter(task, &oldStatus)
	err := s.change(task.TenantID, "task", keyFor(task.ID, task.InsertedAt.Time), task.ExternalID, task.WorkflowRunID, task.ID, task.InsertedAt.Time, task)
	if row, ok := s.changes["task/"+keyFor(task.ID, task.InsertedAt.Time)]; ok && s.existing[keyFor(task.ID, task.InsertedAt.Time)] {
		row.StateOnly = true
		s.changes["task/"+keyFor(task.ID, task.InsertedAt.Time)] = row
	}
	return err == nil, err
}

func (s *runState) reconcile(task *sqlcv1.V1TasksOlap) (bool, error) {
	a := s.attempts[keyFor(task.ID, task.InsertedAt.Time)]
	if a == nil {
		return false, nil
	}
	return s.updateTask(task, a.Status, a.Retry, a.Worker)
}

func (s *runState) rollup(affected map[string]struct{}) ([]repository.UpdateDAGStatusRow, error) {
	var result []repository.UpdateDAGStatusRow
	for key := range affected {
		dag := s.dags[key]
		if dag == nil {
			if c := s.counters[key]; c != nil {
				for _, t := range s.tasks {
					if t.DagID.Valid && keyFor(t.DagID.Int64, t.DagInsertedAt.Time) == key {
						if err := s.pending(t.TenantID, c.ID, timestamp(c.At), c.ExternalID, "dag", false); err != nil {
							return nil, err
						}
						break
					}
				}
			}
			continue
		}
		if dag.IsDagOperator {
			continue
		}
		counter := s.counters[key]
		if counter == nil {
			continue
		}
		status := counter.rollup(dag.ReadableStatus, dag.TotalTasks)
		if status == dag.ReadableStatus {
			continue
		}
		dag.ReadableStatus = status
		if err := s.change(dag.TenantID, "dag", key, dag.ExternalID, dag.ExternalID, dag.ID, dag.InsertedAt.Time, dag); err != nil {
			return nil, err
		}
		result = append(result, dagOutcome(dag))
	}
	return result, nil
}

func releaseLocks(locks []*lease) {
	for i := len(locks) - 1; i >= 0; i-- {
		_ = locks[i].release()
	}
}

func availableRuns(runs []uuid.UUID, blocked map[uuid.UUID]struct{}) []uuid.UUID {
	result := make([]uuid.UUID, 0, len(runs))
	seen := make(map[uuid.UUID]struct{})
	for _, id := range runs {
		if _, no := blocked[id]; no {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

func (w *stateWriter) CreateTasks(ctx context.Context, tenant uuid.UUID, tasks []*repository.V1TaskWithPayload) (*repository.StatusUpdateResult, map[uuid.UUID]struct{}, error) {
	runs := make([]uuid.UUID, 0, len(tasks))
	for _, task := range tasks {
		runs = append(runs, task.WorkflowRunID)
	}
	locks, blocked, err := w.store.runLocks(ctx, tenant, runs)
	if err != nil {
		return nil, nil, err
	}
	defer releaseLocks(locks)
	ctx = withLease(ctx, locks)
	needsCounters := false
	for _, task := range tasks {
		needsCounters = needsCounters || task.DagID.Valid
	}
	ctx = context.WithValue(ctx, counterRequestKey{}, needsCounters)
	currentRuns := availableRuns(runs, blocked)
	// The initialization bit is read under the run lock and committed with every
	// run publication. An untouched standalone run has no earlier status to merge.
	allFresh := len(tasks) > 0 && !needsCounters
	for _, task := range tasks {
		_, fresh := locks[0].freshRuns[task.WorkflowRunID]
		allFresh = allFresh && fresh && task.WorkflowRunID == task.ExternalID
	}
	if allFresh {
		currentRuns = nil
	}
	state, err := w.loadCurrent(ctx, tenant, currentRuns, taskIDsOf(tasks), taskDates(tasks)...)
	if err != nil {
		return nil, nil, err
	}
	result := &repository.StatusUpdateResult{}
	affected := make(map[string]struct{})
	for _, task := range tasks {
		if _, no := blocked[task.WorkflowRunID]; no {
			continue
		}
		key := keyFor(task.ID, task.InsertedAt.Time)
		row := state.tasks[key]
		if row == nil {
			data, err := json.Marshal(task.V1Task)
			if err != nil {
				return nil, nil, err
			}
			row = new(sqlcv1.V1TasksOlap)
			if err = json.Unmarshal(data, row); err != nil {
				return nil, nil, err
			}
			row.IsDurable = task.IsDurable.Bool
			row.Input = []byte("{}")
			row.ReadableStatus = sqlcv1.V1ReadableStatusOlapQUEUED
			switch task.InitialState {
			case sqlcv1.V1TaskInitialStateFAILED:
				row.ReadableStatus = sqlcv1.V1ReadableStatusOlapFAILED
			case sqlcv1.V1TaskInitialStateCANCELLED:
				row.ReadableStatus = sqlcv1.V1ReadableStatusOlapCANCELLED
			case sqlcv1.V1TaskInitialStateSKIPPED:
				row.ReadableStatus = sqlcv1.V1ReadableStatusOlapCOMPLETED
			}
			state.tasks[key] = row
			state.adjustCounter(row, nil)
			if err = state.change(tenant, "task", key, row.ExternalID, row.WorkflowRunID, row.ID, row.InsertedAt.Time, row); err != nil {
				return nil, nil, err
			}
			if row.ReadableStatus != sqlcv1.V1ReadableStatusOlapQUEUED {
				result.TaskRows = append(result.TaskRows, taskOutcome(row))
				if row.DagID.Valid {
					affected[keyFor(row.DagID.Int64, row.DagInsertedAt.Time)] = struct{}{}
				}
			}
		}
		if row.DagID.Valid {
			affected[keyFor(row.DagID.Int64, row.DagInsertedAt.Time)] = struct{}{}
		}
		updated, err := state.reconcile(row)
		if err != nil {
			return nil, nil, err
		}
		if updated {
			result.TaskRows = append(result.TaskRows, taskOutcome(row))
			if row.DagID.Valid {
				affected[keyFor(row.DagID.Int64, row.DagInsertedAt.Time)] = struct{}{}
			}
		}
		if state.attempts[key] != nil {
			if err = state.pending(tenant, row.ID, row.InsertedAt, row.WorkflowRunID, "task", true); err != nil {
				return nil, nil, err
			}
		}
		payload := task.Payload
		if len(payload) == 0 {
			payload = task.Input
		}
		if err = state.payload(tenant, row.ExternalID, row.WorkflowRunID, row.InsertedAt.Time, payload); err != nil {
			return nil, nil, err
		}
	}
	result.DAGRows, err = state.rollup(affected)
	if err != nil {
		return nil, nil, err
	}
	if err = w.store.publish(ctx, state.rows(), locks...); err != nil {
		return nil, nil, err
	}
	return result, blocked, nil
}

func (w *stateWriter) CreateDAGs(ctx context.Context, tenant uuid.UUID, dags []*repository.DAGWithData) (map[uuid.UUID]struct{}, error) {
	runs := make([]uuid.UUID, 0, len(dags))
	for _, dag := range dags {
		runs = append(runs, dag.ExternalID)
	}
	locks, blocked, err := w.store.runLocks(ctx, tenant, runs)
	if err != nil {
		return nil, err
	}
	defer releaseLocks(locks)
	ctx = withLease(ctx, locks)
	ctx = context.WithValue(ctx, counterRequestKey{}, true)
	state, err := w.loadCurrent(ctx, tenant, availableRuns(runs, blocked), dagIDsOf(dags), dagDates(dags)...)
	if err != nil {
		return nil, err
	}
	for _, dag := range dags {
		if _, no := blocked[dag.ExternalID]; no {
			continue
		}
		key := keyFor(dag.ID, dag.InsertedAt.Time)
		row := state.dags[key]
		if row == nil {
			row = &sqlcv1.V1DagsOlap{ID: dag.ID, TenantID: tenant, InsertedAt: dag.InsertedAt, ExternalID: dag.ExternalID, WorkflowID: dag.WorkflowID, ReadableStatus: sqlcv1.V1ReadableStatusOlapQUEUED, Input: []byte("{}"), IsDagOperator: dag.IsOperatorRun}
			state.dags[key] = row
		}
		row.DisplayName = dag.DisplayName
		row.WorkflowVersionID = dag.WorkflowVersionID
		row.AdditionalMetadata = dag.AdditionalMetadata
		row.ParentTaskExternalID = dag.ParentTaskExternalID
		row.TotalTasks = int32(dag.TotalTasks)
		row.IdempotencyKey = dag.IdempotencyKey
		counter := state.counters[key]
		if counter != nil && !dag.IsOperatorRun {
			row.ReadableStatus = counter.rollup(row.ReadableStatus, row.TotalTasks)
		}
		if dag.IsOperatorRun {
			if a := state.attempts[key]; a != nil && shouldUpdateStatus(row.ReadableStatus, row.LatestRetryCount, a.Status, a.Retry) {
				row.ReadableStatus = a.Status
				row.LatestRetryCount = a.Retry
			}
		}
		if err = state.pending(tenant, row.ID, row.InsertedAt, row.ExternalID, "dag", true); err != nil {
			return nil, err
		}
		if err = state.change(tenant, "dag", key, row.ExternalID, row.ExternalID, row.ID, row.InsertedAt.Time, row); err != nil {
			return nil, err
		}
		if err = state.payload(tenant, dag.ExternalID, dag.ExternalID, dag.InsertedAt.Time, dag.Input); err != nil {
			return nil, err
		}
	}
	if err = w.store.publish(ctx, state.rows(), locks...); err != nil {
		return nil, err
	}
	return blocked, nil
}

func (w *stateWriter) CreateTaskEvents(ctx context.Context, tenant uuid.UUID, events []sqlcv1.CreateTaskEventsOLAPParams, eventRuns map[uuid.UUID]uuid.UUID, updates []repository.OrchestratorDAGStatusUpdateOpt, operatorRuns map[uuid.UUID]struct{}) (*repository.StatusUpdateResult, map[uuid.UUID]struct{}, error) {
	runs := make([]uuid.UUID, 0, len(events)+len(updates))
	for _, run := range eventRuns {
		runs = append(runs, run)
	}
	for _, update := range updates {
		runs = append(runs, update.ExternalId)
	}
	locks, blocked, err := w.store.runLocks(ctx, tenant, runs)
	if err != nil {
		return nil, nil, err
	}
	defer releaseLocks(locks)
	ctx = withLease(ctx, locks)
	ctx = context.WithValue(ctx, skipAttemptsKey{}, true)
	if len(availableRuns(runs, blocked)) == 0 {
		return &repository.StatusUpdateResult{}, blocked, nil
	}
	state, err := w.loadCurrent(ctx, tenant, availableRuns(runs, blocked), eventIDsOf(events, updates), eventDates(events, updates)...)
	if err != nil {
		return nil, nil, err
	}
	result := &repository.StatusUpdateResult{}
	affected := make(map[string]struct{})
	winners := make(map[string]sqlcv1.CreateTaskEventsOLAPParams)
	eventIDs, err := w.store.coordinator.nextEventIDs(ctx, len(events))
	if err != nil {
		return nil, nil, err
	}
	var candidates []entity
	for _, event := range events {
		run, ok := eventRuns[event.ExternalID]
		if !ok {
			continue
		}
		if _, no := blocked[run]; no {
			continue
		}
		key := taskEventKey(event)
		candidates = append(candidates, entity{Tenant: tenant, ExternalID: event.ExternalID, Key: key, InsertedAt: dateArg(event.TaskInsertedAt.Time)})
	}
	fresh, err := w.store.freshTaskEvents(ctx, ctx.Value(transactionKey{}).(*sql.Tx), candidates)
	if err != nil {
		return nil, nil, err
	}
	freshKeys := map[string]bool{}
	for _, row := range fresh {
		freshKeys[row.Key] = true
	}
	for i, event := range events {
		run, ok := eventRuns[event.ExternalID]
		if !ok {
			continue
		}
		if _, no := blocked[run]; no {
			continue
		}
		key := keyFor(event.TaskID, event.TaskInsertedAt.Time)

		eventKey := taskEventKey(event)
		if !freshKeys[eventKey] {
			continue
		}
		delete(freshKeys, eventKey)
		if old, ok := winners[key]; !ok || event.RetryCount > old.RetryCount || event.RetryCount == old.RetryCount && statusPriority(event.ReadableStatus) > statusPriority(old.ReadableStatus) {
			winners[key] = event
		}
		if _, persisted := state.events[eventKey]; !persisted {
			row := &taskEvent{CreateTaskEventsOLAPParams: event, ID: eventIDs[i], InsertedAt: pgtype.Timestamptz{Time: time.Now().UTC().Truncate(time.Microsecond), Valid: true}}
			if row.Output != nil {
				row.Output = []byte("{}")
			}
			state.events[eventKey] = row
			if err = state.change(tenant, "task_event", eventKey, event.ExternalID, run, event.TaskID, event.TaskInsertedAt.Time, row); err != nil {
				return nil, nil, err
			}
			e := state.changes["task_event/"+eventKey]
			e.KnownFresh = true
			state.changes["task_event/"+eventKey] = e
		}
		if err = state.payload(tenant, event.ExternalID, run, state.events[eventKey].InsertedAt.Time, event.Output); err != nil {
			return nil, nil, err
		}
	}
	for key, event := range winners {
		task := state.tasks[key]
		if task == nil {
			if err = state.pending(tenant, event.TaskID, event.TaskInsertedAt, eventRuns[event.ExternalID], "task", false); err != nil {
				return nil, nil, err
			}
			continue
		}
		updated, err := state.updateTask(task, event.ReadableStatus, event.RetryCount, event.WorkerID)
		if err != nil {
			return nil, nil, err
		}
		if updated {
			result.TaskRows = append(result.TaskRows, taskOutcome(task))
		}
		if task.DagID.Valid {
			affected[keyFor(task.DagID.Int64, task.DagInsertedAt.Time)] = struct{}{}
		}
	}
	result.DAGRows, err = state.rollup(affected)
	if err != nil {
		return nil, nil, err
	}
	operatorWinners := make(map[string]repository.OrchestratorDAGStatusUpdateOpt)
	for _, update := range updates {
		key := keyFor(update.DagId, update.DagInsertedAt.Time)
		old, ok := operatorWinners[key]
		if !ok || update.RetryCount > old.RetryCount || update.RetryCount == old.RetryCount && statusPriority(update.ReadableStatus) > statusPriority(old.ReadableStatus) {
			operatorWinners[key] = update
		}
	}
	for key, update := range operatorWinners {
		if _, no := blocked[update.ExternalId]; no {
			continue
		}
		row := state.dags[key]
		if row == nil {
			row = &sqlcv1.V1DagsOlap{ID: update.DagId, TenantID: tenant, InsertedAt: update.DagInsertedAt, ExternalID: update.ExternalId, DisplayName: update.DisplayName, WorkflowID: update.WorkflowId, WorkflowVersionID: update.WorkflowVersionId, Input: []byte("{}"), AdditionalMetadata: update.AdditionalMetadata, IsDagOperator: true, ReadableStatus: sqlcv1.V1ReadableStatusOlapQUEUED, LatestRetryCount: -1}
			state.dags[key] = row
		}
		if !shouldUpdateStatus(row.ReadableStatus, row.LatestRetryCount, update.ReadableStatus, update.RetryCount) {
			continue
		}
		row.ReadableStatus = update.ReadableStatus
		row.LatestRetryCount = update.RetryCount
		if err = state.change(tenant, "dag", key, row.ExternalID, row.ExternalID, row.ID, row.InsertedAt.Time, row); err != nil {
			return nil, nil, err
		}
		result.DAGRows = append(result.DAGRows, dagOutcome(row))
	}
	if err = w.store.publish(ctx, state.rows(), locks...); err != nil {
		return nil, nil, err
	}
	return result, blocked, nil
}

func taskEventKey(e sqlcv1.CreateTaskEventsOLAPParams) string {
	return fmt.Sprintf("%s/%s/%d/%d/%s", keyFor(e.TaskID, e.TaskInsertedAt.Time), e.EventType, e.RetryCount, e.DurableInvocationCount, e.ExternalID)
}
