package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
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
	store      *store
	eventCache *lru.Cache[string, bool]
}

func newStateWriter(s *store) *stateWriter {
	cache, _ := lru.New[string, bool](100000)
	return &stateWriter{store: s, eventCache: cache}
}

type runState struct {
	tasks    map[string]*sqlcv1.V1TasksOlap
	dags     map[string]*sqlcv1.V1DagsOlap
	events   map[string]*taskEvent
	payloads map[string]*storedPayload
	changes  map[string]entity
}

func keyFor(id int64, ts time.Time) string { return fmt.Sprintf("%d/%d", id, ts.UnixMicro()) }

func decodeEntity[T any](row entity) (*T, error) {
	var value T
	if err := json.Unmarshal([]byte(row.Body), &value); err != nil {
		return nil, fmt.Errorf("decode ClickHouse %s entity: %w", row.Kind, err)
	}
	return &value, nil
}

func (w *stateWriter) load(ctx context.Context, tenant uuid.UUID, runs []uuid.UUID) (*runState, error) {
	seq, err := w.store.barrier(ctx)
	if err != nil {
		return nil, err
	}
	state := &runState{tasks: make(map[string]*sqlcv1.V1TasksOlap), dags: make(map[string]*sqlcv1.V1DagsOlap), events: make(map[string]*taskEvent), payloads: make(map[string]*storedPayload), changes: make(map[string]entity)}
	if len(runs) == 0 {
		return state, nil
	}
	rows, err := w.store.read(ctx, seq, entityFilter{Tenant: &tenant, Kinds: []string{"task", "dag", "task_event", "payload"}, RunIDs: runs})
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		switch row.Kind {
		case "task":
			v, err := decodeEntity[sqlcv1.V1TasksOlap](row)
			if err != nil {
				return nil, err
			}
			state.tasks[row.Key] = v
		case "dag":
			v, err := decodeEntity[sqlcv1.V1DagsOlap](row)
			if err != nil {
				return nil, err
			}
			state.dags[row.Key] = v
		case "task_event":
			v, err := decodeEntity[taskEvent](row)
			if err != nil {
				return nil, err
			}
			state.events[row.Key] = v
		case "payload":
			v, err := decodeEntity[storedPayload](row)
			if err != nil {
				return nil, err
			}
			state.payloads[row.Key] = v
		}
	}
	return state, nil
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
	task.ReadableStatus = status
	task.LatestRetryCount = retry
	if worker != nil && *worker != uuid.Nil {
		id := *worker
		task.LatestWorkerID = &id
	}
	err := s.change(task.TenantID, "task", keyFor(task.ID, task.InsertedAt.Time), task.ExternalID, task.WorkflowRunID, task.ID, task.InsertedAt.Time, task)
	return err == nil, err
}

func (s *runState) reconcile(task *sqlcv1.V1TasksOlap) (bool, error) {
	var winner *taskEvent
	var worker *uuid.UUID
	for _, event := range s.events {
		if event.TaskID != task.ID || !event.TaskInsertedAt.Time.Equal(task.InsertedAt.Time) {
			continue
		}
		if winner == nil || event.RetryCount > winner.RetryCount {
			winner = event
			worker = event.WorkerID
		} else if event.RetryCount == winner.RetryCount {
			if statusPriority(event.ReadableStatus) > statusPriority(winner.ReadableStatus) {
				winner = event
			}
			if event.WorkerID != nil && (worker == nil || event.WorkerID.String() > worker.String()) {
				worker = event.WorkerID
			}
		}
	}
	if winner == nil {
		return false, nil
	}
	return s.updateTask(task, winner.ReadableStatus, winner.RetryCount, worker)
}

func (s *runState) rollup(affected map[string]struct{}) ([]repository.UpdateDAGStatusRow, error) {
	var result []repository.UpdateDAGStatusRow
	for key := range affected {
		dag := s.dags[key]
		if dag == nil || dag.IsDagOperator {
			continue
		}
		var statuses []sqlcv1.V1ReadableStatusOlap
		for _, task := range s.tasks {
			if task.DagID.Valid && task.DagID.Int64 == dag.ID && task.DagInsertedAt.Time.Equal(dag.InsertedAt.Time) {
				statuses = append(statuses, task.ReadableStatus)
			}
		}
		status := rollupDAGStatus(dag.ReadableStatus, int(dag.TotalTasks), statuses)
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
	state, err := w.load(ctx, tenant, availableRuns(runs, blocked))
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
	state, err := w.load(ctx, tenant, availableRuns(runs, blocked))
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
		var statuses []sqlcv1.V1ReadableStatusOlap
		for _, task := range state.tasks {
			if task.DagID.Valid && task.DagID.Int64 == dag.ID && task.DagInsertedAt.Time.Equal(dag.InsertedAt.Time) {
				statuses = append(statuses, task.ReadableStatus)
			}
		}
		computed := rollupDAGStatus(sqlcv1.V1ReadableStatusOlapQUEUED, dag.TotalTasks, statuses)
		if statusPriority(computed) > statusPriority(row.ReadableStatus) {
			row.ReadableStatus = computed
		}
		if dag.IsOperatorRun {
			var winner *taskEvent
			for _, event := range state.events {
				if event.TaskID != dag.ID || !event.TaskInsertedAt.Time.Equal(dag.InsertedAt.Time) {
					continue
				}
				if winner == nil || event.RetryCount > winner.RetryCount || event.RetryCount == winner.RetryCount && (statusPriority(event.ReadableStatus) > statusPriority(winner.ReadableStatus) || event.ReadableStatus == winner.ReadableStatus && event.ID > winner.ID) {
					winner = event
				}
			}
			if winner != nil && shouldUpdateStatus(row.ReadableStatus, row.LatestRetryCount, winner.ReadableStatus, winner.RetryCount) {
				row.ReadableStatus = winner.ReadableStatus
				row.LatestRetryCount = winner.RetryCount
			}
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
	state, err := w.load(ctx, tenant, availableRuns(runs, blocked))
	if err != nil {
		return nil, nil, err
	}
	result := &repository.StatusUpdateResult{}
	affected := make(map[string]struct{})
	winners := make(map[string]sqlcv1.CreateTaskEventsOLAPParams)
	firstID, err := w.store.keeper.nextLogIDs(ctx, uint64(len(events)))
	if err != nil {
		return nil, nil, err
	}
	var cacheKeys []string
	for i, event := range events {
		run, ok := eventRuns[event.ExternalID]
		if !ok {
			continue
		}
		if _, no := blocked[run]; no {
			continue
		}
		key := keyFor(event.TaskID, event.TaskInsertedAt.Time)
		if old, ok := winners[key]; !ok || event.RetryCount > old.RetryCount || event.RetryCount == old.RetryCount && statusPriority(event.ReadableStatus) > statusPriority(old.ReadableStatus) {
			winners[key] = event
		}
		cacheKey := fmt.Sprintf("%d-%s-%d-%d", event.TaskID, event.EventType, event.RetryCount, event.DurableInvocationCount)
		_, cached := w.eventCache.Get(cacheKey)
		if !cached {
			row := &taskEvent{CreateTaskEventsOLAPParams: event, ID: int64(firstID + uint64(i)), InsertedAt: pgtype.Timestamptz{Time: time.Now().UTC().Truncate(time.Microsecond), Valid: true}}
			if row.Output != nil {
				row.Output = []byte("{}")
			}
			eventKey := fmt.Sprint(row.ID)
			state.events[eventKey] = row
			cacheKeys = append(cacheKeys, cacheKey)
			if err = state.change(tenant, "task_event", eventKey, event.ExternalID, run, event.TaskID, event.TaskInsertedAt.Time, row); err != nil {
				return nil, nil, err
			}
		}
		if err = state.payload(tenant, event.ExternalID, run, time.Now(), event.Output); err != nil {
			return nil, nil, err
		}
	}
	for key, event := range winners {
		task := state.tasks[key]
		if task == nil {
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
	for _, key := range cacheKeys {
		w.eventCache.Add(key, true)
	}
	return result, blocked, nil
}
