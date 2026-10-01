package tidb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type hydrationKey struct{}
type pageHydration struct {
	attempts []*attempt
	payloads map[uuid.UUID][]byte
}

func (r *Repository) readAttempts(ctx context.Context, tenant uuid.UUID, tasks []*sqlcv1.V1TasksOlap, runs []uuid.UUID) ([]*attempt, error) {
	if len(tasks) == 0 && len(runs) == 0 {
		return []*attempt{}, nil
	}
	if cache, ok := ctx.Value(hydrationKey{}).(*pageHydration); ok {
		return selectAttempts(cache.attempts, tasks, runs), nil
	}
	var parts []string
	args := []any{uuidArg(tenant)}
	if len(tasks) > 0 {
		var tuples []string
		var dates []time.Time
		for _, t := range tasks {
			tuples = append(tuples, "(?,?)")
			args = append(args, t.ID, stampArg(t.InsertedAt))
			dates = append(dates, t.InsertedAt.Time)
		}
		dateWhere, dateArgs := timeBounds("task_inserted_at", dates)
		parts = append(parts, "((task_id,task_inserted_at) IN ("+strings.Join(tuples, ",")+") AND "+dateWhere+")")
		args = append(args, dateArgs...)
	}
	if len(runs) > 0 {
		parts = append(parts, "run_id IN ("+placeholders(len(runs))+")")
		for _, id := range runs {
			args = append(args, uuidArg(id))
		}
	}
	cutoff := time.Now().UTC().Add(-r.retention).Truncate(24 * time.Hour)
	args = append(args, cutoff)
	rows, err := executor(ctx, r.store.db).QueryContext(ctx, "SELECT /*+ READ_FROM_STORAGE(TIKV[v1_task_attempts_olap]) */ "+attemptColumns+" FROM v1_task_attempts_olap WHERE tenant_id=? AND ("+strings.Join(parts, " OR ")+") AND task_inserted_at>=?", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*attempt, 0)
	for rows.Next() {
		a, err := scanAttempt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func selectAttempts(input []*attempt, tasks []*sqlcv1.V1TasksOlap, runs []uuid.UUID) []*attempt {
	keys := map[string]bool{}
	runSet := map[uuid.UUID]bool{}
	for _, t := range tasks {
		keys[keyFor(t.ID, t.InsertedAt.Time)] = true
	}
	for _, id := range runs {
		runSet[id] = true
	}
	out := make([]*attempt, 0)
	for _, a := range input {
		if keys[keyFor(a.TaskID, a.TaskAt.Time)] || runSet[a.RunID] {
			out = append(out, a)
		}
	}
	return out
}
func (r *Repository) hydratePage(ctx context.Context, tenant uuid.UUID, tasks []*sqlcv1.V1TasksOlap, dags []*sqlcv1.V1DagsOlap, include bool) (context.Context, error) {
	if _, ok := ctx.Value(hydrationKey{}).(*pageHydration); ok {
		return ctx, nil
	}
	runs := make([]uuid.UUID, 0, len(dags))
	for _, d := range dags {
		runs = append(runs, d.ExternalID)
	}
	attempts, err := r.readAttempts(ctx, tenant, tasks, runs)
	if err != nil {
		return ctx, err
	}
	cache := &pageHydration{attempts: attempts, payloads: map[uuid.UUID][]byte{}}
	if include {
		ids := map[uuid.UUID]bool{}
		for _, t := range tasks {
			ids[t.ExternalID] = true
			if t.DagID.Valid {
				ids[t.WorkflowRunID] = true
			}
		}
		for _, d := range dags {
			ids[d.ExternalID] = true
		}
		for _, a := range attempts {
			for _, id := range []*uuid.UUID{a.OutputID, a.RunOutputID, a.DetailOutputID} {
				if id != nil {
					ids[*id] = true
				}
			}
		}
		wanted := make([]uuid.UUID, 0, len(ids))
		for id := range ids {
			wanted = append(wanted, id)
			cache.payloads[id] = nil
		}
		cache.payloads, err = r.payloadBatch(ctx, tenant, wanted)
		if err != nil {
			return ctx, err
		}
	}
	return context.WithValue(ctx, hydrationKey{}, cache), nil
}
func (r *Repository) taskProjection(ctx context.Context, tenant uuid.UUID, tasks []*sqlcv1.V1TasksOlap, include bool, retry *int) ([]*repository.TaskWithPayloads, error) {
	if len(tasks) == 0 {
		return []*repository.TaskWithPayloads{}, nil
	}
	ctx, err := r.hydratePage(ctx, tenant, tasks, nil, include)
	if err != nil {
		return nil, err
	}
	attempts, err := r.readAttempts(ctx, tenant, tasks, nil)
	if err != nil {
		return nil, err
	}
	byTask := map[string][]*attempt{}
	for _, a := range attempts {
		key := keyFor(a.TaskID, a.TaskAt.Time)
		byTask[key] = append(byTask[key], a)
	}
	out := make([]*repository.TaskWithPayloads, 0, len(tasks))
	for _, t := range tasks {
		raw, err := json.Marshal(t)
		if err != nil {
			return nil, err
		}
		row := new(sqlcv1.PopulateTaskRunDataRow)
		if err = json.Unmarshal(raw, row); err != nil {
			return nil, err
		}
		row.Status = t.ReadableStatus
		row.IsStandalone = !t.DagID.Valid
		row.Input = []byte("{}")
		row.Output = []byte("{}")
		row.RetryCount = t.LatestRetryCount
		selected := int32(0)
		for _, a := range byTask[keyFor(t.ID, t.InsertedAt.Time)] {
			if a.Retry > selected {
				selected = a.Retry
			}
		}
		if retry != nil {
			selected = int32(*retry)
			row.RetryCount = selected
		}
		var output *attempt
		for _, a := range byTask[keyFor(t.ID, t.InsertedAt.Time)] {
			if a.Retry == selected {
				row.QueuedAt = a.QueuedLast
				row.StartedAt = a.StartedLast
				row.FinishedAt = a.FinishedLast
				row.ErrorMessage = a.Error
			}
			if a.OutputID != nil && (output == nil || a.OutputTime.Time.After(output.OutputTime.Time)) {
				output = a
			}
		}
		if output != nil {
			row.OutputEventExternalID = output.OutputID
			row.OutputEventInsertedAt = output.OutputAt
		}
		value := &repository.TaskWithPayloads{PopulateTaskRunDataRow: row, InputPayload: row.Input, OutputPayload: row.Output}
		if include {
			if p, err := r.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: t.ExternalID, InsertedAt: t.InsertedAt}); err != nil {
				return nil, err
			} else if p != nil {
				value.InputPayload = p
			}
			if output != nil {
				p, err := r.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: *output.OutputID, InsertedAt: output.OutputAt})
				if err != nil {
					return nil, err
				}
				if p != nil {
					value.OutputPayload = p
				}
			}
		}
		out = append(out, value)
	}
	return out, nil
}
func (r *Repository) dagProjection(ctx context.Context, dag *sqlcv1.V1DagsOlap, include, single bool) (*repository.WorkflowRunData, []repository.TaskMetadata, error) {
	ctx, err := r.hydratePage(ctx, dag.TenantID, nil, []*sqlcv1.V1DagsOlap{dag}, include)
	if err != nil {
		return nil, nil, err
	}
	attempts, err := r.readAttempts(ctx, dag.TenantID, nil, []uuid.UUID{dag.ExternalID})
	if err != nil {
		return nil, nil, err
	}
	row := &repository.WorkflowRunData{TenantID: dag.TenantID, ID: dag.ID, ExternalID: dag.ExternalID, InsertedAt: dag.InsertedAt, Kind: sqlcv1.V1RunKindDAG, WorkflowID: dag.WorkflowID, WorkflowVersionId: dag.WorkflowVersionID, ReadableStatus: dag.ReadableStatus, DisplayName: dag.DisplayName, AdditionalMetadata: dag.AdditionalMetadata, ParentTaskExternalId: dag.ParentTaskExternalID, Input: []byte("{}"), Output: []byte("{}")}
	if dag.IdempotencyKey.Valid {
		k := dag.IdempotencyKey.String
		row.IdempotencyKey = &k
	}
	latest := map[string]int32{}
	maxRetry := int32(0)
	for _, a := range attempts {
		key := keyFor(a.TaskID, a.TaskAt.Time)
		if a.Retry > latest[key] {
			latest[key] = a.Retry
		}
		if a.Retry > maxRetry {
			maxRetry = a.Retry
		}
	}
	var meta []repository.TaskMetadata
	var output, failure *attempt
	for _, a := range attempts {
		if single && a.Retry == latest[keyFor(a.TaskID, a.TaskAt.Time)] || !single && a.Retry == maxRetry {
			row.CreatedAt = minTimestamp(row.CreatedAt, a.RunCreated)
			row.StartedAt = minTimestamp(row.StartedAt, a.RunStarted)
			row.FinishedAt = maxTimestamp(row.FinishedAt, a.RunFinished)
			for i := int64(0); i < a.Count; i++ {
				meta = append(meta, repository.TaskMetadata{TaskID: a.TaskID, TaskInsertedAt: a.TaskAt.Time})
			}
		}
		if a.ErrorID.Valid && (failure == nil || a.Retry > failure.Retry) {
			failure = a
		}
		if a.RunOutputID != nil && (output == nil || a.RunOutputAt.Time.After(output.RunOutputAt.Time)) {
			output = a
		}
	}
	if failure != nil {
		row.ErrorMessage = failure.Error.String
	}
	retry := int(maxRetry)
	row.RetryCount = &retry
	if include {
		p, err := r.ReadPayload(ctx, dag.TenantID, repository.ReadOLAPPayloadOpts{ExternalId: dag.ExternalID, InsertedAt: dag.InsertedAt})
		if err != nil {
			return nil, nil, err
		}
		if p != nil || single {
			row.Input = p
		}
		if single {
			row.Output = nil
		}
		if output != nil {
			row.Output, err = r.ReadPayload(ctx, dag.TenantID, repository.ReadOLAPPayloadOpts{ExternalId: *output.RunOutputID, InsertedAt: output.RunOutputAt})
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return row, meta, nil
}
func (r *Repository) readWorkflowProjection(ctx context.Context, id uuid.UUID) (*repository.V1WorkflowRunPopulator, error) {
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, err
	}
	entities, err := r.scan(ctx, entityFilter{Kinds: []string{"task", "dag"}, ExternalIDs: []uuid.UUID{id}})
	if err != nil {
		return nil, err
	}
	for _, e := range entities {
		if e.Kind == "dag" {
			dag, err := decodeEntity[sqlcv1.V1DagsOlap](e)
			if err != nil {
				return nil, err
			}
			row, meta, err := r.dagProjection(ctx, dag, true, true)
			if row != nil {
				clearReadWorkflowFields(row)
			}
			return &repository.V1WorkflowRunPopulator{WorkflowRun: row, TaskMetadata: meta}, err
		}
	}
	if len(entities) == 0 {
		return nil, pgx.ErrNoRows
	}
	task, err := decodeEntity[sqlcv1.V1TasksOlap](entities[0])
	if err != nil {
		return nil, err
	}
	if task.DagID.Valid {
		return nil, pgx.ErrNoRows
	}
	ctx, err = r.hydratePage(ctx, task.TenantID, []*sqlcv1.V1TasksOlap{task}, nil, true)
	if err != nil {
		return nil, err
	}
	rows, err := r.taskProjection(ctx, task.TenantID, []*sqlcv1.V1TasksOlap{task}, false, nil)
	if err != nil {
		return nil, err
	}
	row := taskWorkflow(rows[0])
	clearReadWorkflowFields(row)
	row.ParentTaskExternalId = nil
	row.CreatedAt = pgtype.Timestamptz{}
	row.StartedAt = pgtype.Timestamptz{}
	row.FinishedAt = pgtype.Timestamptz{}
	attempts, err := r.readAttempts(ctx, task.TenantID, []*sqlcv1.V1TasksOlap{task}, nil)
	if err != nil {
		return nil, err
	}
	latest := int32(0)
	for _, a := range attempts {
		if a.Retry > latest {
			latest = a.Retry
		}
	}
	var meta []repository.TaskMetadata
	var output *attempt
	for _, a := range attempts {
		if a.Retry == latest {
			row.CreatedAt = a.RunCreated
			row.StartedAt = a.RunStarted
			row.FinishedAt = a.RunFinished
			for i := int64(0); i < a.Count; i++ {
				meta = append(meta, repository.TaskMetadata{TaskID: a.TaskID, TaskInsertedAt: a.TaskAt.Time})
			}
		}
		if a.RunOutputID != nil && (output == nil || a.RunOutputAt.Time.After(output.RunOutputAt.Time)) {
			output = a
		}
	}
	row.Input, err = r.ReadPayload(ctx, task.TenantID, repository.ReadOLAPPayloadOpts{ExternalId: id, InsertedAt: task.InsertedAt})
	if err != nil {
		return nil, err
	}
	row.Output = nil
	if output != nil {
		row.Output, err = r.ReadPayload(ctx, task.TenantID, repository.ReadOLAPPayloadOpts{ExternalId: *output.RunOutputID, InsertedAt: output.RunOutputAt})
		if err != nil {
			return nil, err
		}
	}
	return &repository.V1WorkflowRunPopulator{WorkflowRun: row, TaskMetadata: meta}, nil
}
func (r *Repository) readTaskProjection(ctx context.Context, tenant uuid.UUID, id int64, at pgtype.Timestamptz, retry *int) (*repository.TaskWithPayloads, uuid.UUID, error) {
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, uuid.Nil, err
	}
	tasks, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "task", Keys: []string{keyFor(id, at.Time)}})
	if err != nil {
		return nil, uuid.Nil, err
	}
	if len(tasks) == 0 {
		return nil, uuid.Nil, pgx.ErrNoRows
	}
	ctx, err = r.hydratePage(ctx, tenant, tasks, nil, true)
	if err != nil {
		return nil, uuid.Nil, err
	}
	values, err := r.taskProjection(ctx, tenant, tasks, false, retry)
	if err != nil {
		return nil, uuid.Nil, err
	}
	row := values[0]
	attempts, err := r.readAttempts(ctx, tenant, tasks, nil)
	if err != nil {
		return nil, uuid.Nil, err
	}
	selected := int32(0)
	for _, a := range attempts {
		if a.Retry > selected {
			selected = a.Retry
		}
	}
	if retry != nil {
		selected = int32(*retry)
	}
	var a *attempt
	for _, candidate := range attempts {
		if candidate.Retry == selected {
			a = candidate
			break
		}
	}
	if a == nil {
		return nil, uuid.Nil, fmt.Errorf("task retry has no status events")
	}
	row.RetryCount = selected
	row.Status = a.DetailStatus
	row.ErrorMessage = a.DetailError
	row.Output = nil
	row.OutputPayload = nil
	row.OutputEventExternalID = a.DetailOutputID
	row.OutputEventInsertedAt = a.DetailOutputAt
	if a.DetailOutputID != nil {
		row.Output = []byte("{}")
		row.OutputPayload, err = r.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: *a.DetailOutputID, InsertedAt: a.DetailOutputAt})
		if err != nil {
			return nil, uuid.Nil, err
		}
		if row.OutputPayload == nil {
			row.OutputPayload = row.Output
		}
	}
	run := tasks[0].ExternalID
	inputAt := tasks[0].InsertedAt
	if tasks[0].DagID.Valid {
		run = tasks[0].WorkflowRunID
		inputAt = tasks[0].DagInsertedAt
	}
	p, err := r.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: run, InsertedAt: inputAt})
	if err != nil {
		return nil, uuid.Nil, err
	}
	if p != nil || tasks[0].DagID.Valid {
		row.InputPayload = p
	}
	row.WorkflowRunID = run
	cutoff := time.Now().UTC().Add(-r.retention).Truncate(24 * time.Hour)
	if err = executor(ctx, r.store.db).QueryRowContext(ctx, "SELECT /*+ READ_FROM_STORAGE(TIKV[v1_runs_olap]) */ COUNT(*) FROM v1_runs_olap WHERE tenant_id=? AND parent_external_id=? AND inserted_at>=? AND is_placeholder=FALSE AND (kind='dag' OR is_dag_child=FALSE)", uuidArg(tenant), uuidArg(row.ExternalID), cutoff).Scan(&row.NumSpawnedChildren); err != nil {
		return nil, uuid.Nil, err
	}
	return row, run, nil
}

func (r *Repository) payloadBatch(ctx context.Context, tenant uuid.UUID, wanted []uuid.UUID) (map[uuid.UUID][]byte, error) {
	payloads := make(map[uuid.UUID][]byte, len(wanted))
	if len(wanted) == 0 {
		return payloads, nil
	}
	for _, id := range wanted {
		payloads[id] = nil
	}
	rows, err := r.scan(ctx, entityFilter{Tenant: &tenant, Kind: "payload", ExternalIDs: wanted})
	if err != nil {
		return nil, err
	}
	var requests []repository.RetrieveFromExternalOpts
	requestIDs := map[repository.RetrieveFromExternalOpts]uuid.UUID{}
	for _, row := range rows {
		p, err := decodeEntity[storedPayload](row)
		if err != nil {
			return nil, err
		}
		if p.ExternalKey == "" {
			payloads[row.ExternalID] = p.Inline
			continue
		}
		if r.payloadStore == nil || !r.payloadStore.ExternalStoreEnabled() {
			continue
		}
		q := repository.RetrieveFromExternalOpts{Method: repository.RetrieveFromExternalByKey, ByKey: &repository.RetrieveFromExternalByKeyOpt{Key: repository.ExternalPayloadLocationKey(p.ExternalKey)}}
		if p.IndexFile {
			q = repository.RetrieveFromExternalOpts{Method: repository.RetrieveFromExternalByIndexFile, ByIndexFile: &repository.RetrieveFromExternalByIndexFileOpt{IndexFileKey: repository.ExternalIndexFileLocationKey(p.ExternalKey), ExternalId: row.ExternalID}}
		}
		requests = append(requests, q)
		requestIDs[q] = row.ExternalID
	}
	if len(requests) > 0 {
		values, err := r.payloadStore.RetrieveFromExternal(ctx, requests...)
		if err != nil {
			return nil, err
		}
		for q, id := range requestIDs {
			payloads[id] = values[q]
		}
	}
	return payloads, nil
}
