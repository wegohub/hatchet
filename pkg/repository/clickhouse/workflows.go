package clickhouse

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func taskWorkflow(row *repository.TaskWithPayloads) *repository.WorkflowRunData {
	retry := int(row.RetryCount)
	var key *string
	if row.IdempotencyKey.Valid {
		v := row.IdempotencyKey.String
		key = &v
	}
	return &repository.WorkflowRunData{TenantID: row.TenantID, ID: row.ID, ExternalID: row.ExternalID, InsertedAt: row.InsertedAt, CreatedAt: row.InsertedAt, Kind: sqlcv1.V1RunKindTASK, WorkflowID: row.WorkflowID, WorkflowVersionId: row.WorkflowVersionID, ReadableStatus: row.Status, DisplayName: row.DisplayName, AdditionalMetadata: row.AdditionalMetadata, ParentTaskExternalId: row.ParentTaskExternalID, Input: row.InputPayload, Output: row.OutputPayload, StartedAt: row.StartedAt, FinishedAt: row.FinishedAt, ErrorMessage: row.ErrorMessage.String, RetryCount: &retry, StepId: &row.StepID, TaskId: &row.ID, TaskExternalId: &row.ExternalID, TaskInsertedAt: &row.InsertedAt, IdempotencyKey: key}
}

func (r *Repository) dagWorkflow(ctx context.Context, dag *sqlcv1.V1DagsOlap, include, single bool) (*repository.WorkflowRunData, []repository.TaskMetadata, error) {
	row := &repository.WorkflowRunData{TenantID: dag.TenantID, ID: dag.ID, ExternalID: dag.ExternalID, InsertedAt: dag.InsertedAt, CreatedAt: pgtype.Timestamptz{}, Kind: sqlcv1.V1RunKindDAG, WorkflowID: dag.WorkflowID, WorkflowVersionId: dag.WorkflowVersionID, ReadableStatus: dag.ReadableStatus, DisplayName: dag.DisplayName, AdditionalMetadata: dag.AdditionalMetadata, ParentTaskExternalId: dag.ParentTaskExternalID, Input: []byte("{}"), Output: []byte("{}")}
	if dag.IdempotencyKey.Valid {
		v := dag.IdempotencyKey.String
		row.IdempotencyKey = &v
	}
	events, err := scanValues[taskEvent](ctx, r, entityFilter{Tenant: &dag.TenantID, Kind: "task_event", RunIDs: []uuid.UUID{dag.ExternalID}})
	if err != nil {
		return nil, nil, err
	}
	maxRetry := int32(0)
	latest := make(map[string]int32)
	for _, e := range events {
		if e.RetryCount > maxRetry {
			maxRetry = e.RetryCount
		}
		key := keyFor(e.TaskID, e.TaskInsertedAt.Time)
		if e.RetryCount > latest[key] {
			latest[key] = e.RetryCount
		}
	}
	var meta []repository.TaskMetadata
	var output *taskEvent
	var failure *taskEvent
	for _, e := range events {
		if single && e.RetryCount == latest[keyFor(e.TaskID, e.TaskInsertedAt.Time)] || !single && e.RetryCount == maxRetry {
			row.CreatedAt = minTimestamp(row.CreatedAt, e.InsertedAt)
			if e.ReadableStatus == "RUNNING" {
				row.StartedAt = minTimestamp(row.StartedAt, e.InsertedAt)
			}
			if terminal(e.ReadableStatus) {
				row.FinishedAt = maxTimestamp(row.FinishedAt, e.InsertedAt)
			}
			meta = append(meta, repository.TaskMetadata{TaskID: e.TaskID, TaskInsertedAt: e.TaskInsertedAt.Time})
		}
		if e.ReadableStatus == "FAILED" && (failure == nil || e.RetryCount > failure.RetryCount) {
			failure = e
		}
		if e.EventType == "FINISHED" && (output == nil || e.InsertedAt.Time.After(output.InsertedAt.Time)) {
			output = e
		}
	}
	if failure != nil {
		row.ErrorMessage = failure.ErrorMessage.String
	}
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
			p, err = r.ReadPayload(ctx, dag.TenantID, repository.ReadOLAPPayloadOpts{ExternalId: output.ExternalID, InsertedAt: output.InsertedAt})
			if err != nil {
				return nil, nil, err
			}
			row.Output = p
		}
	}
	retry := int(maxRetry)
	row.RetryCount = &retry
	return row, meta, nil
}

func (r *Repository) ReadWorkflowRun(ctx context.Context, id uuid.UUID) (*repository.V1WorkflowRunPopulator, error) {
	ctx, err := r.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	entities, err := r.scan(ctx, entityFilter{Kinds: []string{"task", "dag"}, ExternalIDs: []uuid.UUID{id}})
	if err != nil {
		return nil, err
	}
	var tasks []*sqlcv1.V1TasksOlap
	for _, entity := range entities {
		if entity.Kind == "dag" {
			dag, err := decodeEntity[sqlcv1.V1DagsOlap](entity)
			if err != nil {
				return nil, err
			}
			row, meta, err := r.dagWorkflow(ctx, dag, true, true)
			if row != nil {
				clearReadWorkflowFields(row)
			}
			return &repository.V1WorkflowRunPopulator{WorkflowRun: row, TaskMetadata: meta}, err
		}
		task, err := decodeEntity[sqlcv1.V1TasksOlap](entity)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	if len(tasks) == 0 || tasks[0].DagID.Valid {
		return nil, pgx.ErrNoRows
	}
	events, err := scanValues[taskEvent](ctx, r, entityFilter{Tenant: &tasks[0].TenantID, Kind: "task_event", TaskIDs: []int64{tasks[0].ID}})
	if err != nil {
		return nil, err
	}
	rows, err := r.taskDataWithEvents(ctx, tasks[0].TenantID, tasks, false, nil, events)
	if err != nil {
		return nil, err
	}
	row := taskWorkflow(rows[0])
	clearReadWorkflowFields(row)
	row.ParentTaskExternalId = nil
	row.CreatedAt = pgtype.Timestamptz{}
	row.StartedAt = pgtype.Timestamptz{}
	row.FinishedAt = pgtype.Timestamptz{}
	retry := int32(0)
	for _, e := range events {
		if e.TaskInsertedAt.Time.Equal(tasks[0].InsertedAt.Time) && e.RetryCount > retry {
			retry = e.RetryCount
		}
	}
	var meta []repository.TaskMetadata
	var output *taskEvent
	for _, e := range events {
		if !e.TaskInsertedAt.Time.Equal(tasks[0].InsertedAt.Time) {
			continue
		}
		if e.RetryCount == retry {
			row.CreatedAt = minTimestamp(row.CreatedAt, e.InsertedAt)
			if e.ReadableStatus == "RUNNING" {
				row.StartedAt = minTimestamp(row.StartedAt, e.InsertedAt)
			}
			if terminal(e.ReadableStatus) {
				row.FinishedAt = maxTimestamp(row.FinishedAt, e.InsertedAt)
			}
			meta = append(meta, repository.TaskMetadata{TaskID: e.TaskID, TaskInsertedAt: e.TaskInsertedAt.Time})
		}
		if e.EventType == "FINISHED" && (output == nil || e.InsertedAt.Time.After(output.InsertedAt.Time)) {
			output = e
		}
	}
	row.Input, err = r.ReadPayload(ctx, tasks[0].TenantID, repository.ReadOLAPPayloadOpts{ExternalId: id, InsertedAt: tasks[0].InsertedAt})
	if err != nil {
		return nil, err
	}
	row.Output = nil
	if output != nil {
		row.Output, err = r.ReadPayload(ctx, tasks[0].TenantID, repository.ReadOLAPPayloadOpts{ExternalId: output.ExternalID, InsertedAt: output.InsertedAt})
		if err != nil {
			return nil, err
		}
	}
	return &repository.V1WorkflowRunPopulator{WorkflowRun: row, TaskMetadata: meta}, nil
}

func (r *Repository) ListWorkflowRuns(ctx context.Context, tenant uuid.UUID, opts repository.ListWorkflowRunOpts) ([]*repository.WorkflowRunData, int, error) {
	ctx, err := r.snapshot(ctx)
	if err != nil {
		return nil, 0, err
	}
	type candidate struct {
		task *sqlcv1.V1TasksOlap
		dag  *sqlcv1.V1DagsOlap
		id   int64
		at   time.Time
	}
	var candidates []candidate
	var eventRuns map[string]bool
	if opts.TriggeringEventExternalId != nil {
		eventRuns, err = r.triggeredRunKeys(ctx, tenant, *opts.TriggeringEventExternalId)
		if err != nil {
			return nil, 0, err
		}
	}
	for _, kind := range []string{"task", "dag"} {
		f := runFilter(tenant, kind, opts.CreatedAfter, opts.FinishedBefore, opts.Statuses, opts.WorkflowIds, opts.IdempotencyKeys, opts.ParentTaskExternalId)
		if kind == "task" {
			f.Predicate += " AND JSONExtractRaw(body,'dag_id') = 'null'"
		}
		rows, err := r.scan(ctx, f)
		if err != nil {
			return nil, 0, err
		}
		for _, row := range rows {
			if eventRuns != nil && !eventRuns[keyFor(row.TaskID, row.InsertedAt)] {
				continue
			}
			if kind == "task" {
				t, err := decodeEntity[sqlcv1.V1TasksOlap](row)
				if err != nil {
					return nil, 0, err
				}
				if t.DagID.Valid || !metadataMatches(t.AdditionalMetadata, opts.AdditionalMetadata, opts.AdditionalMetadataOperator) {
					continue
				}
				candidates = append(candidates, candidate{task: t, id: t.ID, at: t.InsertedAt.Time})
			} else {
				d, err := decodeEntity[sqlcv1.V1DagsOlap](row)
				if err != nil {
					return nil, 0, err
				}
				if !metadataMatches(d.AdditionalMetadata, opts.AdditionalMetadata, opts.AdditionalMetadataOperator) {
					continue
				}
				candidates = append(candidates, candidate{dag: d, id: d.ID, at: d.InsertedAt.Time})
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].at.Equal(candidates[j].at) {
			return candidates[i].id > candidates[j].id
		}
		return candidates[i].at.After(candidates[j].at)
	})
	count := len(candidates)
	candidates, err = page(candidates, opts.Limit, opts.Offset)
	if err != nil {
		return nil, 0, err
	}
	result := make([]*repository.WorkflowRunData, 0, len(candidates))
	for _, c := range candidates {
		if c.dag != nil {
			row, _, err := r.dagWorkflow(ctx, c.dag, opts.IncludePayloads, false)
			if err != nil {
				return nil, 0, err
			}
			result = append(result, row)
		} else {
			rows, err := r.taskData(ctx, tenant, []*sqlcv1.V1TasksOlap{c.task}, opts.IncludePayloads, nil)
			if err != nil {
				return nil, 0, err
			}
			result = append(result, taskWorkflow(rows[0]))
		}
	}
	return result, count, nil
}

func (r *Repository) ListWorkflowRunExternalIds(ctx context.Context, tenant uuid.UUID, opts repository.ListWorkflowRunOpts) ([]uuid.UUID, error) {
	rows, _, err := r.ListWorkflowRuns(ctx, tenant, opts)
	if err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ExternalID)
	}
	return ids, nil
}

func (r *Repository) ReadTaskRunMetrics(ctx context.Context, tenant uuid.UUID, opts repository.ReadTaskRunMetricsOpts) ([]repository.TaskRunMetric, error) {
	rows, _, err := r.ListWorkflowRuns(ctx, tenant, repository.ListWorkflowRunOpts{CreatedAfter: opts.CreatedAfter, FinishedBefore: opts.CreatedBefore, WorkflowIds: opts.WorkflowIds, ParentTaskExternalId: opts.ParentTaskExternalID, TriggeringEventExternalId: opts.TriggeringEventExternalId, AdditionalMetadata: opts.AdditionalMetadata, Statuses: []sqlcv1.V1ReadableStatusOlap{"QUEUED", "RUNNING", "EVICTED", "COMPLETED", "CANCELLED", "FAILED"}, Limit: math.MaxInt32})
	if err != nil {
		return nil, err
	}
	counts := make(map[string]uint64)
	for _, row := range rows {
		counts[string(row.ReadableStatus)]++
	}
	return []repository.TaskRunMetric{{Status: "QUEUED", Count: counts["QUEUED"]}, {Status: "RUNNING", Count: counts["RUNNING"] + counts["EVICTED"], EvictedCount: counts["EVICTED"], OnWorkerCount: counts["RUNNING"]}, {Status: "COMPLETED", Count: counts["COMPLETED"]}, {Status: "CANCELLED", Count: counts["CANCELLED"]}, {Status: "FAILED", Count: counts["FAILED"]}}, nil
}

func (r *Repository) GetTaskTimings(ctx context.Context, tenant, run uuid.UUID, depth int32) ([]*sqlcv1.PopulateTaskRunDataRow, map[uuid.UUID]int32, error) {
	if depth > 10 {
		return nil, nil, fmt.Errorf("depth too large")
	}
	ctx, err := r.snapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	roots, err := r.ListTasksByExternalIds(ctx, tenant, []uuid.UUID{run})
	if err != nil {
		return nil, nil, err
	}
	meta := make([]repository.TaskMetadata, 0, len(roots))
	levels := make(map[uuid.UUID]int32)
	parents := make([]string, 0, len(roots))
	min := time.Now().UTC()
	for _, t := range roots {
		meta = append(meta, repository.TaskMetadata{TaskID: t.ID, TaskInsertedAt: t.InsertedAt.Time})
		levels[t.ExternalID] = 0
		parents = append(parents, t.ExternalID.String())
		if t.InsertedAt.Time.Before(min) {
			min = t.InsertedAt.Time
		}
	}
	sevenDays := time.Now().Add(-7 * 24 * time.Hour)
	if min.Before(sevenDays) {
		min = sevenDays
	}
	for d := int32(1); d <= depth && len(parents) > 0; d++ {
		var next []string
		for _, kind := range []string{"task", "dag"} {
			rows, err := r.scan(ctx, entityFilter{Tenant: &tenant, Kind: kind, After: &min, Predicate: "JSONExtractString(body,'parent_task_external_id') IN (?)", Arguments: []any{parents}})
			if err != nil {
				return nil, nil, err
			}
			for _, row := range rows {
				if kind == "task" {
					t, err := decodeEntity[sqlcv1.V1TasksOlap](row)
					if err != nil {
						return nil, nil, err
					}
					if t.DagID.Valid {
						continue
					}
					if _, ok := levels[t.ExternalID]; ok {
						continue
					}
					meta = append(meta, repository.TaskMetadata{TaskID: t.ID, TaskInsertedAt: t.InsertedAt.Time})
					levels[t.ExternalID] = d
					next = append(next, t.ExternalID.String())
				} else {
					children, err := r.ListTasksByExternalIds(ctx, tenant, []uuid.UUID{row.ExternalID})
					if err != nil {
						return nil, nil, err
					}
					for _, t := range children {
						if _, ok := levels[t.ExternalID]; ok {
							continue
						}
						meta = append(meta, repository.TaskMetadata{TaskID: t.ID, TaskInsertedAt: t.InsertedAt.Time})
						levels[t.ExternalID] = d
						next = append(next, t.ExternalID.String())
					}
				}
			}
		}
		parents = next
	}
	rows, err := r.ListTasksByIdAndInsertedAt(ctx, tenant, meta, false)
	if err != nil {
		return nil, nil, err
	}
	result := make([]*sqlcv1.PopulateTaskRunDataRow, 0, len(rows))
	for _, row := range rows {
		result = append(result, row.PopulateTaskRunDataRow)
	}
	return result, levels, nil
}

func (r *Repository) GetDAGDurations(ctx context.Context, tenant uuid.UUID, ids []uuid.UUID, min pgtype.Timestamptz) (map[string]*sqlcv1.GetDagDurationsRow, error) {
	dags, err := scanValues[sqlcv1.V1DagsOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "dag", ExternalIDs: ids, After: &min.Time})
	if err != nil {
		return nil, err
	}
	result := make(map[string]*sqlcv1.GetDagDurationsRow)
	for _, dag := range dags {
		events, err := scanValues[taskEvent](ctx, r, entityFilter{Tenant: &tenant, Kind: "task_event", RunIDs: []uuid.UUID{dag.ExternalID}})
		row := &repository.WorkflowRunData{}
		for _, e := range events {
			if e.ReadableStatus == "RUNNING" {
				row.StartedAt = minTimestamp(row.StartedAt, e.EventTimestamp)
			}
			if terminal(e.ReadableStatus) {
				row.FinishedAt = maxTimestamp(row.FinishedAt, e.EventTimestamp)
			}
		}
		if err != nil {
			return nil, err
		}
		result[dag.ExternalID.String()] = &sqlcv1.GetDagDurationsRow{ExternalID: dag.ExternalID, StartedAt: row.StartedAt, FinishedAt: row.FinishedAt}
	}
	return result, nil
}

func (r *Repository) GetTaskDurationsByTaskIds(ctx context.Context, tenant uuid.UUID, ids []int64, ats []pgtype.Timestamptz, statuses []sqlcv1.V1ReadableStatusOlap) (map[int64]*sqlcv1.GetTaskDurationsByTaskIdsRow, error) {
	if len(ids) != len(ats) || len(ids) != len(statuses) {
		return nil, fmt.Errorf("task IDs, timestamps and statuses must have equal length")
	}
	ctx, err := r.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	tasks, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "task", TaskIDs: ids})
	if err != nil {
		return nil, err
	}
	events, err := scanValues[taskEvent](ctx, r, entityFilter{Tenant: &tenant, Kind: "task_event", TaskIDs: ids})
	if err != nil {
		return nil, err
	}
	result := make(map[int64]*sqlcv1.GetTaskDurationsByTaskIdsRow)
	for i, id := range ids {
		for _, task := range tasks {
			if task.ID != id || !task.InsertedAt.Time.Equal(ats[i].Time) || task.ReadableStatus != statuses[i] {
				continue
			}
			row := &sqlcv1.GetTaskDurationsByTaskIdsRow{}
			for _, e := range events {
				if e.TaskID != id || !e.TaskInsertedAt.Time.Equal(ats[i].Time) || e.RetryCount != task.LatestRetryCount {
					continue
				}
				if e.EventType == "STARTED" {
					row.StartedAt = minTimestamp(row.StartedAt, e.EventTimestamp)
				}
				if terminal(e.ReadableStatus) {
					row.FinishedAt = maxTimestamp(row.FinishedAt, e.EventTimestamp)
				}
			}
			result[id] = row
		}
	}
	return result, nil
}

func (r *Repository) GetTaskStartedTimestamps(ctx context.Context, tenant uuid.UUID, ids []int64, ats []time.Time, retries []int32) ([]*sqlcv1.GetTaskStartedTimestampsRow, error) {
	if len(ids) != len(ats) || len(ids) != len(retries) {
		return nil, fmt.Errorf("task metadata arrays must have equal length")
	}
	events, err := scanValues[taskEvent](ctx, r, entityFilter{Tenant: &tenant, Kind: "task_event", TaskIDs: ids})
	if err != nil {
		return nil, err
	}
	var result []*sqlcv1.GetTaskStartedTimestampsRow
	for i, id := range ids {
		var start pgtype.Timestamptz
		for _, e := range events {
			if e.TaskID == id && e.TaskInsertedAt.Time.Equal(ats[i]) && e.RetryCount == retries[i] && e.EventType == "STARTED" {
				start = minTimestamp(start, e.EventTimestamp)
			}
		}
		if start.Valid {
			result = append(result, &sqlcv1.GetTaskStartedTimestampsRow{TaskID: id, TaskInsertedAt: pgtype.Timestamptz{Time: ats[i], Valid: true}, RetryCount: retries[i], StartedAt: start})
		}
	}
	return result, nil
}

func clearReadWorkflowFields(row *repository.WorkflowRunData) {
	row.ID = 0
	row.RetryCount = nil
	row.IdempotencyKey = nil
	row.StepId = nil
	row.TaskId = nil
	row.TaskExternalId = nil
	row.TaskInsertedAt = nil
}
