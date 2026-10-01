package tidb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
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
	return r.dagProjection(ctx, dag, include, single)
}

func (r *Repository) ReadWorkflowRun(ctx context.Context, id uuid.UUID) (*repository.V1WorkflowRunPopulator, error) {
	return r.readWorkflowProjection(ctx, id)
}

func (r *Repository) ListWorkflowRuns(ctx context.Context, tenant uuid.UUID, opts repository.ListWorkflowRunOpts) ([]*repository.WorkflowRunData, int, error) {
	return r.listWorkflowRunsSQL(ctx, tenant, opts)
}

func (r *Repository) ListWorkflowRunExternalIds(ctx context.Context, tenant uuid.UUID, opts repository.ListWorkflowRunOpts) ([]uuid.UUID, error) {
	base := repository.ListWorkflowRunOpts{CreatedAfter: opts.CreatedAfter, FinishedBefore: opts.FinishedBefore, Statuses: opts.Statuses, WorkflowIds: opts.WorkflowIds, AdditionalMetadata: opts.AdditionalMetadata, AdditionalMetadataOperator: repository.AdditionalMetadataOperatorOr}
	where, args, err := r.runWhere(tenant, base, false, nil)
	if err != nil {
		return nil, err
	}
	where = strings.Replace(where, "u.inserted_at>=?", "u.inserted_at>?", 1)
	rows, err := executor(ctx, r.store.db).QueryContext(ctx, "SELECT /*+ READ_FROM_STORAGE(TIKV[u]) */ u.external_id FROM v1_runs_olap u WHERE "+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *Repository) ReadTaskRunMetrics(ctx context.Context, tenant uuid.UUID, opts repository.ReadTaskRunMetricsOpts) ([]repository.TaskRunMetric, error) {
	return r.runMetricsSQL(ctx, tenant, opts)
}

func (r *Repository) GetTaskTimings(ctx context.Context, tenant, run uuid.UUID, depth int32) ([]*sqlcv1.PopulateTaskRunDataRow, map[uuid.UUID]int32, error) {
	if depth > 10 {
		return nil, nil, fmt.Errorf("depth too large")
	}
	ctx, release, err := r.snapshot(ctx)
	defer release()
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
		boundParents := make([]any, 0, len(parents))
		for _, parent := range parents {
			id, err := uuid.Parse(parent)
			if err != nil {
				return nil, nil, err
			}
			boundParents = append(boundParents, uuidArg(id))
		}
		for _, kind := range []string{"task", "dag"} {
			rows, err := r.scan(ctx, entityFilter{Tenant: &tenant, Kind: kind, After: &min, Where: "e.parent_task_external_id IN (" + placeholders(len(parents)) + ")", Args: boundParents})
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
	result := map[string]*sqlcv1.GetDagDurationsRow{}
	if len(ids) == 0 {
		return result, nil
	}
	args := []any{uuidArg(tenant), stampArg(min)}
	for _, id := range ids {
		args = append(args, uuidArg(id))
	}
	query := "SELECT d.external_id,MIN(IF(e.readable_status='RUNNING',e.event_timestamp,NULL)),MAX(IF(e.readable_status IN('COMPLETED','FAILED','CANCELLED'),e.event_timestamp,NULL)) FROM v1_dags_olap d JOIN v1_tasks_olap t ON t.tenant_id=d.tenant_id AND t.dag_id=d.id AND t.dag_inserted_at=d.inserted_at JOIN v1_task_events_olap e ON e.tenant_id=t.tenant_id AND e.task_id=t.id AND e.inserted_at=t.inserted_at WHERE d.tenant_id=? AND d.inserted_at>=? AND d.external_id IN (" + placeholders(len(ids)) + ") GROUP BY d.external_id"
	rows, err := executor(ctx, r.store.db).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		v := new(sqlcv1.GetDagDurationsRow)
		if err = rows.Scan(&v.ExternalID, &v.StartedAt, &v.FinishedAt); err != nil {
			return nil, err
		}
		result[v.ExternalID.String()] = v
	}
	return result, rows.Err()
}

func (r *Repository) GetTaskDurationsByTaskIds(ctx context.Context, tenant uuid.UUID, ids []int64, ats []pgtype.Timestamptz, statuses []sqlcv1.V1ReadableStatusOlap) (map[int64]*sqlcv1.GetTaskDurationsByTaskIdsRow, error) {
	if len(ids) != len(ats) || len(ids) != len(statuses) {
		return nil, fmt.Errorf("task metadata arrays must have equal length")
	}
	result := map[int64]*sqlcv1.GetTaskDurationsByTaskIdsRow{}
	if len(ids) == 0 {
		return result, nil
	}
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, err
	}
	tasks, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "task", TaskIDs: ids})
	if err != nil {
		return nil, err
	}
	attempts, err := r.readAttempts(ctx, tenant, tasks, nil)
	if err != nil {
		return nil, err
	}
	for i, id := range ids {
		for _, t := range tasks {
			if t.ID != id || !t.InsertedAt.Time.Equal(ats[i].Time) || t.ReadableStatus != statuses[i] {
				continue
			}
			row := new(sqlcv1.GetTaskDurationsByTaskIdsRow)
			for _, a := range attempts {
				if a.TaskID == id && a.TaskAt.Time.Equal(ats[i].Time) && a.Retry == t.LatestRetryCount {
					row.StartedAt = a.StartedFirst
					row.FinishedAt = a.FinishedLast
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
	if len(ids) == 0 {
		return []*sqlcv1.GetTaskStartedTimestampsRow{}, nil
	}
	var tuples []string
	args := []any{uuidArg(tenant)}
	for i, id := range ids {
		tuples = append(tuples, "(?,?,?)")
		args = append(args, id, dateArg(ats[i]), retries[i])
	}
	rows, err := executor(ctx, r.store.db).QueryContext(ctx, "SELECT task_id,task_inserted_at,retry_count,started_first FROM v1_task_attempts_olap WHERE tenant_id=? AND (task_id,task_inserted_at,retry_count) IN ("+strings.Join(tuples, ",")+") AND started_first IS NOT NULL", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*sqlcv1.GetTaskStartedTimestampsRow{}
	for rows.Next() {
		row := new(sqlcv1.GetTaskStartedTimestampsRow)
		if err = rows.Scan(&row.TaskID, &row.TaskInsertedAt, &row.RetryCount, &row.StartedAt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
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
