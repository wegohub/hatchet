package tidb

import (
	"context"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (r *Repository) ReadTaskRun(ctx context.Context, id uuid.UUID) (*sqlcv1.V1TasksOlap, error) {
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, err
	}
	rows, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Kind: "task", ExternalIDs: []uuid.UUID{id}, Where: "EXISTS(SELECT 1 FROM v1_task_attempts_olap a WHERE a.tenant_id=e.tenant_id AND a.task_id=e.id AND a.task_inserted_at=e.inserted_at AND a.retry_count=r.latest_retry_count AND a.readable_status=r.readable_status)"})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, pgx.ErrNoRows
	}
	task := rows[0]
	task.WorkflowVersionID = uuid.Nil
	task.WorkflowRunID = uuid.Nil
	task.IsDurable = false
	task.IdempotencyKey = pgtype.Text{}
	return task, nil
}
func (r *Repository) ReadDAG(ctx context.Context, id uuid.UUID) (*sqlcv1.V1DagsOlap, error) {
	rows, err := scanValues[sqlcv1.V1DagsOlap](ctx, r, entityFilter{Kind: "dag", ExternalIDs: []uuid.UUID{id}})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, pgx.ErrNoRows
	}
	return rows[0], nil
}

func (r *Repository) taskData(ctx context.Context, tenant uuid.UUID, tasks []*sqlcv1.V1TasksOlap, payloads bool, retry *int) ([]*repository.TaskWithPayloads, error) {
	return r.taskProjection(ctx, tenant, tasks, payloads, retry)
}

func maxTimestamp(a, b pgtype.Timestamptz) pgtype.Timestamptz {
	if !a.Valid || b.Valid && b.Time.After(a.Time) {
		return b
	}
	return a
}
func minTimestamp(a, b pgtype.Timestamptz) pgtype.Timestamptz {
	if !a.Valid || b.Valid && b.Time.Before(a.Time) {
		return b
	}
	return a
}
func terminal(s sqlcv1.V1ReadableStatusOlap) bool {
	return s == "COMPLETED" || s == "FAILED" || s == "CANCELLED"
}

func (r *Repository) ReadTaskRunData(ctx context.Context, tenant uuid.UUID, id int64, at pgtype.Timestamptz, retry *int) (*repository.TaskWithPayloads, uuid.UUID, error) {
	return r.readTaskProjection(ctx, tenant, id, at, retry)
}

func (r *Repository) ListTasksByIdAndInsertedAt(ctx context.Context, tenant uuid.UUID, meta []repository.TaskMetadata, include bool) ([]*repository.TaskWithPayloads, error) {
	if len(meta) == 0 {
		return []*repository.TaskWithPayloads{}, nil
	}
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(meta))
	for _, m := range meta {
		keys = append(keys, keyFor(m.TaskID, m.TaskInsertedAt))
	}
	tasks, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "task", Keys: keys})
	if err != nil {
		return nil, err
	}
	return r.taskData(ctx, tenant, tasks, include, nil)
}

func (r *Repository) ListTasksByDAGId(ctx context.Context, tenant uuid.UUID, ids []uuid.UUID, include bool) ([]*repository.TaskWithPayloads, map[int64]uuid.UUID, error) {
	mapping := make(map[int64]uuid.UUID)
	if len(ids) == 0 {
		return []*repository.TaskWithPayloads{}, mapping, nil
	}
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, nil, err
	}
	tasks, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "task", RunIDs: ids})
	if err != nil {
		return nil, nil, err
	}
	filtered := tasks[:0]
	for _, task := range tasks {
		if task.DagID.Valid {
			mapping[task.ID] = task.WorkflowRunID
			filtered = append(filtered, task)
		}
	}
	rows, err := r.taskData(ctx, tenant, filtered, include, nil)
	return rows, mapping, err
}

func (r *Repository) ListTasksByExternalIds(ctx context.Context, tenant uuid.UUID, ids []uuid.UUID) ([]*sqlcv1.FlattenTasksByExternalIdsRow, error) {
	if len(ids) == 0 {
		return []*sqlcv1.FlattenTasksByExternalIdsRow{}, nil
	}
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, err
	}
	direct, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "task", ExternalIDs: ids})
	if err != nil {
		return nil, err
	}
	dags, err := scanValues[sqlcv1.V1DagsOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "dag", ExternalIDs: ids})
	if err != nil {
		return nil, err
	}
	dagIDs := make([]uuid.UUID, 0, len(dags))
	for _, d := range dags {
		dagIDs = append(dagIDs, d.ExternalID)
	}
	if len(dagIDs) > 0 {
		children, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "task", RunIDs: dagIDs})
		if err != nil {
			return nil, err
		}
		direct = append(direct, children...)
	}
	result := make([]*sqlcv1.FlattenTasksByExternalIdsRow, 0, len(direct))
	for _, t := range direct {
		result = append(result, &sqlcv1.FlattenTasksByExternalIdsRow{TenantID: tenant, ID: t.ID, InsertedAt: t.InsertedAt, ExternalID: t.ExternalID, RetryCount: t.LatestRetryCount})
	}
	return result, nil
}

func defaultStatuses(statuses []sqlcv1.V1ReadableStatusOlap) []sqlcv1.V1ReadableStatusOlap {
	if len(statuses) > 0 {
		return statuses
	}
	return []sqlcv1.V1ReadableStatusOlap{"QUEUED", "RUNNING", "COMPLETED", "CANCELLED", "FAILED"}
}
func (r *Repository) ListTasks(ctx context.Context, tenant uuid.UUID, opts repository.ListTaskRunOpts) ([]*repository.TaskWithPayloads, int, error) {
	return r.listTasksSQL(ctx, tenant, opts)
}

func (r *Repository) ListWorkflowRunDisplayNames(ctx context.Context, tenant uuid.UUID, ids []uuid.UUID) ([]*sqlcv1.ListWorkflowRunDisplayNamesRow, error) {
	result := make([]*sqlcv1.ListWorkflowRunDisplayNamesRow, 0)
	if len(ids) == 0 {
		return result, nil
	}
	args := []any{uuidArg(tenant)}
	for _, id := range ids {
		args = append(args, uuidArg(id))
	}
	rows, err := executor(ctx, r.store.db).QueryContext(ctx, "SELECT /*+ READ_FROM_STORAGE(TIKV[u,t,d]) */ u.external_id,COALESCE(t.display_name,d.display_name),u.inserted_at FROM v1_runs_olap u LEFT JOIN v1_tasks_olap t ON u.kind='task' AND t.tenant_id=u.tenant_id AND t.external_id=u.external_id AND t.inserted_at=u.inserted_at LEFT JOIN v1_dags_olap d ON u.kind='dag' AND d.tenant_id=u.tenant_id AND d.external_id=u.external_id AND d.inserted_at=u.inserted_at WHERE u.tenant_id=? AND u.external_id IN ("+placeholders(len(ids))+") AND u.is_placeholder=FALSE LIMIT 10000", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		row := new(sqlcv1.ListWorkflowRunDisplayNamesRow)
		if err = rows.Scan(&row.ExternalID, &row.DisplayName, &row.InsertedAt); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
