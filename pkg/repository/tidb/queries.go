package tidb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

type runKey struct {
	id   uuid.UUID
	kind string
}

func (r *Repository) runWhere(tenant uuid.UUID, opts repository.ListWorkflowRunOpts, taskOnly bool, worker *uuid.UUID) (string, []any, error) {
	cutoff := time.Now().UTC().Add(-r.retention).Truncate(24 * time.Hour)
	after := opts.CreatedAfter
	if after.Before(cutoff) {
		after = cutoff
	}
	parts := []string{"u.tenant_id=?", "u.inserted_at>=?", "u.is_placeholder=FALSE"}
	args := []any{uuidArg(tenant), dateArg(after)}
	if taskOnly {
		parts = append(parts, "u.kind='task'")
	} else {
		parts = append(parts, "(u.kind='dag' OR u.is_dag_child=FALSE)")
	}
	statuses := defaultStatuses(opts.Statuses)
	parts = append(parts, "u.readable_status IN ("+placeholders(len(statuses))+")")
	for _, s := range statuses {
		args = append(args, string(s))
	}
	if opts.FinishedBefore != nil {
		parts = append(parts, "u.inserted_at<=?")
		args = append(args, dateArg(*opts.FinishedBefore))
	}
	if len(opts.WorkflowIds) > 0 {
		parts = append(parts, "u.workflow_id IN ("+placeholders(len(opts.WorkflowIds))+")")
		for _, id := range opts.WorkflowIds {
			args = append(args, uuidArg(id))
		}
	}
	if opts.IdempotencyKeys != nil {
		if len(*opts.IdempotencyKeys) == 0 {
			parts = append(parts, "FALSE")
		} else {
			parts = append(parts, "BINARY u.idempotency_key IN ("+placeholders(len(*opts.IdempotencyKeys))+")")
			for _, v := range *opts.IdempotencyKeys {
				args = append(args, []byte(v))
			}
		}
	}
	if opts.ParentTaskExternalId != nil {
		parts = append(parts, "u.parent_external_id=?")
		args = append(args, uuidArg(*opts.ParentTaskExternalId))
	}
	if worker != nil {
		parts = append(parts, "u.latest_worker_id=?")
		args = append(args, uuidArg(*worker))
	}
	if opts.TriggeringEventExternalId != nil {
		parts = append(parts, "EXISTS(SELECT 1 FROM v1_event_to_run_olap er WHERE er.tenant_id=u.tenant_id AND er.linked_run_id=u.id AND er.run_inserted_at=u.inserted_at AND er.event_external_id=?)")
		args = append(args, uuidArg(*opts.TriggeringEventExternalId))
	}
	q, a, err := metadataPredicate("u", "u.kind", opts.AdditionalMetadata, opts.AdditionalMetadataOperator)
	if err != nil {
		return "", nil, err
	}
	if q != "" {
		parts = append(parts, q)
		args = append(args, a...)
	}
	return strings.Join(parts, " AND "), args, nil
}
func (r *Repository) runPage(ctx context.Context, tenant uuid.UUID, opts repository.ListWorkflowRunOpts, taskOnly bool, worker *uuid.UUID) ([]runKey, int, error) {
	if opts.Limit < 0 || opts.Offset < 0 {
		return nil, 0, fmt.Errorf("LIMIT and OFFSET must not be negative")
	}
	where, args, err := r.runWhere(tenant, opts, taskOnly, worker)
	if err != nil {
		return nil, 0, err
	}
	// List totals are capped; status metrics count the entire matching population.
	count, err := readAnalytics(ctx, r.store, "v1_runs_olap", "SELECT COUNT(*) FROM (SELECT 1 FROM v1_runs_olap u WHERE "+where+" LIMIT 20000) included", args, func(rows *sql.Rows) (int, error) {
		var count int
		if !rows.Next() {
			return 0, rows.Err()
		}
		if err := rows.Scan(&count); err != nil {
			return 0, err
		}
		return count, rows.Err()
	})
	if err != nil {
		return nil, 0, err
	}
	args = append(args, opts.Limit, opts.Offset)
	rows, err := executor(ctx, r.store.db).QueryContext(ctx, "SELECT /*+ READ_FROM_STORAGE(TIKV[u]) */ u.external_id,u.kind FROM v1_runs_olap u WHERE "+where+" ORDER BY u.inserted_at DESC,u.id DESC LIMIT ? OFFSET ?", args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	keys := make([]runKey, 0)
	for rows.Next() {
		var k runKey
		if err = rows.Scan(&k.id, &k.kind); err != nil {
			return nil, 0, err
		}
		keys = append(keys, k)
	}
	return keys, count, rows.Err()
}
func (r *Repository) listTasksSQL(ctx context.Context, tenant uuid.UUID, opts repository.ListTaskRunOpts) ([]*repository.TaskWithPayloads, int, error) {
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, 0, err
	}
	keys, count, err := r.runPage(ctx, tenant, repository.ListWorkflowRunOpts{CreatedAfter: opts.CreatedAfter, FinishedBefore: opts.FinishedBefore, Statuses: opts.Statuses, WorkflowIds: opts.WorkflowIds, AdditionalMetadata: opts.AdditionalMetadata, AdditionalMetadataOperator: opts.AdditionalMetadataOperator, TriggeringEventExternalId: opts.TriggeringEventExternalId, IdempotencyKeys: opts.IdempotencyKeys, Limit: opts.Limit, Offset: opts.Offset}, true, opts.WorkerId)
	if err != nil {
		return nil, 0, err
	}
	if len(keys) == 0 {
		return []*repository.TaskWithPayloads{}, count, nil
	}
	ids := make([]uuid.UUID, 0, len(keys))
	for _, k := range keys {
		ids = append(ids, k.id)
	}
	tasks, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "task", ExternalIDs: ids, After: &opts.CreatedAfter})
	if err != nil {
		return nil, 0, err
	}
	values, err := r.taskData(ctx, tenant, tasks, opts.IncludePayloads, nil)
	if err != nil {
		return nil, 0, err
	}
	byID := map[uuid.UUID]*repository.TaskWithPayloads{}
	for _, v := range values {
		byID[v.ExternalID] = v
	}
	out := make([]*repository.TaskWithPayloads, 0, len(keys))
	for _, k := range keys {
		if v := byID[k.id]; v != nil {
			out = append(out, v)
		}
	}
	return out, count, nil
}
func (r *Repository) listWorkflowRunsSQL(ctx context.Context, tenant uuid.UUID, opts repository.ListWorkflowRunOpts) ([]*repository.WorkflowRunData, int, error) {
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, 0, err
	}
	keys, count, err := r.runPage(ctx, tenant, opts, false, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("select workflow page: %w", err)
	}
	taskIDs, dagIDs := []uuid.UUID{}, []uuid.UUID{}
	for _, k := range keys {
		if k.kind == "task" {
			taskIDs = append(taskIDs, k.id)
		} else {
			dagIDs = append(dagIDs, k.id)
		}
	}
	tasks, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "task", ExternalIDs: taskIDs, After: &opts.CreatedAfter})
	if err != nil {
		return nil, 0, fmt.Errorf("hydrate workflow tasks: %w", err)
	}
	dags, err := scanValues[sqlcv1.V1DagsOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "dag", ExternalIDs: dagIDs, After: &opts.CreatedAfter})
	if err != nil {
		return nil, 0, fmt.Errorf("hydrate workflow DAGs: %w", err)
	}
	ctx, err = r.hydratePage(ctx, tenant, tasks, dags, opts.IncludePayloads)
	if err != nil {
		return nil, 0, fmt.Errorf("hydrate workflow attempts and payloads: %w", err)
	}
	values, err := r.taskData(ctx, tenant, tasks, opts.IncludePayloads, nil)
	if err != nil {
		return nil, 0, err
	}
	byID := map[uuid.UUID]*repository.WorkflowRunData{}
	for _, v := range values {
		byID[v.ExternalID] = taskWorkflow(v)
	}
	for _, d := range dags {
		v, _, err := r.dagWorkflow(ctx, d, opts.IncludePayloads, false)
		if err != nil {
			return nil, 0, err
		}
		byID[d.ExternalID] = v
	}
	out := make([]*repository.WorkflowRunData, 0, len(keys))
	for _, k := range keys {
		if v := byID[k.id]; v != nil {
			out = append(out, v)
		}
	}
	return out, count, nil
}
func (r *Repository) runMetricsSQL(ctx context.Context, tenant uuid.UUID, opts repository.ReadTaskRunMetricsOpts) ([]repository.TaskRunMetric, error) {
	where, args, err := r.runWhere(tenant, repository.ListWorkflowRunOpts{CreatedAfter: opts.CreatedAfter, FinishedBefore: opts.CreatedBefore, Statuses: []sqlcv1.V1ReadableStatusOlap{"QUEUED", "RUNNING", "EVICTED", "COMPLETED", "CANCELLED", "FAILED"}, WorkflowIds: opts.WorkflowIds, ParentTaskExternalId: opts.ParentTaskExternalID, AdditionalMetadata: opts.AdditionalMetadata, AdditionalMetadataOperator: repository.AdditionalMetadataOperatorOr, TriggeringEventExternalId: opts.TriggeringEventExternalId}, false, nil)
	if err != nil {
		return nil, err
	}
	counts, err := readAnalytics(ctx, r.store, "v1_runs_olap", "SELECT u.readable_status,COUNT(*) FROM v1_runs_olap u WHERE "+where+" GROUP BY u.readable_status", args, func(rows *sql.Rows) (map[string]uint64, error) {
		out := map[string]uint64{}
		for rows.Next() {
			var status string
			var count uint64
			if err := rows.Scan(&status, &count); err != nil {
				return nil, err
			}
			out[status] = count
		}
		return out, rows.Err()
	})
	if err != nil {
		return nil, err
	}
	if len(counts) == 0 {
		return []repository.TaskRunMetric{}, nil
	}
	return []repository.TaskRunMetric{{Status: "QUEUED", Count: counts["QUEUED"]}, {Status: "RUNNING", Count: counts["RUNNING"] + counts["EVICTED"], EvictedCount: counts["EVICTED"], OnWorkerCount: counts["RUNNING"]}, {Status: "COMPLETED", Count: counts["COMPLETED"]}, {Status: "CANCELLED", Count: counts["CANCELLED"]}, {Status: "FAILED", Count: counts["FAILED"]}}, nil
}
