package tidb

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
)

type eventGroup struct {
	first    *taskEvent
	min, max pgtype.Timestamptz
	count    int64
}

func (r *Repository) ListTaskRunEvents(ctx context.Context, tenant uuid.UUID, id int64, at pgtype.Timestamptz, limit, offset *int64) ([]*sqlcv1.ListTaskEventsRow, error) {
	if limit == nil {
		zero := int64(0)
		limit = &zero
	}
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, err
	}
	tasks, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "task", Keys: []string{keyFor(id, at.Time)}})
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return []*sqlcv1.ListTaskEventsRow{}, nil
	}
	groups, err := r.eventGroups(ctx, tenant, "task_id=? AND inserted_at=?", []any{id, stampArg(at)}, limit, offset)
	if err != nil {
		return nil, err
	}

	rows := make([]*sqlcv1.ListTaskEventsRow, 0, len(groups))
	for _, g := range groups {
		e := g.first
		encoded, _ := json.Marshal(e)
		var row sqlcv1.ListTaskEventsRow
		if err = json.Unmarshal(encoded, &row); err != nil {
			return nil, err
		}
		row.ID = e.ID
		row.EventExternalID = e.ExternalID
		row.TimeFirstSeen = g.min.Time
		row.TimeLastSeen = g.max.Time
		row.Count = g.count
		row.TaskDisplayName = tasks[0].DisplayName
		rows = append(rows, &row)
	}
	return rows, nil
}
func (r *Repository) ListTaskRunEventsByWorkflowRunId(ctx context.Context, tenant, id uuid.UUID, include bool) ([]*repository.TaskEventWithPayloads, error) {
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, err
	}
	dags, err := scanValues[sqlcv1.V1DagsOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "dag", ExternalIDs: []uuid.UUID{id}})
	if err != nil {
		return nil, err
	}
	if len(dags) == 0 {
		return []*repository.TaskEventWithPayloads{}, nil
	}
	dag := dags[0]
	tasks, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "task", RunIDs: []uuid.UUID{id}})
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]*sqlcv1.V1TasksOlap)
	for _, t := range tasks {
		byKey[keyFor(t.ID, t.InsertedAt.Time)] = t
	}
	where := "run_id=? AND (EXISTS(SELECT 1 FROM v1_tasks_olap t WHERE t.tenant_id=v1_task_events_olap.tenant_id AND t.id=v1_task_events_olap.task_id AND t.inserted_at=v1_task_events_olap.inserted_at AND t.run_id=?) OR (task_id=? AND inserted_at=?))"
	args := []any{uuidArg(id), uuidArg(id), dag.ID, stampArg(dag.InsertedAt)}
	if !include && len(tasks) > 0 {
		where += " AND NOT(task_id=? AND inserted_at=?)"
		args = append(args, dag.ID, stampArg(dag.InsertedAt))
	}
	groups, err := r.eventGroups(ctx, tenant, where, args, nil, nil)
	if err != nil {
		return nil, err
	}
	wanted := make([]uuid.UUID, 0, len(groups))
	for _, group := range groups {
		wanted = append(wanted, group.first.ExternalID)
	}
	payloads, err := r.payloadBatch(ctx, tenant, wanted)
	if err != nil {
		return nil, err
	}

	rows := make([]*repository.TaskEventWithPayloads, 0, len(groups))
	for _, g := range groups {
		e := g.first
		encoded, _ := json.Marshal(e)
		var row sqlcv1.ListTaskEventsForWorkflowRunRow
		if err = json.Unmarshal(encoded, &row); err != nil {
			return nil, err
		}
		row.ID = e.ID
		row.EventExternalID = e.ExternalID
		row.TimeFirstSeen = g.min
		row.TimeLastSeen = g.max
		row.Count = g.count
		row.DisplayName = dag.DisplayName
		row.TaskExternalID = dag.ExternalID
		if t := byKey[keyFor(e.TaskID, e.TaskInsertedAt.Time)]; t != nil {
			row.DisplayName = t.DisplayName
			row.TaskExternalID = t.ExternalID
		}
		p := payloads[e.ExternalID]
		if p == nil {
			p = e.Output
		}
		rows = append(rows, &repository.TaskEventWithPayloads{ListTaskEventsForWorkflowRunRow: &row, OutputPayload: p})
	}
	return rows, nil
}
