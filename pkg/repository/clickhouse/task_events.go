package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

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

func aggregateEvents(events []*taskEvent) []*eventGroup {
	groups := make(map[string]*eventGroup)
	for _, e := range events {
		key := fmt.Sprintf("%s/%d/%s/%d", keyFor(e.TaskID, e.TaskInsertedAt.Time), e.RetryCount, e.EventType, e.DurableInvocationCount)
		g := groups[key]
		if g == nil {
			g = &eventGroup{first: e}
			groups[key] = g
		}
		g.count++
		g.min = minTimestamp(g.min, e.EventTimestamp)
		g.max = maxTimestamp(g.max, e.EventTimestamp)
		if e.ID < g.first.ID {
			g.first = e
		}
	}
	rows := make([]*eventGroup, 0, len(groups))
	for _, g := range groups {
		rows = append(rows, g)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].min.Time.Equal(rows[j].min.Time) {
			return rows[i].first.EventTimestamp.Time.After(rows[j].first.EventTimestamp.Time)
		}
		return rows[i].min.Time.After(rows[j].min.Time)
	})
	return rows
}
func (r *Repository) ListTaskRunEvents(ctx context.Context, tenant uuid.UUID, id int64, at pgtype.Timestamptz, limit, offset *int64) ([]*sqlcv1.ListTaskEventsRow, error) {
	ctx, err := r.snapshot(ctx)
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
	events, err := scanValues[taskEvent](ctx, r, entityFilter{Tenant: &tenant, Kind: "task_event", TaskIDs: []int64{id}})
	if err != nil {
		return nil, err
	}
	filtered := events[:0]
	for _, e := range events {
		if e.TaskInsertedAt.Time.Equal(at.Time) {
			filtered = append(filtered, e)
		}
	}
	groups := aggregateEvents(filtered)
	l, o := int64(0), int64(0)
	if limit != nil {
		l = *limit
	}
	if offset != nil {
		o = *offset
	}
	groups, err = page(groups, l, o)
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
	ctx, err := r.snapshot(ctx)
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
	events, err := scanValues[taskEvent](ctx, r, entityFilter{Tenant: &tenant, Kind: "task_event", RunIDs: []uuid.UUID{id}})
	if err != nil {
		return nil, err
	}
	filtered := events[:0]
	for _, e := range events {
		_, child := byKey[keyFor(e.TaskID, e.TaskInsertedAt.Time)]
		self := e.TaskID == dag.ID && e.TaskInsertedAt.Time.Equal(dag.InsertedAt.Time)
		if child || (self && (include || len(tasks) == 0)) {
			filtered = append(filtered, e)
		}
	}
	groups := aggregateEvents(filtered)
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
		p, err := r.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: e.ExternalID, InsertedAt: e.EventTimestamp})
		if err != nil {
			return nil, err
		}
		if p == nil {
			p = e.Output
		}
		rows = append(rows, &repository.TaskEventWithPayloads{ListTaskEventsForWorkflowRunRow: &row, OutputPayload: p})
	}
	return rows, nil
}
