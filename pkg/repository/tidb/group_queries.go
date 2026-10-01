package tidb

import (
	"context"
	"fmt"
	"github.com/google/uuid"
)

// Only the first event of each group is hydrated after SQL aggregation and paging.
func (r *Repository) eventGroups(ctx context.Context, tenant uuid.UUID, where string, args []any, limit, offset *int64) ([]*eventGroup, error) {
	query := "SELECT g.first_id,g.first_seen,g.last_seen,g.n FROM (SELECT MIN(id) first_id,MIN(event_timestamp) first_seen,MAX(event_timestamp) last_seen,COUNT(*) n,task_id,inserted_at FROM v1_task_events_olap WHERE tenant_id=? AND " + where + " GROUP BY task_id,inserted_at,retry_count,event_type,durable_invocation_count) g JOIN v1_task_events_olap e ON e.tenant_id=? AND e.task_id=g.task_id AND e.inserted_at=g.inserted_at AND e.id=g.first_id ORDER BY g.first_seen DESC,e.event_timestamp DESC"
	values := []any{uuidArg(tenant)}
	values = append(values, args...)
	values = append(values, uuidArg(tenant))
	if limit != nil {
		if *limit < 0 {
			return nil, fmt.Errorf("pagination must be non-negative")
		}
		query += " LIMIT ?"
		values = append(values, *limit)
		if offset != nil {
			if *offset < 0 {
				return nil, fmt.Errorf("pagination must be non-negative")
			}
			query += " OFFSET ?"
			values = append(values, *offset)
		}
	}
	rows, err := executor(ctx, r.store.db).QueryContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}
	type group struct {
		id    int64
		value *eventGroup
	}
	var groups []group
	for rows.Next() {
		g := group{value: new(eventGroup)}
		if err = rows.Scan(&g.id, &g.value.min, &g.value.max, &g.value.count); err != nil {
			rows.Close()
			return nil, err
		}
		groups = append(groups, g)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return []*eventGroup{}, nil
	}
	ids := make([]any, 0, len(groups))
	for _, g := range groups {
		ids = append(ids, g.id)
	}
	events, err := scanValues[taskEvent](ctx, r, entityFilter{Tenant: &tenant, Kind: "task_event", Where: "e.id IN (" + placeholders(len(ids)) + ")", Args: ids})
	if err != nil {
		return nil, err
	}
	byID := map[int64]*taskEvent{}
	for _, e := range events {
		byID[e.ID] = e
	}
	out := make([]*eventGroup, 0, len(groups))
	for _, g := range groups {
		if e := byID[g.id]; e != nil {
			g.value.first = e
			out = append(out, g.value)
		}
	}
	return out, nil
}
