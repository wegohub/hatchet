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
)

func (r *Repository) BulkCreateEventsAndTriggers(ctx context.Context, opts repository.BulkCreateEventsAndTriggersParams, triggers []repository.EventTriggersFromExternalId) error {
	if opts.BulkCreateEventsOLAPParams == nil {
		return fmt.Errorf("event parameters cannot be nil")
	}
	n := len(opts.Externalids)
	if len(opts.Tenantids) != n || len(opts.Seenats) != n || len(opts.Keys) != n || len(opts.Additionalmetadatas) != n || len(opts.Scopes) != n || len(opts.TriggeringWebhookNames) != n || len(opts.Payloads) != n {
		return fmt.Errorf("event columns have unequal lengths")
	}
	locks, _, err := r.store.runLocks(ctx, uuid.Nil, nil)
	if err != nil {
		return err
	}
	defer releaseLocks(locks)
	ctx = withLease(ctx, locks)
	existingByID := map[uuid.UUID]*sqlcv1.V1EventsOlap{}
	grouped := map[uuid.UUID][]uuid.UUID{}
	for i, id := range opts.Externalids {
		grouped[opts.Tenantids[i]] = append(grouped[opts.Tenantids[i]], id)
	}
	for tenant, ids := range grouped {
		found, err := scanValues[sqlcv1.V1EventsOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "event", ExternalIDs: ids, ForUpdate: true})
		if err != nil {
			return err
		}
		for _, event := range found {
			existingByID[event.ExternalID] = event
		}
	}
	allocated, err := r.store.coordinator.nextEventIDs(ctx, n)
	if err != nil {
		return err
	}
	var rows []entity
	events := make(map[uuid.UUID]*sqlcv1.V1EventsOlap)
	for i, id := range opts.Externalids {
		event := &sqlcv1.V1EventsOlap{TenantID: opts.Tenantids[i], ID: allocated[i], ExternalID: id, SeenAt: opts.Seenats[i], Key: opts.Keys[i], AdditionalMetadata: opts.Additionalmetadatas[i], Scope: opts.Scopes[i], TriggeringWebhookName: opts.TriggeringWebhookNames[i], Payload: []byte("{}")}
		if existing := existingByID[id]; existing != nil {
			event = existing
		} else {
			row, err := makeEntity(event.TenantID, "event", id.String(), id, id, event.ID, event.SeenAt.Time, event)
			if err != nil {
				return err
			}
			rows = append(rows, row)
			if len(opts.Payloads[i]) > 0 && string(opts.Payloads[i]) != "{}" {
				if !json.Valid(opts.Payloads[i]) {
					return fmt.Errorf("event payload is not JSON")
				}
				row, err = makeEntity(event.TenantID, "payload", id.String(), id, id, 0, event.SeenAt.Time, storedPayload{Inline: opts.Payloads[i]})
				if err != nil {
					return err
				}
				rows = append(rows, row)
			}
		}
		events[id] = event
	}
	for _, t := range triggers {
		e, ok := events[t.EventExternalId]
		if !ok {
			return fmt.Errorf("event external id %s not found in events", t.EventExternalId)
		}
		key := e.ExternalID.String() + "/" + keyFor(t.RunID, t.RunInsertedAt.Time)
		if t.FilterId != nil {
			key += "/" + t.FilterId.String()
		}
		row, err := makeEntity(e.TenantID, "event_trigger", key, e.ExternalID, uuid.Nil, t.RunID, t.RunInsertedAt.Time, t)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}
	// Retries may contain the same association more than once.
	unique := make(map[string]entity)
	for _, row := range rows {
		unique[row.Tenant.String()+"/"+row.Kind+"/"+row.Key] = row
	}
	rows = rows[:0]
	for _, row := range unique {
		rows = append(rows, row)
	}
	return r.store.publish(ctx, rows, locks...)
}
func (r *Repository) triggeredRunKeys(ctx context.Context, tenant, id uuid.UUID) (map[string]bool, error) {
	rows, err := scanValues[repository.EventTriggersFromExternalId](ctx, r, entityFilter{Tenant: &tenant, Kind: "event_trigger", ExternalIDs: []uuid.UUID{id}})
	result := make(map[string]bool)
	for _, row := range rows {
		result[keyFor(row.RunID, row.RunInsertedAt.Time)] = true
	}
	return result, err
}
func (r *Repository) GetEvent(ctx context.Context, id uuid.UUID) (*sqlcv1.V1EventsOlap, error) {
	rows, err := scanValues[sqlcv1.V1EventsOlap](ctx, r, entityFilter{Kind: "event", ExternalIDs: []uuid.UUID{id}})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, pgx.ErrNoRows
	}
	return rows[0], nil
}

type linkedRun struct {
	Run      *repository.WorkflowRunData
	FilterID *uuid.UUID
}

func (r *Repository) eventData(ctx context.Context, e *sqlcv1.V1EventsOlap, min time.Time, list bool) (*repository.EventWithPayload, []linkedRun, error) {
	cache, ok := ctx.Value(eventHydrationKey{}).(*eventHydration)
	if !ok {
		hydrated, err := r.hydrateEvents(ctx, e.TenantID, []*sqlcv1.V1EventsOlap{e}, min)
		if err != nil {
			return nil, nil, err
		}
		cache = hydrated.Value(eventHydrationKey{}).(*eventHydration)
	}
	triggers := cache.triggers[e.ExternalID]
	byKey := cache.runs
	var err error
	v := &repository.ListEventsRow{TenantID: e.TenantID, EventID: e.ID, EventExternalID: e.ExternalID, EventSeenAt: e.SeenAt, EventKey: e.Key, EventAdditionalMetadata: e.AdditionalMetadata}
	if !list || e.TriggeringWebhookName.Valid {
		v.TriggeringWebhookName = &e.TriggeringWebhookName.String
	}
	if e.Scope.Valid && e.Scope.String != "" {
		v.EventScope = &e.Scope.String
	}
	type association struct {
		RunExternalID uuid.UUID  `json:"run_external_id"`
		FilterID      *uuid.UUID `json:"filter_id"`
	}
	var selected []association
	var linked []linkedRun
	for _, t := range triggers {
		if !t.EventSeenAt.Time.Equal(e.SeenAt.Time) {
			continue
		}
		run := byKey[keyFor(t.RunID, t.RunInsertedAt.Time)]
		if run == nil {
			continue
		}
		selected = append(selected, association{run.ExternalID, t.FilterId})
		linked = append(linked, linkedRun{run, t.FilterId})
		switch run.ReadableStatus {
		case "QUEUED":
			v.QueuedCount++
		case "RUNNING":
			v.RunningCount++
		case "COMPLETED":
			v.CompletedCount++
		case "FAILED":
			v.FailedCount++
		case "CANCELLED":
			v.CancelledCount++
		}
	}
	if len(selected) > 0 {
		v.TriggeredRuns, err = json.Marshal(selected)
		if err != nil {
			return nil, nil, err
		}
	}
	payload := cache.payloads[e.ExternalID]
	if list && payload == nil {
		payload = e.Payload
	}
	v.EventPayload = payload
	return &repository.EventWithPayload{ListEventsRow: v, Payload: payload}, linked, nil
}
func (r *Repository) GetEventWithPayload(ctx context.Context, id, tenant uuid.UUID) (*repository.EventWithPayload, error) {
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, err
	}
	rows, err := scanValues[sqlcv1.V1EventsOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "event", ExternalIDs: []uuid.UUID{id}})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, pgx.ErrNoRows
	}
	result, _, err := r.eventData(ctx, rows[0], rows[0].SeenAt.Time, false)
	return result, err
}
func (r *Repository) ListEventKeys(ctx context.Context, tenant uuid.UUID) ([]string, error) {
	cutoff := time.Now().Add(-24 * time.Hour)
	rows, err := executor(ctx, r.store.db).QueryContext(ctx, "SELECT DISTINCT `key` FROM v1_events_olap WHERE tenant_id=? AND inserted_at>=? ORDER BY BINARY `key`", uuidArg(tenant), dateArg(cutoff))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := []string{}
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func (r *Repository) ListEvents(ctx context.Context, opts sqlcv1.ListEventsParams) ([]*repository.EventWithPayload, *int64, error) {
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, nil, err
	}
	where := []string{"e.seen_at>=?"}
	args := []any{stampArg(opts.Since)}
	if opts.Until.Valid {
		where = append(where, "e.seen_at<=?")
		args = append(args, stampArg(opts.Until))
	}
	for _, filter := range []struct {
		column string
		values []string
	}{{"e.`key`", opts.Keys}, {"e.scope", opts.Scopes}} {
		if filter.values != nil {
			if len(filter.values) == 0 {
				where = append(where, "FALSE")
			} else {
				where = append(where, "BINARY "+filter.column+" IN ("+placeholders(len(filter.values))+")")
				for _, v := range filter.values {
					args = append(args, []byte(v))
				}
			}
		}
	}
	if opts.AdditionalMetadata != nil {
		value, err := parseJSON(opts.AdditionalMetadata)
		if err != nil {
			return nil, nil, err
		}
		expected, ok := value.(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("event metadata filter must be an object")
		}
		q, a, err := metadataPredicate("e", "'event'", expected, repository.AdditionalMetadataOperatorAnd)
		if err != nil {
			return nil, nil, err
		}
		if q != "" {
			where = append(where, q)
			args = append(args, a...)
		}
	}
	relation := "SELECT 1 FROM v1_event_to_run_olap er JOIN v1_runs_olap u ON u.tenant_id=er.tenant_id AND u.id=er.linked_run_id AND u.inserted_at=er.run_inserted_at WHERE er.tenant_id=e.tenant_id AND er.event_external_id=e.external_id AND er.event_seen_at=e.seen_at AND u.inserted_at>=? AND u.is_placeholder=FALSE AND (u.kind='dag' OR u.is_dag_child=FALSE) AND "
	if opts.WorkflowIds != nil {
		if len(opts.WorkflowIds) == 0 {
			where = append(where, "FALSE")
		} else {
			where = append(where, "EXISTS("+relation+"u.workflow_id IN ("+placeholders(len(opts.WorkflowIds))+"))")
			args = append(args, stampArg(opts.Since))
			for _, id := range opts.WorkflowIds {
				args = append(args, uuidArg(id))
			}
		}
	}
	if opts.Statuses != nil {
		if len(opts.Statuses) == 0 {
			where = append(where, "FALSE")
		} else {
			where = append(where, "EXISTS("+relation+"u.readable_status IN ("+placeholders(len(opts.Statuses))+"))")
			args = append(args, stampArg(opts.Since))
			for _, v := range opts.Statuses {
				args = append(args, v)
			}
		}
	}
	limit, offset := int64(50), int64(0)
	if opts.Limit.Valid {
		limit = opts.Limit.Int64
	}
	if opts.Offset.Valid {
		offset = opts.Offset.Int64
	}
	if limit < 0 || offset < 0 {
		return nil, nil, fmt.Errorf("pagination must be non-negative")
	}
	f := entityFilter{Tenant: &opts.Tenantid, Kind: "event", After: &opts.Since.Time, ExternalIDs: opts.EventIds, Where: strings.Join(where, " AND "), Args: args, Order: "e.seen_at DESC,e.id ASC", Limit: &limit, Offset: &offset}
	cutoff := time.Now().UTC().Add(-r.retention).Truncate(24 * time.Hour)
	if f.After.Before(cutoff) {
		f.After = &cutoff
	}
	from, a, err := r.store.fromFilter(f)
	if err != nil {
		return nil, nil, err
	}
	var count int64
	if err = executor(ctx, r.store.db).QueryRowContext(ctx, "SELECT COUNT(*) FROM (SELECT e.id FROM "+from+" LIMIT 20000) included", a...).Scan(&count); err != nil {
		return nil, nil, err
	}
	events, err := scanValues[sqlcv1.V1EventsOlap](ctx, r, f)
	if err != nil {
		return nil, nil, err
	}
	ctx, err = r.hydrateEvents(ctx, opts.Tenantid, events, opts.Since.Time)
	if err != nil {
		return nil, nil, err
	}
	out := make([]*repository.EventWithPayload, 0, len(events))
	for _, e := range events {
		v, _, err := r.eventData(ctx, e, opts.Since.Time, true)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, v)
	}
	return out, &count, nil
}
