package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
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
	lock, err := r.store.keeper.lock(ctx, "events")
	if err != nil {
		return err
	}
	defer lock.release()
	first, err := r.store.keeper.nextLogIDs(ctx, uint64(n))
	if err != nil {
		return err
	}
	var rows []entity
	events := make(map[uuid.UUID]*sqlcv1.V1EventsOlap)
	for i, id := range opts.Externalids {
		event := &sqlcv1.V1EventsOlap{TenantID: opts.Tenantids[i], ID: int64(first) + int64(i), ExternalID: id, SeenAt: opts.Seenats[i], Key: opts.Keys[i], AdditionalMetadata: opts.Additionalmetadatas[i], Scope: opts.Scopes[i], TriggeringWebhookName: opts.TriggeringWebhookNames[i], Payload: []byte("{}")}
		existing, err := scanValues[sqlcv1.V1EventsOlap](ctx, r, entityFilter{Tenant: &event.TenantID, Kind: "event", ExternalIDs: []uuid.UUID{id}})
		if err != nil {
			return err
		}
		if len(existing) > 0 {
			event = existing[0]
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
	return r.store.publish(ctx, rows, lock)
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
	triggers, err := scanValues[repository.EventTriggersFromExternalId](ctx, r, entityFilter{Tenant: &e.TenantID, Kind: "event_trigger", ExternalIDs: []uuid.UUID{e.ExternalID}})
	if err != nil {
		return nil, nil, err
	}
	var keys []string
	for _, t := range triggers {
		if t.EventSeenAt.Time.Equal(e.SeenAt.Time) {
			keys = append(keys, keyFor(t.RunID, t.RunInsertedAt.Time))
		}
	}
	byKey := make(map[string]*repository.WorkflowRunData)
	if len(keys) > 0 {
		for _, kind := range []string{"task", "dag"} {
			rows, err := r.scan(ctx, entityFilter{Tenant: &e.TenantID, Kind: kind, Keys: keys, After: &min})
			if err != nil {
				return nil, nil, err
			}
			for _, row := range rows {
				if kind == "task" {
					task, err := decodeEntity[sqlcv1.V1TasksOlap](row)
					if err != nil {
						return nil, nil, err
					}
					if task.DagID.Valid {
						continue
					}
				}
				run, err := decodeEntity[repository.WorkflowRunData](row)
				if err != nil {
					return nil, nil, err
				}
				byKey[row.Key] = run
			}
		}
	}
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
	payload, err := r.ReadPayload(ctx, e.TenantID, repository.ReadOLAPPayloadOpts{ExternalId: e.ExternalID, InsertedAt: e.SeenAt})
	if err != nil {
		return nil, nil, err
	}
	if list && payload == nil {
		payload = e.Payload
	}
	v.EventPayload = payload
	return &repository.EventWithPayload{ListEventsRow: v, Payload: payload}, linked, nil
}
func (r *Repository) GetEventWithPayload(ctx context.Context, id, tenant uuid.UUID) (*repository.EventWithPayload, error) {
	ctx, err := r.snapshot(ctx)
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
	rows, err := scanValues[sqlcv1.V1EventsOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "event", After: &cutoff})
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	keys := make([]string, 0)
	for _, row := range rows {
		if !seen[row.Key] {
			seen[row.Key] = true
			keys = append(keys, row.Key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}
func (r *Repository) ListEvents(ctx context.Context, opts sqlcv1.ListEventsParams) ([]*repository.EventWithPayload, *int64, error) {
	ctx, err := r.snapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	rows, err := scanValues[sqlcv1.V1EventsOlap](ctx, r, entityFilter{Tenant: &opts.Tenantid, Kind: "event", After: &opts.Since.Time})
	if err != nil {
		return nil, nil, err
	}
	var filtered []*repository.EventWithPayload
	for _, row := range rows {
		if opts.Until.Valid && row.SeenAt.Time.After(opts.Until.Time) || opts.Keys != nil && !slices.Contains(opts.Keys, row.Key) || opts.EventIds != nil && !slices.Contains(opts.EventIds, row.ExternalID) || opts.Scopes != nil && (!row.Scope.Valid || !slices.Contains(opts.Scopes, row.Scope.String)) {
			continue
		}
		if opts.AdditionalMetadata != nil {
			var expected map[string]any
			if err = json.Unmarshal(opts.AdditionalMetadata, &expected); err != nil {
				return nil, nil, err
			}
			if !metadataMatches(row.AdditionalMetadata, expected, repository.AdditionalMetadataOperator("AND")) {
				continue
			}
		}
		v, links, err := r.eventData(ctx, row, opts.Since.Time, true)
		if err != nil {
			return nil, nil, err
		}
		if opts.WorkflowIds != nil || opts.Statuses != nil {

			workflowMatch, statusMatch := opts.WorkflowIds == nil, opts.Statuses == nil
			for _, link := range links {
				run := link.Run
				if run.InsertedAt.Time.Before(opts.Since.Time) {
					continue
				}
				workflowMatch = workflowMatch || slices.Contains(opts.WorkflowIds, run.WorkflowID)
				statusMatch = statusMatch || slices.Contains(opts.Statuses, string(run.ReadableStatus))
			}
			if !workflowMatch || !statusMatch {
				continue
			}
		}
		filtered = append(filtered, v)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].EventSeenAt.Time.Equal(filtered[j].EventSeenAt.Time) {
			return filtered[i].EventID < filtered[j].EventID
		}
		return filtered[i].EventSeenAt.Time.After(filtered[j].EventSeenAt.Time)
	})
	count := int64(len(filtered))
	if count > 20000 {
		count = 20000
	}
	limit, offset := int64(50), int64(0)
	if opts.Limit.Valid {
		limit = opts.Limit.Int64
	}
	if opts.Offset.Valid {
		offset = opts.Offset.Int64
	}
	filtered, err = page(filtered, limit, offset)
	return filtered, &count, err
}
