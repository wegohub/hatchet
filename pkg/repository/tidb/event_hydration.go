package tidb

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

type eventHydrationKey struct{}
type eventHydration struct {
	triggers map[uuid.UUID][]*repository.EventTriggersFromExternalId
	runs     map[string]*repository.WorkflowRunData
	payloads map[uuid.UUID][]byte
}

func (r *Repository) hydrateEvents(ctx context.Context, tenant uuid.UUID, events []*sqlcv1.V1EventsOlap, min time.Time) (context.Context, error) {
	ids := make([]uuid.UUID, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.ExternalID)
	}
	cache := &eventHydration{triggers: map[uuid.UUID][]*repository.EventTriggersFromExternalId{}, runs: map[string]*repository.WorkflowRunData{}}
	triggers, err := r.scan(ctx, entityFilter{Tenant: &tenant, Kind: "event_trigger", ExternalIDs: ids})
	if err != nil {
		return ctx, err
	}
	var keys []string
	for _, row := range triggers {
		trigger, err := decodeEntity[repository.EventTriggersFromExternalId](row)
		if err != nil {
			return ctx, err
		}
		cache.triggers[row.ExternalID] = append(cache.triggers[row.ExternalID], trigger)
		keys = append(keys, keyFor(trigger.RunID, trigger.RunInsertedAt.Time))
	}
	if len(keys) > 0 {
		for _, kind := range []string{"task", "dag"} {
			rows, err := r.scan(ctx, entityFilter{Tenant: &tenant, Kind: kind, Keys: keys, After: &min})
			if err != nil {
				return ctx, err
			}
			for _, row := range rows {
				if kind == "task" {
					task, err := decodeEntity[sqlcv1.V1TasksOlap](row)
					if err != nil {
						return ctx, err
					}
					if task.DagID.Valid {
						continue
					}
				}
				run, err := decodeEntity[repository.WorkflowRunData](row)
				if err != nil {
					return ctx, err
				}
				cache.runs[row.Key] = run
			}
		}
	}
	cache.payloads, err = r.payloadBatch(ctx, tenant, ids)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, eventHydrationKey{}, cache), nil
}
