package tidb

import (
	"context"
	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func (w *stateWriter) load(ctx context.Context, tenant uuid.UUID, runs []uuid.UUID) (*runState, error) {
	seq, err := w.store.barrier(ctx)
	if err != nil {
		return nil, err
	}
	state := &runState{tasks: make(map[string]*sqlcv1.V1TasksOlap), dags: make(map[string]*sqlcv1.V1DagsOlap), events: make(map[string]*taskEvent), payloads: make(map[string]*storedPayload), changes: make(map[string]entity)}
	if len(runs) == 0 {
		return state, nil
	}
	rows, err := w.store.read(ctx, seq, entityFilter{Tenant: &tenant, Kinds: []string{"task", "dag", "task_event", "payload"}, RunIDs: runs})
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		switch row.Kind {
		case "task":
			v, err := decodeEntity[sqlcv1.V1TasksOlap](row)
			if err != nil {
				return nil, err
			}
			state.tasks[row.Key] = v
		case "dag":
			v, err := decodeEntity[sqlcv1.V1DagsOlap](row)
			if err != nil {
				return nil, err
			}
			state.dags[row.Key] = v
		case "task_event":
			v, err := decodeEntity[taskEvent](row)
			if err != nil {
				return nil, err
			}
			state.events[row.Key] = v
		case "payload":
			v, err := decodeEntity[storedPayload](row)
			if err != nil {
				return nil, err
			}
			state.payloads[row.Key] = v
		}
	}
	return state, nil
}
