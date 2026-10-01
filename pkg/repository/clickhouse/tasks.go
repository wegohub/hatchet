package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (r *Repository) ReadTaskRun(ctx context.Context, id uuid.UUID) (*sqlcv1.V1TasksOlap, error) {
	ctx, err := r.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Kind: "task", ExternalIDs: []uuid.UUID{id}})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, pgx.ErrNoRows
	}
	task := rows[0]
	events, err := scanValues[taskEvent](ctx, r, entityFilter{Tenant: &task.TenantID, Kind: "task_event", TaskIDs: []int64{task.ID}})
	if err != nil {
		return nil, err
	}
	found := false
	for _, e := range events {
		if e.ReadableStatus == task.ReadableStatus && e.RetryCount == task.LatestRetryCount {
			found = true
			break
		}
	}
	if !found {
		return nil, pgx.ErrNoRows
	}
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
	if len(tasks) == 0 {
		return []*repository.TaskWithPayloads{}, nil
	}
	ids := make([]int64, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.ID)
	}
	events, err := scanValues[taskEvent](ctx, r, entityFilter{Tenant: &tenant, Kind: "task_event", TaskIDs: ids})
	if err != nil {
		return nil, err
	}
	return r.taskDataWithEvents(ctx, tenant, tasks, payloads, retry, events)
}

func (r *Repository) taskDataWithEvents(ctx context.Context, tenant uuid.UUID, tasks []*sqlcv1.V1TasksOlap, payloads bool, retry *int, events []*taskEvent) ([]*repository.TaskWithPayloads, error) {
	var err error
	byTask := make(map[string][]*taskEvent)
	for _, event := range events {
		key := keyFor(event.TaskID, event.TaskInsertedAt.Time)
		byTask[key] = append(byTask[key], event)
	}
	var result []*repository.TaskWithPayloads
	for _, task := range tasks {
		encoded, _ := json.Marshal(task)
		row := new(sqlcv1.PopulateTaskRunDataRow)
		if err = json.Unmarshal(encoded, row); err != nil {
			return nil, err
		}
		row.Status = task.ReadableStatus
		row.IsStandalone = !task.DagID.Valid
		row.RetryCount = task.LatestRetryCount
		row.Input = []byte("{}")
		row.Output = []byte("{}")
		history := byTask[keyFor(task.ID, task.InsertedAt.Time)]
		latest := int32(0)
		for _, e := range history {
			if e.RetryCount > latest {
				latest = e.RetryCount
			}
		}
		if retry != nil {
			latest = int32(*retry)
			row.RetryCount = latest
		}
		var output *taskEvent
		for _, e := range history {
			if e.EventType == sqlcv1.V1EventTypeOlapFINISHED && e.ReadableStatus == sqlcv1.V1ReadableStatusOlapCOMPLETED && (output == nil || e.EventTimestamp.Time.After(output.EventTimestamp.Time)) {
				output = e
			}
			if e.RetryCount != latest {
				continue
			}
			if e.EventType == sqlcv1.V1EventTypeOlapQUEUED {
				row.QueuedAt = maxTimestamp(row.QueuedAt, e.EventTimestamp)
			}
			if e.EventType == sqlcv1.V1EventTypeOlapSTARTED {
				row.StartedAt = maxTimestamp(row.StartedAt, e.EventTimestamp)
			}
			if terminal(e.ReadableStatus) {
				row.FinishedAt = maxTimestamp(row.FinishedAt, e.EventTimestamp)
			}
			if e.ReadableStatus == sqlcv1.V1ReadableStatusOlapFAILED {
				row.ErrorMessage = e.ErrorMessage
			}
		}
		if output != nil {
			id := output.ExternalID
			row.OutputEventExternalID = &id
			row.OutputEventInsertedAt = output.InsertedAt
			if payloads {
				row.Output = output.Output
			}
		}
		res := &repository.TaskWithPayloads{PopulateTaskRunDataRow: row, InputPayload: row.Input, OutputPayload: row.Output}
		if payloads {
			p, err := r.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: task.ExternalID, InsertedAt: task.InsertedAt})
			if err != nil {
				return nil, err
			}
			if p != nil {
				res.InputPayload = p
			}
			if output != nil {
				p, err = r.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: output.ExternalID, InsertedAt: output.InsertedAt})
				if err != nil {
					return nil, err
				}
				if p != nil {
					res.OutputPayload = p
				}
			}
		}
		result = append(result, res)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].InsertedAt.Time.Equal(result[j].InsertedAt.Time) {
			return result[i].ID < result[j].ID
		}
		return result[i].InsertedAt.Time.After(result[j].InsertedAt.Time)
	})
	return result, nil
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
	ctx, err := r.snapshot(ctx)
	if err != nil {
		return nil, uuid.Nil, err
	}
	tasks, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, entityFilter{Tenant: &tenant, Kind: "task", Keys: []string{keyFor(id, at.Time)}})
	if err != nil {
		return nil, uuid.Nil, err
	}
	if len(tasks) == 0 {
		return nil, uuid.Nil, pgx.ErrNoRows
	}
	history, err := scanValues[taskEvent](ctx, r, entityFilter{Tenant: &tenant, Kind: "task_event", TaskIDs: []int64{id}})
	if err != nil {
		return nil, uuid.Nil, err
	}
	rows, err := r.taskDataWithEvents(ctx, tenant, tasks, false, retry, history)
	if err != nil {
		return nil, uuid.Nil, err
	}
	row := rows[0]
	selected := int32(0)
	for _, e := range history {
		if e.TaskInsertedAt.Time.Equal(at.Time) && e.RetryCount > selected {
			selected = e.RetryCount
		}
	}
	if retry != nil {
		selected = int32(*retry)
	}
	row.RetryCount = selected
	row.Status = ""
	row.Output = nil
	row.OutputPayload = nil
	row.OutputEventExternalID = nil
	row.OutputEventInsertedAt = pgtype.Timestamptz{}
	row.ErrorMessage = pgtype.Text{}
	var statusEvent, outputEvent, errorEvent *taskEvent
	for _, e := range history {
		if !e.TaskInsertedAt.Time.Equal(at.Time) || e.RetryCount != selected {
			continue
		}
		if statusEvent == nil || terminal(e.ReadableStatus) && !terminal(statusEvent.ReadableStatus) || terminal(e.ReadableStatus) == terminal(statusEvent.ReadableStatus) && e.EventTimestamp.Time.After(statusEvent.EventTimestamp.Time) {
			statusEvent = e
		}
		if e.EventType == "FINISHED" && (outputEvent == nil || e.ID < outputEvent.ID) {
			outputEvent = e
		}
		if e.ReadableStatus == "FAILED" && (errorEvent == nil || e.EventTimestamp.Time.After(errorEvent.EventTimestamp.Time)) {
			errorEvent = e
		}
	}
	if statusEvent == nil {
		return nil, uuid.Nil, fmt.Errorf("task retry has no status events")
	}
	row.Status = statusEvent.ReadableStatus
	if errorEvent != nil {
		row.ErrorMessage = errorEvent.ErrorMessage
	}
	if outputEvent != nil {
		eid := outputEvent.ExternalID
		row.OutputEventExternalID = &eid
		row.OutputEventInsertedAt = outputEvent.InsertedAt
		row.Output = outputEvent.Output
		row.OutputPayload, err = r.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: eid, InsertedAt: outputEvent.InsertedAt})
		if err != nil {
			return nil, uuid.Nil, err
		}
		if row.OutputPayload == nil {
			row.OutputPayload = row.Output
		}
	}
	run := tasks[0].ExternalID
	if tasks[0].DagID.Valid {
		run = tasks[0].WorkflowRunID
		row.InputPayload, err = r.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: run, InsertedAt: tasks[0].DagInsertedAt})
		if err != nil {
			return nil, uuid.Nil, err
		}
	}
	if !tasks[0].DagID.Valid {
		p, err := r.ReadPayload(ctx, tenant, repository.ReadOLAPPayloadOpts{ExternalId: tasks[0].ExternalID, InsertedAt: tasks[0].InsertedAt})
		if err != nil {
			return nil, uuid.Nil, err
		}
		if p != nil {
			row.InputPayload = p
		}
	}
	row.WorkflowRunID = run
	children, err := r.scan(ctx, entityFilter{Tenant: &tenant, Kinds: []string{"task", "dag"}, Predicate: "JSONExtractString(body,'parent_task_external_id') = ?", Arguments: []any{row.ExternalID.String()}})
	if err != nil {
		return nil, uuid.Nil, err
	}
	row.NumSpawnedChildren = 0
	for _, child := range children {
		if child.Kind == "dag" {
			row.NumSpawnedChildren++
			continue
		}
		t, err := decodeEntity[sqlcv1.V1TasksOlap](child)
		if err != nil {
			return nil, uuid.Nil, err
		}
		if !t.DagID.Valid {
			row.NumSpawnedChildren++
		}
	}
	return row, run, nil
}

func (r *Repository) ListTasksByIdAndInsertedAt(ctx context.Context, tenant uuid.UUID, meta []repository.TaskMetadata, include bool) ([]*repository.TaskWithPayloads, error) {
	if len(meta) == 0 {
		return []*repository.TaskWithPayloads{}, nil
	}
	ctx, err := r.snapshot(ctx)
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
	ctx, err := r.snapshot(ctx)
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
	ctx, err := r.snapshot(ctx)
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
func page[T any](rows []T, limit, offset int64) ([]T, error) {
	if limit < 0 || offset < 0 {
		return nil, fmt.Errorf("LIMIT and OFFSET must not be negative")
	}
	if offset >= int64(len(rows)) {
		return []T{}, nil
	}
	end := offset + limit
	if end > int64(len(rows)) || end < offset {
		end = int64(len(rows))
	}
	return rows[offset:end], nil
}

func (r *Repository) ListTasks(ctx context.Context, tenant uuid.UUID, opts repository.ListTaskRunOpts) ([]*repository.TaskWithPayloads, int, error) {
	ctx, err := r.snapshot(ctx)
	if err != nil {
		return nil, 0, err
	}
	f := runFilter(tenant, "task", opts.CreatedAfter, opts.FinishedBefore, opts.Statuses, opts.WorkflowIds, opts.IdempotencyKeys, nil)
	if opts.WorkerId != nil {
		f.Predicate += " AND JSONExtractString(body,'latest_worker_id') = ?"
		f.Arguments = append(f.Arguments, opts.WorkerId.String())
	}
	tasks, err := scanValues[sqlcv1.V1TasksOlap](ctx, r, f)
	if err != nil {
		return nil, 0, err
	}
	var eventRuns map[string]bool
	if opts.TriggeringEventExternalId != nil {
		eventRuns, err = r.triggeredRunKeys(ctx, tenant, *opts.TriggeringEventExternalId)
		if err != nil {
			return nil, 0, err
		}
	}
	filtered := make([]*sqlcv1.V1TasksOlap, 0, len(tasks))
	for _, t := range tasks {
		if !metadataMatches(t.AdditionalMetadata, opts.AdditionalMetadata, opts.AdditionalMetadataOperator) {
			continue
		}
		if eventRuns != nil && !eventRuns[keyFor(t.ID, t.InsertedAt.Time)] {
			continue
		}
		filtered = append(filtered, t)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].InsertedAt.Time.Equal(filtered[j].InsertedAt.Time) {
			return filtered[i].ID > filtered[j].ID
		}
		return filtered[i].InsertedAt.Time.After(filtered[j].InsertedAt.Time)
	})
	count := len(filtered)
	filtered, err = page(filtered, opts.Limit, opts.Offset)
	if err != nil {
		return nil, 0, err
	}
	rows, err := r.taskData(ctx, tenant, filtered, opts.IncludePayloads, nil)
	return rows, count, err
}

func runFilter(tenant uuid.UUID, kind string, after time.Time, before *time.Time, status []sqlcv1.V1ReadableStatusOlap, workflows []uuid.UUID, idempotency *[]string, parent *uuid.UUID) entityFilter {
	f := entityFilter{Tenant: &tenant, Kind: kind, After: &after, Predicate: "JSONExtractString(body,'readable_status') IN (?)"}
	statuses := make([]string, 0)
	for _, s := range defaultStatuses(status) {
		statuses = append(statuses, string(s))
	}
	f.Arguments = []any{statuses}
	if before != nil {
		f.Predicate += " AND inserted_at <= ?"
		f.Arguments = append(f.Arguments, dateArg(*before))
	}
	if len(workflows) > 0 {
		f.Predicate += " AND JSONExtractString(body,'workflow_id') IN (?)"
		ids := make([]string, 0, len(workflows))
		for _, id := range workflows {
			ids = append(ids, id.String())
		}
		f.Arguments = append(f.Arguments, ids)
	}
	if parent != nil {
		f.Predicate += " AND JSONExtractString(body,'parent_task_external_id') = ?"
		f.Arguments = append(f.Arguments, parent.String())
	}
	if idempotency != nil {
		f.Predicate += " AND JSONExtractString(body,'idempotency_key') IN (?)"
		f.Arguments = append(f.Arguments, *idempotency)
	}
	return f
}

func metadataMatches(data []byte, wanted map[string]interface{}, operator repository.AdditionalMetadataOperator) bool {
	if len(wanted) == 0 {
		return true
	}
	var actual map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if decoder.Decode(&actual) != nil {
		return false
	}
	if operator == repository.AdditionalMetadataOperatorAnd {
		encoded, _ := json.Marshal(wanted)
		var target any
		d := json.NewDecoder(bytes.NewReader(encoded))
		d.UseNumber()
		_ = d.Decode(&target)
		return jsonContains(actual, target)
	}
	for k, v := range wanted {
		a, ok := actual[k]
		if !ok {
			continue
		}
		s, ok := a.(string)
		if !ok {
			encoded, _ := json.Marshal(a)
			s = string(encoded)
		}
		if s == fmt.Sprint(v) {
			return true
		}
	}
	return false
}

func jsonContains(actual, target any) bool {
	switch t := target.(type) {
	case map[string]any:
		a, ok := actual.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range t {
			av, ok := a[k]
			if !ok || !jsonContains(av, v) {
				return false
			}
		}
		return true
	case []any:
		a, ok := actual.([]any)
		if !ok {
			return false
		}
		for _, v := range t {
			matched := false
			for _, av := range a {
				if jsonContains(av, v) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
		return true
	default:
		if a, ok := actual.([]any); ok {
			for _, v := range a {
				if jsonContains(v, target) {
					return true
				}
			}
			return false
		}
		if a, ok := actual.(json.Number); ok {
			if b, ok := target.(json.Number); ok {
				x, xo := new(big.Rat).SetString(string(a))
				y, yo := new(big.Rat).SetString(string(b))
				return xo && yo && x.Cmp(y) == 0
			}
		}
		return reflect.DeepEqual(actual, target)
	}
}

func (r *Repository) ListWorkflowRunDisplayNames(ctx context.Context, tenant uuid.UUID, ids []uuid.UUID) ([]*sqlcv1.ListWorkflowRunDisplayNamesRow, error) {
	var result []*sqlcv1.ListWorkflowRunDisplayNamesRow
	for _, kind := range []string{"task", "dag"} {
		rows, err := r.scan(ctx, entityFilter{Tenant: &tenant, Kind: kind, ExternalIDs: ids})
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			var value struct {
				DisplayName string `json:"display_name"`
			}
			if err = json.Unmarshal([]byte(row.Body), &value); err != nil {
				return nil, err
			}
			result = append(result, &sqlcv1.ListWorkflowRunDisplayNamesRow{ExternalID: row.ExternalID, DisplayName: value.DisplayName, InsertedAt: pgtype.Timestamptz{Time: row.InsertedAt, Valid: true}})
		}
	}
	if len(result) > 10000 {
		result = result[:10000]
	}
	return result, nil
}
