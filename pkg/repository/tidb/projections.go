package tidb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
)

type dagCounter struct {
	ExternalID, WorkflowID uuid.UUID
	ID                     int64
	At                     time.Time
	Counts                 [6]int64
}

func statusIndex(status sqlcv1.V1ReadableStatusOlap) int {
	switch status {
	case "QUEUED":
		return 0
	case "RUNNING":
		return 1
	case "EVICTED":
		return 2
	case "COMPLETED":
		return 3
	case "CANCELLED":
		return 4
	case "FAILED":
		return 5
	}
	return -1
}
func (c *dagCounter) total() int64 {
	var total int64
	for _, n := range c.Counts {
		total += n
	}
	return total
}
func (c *dagCounter) rollup(current sqlcv1.V1ReadableStatusOlap, total int32) sqlcv1.V1ReadableStatusOlap {
	n := c.total()
	if n == 0 || c.Counts[0] == n {
		return current
	}
	if n != int64(total) || c.Counts[0] > 0 || c.Counts[1] > 0 {
		return "RUNNING"
	}
	if c.Counts[2] == n {
		return "EVICTED"
	}
	if c.Counts[5] > 0 {
		return "FAILED"
	}
	if c.Counts[4] > 0 {
		return "CANCELLED"
	}
	if c.Counts[3] == n {
		return "COMPLETED"
	}
	return "RUNNING"
}
func (s *store) publishCounters(ctx context.Context, tx *sql.Tx, rows []entity) error {
	for _, row := range rows {
		v, err := decodeEntity[dagCounter](row)
		if err != nil {
			return err
		}
		args := []any{uuidArg(row.Tenant), uuidArg(v.ExternalID), []byte(keyFor(v.ID, v.At)), v.ID, dateArg(v.At), uuidArg(v.WorkflowID), uuidArg(v.ExternalID)}
		for _, n := range v.Counts {
			if n < 0 {
				return fmt.Errorf("negative DAG state counter")
			}
			args = append(args, n)
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO v1_runs_olap(tenant_id,kind,external_id,entity_key,id,inserted_at,workflow_id,workflow_run_id,readable_status,is_placeholder,queued_count,running_count,evicted_count,completed_count,cancelled_count,failed_count) VALUES (?,'dag',?,?,?,?,?,?,'QUEUED',TRUE,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE queued_count=VALUES(queued_count),running_count=VALUES(running_count),evicted_count=VALUES(evicted_count),completed_count=VALUES(completed_count),cancelled_count=VALUES(cancelled_count),failed_count=VALUES(failed_count)", args...)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *store) freshTaskEvents(ctx context.Context, tx *sql.Tx, rows []entity) ([]entity, error) {
	known := true
	for _, r := range rows {
		known = known && r.KnownFresh
	}
	if known {
		return rows, nil
	}
	if len(rows) == 0 {
		return rows, nil
	}
	if len(rows) > 200 {
		out := make([]entity, 0, len(rows))
		for i := 0; i < len(rows); i += 200 {
			batch, err := s.freshTaskEvents(ctx, tx, rows[i:min(i+200, len(rows))])
			if err != nil {
				return nil, err
			}
			out = append(out, batch...)
		}
		return out, nil
	}
	var args []any
	var where []string
	for _, r := range rows {
		where = append(where, "(tenant_id=? AND external_id=? AND inserted_at=? AND entity_key=?)")
		args = append(args, uuidArg(r.Tenant), uuidArg(r.ExternalID), r.InsertedAt, []byte(r.Key))
	}
	found, err := tx.QueryContext(ctx, "SELECT tenant_id,entity_key FROM v1_task_events_olap WHERE "+strings.Join(where, " OR ")+" FOR UPDATE", args...)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for found.Next() {
		var tenant uuid.UUID
		var key string
		if err = found.Scan(&tenant, &key); err != nil {
			found.Close()
			return nil, err
		}
		seen[tenant.String()+key] = true
	}
	err = found.Err()
	found.Close()
	if err != nil {
		return nil, err
	}
	out := make([]entity, 0, len(rows))
	for _, r := range rows {
		key := r.Tenant.String() + r.Key
		if !seen[key] {
			out = append(out, r)
			seen[key] = true
		}
	}
	return out, nil
}
func (s *store) publishAttempts(ctx context.Context, tx *sql.Tx, rows []entity) error {
	var values []string
	var args []any
	for _, row := range rows {
		v, err := decodeEntity[taskEvent](row)
		if err != nil {
			return err
		}
		var q, st, fin, runStart, runFinish, outID, outAt, outTime, runOutID, runOutAt, errMsg, errID, detailOutID, detailOutAt, detailOutEventID, detailErrAt any
		if v.EventType == "QUEUED" {
			q = stampArg(v.EventTimestamp)
		}
		if v.EventType == "STARTED" {
			st = stampArg(v.EventTimestamp)
		}
		if terminal(v.ReadableStatus) {
			fin = stampArg(v.EventTimestamp)
			runFinish = stampArg(v.InsertedAt)
		}
		if v.ReadableStatus == "RUNNING" {
			runStart = stampArg(v.InsertedAt)
		}
		if v.ReadableStatus == "FAILED" {
			errMsg = textArg(v.ErrorMessage)
			errID = v.ID
			detailErrAt = stampArg(v.EventTimestamp)
		}
		if v.EventType == "FINISHED" {
			runOutID = uuidArg(v.ExternalID)
			runOutAt = stampArg(v.InsertedAt)
			detailOutID = uuidArg(v.ExternalID)
			detailOutAt = stampArg(v.InsertedAt)
			detailOutEventID = v.ID
			if v.ReadableStatus == "COMPLETED" {
				outID = uuidArg(v.ExternalID)
				outAt = stampArg(v.InsertedAt)
				outTime = stampArg(v.EventTimestamp)
			}
		}
		values = append(values, "("+placeholders(35)+")")
		args = append(args, uuidArg(row.Tenant), v.TaskID, stampArg(v.TaskInsertedAt), uuidArg(row.RunID), v.RetryCount, string(v.ReadableStatus), statusPriority(v.ReadableStatus), optionalUUID(v.WorkerID), int64(1), v.ID, v.ID, q, q, st, st, fin, fin, stampArg(v.InsertedAt), runStart, runFinish, errMsg, errID, outID, outAt, outTime, runOutID, runOutAt, string(v.ReadableStatus), terminal(v.ReadableStatus), stampArg(v.EventTimestamp), detailOutID, detailOutAt, detailOutEventID, errMsg, detailErrAt)
	}
	if len(values) == 0 {
		return nil
	}
	cols := []string{"tenant_id", "task_id", "task_inserted_at", "run_id", "retry_count", "readable_status", "status_priority", "worker_id", "event_count", "first_event_id", "last_event_id", "queued_first", "queued_last", "started_first", "started_last", "finished_first", "finished_last", "run_created_first", "run_started_first", "run_finished_last", "error_message", "error_event_id", "output_external_id", "output_inserted_at", "output_event_timestamp", "run_output_external_id", "run_output_inserted_at", "detail_status", "detail_status_terminal", "detail_status_at", "detail_output_external_id", "detail_output_inserted_at", "detail_output_id", "detail_error_message", "detail_error_at"}
	updates := []string{"readable_status=IF(VALUES(status_priority)>status_priority OR readable_status='EVICTED' AND VALUES(readable_status)<>'EVICTED',VALUES(readable_status),readable_status)", "status_priority=CASE readable_status WHEN 'QUEUED' THEN 1 WHEN 'RUNNING' THEN 2 WHEN 'EVICTED' THEN 3 WHEN 'CANCELLED' THEN 4 WHEN 'FAILED' THEN 5 WHEN 'COMPLETED' THEN 6 ELSE 0 END", "worker_id=GREATEST(COALESCE(worker_id,VALUES(worker_id)),COALESCE(VALUES(worker_id),worker_id))", "event_count=event_count+VALUES(event_count)", "first_event_id=LEAST(first_event_id,VALUES(first_event_id))", "last_event_id=GREATEST(last_event_id,VALUES(last_event_id))"}
	for _, col := range []string{"queued_first", "started_first", "finished_first", "run_created_first", "run_started_first"} {
		updates = append(updates, col+"=LEAST(COALESCE("+col+",VALUES("+col+")),COALESCE(VALUES("+col+"),"+col+"))")
	}
	for _, col := range []string{"queued_last", "started_last", "finished_last", "run_finished_last"} {
		updates = append(updates, col+"=GREATEST(COALESCE("+col+",VALUES("+col+")),COALESCE(VALUES("+col+"),"+col+"))")
	}
	updates = append(updates, "error_message=IF(VALUES(error_event_id) IS NOT NULL AND (error_event_id IS NULL OR VALUES(error_event_id)<error_event_id),VALUES(error_message),error_message)", "error_event_id=LEAST(COALESCE(error_event_id,VALUES(error_event_id)),COALESCE(VALUES(error_event_id),error_event_id))")
	for _, col := range []string{"output_external_id", "output_inserted_at"} {
		updates = append(updates, col+"=IF(VALUES(output_event_timestamp) IS NOT NULL AND (output_event_timestamp IS NULL OR VALUES(output_event_timestamp)>output_event_timestamp),VALUES("+col+"),"+col+")")
	}
	updates = append(updates, "output_event_timestamp=GREATEST(COALESCE(output_event_timestamp,VALUES(output_event_timestamp)),COALESCE(VALUES(output_event_timestamp),output_event_timestamp))", "run_output_external_id=IF(VALUES(run_output_inserted_at) IS NOT NULL AND (run_output_inserted_at IS NULL OR VALUES(run_output_inserted_at)>run_output_inserted_at),VALUES(run_output_external_id),run_output_external_id)", "run_output_inserted_at=GREATEST(COALESCE(run_output_inserted_at,VALUES(run_output_inserted_at)),COALESCE(VALUES(run_output_inserted_at),run_output_inserted_at))")
	detailWins := "VALUES(detail_status_terminal)>detail_status_terminal OR VALUES(detail_status_terminal)=detail_status_terminal AND VALUES(detail_status_at)>detail_status_at"
	updates = append(updates, "detail_status=IF("+detailWins+",VALUES(detail_status),detail_status)", "detail_status_at=IF("+detailWins+",VALUES(detail_status_at),detail_status_at)", "detail_status_terminal=GREATEST(detail_status_terminal,VALUES(detail_status_terminal))")
	for _, col := range []string{"detail_output_external_id", "detail_output_inserted_at"} {
		updates = append(updates, col+"=IF(VALUES(detail_output_id) IS NOT NULL AND (detail_output_id IS NULL OR VALUES(detail_output_id)<detail_output_id),VALUES("+col+"),"+col+")")
	}
	updates = append(updates, "detail_output_id=LEAST(COALESCE(detail_output_id,VALUES(detail_output_id)),COALESCE(VALUES(detail_output_id),detail_output_id))", "detail_error_message=IF(VALUES(detail_error_at) IS NOT NULL AND (detail_error_at IS NULL OR VALUES(detail_error_at)>detail_error_at),VALUES(detail_error_message),detail_error_message)", "detail_error_at=GREATEST(COALESCE(detail_error_at,VALUES(detail_error_at)),COALESCE(VALUES(detail_error_at),detail_error_at))")
	_, err := tx.ExecContext(ctx, "INSERT INTO v1_task_attempts_olap("+strings.Join(cols, ",")+") VALUES "+strings.Join(values, ",")+" ON DUPLICATE KEY UPDATE "+strings.Join(updates, ","), args...)
	return err
}

type attempt struct {
	DetailStatus        sqlcv1.V1ReadableStatusOlap
	DetailTerminal      bool
	DetailAt            pgtype.Timestamptz
	DetailOutputID      *uuid.UUID
	DetailOutputAt      pgtype.Timestamptz
	DetailOutputEventID pgtype.Int8
	DetailError         pgtype.Text
	DetailErrorAt       pgtype.Timestamptz

	TaskID                                                                          int64
	TaskAt                                                                          pgtype.Timestamptz
	RunID                                                                           uuid.UUID
	Retry                                                                           int32
	Status                                                                          sqlcv1.V1ReadableStatusOlap
	Worker                                                                          *uuid.UUID
	Count, FirstID, LastID                                                          int64
	QueuedFirst, QueuedLast, StartedFirst, StartedLast, FinishedFirst, FinishedLast pgtype.Timestamptz
	RunCreated, RunStarted, RunFinished                                             pgtype.Timestamptz
	Error                                                                           pgtype.Text
	ErrorID                                                                         pgtype.Int8
	OutputID                                                                        *uuid.UUID
	OutputAt, OutputTime                                                            pgtype.Timestamptz
	RunOutputID                                                                     *uuid.UUID
	RunOutputAt                                                                     pgtype.Timestamptz
}

const attemptColumns = "task_id,task_inserted_at,run_id,retry_count,readable_status,worker_id,event_count,first_event_id,last_event_id,queued_first,queued_last,started_first,started_last,finished_first,finished_last,run_created_first,run_started_first,run_finished_last,error_message,error_event_id,output_external_id,output_inserted_at,output_event_timestamp,run_output_external_id,run_output_inserted_at,detail_status,detail_status_terminal,detail_status_at,detail_output_external_id,detail_output_inserted_at,detail_output_id,detail_error_message,detail_error_at"

func scanAttempt(rows *sql.Rows) (*attempt, error) {
	a := new(attempt)
	err := rows.Scan(&a.TaskID, &a.TaskAt, &a.RunID, &a.Retry, &a.Status, &a.Worker, &a.Count, &a.FirstID, &a.LastID, &a.QueuedFirst, &a.QueuedLast, &a.StartedFirst, &a.StartedLast, &a.FinishedFirst, &a.FinishedLast, &a.RunCreated, &a.RunStarted, &a.RunFinished, &a.Error, &a.ErrorID, &a.OutputID, &a.OutputAt, &a.OutputTime, &a.RunOutputID, &a.RunOutputAt, &a.DetailStatus, &a.DetailTerminal, &a.DetailAt, &a.DetailOutputID, &a.DetailOutputAt, &a.DetailOutputEventID, &a.DetailError, &a.DetailErrorAt)
	return a, err
}
