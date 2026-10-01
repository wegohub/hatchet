package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/hatchet-dev/hatchet/pkg/validator"
	"github.com/jackc/pgx/v5/pgtype"
)

var _ repository.LogLineRepository = (*Logs)(nil)

type taskFlattener interface {
	ListTasksByExternalIds(context.Context, uuid.UUID, []uuid.UUID) ([]*sqlcv1.FlattenTasksByExternalIdsRow, error)
}

type logRequest struct {
	row    sqlcv1.V1LogLine
	result chan error
	size   int
}

type Logs struct {
	store     *store
	tasks     repository.TaskRepository
	olap      taskFlattener
	validate  validator.Validator
	retention time.Duration
	timeout   time.Duration
	queue     chan logRequest
	done      chan struct{}
	mu        sync.RWMutex
	closed    bool
}

func newLogs(s *store, tasks repository.TaskRepository, olap taskFlattener, retention, timeout time.Duration) *Logs {
	l := &Logs{store: s, tasks: tasks, olap: olap, validate: validator.NewDefaultValidator(), retention: retention, timeout: timeout, queue: make(chan logRequest, 512), done: make(chan struct{})}
	go l.run()
	return l
}

func (l *Logs) Close() error {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		close(l.queue)
	}
	l.mu.Unlock()
	<-l.done
	return nil
}

func (l *Logs) PutLog(ctx context.Context, tenant uuid.UUID, opts *repository.CreateLogLineOpts) error {
	if err := l.validate.Validate(opts); err != nil {
		return err
	}
	if opts.Metadata != nil && !json.Valid(opts.Metadata) {
		return errors.New("log metadata must be valid JSON")
	}
	level := sqlcv1.V1LogLineLevelINFO
	if opts.Level != nil {
		level = sqlcv1.V1LogLineLevel(*opts.Level)
	}
	row := sqlcv1.V1LogLine{TenantID: tenant, TaskID: opts.TaskId, TaskInsertedAt: opts.TaskInsertedAt, CreatedAt: pgtype.Timestamptz{Time: time.Now().UTC().Truncate(time.Microsecond), Valid: true}, Message: opts.Message, Level: level, Metadata: append([]byte(nil), opts.Metadata...), RetryCount: int32(opts.RetryCount), WorkflowID: &opts.WorkflowId, StepID: &opts.StepId}
	encoded, err := json.Marshal(row)
	if err != nil {
		return err
	}
	request := logRequest{row: row, result: make(chan error, 1), size: len(encoded) + len(row.Message) + 200}
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return errors.New("ClickHouse log repository is closed")
	}
	select {
	case l.queue <- request:
		l.mu.RUnlock()
	case <-ctx.Done():
		l.mu.RUnlock()
		return ctx.Err()
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Logs) run() {
	defer close(l.done)
	var carry *logRequest
	for {
		var first logRequest
		var ok bool
		if carry != nil {
			first = *carry
			carry = nil
			ok = true
		} else {
			first, ok = <-l.queue
		}
		if !ok {
			return
		}
		requests := []logRequest{first}
		size := first.size
		timer := time.NewTimer(5 * time.Millisecond)
		closed := false
	collect:
		// An indivisible record can exceed the target byte budget because log
		// metadata has no size limit in the repository contract.
		for len(requests) < 512 && size < 1<<20 {
			select {
			case next, ok := <-l.queue:
				if !ok {
					closed = true
					break collect
				}
				n := next.size
				if size+n > 1<<20 {
					carry = &next
					break collect
				}
				requests = append(requests, next)
				size += n
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), l.timeout)
		err := l.flush(ctx, requests)
		cancel()
		for _, r := range requests {
			r.result <- err
		}
		if closed {
			return
		}
	}
}

func (l *Logs) flush(ctx context.Context, requests []logRequest) error {
	start, err := l.store.keeper.nextLogIDs(ctx, uint64(len(requests)))
	if err != nil {
		return err
	}
	id := uuid.New()
	rows := make([]sqlcv1.V1LogLine, len(requests))
	for i, r := range requests {
		rows[i] = r.row
		rows[i].ID = int64(start + uint64(i))
	}
	for attempt := 0; attempt < 3; attempt++ {
		err = l.insertBatch(ctx, id, rows)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
	}
	return err
}

func (l *Logs) insertBatch(ctx context.Context, id uuid.UUID, rows []sqlcv1.V1LogLine) error {
	writeCtx, err := l.store.writeContext(ctx, "log_lines")
	if err != nil {
		return err
	}
	batch, err := l.store.conn.PrepareBatch(writeCtx, "INSERT INTO log_lines (tenant,id,created_at,task_id,task_inserted_at,workflow_id,step_id,retry_count,level,message,body,batch_id,ordinal)")
	if err != nil {
		return err
	}
	defer batch.Abort()
	for i, row := range rows {
		body, err := json.Marshal(row)
		if err != nil {
			return err
		}
		if err = batch.Append(row.TenantID, row.ID, row.CreatedAt.Time, row.TaskID, row.TaskInsertedAt.Time, *row.WorkflowID, *row.StepID, row.RetryCount, string(row.Level), row.Message, string(body), id, uint64(i+1)); err != nil {
			return err
		}
	}
	return batch.Send()
}

// PostgreSQL LIKE treats '_' as one character and permits escaping wildcards.
func searchPattern(search string) (string, error) {
	pattern := "%" + search + "%"
	var b strings.Builder
	b.WriteString("(?is)^")
	escaped := false
	for _, r := range pattern {
		if escaped {
			b.WriteString(regexp.QuoteMeta(string(r)))
			escaped = false
			continue
		}
		switch r {
		case '\\':
			escaped = true
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	if escaped {
		return "", errors.New("LIKE pattern must not end with escape character")
	}
	b.WriteByte('$')
	return b.String(), nil
}

func (l *Logs) filters(ctx context.Context, tenant uuid.UUID, search *string, levels []string, external, workflows, steps []uuid.UUID) (string, []any, error) {
	where := "tenant = ? AND created_at >= ?"
	args := []any{tenant, dateArg(time.Now().UTC().Add(-l.retention).Truncate(24 * time.Hour))}
	if search != nil {
		pattern, err := searchPattern(*search)
		if err != nil {
			return "", nil, err
		}
		where += " AND match(message, ?)"
		args = append(args, pattern)
	}
	if levels != nil {
		where += " AND level IN (?)"
		args = append(args, levels)
	}
	if workflows != nil {
		where += " AND workflow_id IN (?)"
		args = append(args, workflows)
	}
	if steps != nil {
		where += " AND step_id IN (?)"
		args = append(args, steps)
	}
	if len(external) > 0 {
		rows, err := l.olap.ListTasksByExternalIds(ctx, tenant, external)
		if err != nil {
			return "", nil, err
		}
		ids := make([]int64, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.ID)
		}
		where += " AND task_id IN (?)"
		args = append(args, ids)
	}
	return where, args, nil
}

func (l *Logs) ListLogLines(ctx context.Context, tenant uuid.UUID, opts *repository.ListLogsOpts) ([]*repository.ListLogLineRow, error) {
	if err := l.validate.Validate(opts); err != nil {
		return nil, err
	}
	where, args, err := l.filters(ctx, tenant, opts.Search, opts.Levels, opts.TaskExternalIds, opts.WorkflowIds, opts.StepIds)
	if err != nil {
		return nil, err
	}
	if opts.Since != nil {
		where += " AND created_at > ?"
		args = append(args, dateArg(*opts.Since))
	}
	if opts.Until != nil {
		where += " AND created_at < ?"
		args = append(args, dateArg(*opts.Until))
	}
	if opts.Attempt != nil {
		where += " AND retry_count = ?"
		args = append(args, *opts.Attempt-1)
	}
	direction := "ASC"
	if opts.OrderByDirection != nil {
		direction = *opts.OrderByDirection
	}
	limit, offset := 1000, 0
	if opts.Limit != nil {
		limit = *opts.Limit
	}
	if opts.Offset != nil {
		offset = *opts.Offset
	}
	if offset < 0 {
		return nil, errors.New("OFFSET must not be negative")
	}
	args = append(args, limit, offset)
	result, err := l.store.conn.Query(ctx, "SELECT body FROM log_lines FINAL WHERE "+where+" ORDER BY created_at "+direction+" LIMIT ? OFFSET ?", args...)
	if err != nil {
		return nil, err
	}
	var rows []*sqlcv1.V1LogLine
	ids := make([]int64, 0)
	for result.Next() {
		var body string
		if err = result.Scan(&body); err != nil {
			result.Close()
			return nil, err
		}
		row := new(sqlcv1.V1LogLine)
		if err = json.Unmarshal([]byte(body), row); err != nil {
			result.Close()
			return nil, err
		}
		rows = append(rows, row)
		ids = append(ids, row.TaskID)
	}
	err = result.Err()
	result.Close()
	if err != nil {
		return nil, err
	}
	tasks, err := l.tasks.ListTasks(ctx, tenant, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[int64]*sqlcv1.V1Task, len(tasks))
	for _, task := range tasks {
		byID[task.ID] = task
	}
	res := make([]*repository.ListLogLineRow, len(rows))
	for i, row := range rows {
		if task := byID[row.TaskID]; task != nil {
			res[i] = &repository.ListLogLineRow{V1LogLine: row, TaskDisplayName: task.DisplayName, TaskExternalId: task.ExternalID}
		}
	}
	return res, nil
}

func (l *Logs) GetLogLinePointMetrics(ctx context.Context, tenant uuid.UUID, opts *repository.GetLogLinePointMetricsOpts) ([]*sqlcv1.GetLogLinePointMetricsRow, error) {
	if err := l.validate.Validate(opts); err != nil {
		return nil, err
	}
	if opts.BucketInterval <= 0 {
		return nil, errors.New("bucket interval must be positive")
	}
	levels := opts.Levels
	if len(levels) == 0 {
		levels = nil
	}
	where, args, err := l.filters(ctx, tenant, opts.Search, levels, opts.TaskExternalIds, opts.WorkflowIds, opts.StepIds)
	if err != nil {
		return nil, err
	}
	where += " AND created_at BETWEEN ? AND ?"
	args = append(args, dateArg(opts.StartTimestamp), dateArg(opts.EndTimestamp))
	query := "SELECT fromUnixTimestamp64Micro(intDiv(toUnixTimestamp64Micro(created_at), ?)*?),countIf(level='DEBUG'),countIf(level='INFO'),countIf(level='WARN'),countIf(level='ERROR') FROM log_lines FINAL WHERE " + where + " GROUP BY 1 ORDER BY 1"
	micros := opts.BucketInterval.Microseconds()
	if micros == 0 {
		return nil, fmt.Errorf("bucket interval must be at least one microsecond")
	}
	args = append([]any{micros, micros}, args...)
	rows, err := l.store.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*sqlcv1.GetLogLinePointMetricsRow
	for rows.Next() {
		row := new(sqlcv1.GetLogLinePointMetricsRow)
		var ts time.Time
		var d, i, w, e uint64
		if err = rows.Scan(&ts, &d, &i, &w, &e); err != nil {
			return nil, err
		}
		row.MinuteBucket = pgtype.Timestamptz{Time: ts.UTC(), Valid: true}
		row.DebugCount = int64(d)
		row.InfoCount = int64(i)
		row.WarnCount = int64(w)
		row.ErrorCount = int64(e)
		result = append(result, row)
	}
	return result, rows.Err()
}
