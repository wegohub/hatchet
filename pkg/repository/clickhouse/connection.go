package clickhouse

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// The native driver uses a caller's deadline in preference to ReadTimeout.
// Bound each operation even when its caller has a longer lived context.
type boundedConn struct {
	driver.Conn
	timeout time.Duration
}

func (c *boundedConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.Conn.Select(ctx, dest, query, args...)
}

func (c *boundedConn) Exec(ctx context.Context, query string, args ...any) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.Conn.Exec(ctx, query, args...)
}

func (c *boundedConn) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.Conn.Ping(ctx)
}

func (c *boundedConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	rows, err := c.Conn.Query(ctx, query, args...)
	if err != nil {
		cancel()
		return nil, err
	}
	return &boundedRows{Rows: rows, cancel: cancel}, nil
}

type boundedRows struct {
	driver.Rows
	cancel context.CancelFunc
}

func (r *boundedRows) Close() error { defer r.cancel(); return r.Rows.Close() }
func (r *boundedRows) Next() bool {
	ok := r.Rows.Next()
	if !ok {
		r.cancel()
	}
	return ok
}

func (c *boundedConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	return &boundedRow{Row: c.Conn.QueryRow(ctx, query, args...), cancel: cancel}
}

type boundedRow struct {
	driver.Row
	cancel context.CancelFunc
}

func (r *boundedRow) Scan(dest ...any) error    { defer r.cancel(); return r.Row.Scan(dest...) }
func (r *boundedRow) ScanStruct(dest any) error { defer r.cancel(); return r.Row.ScanStruct(dest) }
func (r *boundedRow) Err() error {
	err := r.Row.Err()
	if err != nil {
		r.cancel()
	}
	return err
}

func (c *boundedConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	batch, err := c.Conn.PrepareBatch(ctx, query, opts...)
	if err != nil {
		cancel()
		return nil, err
	}
	return &boundedBatch{Batch: batch, cancel: cancel}, nil
}

type boundedBatch struct {
	driver.Batch
	cancel context.CancelFunc
}

func (b *boundedBatch) Send() error  { defer b.cancel(); return b.Batch.Send() }
func (b *boundedBatch) Abort() error { defer b.cancel(); return b.Batch.Abort() }
func (b *boundedBatch) Close() error { defer b.cancel(); return b.Batch.Close() }
