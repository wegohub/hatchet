package tidb

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}
type transactionKey struct{}

func executor(ctx context.Context, db *sql.DB) sqlExecutor {
	if tx, ok := ctx.Value(transactionKey{}).(*sql.Tx); ok {
		return tx
	}
	return db
}
func uuidArg(id uuid.UUID) []byte { return id[:] }
func optionalUUID(id *uuid.UUID) any {
	if id == nil {
		return nil
	}
	return uuidArg(*id)
}
func stampArg(v pgtype.Timestamptz) any {
	if !v.Valid {
		return nil
	}
	return dateArg(v.Time)
}
func textArg(v pgtype.Text) any {
	if !v.Valid {
		return nil
	}
	return v.String
}
func int4Arg(v pgtype.Int4) any {
	if !v.Valid {
		return nil
	}
	return v.Int32
}
func int8Arg(v pgtype.Int8) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
}
