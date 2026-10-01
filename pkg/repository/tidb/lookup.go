package tidb

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Lookup timestamps are resolved before planning the partitioned point query.
func (s *store) locate(ctx context.Context, f entityFilter) (entityFilter, bool, error) {
	if f.ExternalIDs == nil || len(f.ExternalIDs) == 0 || f.Kind == "span" {
		return f, len(f.ExternalIDs) == 0 && f.ExternalIDs != nil, nil
	}
	where := []string{"kind=?", "external_id IN (" + placeholders(len(f.ExternalIDs)) + ")"}
	args := []any{f.Kind}
	for _, id := range f.ExternalIDs {
		args = append(args, uuidArg(id))
	}
	if f.Tenant != nil {
		where = append(where, "tenant_id=?")
		args = append(args, uuidArg(*f.Tenant))
	}
	if f.After != nil {
		where = append(where, "inserted_at>=?")
		args = append(args, dateArg(*f.After))
	}
	rows, err := executor(ctx, s.db).QueryContext(ctx, "SELECT tenant_id,external_id,inserted_at FROM v1_lookup_table_olap WHERE "+strings.Join(where, " AND "), args...)
	if err != nil {
		return f, false, err
	}
	var tuples []string
	var bound []any
	var times []time.Time
	var ids []uuid.UUID
	seen := map[uuid.UUID]bool{}
	for rows.Next() {
		var tenant, id uuid.UUID
		var at time.Time
		if err = rows.Scan(&tenant, &id, &at); err != nil {
			rows.Close()
			return f, false, err
		}
		tuples = append(tuples, "(?,?)")
		bound = append(bound, uuidArg(tenant), uuidArg(id))
		times = append(times, at)
		if !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return f, false, err
	}
	if len(tuples) == 0 {
		return f, true, nil
	}
	dates, dateArgs := timeBounds("e.inserted_at", times)
	predicate := dates
	if f.Tenant == nil {
		predicate = "(e.tenant_id,e.external_id) IN (" + strings.Join(tuples, ",") + ") AND " + dates
		bound = append(bound, dateArgs...)
	} else {
		bound = dateArgs
	}
	// The lookup covers every retained entity identity. Its time bounds prune
	// partitions while a single UUID IN predicate avoids Cartesian key ranges.
	f.ExternalIDs = ids
	if f.Where != "" {
		f.Where = "(" + f.Where + ") AND (" + predicate + ")"
		f.Args = append(f.Args, bound...)
	} else {
		f.Where = predicate
		f.Args = bound
	}
	return f, false, nil
}

func timeBounds(column string, times []time.Time) (string, []any) {
	if len(times) == 0 {
		return "FALSE", nil
	}
	lo, hi := times[0], times[0]
	for _, at := range times[1:] {
		if at.Before(lo) {
			lo = at
		}
		if at.After(hi) {
			hi = at
		}
	}
	return column + ">=? AND " + column + "<=?", []any{dateArg(lo), dateArg(hi)}
}
func datePredicate(times []time.Time) (string, []any) {
	if len(times) == 0 {
		return "FALSE", nil
	}
	seen := map[int64]bool{}
	var args []any
	for _, t := range times {
		key := t.UnixMicro()
		if !seen[key] {
			seen[key] = true
			args = append(args, dateArg(t))
		}
	}
	return "e.inserted_at IN (" + placeholders(len(args)) + ")", args
}
