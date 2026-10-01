package clickhouse

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type traceLookup struct {
	Retry int32
	Trace []byte
	Start time.Time
}

func nullableText(s string) pgtype.Text { return pgtype.Text{String: s, Valid: s != ""} }
func (r *Repository) storeUnique(ctx context.Context, tenant uuid.UUID, kind string, rows []entity) error {
	lock, err := r.store.keeper.lock(ctx, kind+"-"+tenant.String())
	if err != nil {
		return err
	}
	defer lock.release()
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, row.Key)
	}
	existing, err := r.scan(ctx, entityFilter{Tenant: &tenant, Kind: kind, Keys: keys})
	if err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, row := range existing {
		seen[row.Key] = true
	}
	unique := rows[:0]
	for _, row := range rows {
		if !seen[row.Key] {
			seen[row.Key] = true
			unique = append(unique, row)
		}
	}
	return r.store.publish(ctx, unique, lock)
}
func (r *Repository) CreateSpans(ctx context.Context, tenant uuid.UUID, opts *repository.CreateSpansOpts) error {
	if opts == nil {
		return fmt.Errorf("opts cannot be nil")
	}
	if err := r.validate.Validate(opts); err != nil {
		return err
	}
	var rows []entity
	for _, s := range opts.Spans {
		if s == nil {
			return fmt.Errorf("span cannot be nil")
		}
		var attrs map[string]any
		if len(s.ResourceAttributes) > 0 {
			if err := json.Unmarshal(s.ResourceAttributes, &attrs); err != nil {
				return err
			}
		}
		service := "unknown"
		if v, ok := attrs["service.name"].(string); ok {
			service = v
		}
		kinds := []sqlcv1.V1OtelSpanKind{"UNSPECIFIED", "INTERNAL", "SERVER", "CLIENT", "PRODUCER", "CONSUMER"}
		kind := kinds[0]
		if int(s.Kind) >= 0 && int(s.Kind) < len(kinds) {
			kind = kinds[int(s.Kind)]
		}
		codes := []sqlcv1.V1OtelStatusCode{"UNSET", "OK", "ERROR"}
		code := codes[0]
		if int(s.StatusCode) >= 0 && int(s.StatusCode) < len(codes) {
			code = codes[int(s.StatusCode)]
		}
		at := time.Unix(0, int64(s.StartTimeUnixNano))
		resource, attributes := s.ResourceAttributes, s.Attributes
		if string(resource) == "{}" {
			resource = nil
		}
		if string(attributes) == "{}" {
			attributes = nil
		}
		row := repository.OtelSpanRow{StartTime: timestamp(at), SpanName: s.Name, TraceID: hex.EncodeToString(s.TraceID), SpanID: hex.EncodeToString(s.SpanID), ParentSpanID: nullableText(hex.EncodeToString(s.ParentSpanID)), SpanKind: kind, ServiceName: service, StatusCode: code, StatusMessage: nullableText(s.StatusMessage), ResourceAttributes: resource, SpanAttributes: attributes, ScopeName: nullableText(s.InstrumentationScope), ScopeVersion: nullableText(s.InstrumentationScope), DurationNs: int64(s.EndTimeUnixNano - s.StartTimeUnixNano), RetryCount: s.RetryCount}
		e, err := makeEntity(tenant, "span", row.TraceID+"/"+keyFor(0, at)+"/"+row.SpanID, uuid.Nil, uuid.Nil, 0, at, row)
		if err != nil {
			return err
		}
		rows = append(rows, e)
	}
	return r.storeUnique(ctx, tenant, "span", rows)
}
func (r *Repository) CreateSpanLookupTableEntries(ctx context.Context, tenant uuid.UUID, opts *repository.CreateSpansOpts) error {
	if opts == nil {
		return fmt.Errorf("opts cannot be nil")
	}
	if err := r.validate.Validate(opts); err != nil {
		return err
	}
	var rows []entity
	seen := make(map[uuid.UUID]bool)
	for _, s := range opts.Spans {
		if s == nil {
			return fmt.Errorf("span cannot be nil")
		}
		if s.TaskRunExternalID == nil {
			continue
		}
		ids := []uuid.UUID{*s.TaskRunExternalID}
		if s.WorkflowRunID != nil {
			ids = append(ids, *s.WorkflowRunID)
		}
		for _, id := range ids {
			if seen[id] {
				continue
			}
			seen[id] = true
			at := time.Unix(0, int64(s.StartTimeUnixNano))
			v := traceLookup{Retry: s.RetryCount, Trace: s.TraceID, Start: at}
			e, err := makeEntity(tenant, "trace_lookup", fmt.Sprintf("%s/%d/%d", id, s.RetryCount, at.UnixMicro()), id, id, 0, at, v)
			if err != nil {
				return err
			}
			rows = append(rows, e)
		}
	}
	return r.storeUnique(ctx, tenant, "trace_lookup", rows)
}
func (r *Repository) LookUpTraceId(ctx context.Context, tenant, id uuid.UUID) ([]byte, error) {
	rows, err := scanValues[traceLookup](ctx, r, entityFilter{Tenant: &tenant, Kind: "trace_lookup", ExternalIDs: []uuid.UUID{id}})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, pgx.ErrNoRows
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Retry != rows[j].Retry {
			return rows[i].Retry > rows[j].Retry
		}
		return rows[i].Start.After(rows[j].Start)
	})
	return rows[0].Trace, nil
}
func (r *Repository) ListSpansByTraceId(ctx context.Context, tenant uuid.UUID, trace []byte, offset, limit int64) (*repository.ListSpansResult, error) {
	rows, err := scanValues[repository.OtelSpanRow](ctx, r, entityFilter{Tenant: &tenant, Kind: "span", Predicate: "JSONExtractString(body,'TraceID') = ?", Arguments: []any{hex.EncodeToString(trace)}})
	if err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].StartTime.Time.Before(rows[j].StartTime.Time) })
	rows, err = page(rows, limit, offset)
	if err != nil {
		return nil, err
	}
	return &repository.ListSpansResult{Rows: rows, Total: int64(len(rows))}, nil
}
