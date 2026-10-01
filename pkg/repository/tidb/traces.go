package tidb

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	seen := map[string]bool{}
	unique := make([]entity, 0, len(rows))
	for _, row := range rows {
		key := row.Key + row.InsertedAt.String()
		if !seen[key] {
			seen[key] = true
			unique = append(unique, row)
		}
	}
	return r.store.publish(ctx, unique)
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
	rows, err := scanValues[traceLookup](ctx, r, entityFilter{Tenant: &tenant, Kind: "trace_lookup", ExternalIDs: []uuid.UUID{id}, Order: "e.retry_count DESC,e.start_time DESC", Limit: int64Pointer(1)})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, pgx.ErrNoRows
	}

	return rows[0].Trace, nil
}
func (r *Repository) ListSpansByTraceId(ctx context.Context, tenant uuid.UUID, trace []byte, offset, limit int64) (*repository.ListSpansResult, error) {
	if limit < 0 || offset < 0 {
		return nil, fmt.Errorf("pagination must be non-negative")
	}
	if limit == 0 {
		return &repository.ListSpansResult{}, nil
	}
	rows, err := scanValues[repository.OtelSpanRow](ctx, r, entityFilter{Tenant: &tenant, Kind: "span", Where: "e.trace_id=?", Args: []any{hex.EncodeToString(trace)}, Order: "e.start_time ASC,e.entity_key ASC", Limit: &limit, Offset: &offset})
	if err != nil {
		return nil, err
	}
	return &repository.ListSpansResult{Rows: rows, Total: int64(len(rows))}, nil
}

func int64Pointer(v int64) *int64 { return &v }
