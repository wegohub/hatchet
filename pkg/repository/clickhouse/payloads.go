package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/sync/errgroup"
)

func (r *Repository) PutPayloads(ctx context.Context, _ sqlcv1.DBTX, tenant uuid.UUID, opts ...repository.StoreOLAPPayloadOpts) error {
	lock, err := r.store.keeper.lock(ctx, "payloads-"+tenant.String())
	if err != nil {
		return err
	}
	defer lock.release()
	state, err := r.loadPayloadState(ctx, tenant)
	if err != nil {
		return err
	}
	for _, opt := range opts {
		if err = state.payload(tenant, opt.ExternalId, opt.ExternalId, opt.InsertedAt.Time, opt.Payload); err != nil {
			return err
		}
	}
	return r.store.publish(ctx, state.rows(), lock)
}
func (r *Repository) loadPayloadState(ctx context.Context, tenant uuid.UUID) (*runState, error) {
	s, err := r.stateWriter.load(ctx, tenant, nil)
	if err != nil {
		return nil, err
	}
	rows, err := r.scan(ctx, entityFilter{Tenant: &tenant, Kind: "payload"})
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		p, err := decodeEntity[storedPayload](row)
		if err != nil {
			return nil, err
		}
		s.payloads[row.Key] = p
	}
	return s, nil
}
func (r *Repository) ReadPayload(ctx context.Context, tenant uuid.UUID, opt repository.ReadOLAPPayloadOpts) ([]byte, error) {
	rows, err := scanValues[storedPayload](ctx, r, entityFilter{Tenant: &tenant, Kind: "payload", ExternalIDs: []uuid.UUID{opt.ExternalId}})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	p := rows[0]
	if p.ExternalKey == "" {
		return p.Inline, nil
	}
	if r.payloadStore == nil || !r.payloadStore.ExternalStoreEnabled() {
		return nil, nil
	}
	q := repository.RetrieveFromExternalOpts{Method: repository.RetrieveFromExternalByKey, ByKey: &repository.RetrieveFromExternalByKeyOpt{Key: repository.ExternalPayloadLocationKey(p.ExternalKey)}}
	if p.IndexFile {
		q = repository.RetrieveFromExternalOpts{Method: repository.RetrieveFromExternalByIndexFile, ByIndexFile: &repository.RetrieveFromExternalByIndexFileOpt{IndexFileKey: repository.ExternalIndexFileLocationKey(p.ExternalKey), ExternalId: opt.ExternalId}}
	}
	values, err := r.payloadStore.RetrieveFromExternal(ctx, q)
	return values[q], err
}
func (r *Repository) OffloadPayloads(ctx context.Context, tenant uuid.UUID, opts []repository.OffloadPayloadOpts) error {
	lock, err := r.store.keeper.lock(ctx, "payloads-"+tenant.String())
	if err != nil {
		return err
	}
	defer lock.release()
	rows, err := r.scan(ctx, entityFilter{Tenant: &tenant, Kind: "payload"})
	if err != nil {
		return err
	}
	byID := make(map[uuid.UUID]entity)
	for _, row := range rows {
		byID[row.ExternalID] = row
	}
	var updates []entity
	for _, opt := range opts {
		row, ok := byID[opt.ExternalId]
		if !ok {
			continue
		}
		row.Body = mustPayloadJSON(storedPayload{ExternalKey: opt.ExternalLocationKey})
		updates = append(updates, row)
	}
	return r.store.publish(ctx, updates, lock)
}
func mustPayloadJSON(p storedPayload) string { b, _ := json.Marshal(p); return string(b) }
func (r *Repository) ProcessOLAPPayloadCutovers(ctx context.Context, enabled bool, ttl *time.Duration, size, concurrency int32, _ bool) error {
	if !enabled {
		return nil
	}
	if ttl == nil {
		return fmt.Errorf("inline store TTL is not set")
	}
	if size <= 0 || concurrency <= 0 {
		return fmt.Errorf("payload cutover batch size and concurrency must be positive")
	}
	if r.payloadStore == nil || r.payloadStore.ExternalStore() == nil {
		return fmt.Errorf("external payload store is not configured")
	}
	lock, err := r.store.keeper.lock(ctx, "payload-cutover")
	if err != nil {
		return err
	}
	defer lock.release()
	rows, err := r.scan(ctx, entityFilter{Kind: "payload"})
	if err != nil {
		return err
	}
	cutoff := time.Now().UTC().Add(-*ttl - 2*time.Hour).Truncate(24 * time.Hour)
	eligible := make([]entity, 0, len(rows))
	for _, row := range rows {
		p, err := decodeEntity[storedPayload](row)
		if err != nil {
			return err
		}
		if p.ExternalKey == "" && row.InsertedAt.Before(cutoff) {
			eligible = append(eligible, row)
		}
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(int(concurrency))
	for start := 0; start < len(eligible); start += int(size) {
		selected := append([]entity(nil), eligible[start:min(start+int(size), len(eligible))]...)
		group.Go(func() error {
			values := make([]repository.OffloadToExternalStoreOpts, 0, len(selected))
			for _, row := range selected {
				p, err := decodeEntity[storedPayload](row)
				if err != nil {
					return err
				}
				values = append(values, repository.OffloadToExternalStoreOpts{TenantId: row.Tenant, ExternalID: row.ExternalID, InsertedAt: timestamp(row.InsertedAt), Payload: p.Inline})
			}
			key, err := r.payloadStore.ExternalStore().Store(groupCtx, values...)
			if err != nil {
				return err
			}
			if key == nil {
				return fmt.Errorf("external payload store returned no index file")
			}
			for i := range selected {
				selected[i].Body = mustPayloadJSON(storedPayload{ExternalKey: string(*key), IndexFile: true})
			}
			return r.store.publish(groupCtx, selected, lock)
		})
	}
	return group.Wait()
}

func timestamp(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: true} }
