package tidb

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/sync/errgroup"
)

func (r *Repository) PutPayloads(ctx context.Context, pgTx sqlcv1.DBTX, tenant uuid.UUID, opts ...repository.StoreOLAPPayloadOpts) error {
	if pgTx != nil {
		return fmt.Errorf("TiDB payload writes cannot join a PostgreSQL transaction")
	}
	state := &runState{payloads: map[string]*storedPayload{}, changes: map[string]entity{}}
	for _, opt := range opts {
		if err := state.payload(tenant, opt.ExternalId, opt.ExternalId, opt.InsertedAt.Time, opt.Payload); err != nil {
			return err
		}
	}
	return r.store.publish(ctx, state.rows())
}

func (r *Repository) ReadPayload(ctx context.Context, tenant uuid.UUID, opt repository.ReadOLAPPayloadOpts) ([]byte, error) {
	if cache, ok := ctx.Value(hydrationKey{}).(*pageHydration); ok {
		if p, found := cache.payloads[opt.ExternalId]; found {
			return p, nil
		}
	}
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
	return r.payloadTransaction(ctx, func(tx *sql.Tx) error {
		for _, opt := range opts {
			if _, err := tx.ExecContext(ctx, "UPDATE v1_payloads_olap SET inline_payload=NULL,external_key=?,index_file=FALSE WHERE tenant_id=? AND external_id=?", opt.ExternalLocationKey, uuidArg(tenant), uuidArg(opt.ExternalId)); err != nil {
				return err
			}
		}
		return nil
	})
}
func mustPayloadJSON(p storedPayload) string { b, _ := json.Marshal(p); return string(b) }

func (r *Repository) payloadTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	release, err := r.store.permit(ctx)
	if err != nil {
		return err
	}
	defer release()
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	id := uuid.New()
	receipt := sha256.Sum256(id[:])
	if err = fn(tx); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO v1_olap_write_receipts(receipt_id) VALUES(?)", receipt[:]); err != nil {
		return err
	}
	if r.store.commitHook != nil {
		err = r.store.commitHook(tx)
	} else {
		err = tx.Commit()
	}
	if err != nil {
		checkCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var n int
		if r.store.db.QueryRowContext(checkCtx, "SELECT 1 FROM v1_olap_write_receipts WHERE receipt_id=?", receipt[:]).Scan(&n) == nil {
			return nil
		}
	}
	return err
}

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
	cutoff := time.Now().UTC().Add(-*ttl - 2*time.Hour).Truncate(24 * time.Hour)
	group, gctx := errgroup.WithContext(ctx)
	group.SetLimit(int(concurrency))
	for i := int32(0); i < concurrency; i++ {
		group.Go(func() error {
			for {
				n, err := r.cutoverBatch(gctx, cutoff, int(size))
				if err != nil {
					return err
				}
				if n == 0 {
					return nil
				}
			}
		})
	}
	return group.Wait()
}
func (r *Repository) cutoverBatch(ctx context.Context, cutoff time.Time, size int) (int, error) {
	job := uuid.New()
	var selected []entity
	err := r.payloadTransaction(ctx, func(tx *sql.Tx) error {
		// Expired upload claims are reclaimable; inline bytes remain the durable copy.
		if _, err := tx.ExecContext(ctx, "DELETE b FROM v1_payloads_olap_offloaded_block_index b JOIN v1_payloads_olap_cutover_job_offset j ON j.job_id=b.job_id WHERE j.state<>'done' AND j.lease_until<=UTC_TIMESTAMP(6)"); err != nil {
			return err
		}
		codec, _ := codecFor("payload")

		rs, err := tx.QueryContext(ctx, "SELECT "+codec.selectColumns+" FROM v1_payloads_olap e WHERE e.inserted_at<? AND e.external_key='' AND NOT EXISTS(SELECT 1 FROM v1_payloads_olap_offloaded_block_index b WHERE b.tenant_id=e.tenant_id AND b.external_id=e.external_id AND b.inserted_at=e.inserted_at) ORDER BY e.inserted_at,e.tenant_id,e.external_id LIMIT ? FOR UPDATE", dateArg(cutoff), size)
		if err != nil {
			return err
		}
		for rs.Next() {
			row, e := scanEntity(rs, "payload")
			if e != nil {
				rs.Close()
				return e
			}
			selected = append(selected, row)
		}
		err = rs.Err()
		rs.Close()
		if err != nil {
			return err
		}
		if len(selected) == 0 {
			return nil
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO v1_payloads_olap_cutover_job_offset(job_id,state,lease_until) VALUES(?,'uploading',?)", uuidArg(job), dateArg(time.Now().Add(5*time.Minute))); err != nil {
			return err
		}
		var values []string
		var args []any
		for _, r := range selected {
			values = append(values, "(?,?,?,?)")
			args = append(args, uuidArg(r.Tenant), uuidArg(r.ExternalID), r.InsertedAt, uuidArg(job))
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO v1_payloads_olap_offloaded_block_index(tenant_id,external_id,inserted_at,job_id) VALUES"+strings.Join(values, ","), args...)
		return err
	})
	if err != nil {
		return 0, err
	}
	if len(selected) == 0 {
		return 0, nil
	}
	values := make([]repository.OffloadToExternalStoreOpts, 0, len(selected))
	for _, row := range selected {
		p, e := decodeEntity[storedPayload](row)
		if e != nil {
			return 0, e
		}
		values = append(values, repository.OffloadToExternalStoreOpts{TenantId: row.Tenant, ExternalID: row.ExternalID, InsertedAt: timestamp(row.InsertedAt), Payload: p.Inline})
	}
	uploadCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	key, err := r.payloadStore.ExternalStore().Store(uploadCtx, values...)
	cancel()
	if err != nil || key == nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = r.store.db.ExecContext(cleanupCtx, "UPDATE v1_payloads_olap_cutover_job_offset SET state='failed',lease_until=UTC_TIMESTAMP(6) WHERE job_id=?", uuidArg(job))
		if err == nil {
			err = fmt.Errorf("external payload store returned no index file")
		}
		return 0, err
	}
	err = r.payloadTransaction(ctx, func(tx *sql.Tx) error {
		var state string
		var until time.Time
		if err := tx.QueryRowContext(ctx, "SELECT state,lease_until FROM v1_payloads_olap_cutover_job_offset WHERE job_id=? FOR UPDATE", uuidArg(job)).Scan(&state, &until); err != nil {
			return err
		}
		if state != "uploading" || until.Before(time.Now()) {
			return fmt.Errorf("payload cutover lease expired")
		}
		for _, row := range selected {
			if _, err := tx.ExecContext(ctx, "UPDATE v1_payloads_olap e JOIN v1_payloads_olap_offloaded_block_index b ON e.tenant_id=b.tenant_id AND e.external_id=b.external_id AND e.inserted_at=b.inserted_at SET e.inline_payload=NULL,e.external_key=?,e.index_file=TRUE WHERE b.job_id=? AND e.tenant_id=? AND e.external_id=? AND e.inserted_at=? AND e.external_key=''", string(*key), uuidArg(job), uuidArg(row.Tenant), uuidArg(row.ExternalID), row.InsertedAt); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, "UPDATE v1_payloads_olap_cutover_job_offset SET state='done',index_file=? WHERE job_id=?", string(*key), uuidArg(job))
		return err
	})
	return len(selected), err
}

func timestamp(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: true} }
