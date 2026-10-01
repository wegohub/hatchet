package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/validator"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ repository.OLAPRepository = (*Repository)(nil)

type Repository struct {
	*stateWriter
	payloadStore             repository.PayloadStoreRepository
	retention, coreRetention time.Duration
	limits                   repository.StatusUpdateBatchSizeLimits
	validate                 validator.Validator
	logs                     *Logs
	closeOnce                sync.Once
	closeErr                 error
}

type Options struct {
	PayloadStore                 repository.PayloadStoreRepository
	Tasks                        repository.TaskRepository
	OLAPRetention, CoreRetention time.Duration
	StatusUpdateLimits           repository.StatusUpdateBatchSizeLimits
}

func New(ctx context.Context, cfg Config, opts Options) (*Repository, *Logs, error) {
	if opts.OLAPRetention <= 0 || opts.CoreRetention <= 0 {
		return nil, nil, fmt.Errorf("ClickHouse retention periods must be positive")
	}
	conn, err := open(ctx, cfg, cfg.Database)
	if err != nil {
		return nil, nil, err
	}
	if err = validateSchema(ctx, conn, cfg.KeeperRoot); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if len(cfg.Addresses) > 1 {
		reference, err := replicaPaths(ctx, conn)
		if err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
		for _, address := range cfg.Addresses {
			nodeCfg := cfg
			nodeCfg.Addresses = []string{address}
			node, err := open(ctx, nodeCfg, cfg.Database)
			if err != nil {
				_ = conn.Close()
				return nil, nil, err
			}
			checkErr := validateSchema(ctx, node, cfg.KeeperRoot)
			if checkErr == nil {
				var paths string
				paths, checkErr = replicaPaths(ctx, node)
				if checkErr == nil && paths != reference {
					checkErr = errors.New("ClickHouse addresses must belong to the same replicated shard")
				}
			}
			_ = node.Close()
			if checkErr != nil {
				_ = conn.Close()
				return nil, nil, checkErr
			}
		}
	}
	k, err := connectKeeper(ctx, cfg)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	failed := true
	defer func() {
		if failed {
			k.close()
			_ = conn.Close()
		}
	}()
	value, err := k.get(ctx, "database")
	if err != nil {
		return nil, nil, err
	}
	if string(value.data) != cfg.Database {
		return nil, nil, errors.New("Keeper namespace does not match ClickHouse database")
	}
	s := &store{conn: conn, keeper: k}
	pub, err := k.lock(ctx, "publication")
	if err != nil {
		return nil, nil, err
	}
	_, _, err = s.recover(ctx, pub)
	_ = pub.release()
	if err != nil {
		return nil, nil, err
	}
	r := &Repository{stateWriter: newStateWriter(s), payloadStore: opts.PayloadStore, retention: opts.OLAPRetention, coreRetention: opts.CoreRetention, limits: opts.StatusUpdateLimits, validate: validator.NewDefaultValidator()}
	r.logs = newLogs(s, opts.Tasks, r, opts.CoreRetention, cfg.QueryTimeout)
	failed = false
	return r, r.logs, nil
}

func (r *Repository) Close() error {
	r.closeOnce.Do(func() { _ = r.logs.Close(); r.store.keeper.close(); r.closeErr = r.store.conn.Close() })
	return r.closeErr
}
func (r *Repository) PayloadStore() repository.PayloadStoreRepository { return r.payloadStore }
func (r *Repository) StatusUpdateBatchSizeLimits() repository.StatusUpdateBatchSizeLimits {
	return r.limits
}
func (r *Repository) SetReadReplicaPool(*pgxpool.Pool)        {}
func (r *Repository) AnalyzeOLAPTables(context.Context) error { return nil }

// Status changes are published synchronously with events, so no deferred queue exists.
func (r *Repository) UpdateTaskStatuses(context.Context, []uuid.UUID) (bool, []repository.UpdateTaskStatusRow, error) {
	return false, nil, nil
}
func (r *Repository) UpdateDAGStatuses(context.Context, []uuid.UUID) (bool, []repository.UpdateDAGStatusRow, error) {
	return false, nil, nil
}
func (r *Repository) CountOLAPTempTableSizeForDAGStatusUpdates(context.Context) (int64, error) {
	return 0, nil
}
func (r *Repository) CountOLAPTempTableSizeForTaskStatusUpdates(context.Context) (int64, error) {
	return 0, nil
}

type snapshotKey struct{}

func (r *Repository) snapshot(ctx context.Context) (context.Context, error) {
	if _, ok := ctx.Value(snapshotKey{}).(uint64); ok {
		return ctx, nil
	}
	seq, err := r.store.barrier(ctx)
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, snapshotKey{}, seq), nil
}
func (r *Repository) scan(ctx context.Context, f entityFilter) ([]entity, error) {
	ctx, err := r.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	cutoff := time.Now().UTC().Add(-r.retention).Truncate(24 * time.Hour)
	if f.After == nil || f.After.Before(cutoff) {
		f.After = &cutoff
	}
	return r.store.read(ctx, ctx.Value(snapshotKey{}).(uint64), f)
}
func scanValues[T any](ctx context.Context, r *Repository, f entityFilter) ([]*T, error) {
	rows, err := r.scan(ctx, f)
	if err != nil {
		return nil, err
	}
	result := make([]*T, 0, len(rows))
	for _, row := range rows {
		v, err := decodeEntity[T](row)
		if err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, nil
}

func replicaPaths(ctx context.Context, conn driver.Conn) (string, error) {
	rows, err := conn.Query(ctx, "SELECT zookeeper_path FROM system.replicas WHERE database=currentDatabase() AND table IN ('schema_version','entities','manifests','commits','log_lines')")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var path string
		if err = rows.Scan(&path); err != nil {
			return "", err
		}
		paths = append(paths, path)
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	sort.Strings(paths)
	return strings.Join(paths, "\n"), nil
}
