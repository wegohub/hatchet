package tidb

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	metrics "github.com/hatchet-dev/hatchet/pkg/integrations/metrics/prometheus"
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
	monitorCancel            context.CancelFunc
	monitorDone              chan struct{}
}

type Options struct {
	PayloadStore                 repository.PayloadStoreRepository
	Tasks                        repository.TaskRepository
	OLAPRetention, CoreRetention time.Duration
	StatusUpdateLimits           repository.StatusUpdateBatchSizeLimits
}

func New(ctx context.Context, cfg Config, opts Options) (*Repository, *Logs, error) {
	if opts.OLAPRetention <= 0 || opts.CoreRetention <= 0 {
		return nil, nil, fmt.Errorf("TiDB retention periods must be positive")
	}
	db, err := open(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	if err = validateSchema(ctx, db); err != nil {
		db.Close()
		return nil, nil, err
	}
	timeout := cfg.QueryTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	s := &store{db: db, coordinator: &coordinator{db: db}, queryTimeout: timeout, tiFlashTimeout: cfg.TiFlashQueryTimeout, writeConcurrency: cfg.WriteConcurrency}
	r := &Repository{stateWriter: newStateWriter(s), payloadStore: opts.PayloadStore, retention: opts.OLAPRetention, coreRetention: opts.CoreRetention, limits: opts.StatusUpdateLimits, validate: validator.NewDefaultValidator()}
	r.logs = newLogs(s, opts.Tasks, opts.CoreRetention, timeout)
	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	s.plans.ctx = monitorCtx
	r.monitorCancel = monitorCancel
	r.monitorDone = make(chan struct{})
	go r.monitorTiFlash(monitorCtx)
	return r, r.logs, nil
}
func (r *Repository) Close() error {
	r.closeOnce.Do(func() {
		r.monitorCancel()
		<-r.monitorDone
		r.store.plans.stop()
		_ = r.logs.Close()
		r.closeErr = r.store.db.Close()
	})
	return r.closeErr
}

func (r *Repository) monitorTiFlash(ctx context.Context) {
	defer close(r.monitorDone)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	var lastStats sql.DBStats
	for {
		stats := r.store.db.Stats()
		metrics.OLAPTiDBPoolWaits.Add(float64(stats.WaitCount - lastStats.WaitCount))
		metrics.OLAPTiDBPoolWaitSeconds.Add((stats.WaitDuration - lastStats.WaitDuration).Seconds())
		metrics.OLAPTiDBPoolConnections.WithLabelValues("in_use").Set(float64(stats.InUse))
		metrics.OLAPTiDBPoolConnections.WithLabelValues("idle").Set(float64(stats.Idle))
		lastStats = stats
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		statuses, err := tiFlashStatusDB(probeCtx, r.store.db)
		cancel()
		if err == nil {
			for _, status := range statuses {
				available := 0.0
				if status.Available {
					available = 1
				}
				metrics.OLAPTiFlashReplicaAvailable.WithLabelValues(status.Table).Set(available)
				metrics.OLAPTiFlashReplicaProgress.WithLabelValues(status.Table).Set(status.Progress)
			}
		} else {
			for _, table := range analyticalTables {
				metrics.OLAPTiFlashReplicaAvailable.WithLabelValues(table).Set(0)
				metrics.OLAPTiFlashReplicaProgress.WithLabelValues(table).Set(0)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Repository) PayloadStore() repository.PayloadStoreRepository { return r.payloadStore }
func (r *Repository) StatusUpdateBatchSizeLimits() repository.StatusUpdateBatchSizeLimits {
	return r.limits
}
func (r *Repository) SetReadReplicaPool(*pgxpool.Pool) {}
func (r *Repository) AnalyzeOLAPTables(ctx context.Context) error {
	for _, table := range analyticalTables {
		if _, err := r.store.db.ExecContext(ctx, "ANALYZE TABLE "+table); err != nil {
			return err
		}
	}
	return nil
}
func (r *Repository) UpdateTaskStatuses(ctx context.Context, tenants []uuid.UUID) (bool, []repository.UpdateTaskStatusRow, error) {
	more, result, err := r.compensate(ctx, tenants, "task")
	if err != nil {
		return false, nil, err
	}
	return more, result.TaskRows, nil
}
func (r *Repository) UpdateDAGStatuses(ctx context.Context, tenants []uuid.UUID) (bool, []repository.UpdateDAGStatusRow, error) {
	more, result, err := r.compensate(ctx, tenants, "dag")
	if err != nil {
		return false, nil, err
	}
	return more, result.DAGRows, nil
}
func (r *Repository) CountOLAPTempTableSizeForDAGStatusUpdates(ctx context.Context) (int64, error) {
	return r.countPending(ctx, nil, "dag")
}
func (r *Repository) CountOLAPTempTableSizeForTaskStatusUpdates(ctx context.Context) (int64, error) {
	return r.countPending(ctx, nil, "task")
}

func (r *Repository) snapshot(ctx context.Context) (context.Context, func(), error) {
	if _, ok := ctx.Value(transactionKey{}).(*sql.Tx); ok {
		return ctx, func() {}, nil
	}
	timeout := r.store.queryTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	start := time.Now()
	tx, err := r.store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	observePhase("read_pool_begin", start)
	if err != nil {
		cancel()
		return ctx, func() {}, err
	}
	return context.WithValue(ctx, transactionKey{}, tx), func() { _ = tx.Rollback(); cancel() }, nil
}
func (r *Repository) scan(ctx context.Context, f entityFilter) ([]entity, error) {
	ctx, release, err := r.snapshot(ctx)
	defer release()
	if err != nil {
		return nil, err
	}
	cutoff := time.Now().UTC().Add(-r.retention).Truncate(24 * time.Hour)
	if f.After == nil || f.After.Before(cutoff) {
		f.After = &cutoff
	}
	return r.store.read(ctx, 0, f)
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
