package tidb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

var errLockBusy = errors.New("TiDB repository lock busy")

type coordinator struct {
	db         *sql.DB
	knownKeys  sync.Map
	cacheMu    sync.Mutex
	cacheCount int
	local      localRunGates
}
type lease struct {
	tx        *sql.Tx
	done      sync.Once
	onRelease func()
	freshRuns map[uuid.UUID]string
}

func (l *lease) release() error {
	if l == nil {
		return nil
	}
	var err error
	l.done.Do(func() {
		if l.tx != nil {
			err = l.tx.Rollback()
		}
		if l.onRelease != nil {
			l.onRelease()
		}
	})
	return err
}
func (k *coordinator) close() {}
func (k *coordinator) seed(ctx context.Context, keys []string) error {
	return k.seedOn(ctx, k.db, keys)
}
func (k *coordinator) seedOn(ctx context.Context, db sqlExecutor, keys []string) error {
	if len(keys) > 200 {
		for from := 0; from < len(keys); from += 200 {
			if err := k.seedOn(ctx, db, keys[from:min(from+200, len(keys))]); err != nil {
				return err
			}
		}
		return nil
	}
	var unknown []string
	var args []any
	for _, key := range keys {
		if len(key) > 255 {
			return errors.New("TiDB lock key too long")
		}
		if _, ok := k.knownKeys.Load(key); !ok {
			unknown = append(unknown, key)
			args = append(args, []byte(key))
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	existing := map[string]bool{}
	// Existing keys must bypass INSERT so a remote holder is reported by NOWAIT.
	rows, err := db.QueryContext(ctx, "SELECT lock_key FROM v1_olap_run_locks WHERE lock_key IN ("+placeholders(len(unknown))+")", args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		existing[key] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	args = nil
	var values []string
	for _, key := range unknown {
		if !existing[key] {
			var tenant, run any
			if strings.HasPrefix(key, "run-") && len(key) == 77 {
				t, te := uuid.Parse(key[4:40])
				r, re := uuid.Parse(key[41:])
				if te == nil && re == nil {
					tenant, run = uuidArg(t), uuidArg(r)
				}
			}
			values = append(values, "(?,?,?)")
			args = append(args, []byte(key), tenant, run)
		}
	}
	if len(values) > 0 {
		if _, err = db.ExecContext(ctx, "INSERT IGNORE INTO v1_olap_run_locks(lock_key,tenant_id,run_id) VALUES "+strings.Join(values, ","), args...); err != nil {
			return err
		}
	}
	k.cacheMu.Lock()
	if k.cacheCount+len(unknown) > 65536 {
		k.knownKeys.Clear()
		k.cacheCount = 0
	}
	for _, key := range unknown {
		k.knownKeys.Store(key, struct{}{})
		k.cacheCount++
	}
	k.cacheMu.Unlock()
	return nil
}
func (k *coordinator) lock(ctx context.Context, key string) (*lease, error) {
	return k.acquire(ctx, key, false)
}
func (k *coordinator) tryLock(ctx context.Context, key string) (*lease, error) {
	return k.acquire(ctx, key, true)
}
func (k *coordinator) acquire(ctx context.Context, key string, nowait bool) (*lease, error) {
	if err := k.seed(ctx, []string{key}); err != nil {
		return nil, err
	}
	tx, err := k.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	q := "SELECT lock_key FROM v1_olap_run_locks WHERE lock_key=? FOR UPDATE"
	if nowait {
		q += " NOWAIT"
	}
	var got []byte
	if err = tx.QueryRowContext(ctx, q, []byte(key)).Scan(&got); err != nil {
		tx.Rollback()
		if isLockBusy(err) {
			return nil, errLockBusy
		}
		return nil, err
	}
	return &lease{tx: tx}, nil
}
func isLockBusy(err error) bool {
	var m *mysql.MySQLError
	return errors.As(err, &m) && (m.Number == 3572 || m.Number == 1205)
}
func (s *store) runLocks(ctx context.Context, tenant uuid.UUID, runs []uuid.UUID) ([]*lease, map[uuid.UUID]struct{}, error) {
	seen := map[uuid.UUID]bool{}
	var ids []uuid.UUID
	for _, id := range runs {
		if !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	blocked := map[uuid.UUID]struct{}{}
	var keys []string
	var args []any
	for _, id := range ids {
		key := "run-" + tenant.String() + "-" + id.String()
		keys = append(keys, key)
		args = append(args, []byte(key))
	}
	localRelease, err := s.coordinator.local.acquire(ctx, keys)
	if err != nil {
		return nil, nil, err
	}
	permitRelease, err := s.permit(ctx)
	if err != nil {
		localRelease()
		return nil, nil, err
	}
	start := time.Now()
	conn, err := s.db.Conn(ctx)
	observePhase("pool_acquire", start)
	if err != nil {
		permitRelease()
		localRelease()
		return nil, nil, err
	}
	release := func() { _ = conn.Close(); permitRelease(); localRelease() }
	if err = s.coordinator.seedOn(ctx, conn, keys); err != nil {
		release()
		return nil, nil, err
	}
	start = time.Now()
	tx, err := conn.BeginTx(ctx, nil)
	observePhase("pool_begin", start)
	if err != nil {
		release()
		return nil, nil, err
	}
	start = time.Now()
	freshRuns := map[uuid.UUID]string{}
	if len(keys) > 0 {
		found := make(map[string]bool, len(keys))
		initialized := make(map[string]bool, len(keys))
		rows, e := tx.QueryContext(ctx, "SELECT lock_key,initialized FROM v1_olap_run_locks WHERE lock_key IN ("+placeholders(len(keys))+") ORDER BY lock_key FOR UPDATE NOWAIT", args...)
		if e == nil {
			for rows.Next() {
				var key []byte
				var present bool
				e = rows.Scan(&key, &present)
				if e != nil {
					break
				}
				found[string(key)] = true
				initialized[string(key)] = present
			}
			if e == nil {
				e = rows.Err()
			}
			rows.Close()
		}
		if e == nil {
			for i, key := range keys {
				if !found[key] {
					blocked[ids[i]] = struct{}{}
					s.coordinator.knownKeys.Delete(key)
				} else if !initialized[key] {
					freshRuns[ids[i]] = key
				}
			}
		}
		if e != nil {
			tx.Rollback()
			if !isLockBusy(e) {
				release()
				return nil, nil, e
			}
			tx, err = conn.BeginTx(ctx, nil)
			if err != nil {
				release()
				return nil, nil, err
			}
			for i, key := range keys {
				var got []byte
				var initialized bool
				e = tx.QueryRowContext(ctx, "SELECT lock_key,initialized FROM v1_olap_run_locks WHERE lock_key=? FOR UPDATE NOWAIT", []byte(key)).Scan(&got, &initialized)
				if isLockBusy(e) || errors.Is(e, sql.ErrNoRows) {
					blocked[ids[i]] = struct{}{}
					if errors.Is(e, sql.ErrNoRows) {
						s.coordinator.knownKeys.Delete(key)
					}
					continue
				}
				if e != nil {
					tx.Rollback()
					release()
					return nil, nil, e
				}
				if !initialized {
					freshRuns[ids[i]] = key
				}
			}
		}
	}
	observePhase("run_lock", start)
	return []*lease{{tx: tx, onRelease: release, freshRuns: freshRuns}}, blocked, nil
}
func (k *coordinator) nextIDs(ctx context.Context, seq string, count int) ([]int64, error) {
	if count == 0 {
		return nil, nil
	}
	if seq != "event_id_seq" && seq != "log_id_seq" {
		return nil, errors.New("invalid TiDB sequence")
	}
	result := make([]int64, 0, count)
	for from := 0; from < count; from += 200 {
		n := min(200, count-from)
		queries := make([]string, n)
		for i := range queries {
			queries[i] = "SELECT NEXTVAL(" + seq + ")"
		}
		rows, err := executor(ctx, k.db).QueryContext(ctx, strings.Join(queries, " UNION ALL "))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			result = append(result, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	if len(result) != count {
		return nil, fmt.Errorf("TiDB allocated %d IDs, expected %d", len(result), count)
	}
	return result, nil
}
func (k *coordinator) nextEventIDs(ctx context.Context, count int) ([]int64, error) {
	return k.nextIDs(ctx, "event_id_seq", count)
}
func (k *coordinator) nextLogIDs(ctx context.Context, count uint64) (uint64, error) {
	ids, err := k.nextIDs(ctx, "log_id_seq", int(count))
	if err != nil || len(ids) == 0 {
		return 0, err
	}
	for i, id := range ids {
		if id != ids[0]+int64(i) {
			return 0, errors.New("noncontiguous ID allocation; use allocated IDs directly")
		}
	}
	return uint64(ids[0]), nil
}
func isRetryable(err error) bool {
	var m *mysql.MySQLError
	return errors.As(err, &m) && (m.Number == 1213 || m.Number == 1205 || m.Number == 9007 || strings.Contains(m.Message, "retryable"))
}
