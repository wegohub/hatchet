package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/google/uuid"
)

type keeper struct {
	mu              sync.Mutex
	config          Config
	invalid, closed bool
	conn            *zk.Conn
	root            string
	timeout         time.Duration
	acl             []zk.ACL
}

type pending struct {
	Batch    uuid.UUID `json:"batch"`
	Sequence uint64    `json:"sequence"`
	Rows     uint64    `json:"rows"`
	Digest   string    `json:"digest"`
}

type head struct {
	Published uint64   `json:"published"`
	Pending   *pending `json:"pending,omitempty"`
}

func connectKeeper(ctx context.Context, cfg Config) (*keeper, error) {
	conn, events, err := zk.Connect(cfg.KeeperAddresses, cfg.KeeperSessionTimeout, zk.WithLogInfo(false))
	if err != nil {
		return nil, fmt.Errorf("connect Keeper: %w", err)
	}
	k := &keeper{config: cfg, conn: conn, root: cfg.KeeperRoot, timeout: cfg.KeeperOperationTimeout, acl: zk.WorldACL(zk.PermAll)}
	ctx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			conn.Close()
			return nil, fmt.Errorf("connect Keeper: %w", ctx.Err())
		case e, ok := <-events:
			if !ok {
				conn.Close()
				return nil, errors.New("Keeper connection closed before establishing session")
			}
			if e.State == zk.StateHasSession {
				if cfg.KeeperAuth != "" {
					if err := conn.AddAuth("digest", []byte(cfg.KeeperAuth)); err != nil {
						conn.Close()
						return nil, fmt.Errorf("Keeper authentication: %w", err)
					}
					parts := strings.SplitN(cfg.KeeperAuth, ":", 2)
					if len(parts) != 2 {
						conn.Close()
						return nil, errors.New("Keeper authentication must contain username and password")
					}
					k.acl = zk.DigestACL(zk.PermAll, parts[0], parts[1])
				}
				return k, nil
			}
		}
	}
}

// call bounds operations without abandoning a mutating request on a live session.
// An uncertain operation invalidates the session, fencing every lease it owns.
func keeperCall[T any](ctx context.Context, k *keeper, fn func(*zk.Conn) (T, error)) (T, error) {
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, err
	}
	conn, err := k.connection(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)
	go func() { value, err := fn(conn); done <- result{value, err} }()
	timer := time.NewTimer(k.timeout)
	defer timer.Stop()
	select {
	case r := <-done:
		return r.value, r.err
	case <-ctx.Done():
		k.invalidate(conn)
		var zero T
		return zero, ctx.Err()
	case <-timer.C:
		k.invalidate(conn)
		var zero T
		return zero, errors.New("Keeper operation timed out; session closed")
	}
}

func (k *keeper) multi(ctx context.Context, ops ...interface{}) error {
	_, err := keeperCall(ctx, k, func(conn *zk.Conn) ([]zk.MultiResponse, error) { return conn.Multi(ops...) })
	return err
}

func (k *keeper) initialize(ctx context.Context) error {
	parts := strings.Split(strings.TrimPrefix(k.root, "/"), "/")
	p := ""
	for _, name := range append(parts, "locks") {
		p += "/" + name
		_, err := keeperCall(ctx, k, func(conn *zk.Conn) (string, error) { return conn.Create(p, nil, 0, k.acl) })
		if err != nil && !errors.Is(err, zk.ErrNodeExists) {
			return err
		}
	}
	for name, value := range map[string][]byte{"head": []byte(`{"published":0}`), "log_id": []byte("0")} {
		_, err := keeperCall(ctx, k, func(conn *zk.Conn) (string, error) { return conn.Create(k.root+"/"+name, value, 0, k.acl) })
		if err != nil && !errors.Is(err, zk.ErrNodeExists) {
			return err
		}
	}
	return nil
}

func (k *keeper) bindNamespace(ctx context.Context, database string) error {
	_, err := keeperCall(ctx, k, func(conn *zk.Conn) (string, error) {
		return conn.Create(k.root+"/database", []byte(database), 0, k.acl)
	})
	if err != nil && !errors.Is(err, zk.ErrNodeExists) {
		return err
	}
	value, err := k.get(ctx, "database")
	if err != nil {
		return err
	}
	if string(value.data) != database {
		return errors.New("Keeper namespace is already assigned to another database")
	}
	return nil
}

type keeperValue struct {
	data []byte
	stat *zk.Stat
}

func (k *keeper) get(ctx context.Context, name string) (keeperValue, error) {
	return keeperCall(ctx, k, func(conn *zk.Conn) (keeperValue, error) {
		data, stat, err := conn.Get(k.root + "/" + name)
		return keeperValue{data, stat}, err
	})
}

func (k *keeper) readHead(ctx context.Context) (head, int32, error) {
	for {
		v, err := k.get(ctx, "head")
		if err != nil {
			return head{}, 0, err
		}
		if err = k.multi(ctx, &zk.CheckVersionRequest{Path: k.root + "/head", Version: v.stat.Version}); errors.Is(err, zk.ErrBadVersion) {
			continue
		}
		if err != nil {
			return head{}, 0, err
		}
		var h head
		if err = json.Unmarshal(v.data, &h); err != nil {
			return head{}, 0, fmt.Errorf("invalid Keeper publication head: %w", err)
		}
		if h.Pending != nil && (h.Published == math.MaxUint64 || h.Pending.Sequence != h.Published+1) {
			return head{}, 0, errors.New("invalid pending publication sequence")
		}
		return h, v.stat.Version, nil
	}
}

type lease struct {
	k            *keeper
	owner, proof string
}

func (k *keeper) tryLock(ctx context.Context, name string) (*lease, error) {
	owner := k.root + "/locks/" + name
	proof := owner + "-" + uuid.NewString()
	err := k.multi(ctx,
		&zk.CreateRequest{Path: owner, Flags: zk.FlagEphemeral, Acl: k.acl},
		&zk.CreateRequest{Path: proof, Flags: zk.FlagEphemeral, Acl: k.acl})
	if err != nil {
		return nil, err
	}
	return &lease{k, owner, proof}, nil
}

func (k *keeper) lock(ctx context.Context, name string) (*lease, error) {
	for {
		l, err := k.tryLock(ctx, name)
		if !errors.Is(err, zk.ErrNodeExists) {
			return l, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (l *lease) check() *zk.CheckVersionRequest {
	return &zk.CheckVersionRequest{Path: l.proof, Version: 0}
}

func (l *lease) release() error {
	ctx, cancel := context.WithTimeout(context.Background(), l.k.timeout)
	defer cancel()
	return l.k.multi(ctx, l.check(), &zk.DeleteRequest{Path: l.owner, Version: 0}, &zk.DeleteRequest{Path: l.proof, Version: 0})
}

func (k *keeper) setHead(ctx context.Context, h head, version int32, leases ...*lease) error {
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	ops := make([]interface{}, 0, len(leases)+2)
	for _, l := range leases {
		ops = append(ops, l.check())
	}
	ops = append(ops, &zk.CheckVersionRequest{Path: k.root + "/head", Version: version}, &zk.SetDataRequest{Path: k.root + "/head", Data: data, Version: version})
	return k.multi(ctx, ops...)
}

func (k *keeper) nextLogIDs(ctx context.Context, count uint64) (uint64, error) {
	if count == 0 {
		return 0, nil
	}
	for {
		v, err := k.get(ctx, "log_id")
		if err != nil {
			return 0, err
		}
		var current uint64
		if err := json.Unmarshal(v.data, &current); err != nil {
			return 0, err
		}
		if current > math.MaxInt64 || count > math.MaxInt64-current {
			return 0, errors.New("log identifier sequence exhausted")
		}
		data, _ := json.Marshal(current + count)
		err = k.multi(ctx, &zk.SetDataRequest{Path: k.root + "/log_id", Data: data, Version: v.stat.Version})
		if errors.Is(err, zk.ErrBadVersion) {
			continue
		}
		return current + 1, err
	}
}

// Reconnection creates a fresh session; unique lease proofs fence every prior owner.
func (k *keeper) connection(ctx context.Context) (*zk.Conn, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed {
		return nil, zk.ErrConnectionClosed
	}
	if k.invalid {
		next, err := connectKeeper(ctx, k.config)
		if err != nil {
			return nil, err
		}
		k.conn = next.conn
		k.invalid = false
	}
	return k.conn, nil
}
func (k *keeper) invalidate(conn *zk.Conn) {
	k.mu.Lock()
	defer k.mu.Unlock()
	conn.Close()
	if k.conn == conn {
		k.invalid = true
	}
}
func (k *keeper) close() { k.mu.Lock(); defer k.mu.Unlock(); k.closed = true; k.conn.Close() }
