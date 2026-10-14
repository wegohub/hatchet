package extra

import (
	"container/list"
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/sync/singleflight"

	authtoken "github.com/hatchet-dev/hatchet/pkg/auth/token"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// cachedToken 只保存鉴权需要的身份和期限，不保存 bearer token 或签名密钥。
type cachedToken struct {
	// id 是数据库 token ID，也是缓存索引；不保存明文 bearer token。
	id uuid.UUID
	// tenant 是已经核验存在的数据库归属。
	tenant uuid.UUID
	// expiresAt 保留数据库有效期，不能通过缓存延长授权。
	expiresAt time.Time
	// validUntil 是包含抖动的回源期限，命中不续期。
	validUntil time.Time
}

// cachedTokens 是实例私有的有界 LRU；JWT 验证发生在访问此仓库之前。
// 过期条目在读取时移除，容量满时淘汰最久未访问的条目，不需要清理 goroutine。
type cachedTokens struct {
	v1.APITokenRepository
	// tenants 用于首次回源时确认身份归属和租户存在性。
	tenants v1.TenantRepository
	// ttl 是缓存期限上限，默认五分钟。
	ttl time.Duration
	// capacity 限制记录数；索引和 LRU 必须始终保持同样的条目数量。
	capacity int
	// loads 按 token ID 合并并发回源，回调由实例而非首个请求持有。
	loads singleflight.Group
	// loadCtx 和 loadSlots 属于实例，单个请求退出不能取消其他等待者的回源。
	loadCtx context.Context
	// cancel 取消实例的所有回源，关闭前必须调用。
	cancel context.CancelFunc
	// loadSlots 限制实际 SQL 并发，客户端提前取消不能绕过该预算。
	loadSlots chan struct{}
	// pending 登记已准入的回源；关闭设置 closed 后才能 Wait。
	pending sync.WaitGroup
	// closed 禁止新的回源和缓存写入，受 mu 保护。
	closed bool
	// mu 同时保护索引和 LRU 顺序；数据库 I/O 和业务回调不能持有此锁。
	mu sync.Mutex
	// entries 直接索引 LRU 节点，命中与淘汰均为常数时间操作。
	entries map[uuid.UUID]*list.Element
	// order 的头部为最近访问，尾部为最久未访问的记录。
	order list.List
}

// withTokenCache 为 Server 创建独立缓存，不改变调用方持有的鉴权器。
func (a *hatchetAuthenticator) withTokenCache(ttl time.Duration, capacity, maxLoads int) (Authenticator, error) {
	loadCtx, cancel := context.WithCancel(context.Background())
	repo := &cachedTokens{
		APITokenRepository: a.tokens,
		tenants:            a.tenants,
		ttl:                ttl,
		capacity:           capacity,
		entries:            make(map[uuid.UUID]*list.Element, min(capacity, 1024)),
		loadCtx:            loadCtx,
		cancel:             cancel,
		loadSlots:          make(chan struct{}, maxLoads),
	}
	manager, err := authtoken.NewJWTManager(a.keys, &snapshotRepository{APITokenRepository: repo}, a.opts)
	if err != nil {
		cancel()
		return nil, err
	}
	return &hatchetAuthenticator{
		manager: manager,
		tenants: a.tenants,
		keys:    a.keys,
		tokens:  a.tokens,
		opts:    a.opts,
		cache:   repo,
	}, nil
}

// read 读取独立记录快照；访问更新 LRU 顺序但不延长有效期。
func (r *cachedTokens) read(id uuid.UUID) *sqlcv1.APIToken {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	element := r.entries[id]
	if element == nil {
		return nil
	}
	token := element.Value.(cachedToken)
	now := time.Now()
	if !now.Before(token.validUntil) || !now.Before(token.expiresAt) {
		delete(r.entries, id)
		r.order.Remove(element)
		return nil
	}
	// 访问更新 LRU 顺序但不续期，热点 token 的撤销仍能在 TTL 上限内生效。
	r.order.MoveToFront(element)
	return &sqlcv1.APIToken{
		ID:       id,
		TenantId: &token.tenant,
		ExpiresAt: pgtype.Timestamp{
			Time:  token.expiresAt,
			Valid: true,
		},
	}
}

// store 在同一把锁内更新索引与 LRU，容量满时淘汰最久未访问的记录。
func (r *cachedTokens) store(token cachedToken) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if element := r.entries[token.id]; element != nil {
		element.Value = token
		r.order.MoveToFront(element)
		return
	}
	if len(r.entries) == r.capacity {
		oldest := r.order.Back()
		delete(r.entries, oldest.Value.(cachedToken).id)
		r.order.Remove(oldest)
	}
	r.entries[token.id] = r.order.PushFront(token)
}

// GetAPITokenById 读取指定 token 记录，缓存与并发合并不得改变记录归属和有效期。
func (r *cachedTokens) GetAPITokenById(ctx context.Context, id uuid.UUID) (*sqlcv1.APIToken, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if row := r.read(id); row != nil {
		return row, nil
	}
	result := r.loads.DoChan(id.String(), func() (any, error) {
		// 同一 token 的并发未命中只回源一次；排队期间可能已填充缓存。
		if row := r.read(id); row != nil {
			return row, nil
		}
		// 注册与关闭用同一把锁，关闭开始后不能再向 WaitGroup 添加回源。
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return nil, errors.New("authentication cache closed")
		}
		r.pending.Add(1)
		r.mu.Unlock()
		defer r.pending.Done()
		select {
		case r.loadSlots <- struct{}{}:
			defer func() {
				<-r.loadSlots
			}()
		default:
			return nil, errors.New("authentication query capacity exhausted")
		}
		budget, cancel := context.WithTimeout(r.loadCtx, 5*time.Second)
		defer cancel()
		row, err := r.APITokenRepository.GetAPITokenById(budget, id)
		if err != nil {
			return nil, err
		}
		if row == nil || row.ID != id {
			return nil, errors.New("token record identity mismatch")
		}
		if !row.Revoked && row.TenantId != nil && *row.TenantId != uuid.Nil && row.ExpiresAt.Valid && time.Now().Before(row.ExpiresAt.Time) {
			// 同时确认租户存在性，缓存命中时两张表都不查询。失败不缓存。
			tenant, err := r.tenants.GetTenantByID(budget, *row.TenantId)
			if err != nil {
				return nil, err
			}
			if tenant == nil || tenant.ID != *row.TenantId {
				return nil, errors.New("tenant record identity mismatch")
			}
			// 例如配置 5 分钟时，实际期限为 4–5 分钟，分散批量 token 的回源。
			lifetime := r.ttl
			if jitter := r.ttl / 5; jitter > 0 {
				// #nosec G404 -- 随机值只分散过期时刻，不参与 token、密钥或授权判断。
				lifetime -= time.Duration(rand.Int64N(int64(jitter)))
			}
			until := time.Now().Add(lifetime)
			if row.ExpiresAt.Time.Before(until) {
				until = row.ExpiresAt.Time
			}
			r.store(cachedToken{
				id:         id,
				tenant:     *row.TenantId,
				expiresAt:  row.ExpiresAt.Time,
				validUntil: until,
			})
		}
		return row, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case loaded := <-result:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if loaded.Err != nil {
			return nil, loaded.Err
		}
		row := loaded.Val.(*sqlcv1.APIToken)
		// 合并请求不能共享可修改的行或 tenant 指针。
		cloned := *row
		if row.TenantId != nil {
			id := *row.TenantId
			cloned.TenantId = &id
		}
		return &cloned, nil
	}
}

// close 取消实例回源并确认退出，再释放缓存；没有每条 token 的清理 goroutine。
func (r *cachedTokens) close() {
	r.mu.Lock()
	r.closed = true
	r.cancel()
	r.mu.Unlock()
	r.pending.Wait()
	r.mu.Lock()
	r.entries = nil
	r.order.Init()
	r.mu.Unlock()
}
