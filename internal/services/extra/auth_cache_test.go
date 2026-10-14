package extra

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	authtoken "github.com/hatchet-dev/hatchet/pkg/auth/token"
	"github.com/hatchet-dev/hatchet/pkg/encryption"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// cachedAuthFixture 仅使用真实 JWT 和可计数数据库，不启动 NATS 或 gRPC。
func cachedAuthFixture(t *testing.T, ttl time.Duration, capacity int) (context.Context, *authRecords, *authtoken.Token, *hatchetAuthenticator) {
	t.Helper()
	master, private, public, clearPublic, err := encryption.GenerateLocalKeys()
	requireOK(t, err)
	keys, err := encryption.NewLocalEncryption(master, private, public)
	requireOK(t, err)
	records := &authRecords{
		id:     uuid.New(),
		tokens: map[uuid.UUID]*sqlcv1.APIToken{},
	}
	const url = "http://localhost:8080"
	manager, err := authtoken.NewJWTManager(keys, records, &authtoken.TokenOpts{
		Issuer:    url,
		Audience:  url,
		ServerURL: url,
	})
	requireOK(t, err)
	token, err := manager.GenerateTenantToken(context.Background(), records.id, "test", false, nil)
	requireOK(t, err)
	auth, err := NewHatchetAuthenticator(clearPublic, url, records, records)
	requireOK(t, err)
	cfg := defaults(Config{
		AuthCacheTTL:        ttl,
		MaxAuthCacheEntries: capacity,
	})
	cached, err := auth.(*hatchetAuthenticator).withTokenCache(cfg.AuthCacheTTL, cfg.MaxAuthCacheEntries, cfg.MaxConcurrentRPCs)
	requireOK(t, err)
	t.Cleanup(cached.(*hatchetAuthenticator).cache.close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx, records, token, cached.(*hatchetAuthenticator)
}

// cacheEntry 在锁内取得值快照，不能让测试与正在鉴权的请求共享可修改状态。
func cacheEntry(t *testing.T, r *cachedTokens, id uuid.UUID) cachedToken {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[id]
	if entry == nil {
		t.Fatal("cache entry missing")
	}
	return entry.Value.(cachedToken)
}

// expireEntry 通过记录快照构造确定的过期边界，避免测试依赖长时间等待。
func expireEntry(t *testing.T, r *cachedTokens, id uuid.UUID) {
	t.Helper()
	entry := cacheEntry(t, r, id)
	entry.validUntil = time.Now().Add(-time.Second)
	r.store(entry)
}

// TestTokenMemoryCache 64 个并发调用只查一次 token 和租户；命中不延长 TTL。
func TestTokenMemoryCache(t *testing.T) {
	ctx, records, token, auth := cachedAuthFixture(t, 0, 0)
	before := time.Now()
	start := make(chan struct{})
	errs := make(chan error, 64)
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			<-start
			id, err := auth.Authenticate(ctx, token.Token)
			if err == nil && id != records.id {
				err = errors.New("incorrect cached tenant")
			}
			errs <- err
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		requireOK(t, err)
	}
	if records.tokenReads.Load() != 1 || records.tenantReads.Load() != 1 {
		t.Fatal("cold burst was not coalesced")
	}
	if auth.cache.ttl != 5*time.Minute || auth.cache.capacity != 32768 {
		t.Fatal("incorrect default TTL or capacity")
	}
	entry := cacheEntry(t, auth.cache, token.TokenId)
	if entry.validUntil.Before(before.Add(4*time.Minute)) || entry.validUntil.After(time.Now().Add(5*time.Minute)) {
		t.Fatal("cache jitter outside bounds")
	}
	// 撤销在缓存到期后生效；非法 JWT 签名仍不能命中身份缓存。
	records.mu.Lock()
	records.tokens[token.TokenId].Revoked = true
	records.mu.Unlock()
	_, err := auth.Authenticate(ctx, token.Token)
	requireOK(t, err)
	_, err = auth.Authenticate(ctx, token.Token+"x")
	if err == nil || records.tokenReads.Load() != 1 {
		t.Fatal("invalid JWT reached database or passed cache")
	}
	if cacheEntry(t, auth.cache, token.TokenId) != entry {
		t.Fatal("cache hit changed validity")
	}
	expireEntry(t, auth.cache, token.TokenId)
	_, err = auth.Authenticate(ctx, token.Token)
	if err == nil || records.tokenReads.Load() != 2 {
		t.Fatal("expired cache did not recheck revocation")
	}
	// 淘汰或进程重建后的空缓存不能复用已撤销身份。
	fresh, err := auth.withTokenCache(auth.cache.ttl, auth.cache.capacity, cap(auth.cache.loadSlots))
	requireOK(t, err)
	t.Cleanup(fresh.(*hatchetAuthenticator).cache.close)
	_, err = fresh.Authenticate(ctx, token.Token)
	if err == nil {
		t.Fatal("independent cache accepted revoked token")
	}
}

// TestTokenCacheExpiryAndFailures 验证真实 TTL 到期、数据库故障不缓存和请求取消。
func TestTokenCacheExpiryAndFailures(t *testing.T) {
	ctx, records, token, auth := cachedAuthFixture(t, 100*time.Millisecond, 2)
	_, err := auth.Authenticate(ctx, token.Token)
	requireOK(t, err)
	eventually(t, func() bool {
		return auth.cache.read(token.TokenId) == nil
	})
	_, err = auth.Authenticate(ctx, token.Token)
	requireOK(t, err)
	if records.tokenReads.Load() != 2 {
		t.Fatal("TTL expiry did not trigger one reload")
	}
	expireEntry(t, auth.cache, token.TokenId)
	records.mu.Lock()
	records.databaseError = errors.New("database unavailable")
	records.mu.Unlock()
	for range 2 {
		_, err = auth.Authenticate(ctx, token.Token)
		if !errors.Is(err, authtoken.ErrCouldNotReadTokenFromDatabase) {
			t.Fatalf("database failure was not preserved: %v", err)
		}
	}
	if records.tokenReads.Load() != 4 {
		t.Fatal("failed verification was cached")
	}
	records.mu.Lock()
	records.databaseError = nil
	records.mu.Unlock()
	_, err = auth.Authenticate(ctx, token.Token)
	requireOK(t, err)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	reads := records.tokenReads.Load()
	_, err = auth.Authenticate(canceled, token.Token)
	if err == nil || records.tokenReads.Load() != reads {
		t.Fatal("canceled request accessed database")
	}
}

// TestTokenCacheValidityAndTenant 缓存不超过数据库有效期，租户验证失败不填充缓存。
func TestTokenCacheValidityAndTenant(t *testing.T) {
	ctx, records, token, auth := cachedAuthFixture(t, 10*time.Minute, 2)
	until := time.Now().Add(30 * time.Second)
	records.mu.Lock()
	records.tokens[token.TokenId].ExpiresAt.Time = until
	records.mu.Unlock()
	_, err := auth.Authenticate(ctx, token.Token)
	requireOK(t, err)
	entry := cacheEntry(t, auth.cache, token.TokenId)
	if !entry.validUntil.Equal(until) || !entry.expiresAt.Equal(until) {
		t.Fatal("cache outlived database token validity")
	}
	expireEntry(t, auth.cache, token.TokenId)
	records.mu.Lock()
	records.tenantMissing = true
	records.mu.Unlock()
	_, err = auth.Authenticate(ctx, token.Token)
	if err == nil {
		t.Fatal("accepted removed tenant on cache miss")
	}
	records.mu.Lock()
	records.tenantMissing = false
	records.mu.Unlock()
	_, err = auth.Authenticate(ctx, token.Token)
	requireOK(t, err)
	if records.tokenReads.Load() != 3 || records.tenantReads.Load() != 3 {
		t.Fatal("failed tenant verification was cached")
	}
	expireEntry(t, auth.cache, token.TokenId)
	records.mu.Lock()
	records.tokens[token.TokenId].ExpiresAt.Time = time.Now().Add(-time.Second)
	records.mu.Unlock()
	_, err = auth.Authenticate(ctx, token.Token)
	if err == nil || auth.cache.read(token.TokenId) != nil {
		t.Fatal("expired token filled the cache")
	}
}

// TestTokenCacheLRU 容量为 2 时访问 A 再插入 C，应淘汰 B；更新 A 不增加条目数。
func TestTokenCacheLRU(t *testing.T) {
	_, _, _, auth := cachedAuthFixture(t, 0, 2)
	r := auth.cache
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	until := time.Now().Add(time.Minute)
	put := func(id uuid.UUID) {
		r.store(cachedToken{
			id:         id,
			tenant:     uuid.New(),
			expiresAt:  until,
			validUntil: until,
		})
	}
	put(a)
	put(b)
	row := r.read(a)
	if row == nil {
		t.Fatal("cache miss for A")
	}
	// 返回行和 tenant 指针都必须独立，调用方修改不能污染后续鉴权。
	*row.TenantId = uuid.Nil
	if *r.read(a).TenantId == uuid.Nil {
		t.Fatal("cache row aliases caller")
	}
	put(c)
	if r.read(b) != nil || r.read(a) == nil || r.read(c) == nil {
		t.Fatal("incorrect LRU eviction")
	}
	put(a)
	r.mu.Lock()
	if len(r.entries) != 2 || r.order.Len() != 2 {
		t.Error("cache exceeded capacity or duplicated entry")
	}
	r.mu.Unlock()
}

// TestTokenCacheConcurrentEviction 验证并发读取和淘汰保持索引、LRU 与容量一致。
func TestTokenCacheConcurrentEviction(t *testing.T) {
	_, _, _, auth := cachedAuthFixture(t, 0, 8)
	r := auth.cache
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			id := uuid.New()
			for range 32 {
				until := time.Now().Add(time.Minute)
				r.store(cachedToken{
					id:         id,
					tenant:     uuid.New(),
					expiresAt:  until,
					validUntil: until,
				})
				_ = r.read(id)
			}
		})
	}
	wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) > r.capacity || r.order.Len() != len(r.entries) {
		t.Fatal("concurrent cache capacity/index inconsistency")
	}
	for element := r.order.Front(); element != nil; element = element.Next() {
		if r.entries[element.Value.(cachedToken).id] != element {
			t.Fatal("LRU and index diverged")
		}
	}
}

// blockingTokens 用屏障控制回源，验证等待者取消不取消另一个仍在等待的请求。
type blockingTokens struct {
	v1.APITokenRepository
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

// GetAPITokenById 读取指定 token 记录，缓存与并发合并不得改变记录归属和有效期。
func (r *blockingTokens) GetAPITokenById(ctx context.Context, id uuid.UUID) (*sqlcv1.APIToken, error) {
	r.once.Do(func() {
		close(r.started)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.release:
		return r.APITokenRepository.GetAPITokenById(ctx, id)
	}
}

// TestTokenCacheCanceledWaiter 验证一个等待者的 deadline 不取消另一个请求的共享回源。
func TestTokenCacheCanceledWaiter(t *testing.T) {
	ctx, records, token, auth := cachedAuthFixture(t, 0, 2)
	blocked := &blockingTokens{
		APITokenRepository: records,
		started:            make(chan struct{}),
		release:            make(chan struct{}),
	}
	auth.cache.APITokenRepository = blocked
	var release sync.Once
	unblock := func() {
		release.Do(func() {
			close(blocked.release)
		})
	}
	defer unblock()
	done := make(chan error, 1)
	go func() {
		_, err := auth.Authenticate(ctx, token.Token)
		done <- err
	}()
	select {
	case <-blocked.started:
	case <-ctx.Done():
		t.Fatal("database read did not start")
	}
	waiter, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, err := auth.Authenticate(waiter, token.Token)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter cancellation lost: %v", err)
	}
	unblock()
	requireOK(t, <-done)
	if records.tokenReads.Load() != 1 {
		t.Fatal("canceled waiter caused an extra database read")
	}
}
