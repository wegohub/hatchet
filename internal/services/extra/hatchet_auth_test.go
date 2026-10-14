package extra

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authtoken "github.com/hatchet-dev/hatchet/pkg/auth/token"
	"github.com/hatchet-dev/hatchet/pkg/encryption"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// authRecords 在互斥锁内提供独立数据库快照，允许测试受控地撤销或删除身份。
type authRecords struct {
	v1.APITokenRepository
	v1.TenantRepository
	mu            sync.RWMutex
	tokens        map[uuid.UUID]*sqlcv1.APIToken
	id            uuid.UUID
	databaseError error
	tenantMissing bool
	tokenReads    atomic.Int64
	tenantReads   atomic.Int64
}

// CreateAPIToken 保存官方签名器生成的 token 身份，测试不伪造 JWT 格式。
func (r *authRecords) CreateAPIToken(ctx context.Context, o *v1.CreateAPITokenOpts) (*sqlcv1.APIToken, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := &sqlcv1.APIToken{
		ID:       o.ID,
		TenantId: o.TenantId,
		ExpiresAt: pgtype.Timestamp{
			Time:  o.ExpiresAt,
			Valid: true,
		},
	}
	r.tokens[o.ID] = v
	cloned := *v
	return &cloned, nil
}

// GetAPITokenById 读取指定 token 记录，缓存与并发合并不得改变记录归属和有效期。
func (r *authRecords) GetAPITokenById(ctx context.Context, id uuid.UUID) (*sqlcv1.APIToken, error) {
	r.tokenReads.Add(1)
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.databaseError != nil {
		return nil, r.databaseError
	}
	v, ok := r.tokens[id]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	cloned := *v
	return &cloned, nil
}

// GetTenantByID 按持久租户 ID 查询存在性，不根据名称猜测身份。
func (r *authRecords) GetTenantByID(ctx context.Context, id uuid.UUID) (*sqlcv1.Tenant, error) {
	r.tenantReads.Add(1)
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.databaseError != nil {
		return nil, r.databaseError
	}
	if r.tenantMissing || id != r.id {
		return nil, pgx.ErrNoRows
	}
	return &sqlcv1.Tenant{ID: id}, nil
}

// TestHatchetJWT 使用真正的 Hatchet ES256 签名与验证器，数据库接口仅替换成受控记录。
func TestHatchetJWT(t *testing.T) {
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
	signed, err := manager.GenerateTenantToken(context.Background(), records.id, "test", false, nil)
	requireOK(t, err)
	a, err := NewHatchetAuthenticator(clearPublic, url, records, records)
	requireOK(t, err)
	id, err := a.Authenticate(context.Background(), signed.Token)
	requireOK(t, err)
	if id != records.id {
		t.Fatal("wrong tenant")
	}
	if _, err = NewHatchetAuthenticator(private, url, records, records); err == nil {
		t.Fatal("accepted private/encrypted keyset as public key")
	}

	// 整个 RPC 链路使用真实 JWT；撤销在缓存期限结束后阻止新的操作。
	f := openFixture(t, Config{
		Authenticator: a,
		AuthCacheTTL:  100 * time.Millisecond,
	})
	f.ctx = metadata.NewOutgoingContext(f.ctx, metadata.Pairs("authorization", "Bearer "+signed.Token))
	f.createBucket(t)
	records.mu.Lock()
	records.tokens[signed.TokenId].Revoked = true
	records.mu.Unlock()
	eventually(t, func() bool {
		_, err := f.kv.GetBucket(f.ctx, f.bucketRef())
		return status.Code(err) == codes.Unauthenticated
	})
	records.mu.Lock()
	records.tokens[signed.TokenId].Revoked = false
	records.tokens[signed.TokenId].ExpiresAt.Time = time.Now().Add(-time.Second)
	records.mu.Unlock()
	_, err = a.Authenticate(context.Background(), signed.Token)
	if err == nil {
		t.Fatal("accepted expired database token")
	}
	records.mu.Lock()
	records.tokens[signed.TokenId].ExpiresAt.Time = time.Now().Add(time.Hour)
	other := uuid.New()
	records.tokens[signed.TokenId].TenantId = &other
	records.mu.Unlock()
	_, err = a.Authenticate(context.Background(), signed.Token)
	if err == nil {
		t.Fatal("accepted tenant mismatch")
	}
	records.mu.Lock()
	records.tokens[signed.TokenId].TenantId = &records.id
	records.tenantMissing = true
	records.mu.Unlock()
	_, err = a.Authenticate(context.Background(), signed.Token)
	if err == nil {
		t.Fatal("accepted removed tenant")
	}
	records.mu.Lock()
	records.tenantMissing = false
	records.databaseError = errors.New("database unavailable")
	records.mu.Unlock()
	_, err = f.kv.GetBucket(f.ctx, f.bucketRef())
	requireCode(t, err, codes.Unavailable)
	records.mu.Lock()
	records.databaseError = nil
	records.mu.Unlock()
	for _, encoded := range []string{
		"invalid",
		signed.Token + "x",
	} {
		_, err = a.Authenticate(context.Background(), encoded)
		if err == nil {
			t.Fatal("accepted invalid JWT")
		}
	}
	// JWT 自身的有效期仍逐次验证，不因 token 记录缓存而放宽。
	past := time.Now().Add(-time.Minute)
	expired, err := manager.GenerateTenantToken(context.Background(), records.id, "expired", false, &past)
	requireOK(t, err)
	reads := records.tokenReads.Load()
	_, err = f.server.service.cfg.Authenticator.Authenticate(context.Background(), expired.Token)
	if err == nil || records.tokenReads.Load() != reads {
		t.Fatal("expired JWT reached database or passed verification")
	}
	wrong, err := NewHatchetAuthenticator(clearPublic, "http://localhost:8081", records, records)
	requireOK(t, err)
	_, err = wrong.Authenticate(context.Background(), signed.Token)
	if err == nil {
		t.Fatal("accepted wrong issuer/audience")
	}
}
