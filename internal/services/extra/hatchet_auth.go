package extra

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/tink-crypto/tink-go/keyset"

	authtoken "github.com/hatchet-dev/hatchet/pkg/auth/token"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// verificationKeys 只暴露公钥验证能力；extra 不加载 Hatchet 私钥或 master key。
type verificationKeys struct {
	// handle 仅包含公钥，不能用于签名或数据加解密。
	handle *keyset.Handle
}

// GetPublicJWTHandle 返回只读公钥验证能力。
func (k *verificationKeys) GetPublicJWTHandle() *keyset.Handle {
	return k.handle
}

// GetPrivateJWTHandle 始终为空，此服务不持有 JWT 签名私钥。
func (k *verificationKeys) GetPrivateJWTHandle() *keyset.Handle {
	return nil
}

// Encrypt 拒绝数据加密，此适配器只供官方 JWT 验证器使用。
func (k *verificationKeys) Encrypt([]byte, string) ([]byte, error) {
	return nil, errors.New("verification-only keys")
}

// Decrypt 拒绝数据解密，不能绕过密钥用途隔离。
func (k *verificationKeys) Decrypt([]byte, string) ([]byte, error) {
	return nil, errors.New("verification-only keys")
}

// EncryptString 拒绝字符串加密，公钥验证服务不提供此能力。
func (k *verificationKeys) EncryptString(string, string) (string, error) {
	return "", errors.New("verification-only keys")
}

// DecryptString 拒绝字符串解密，不加载 Hatchet master key。
func (k *verificationKeys) DecryptString(string, string) (string, error) {
	return "", errors.New("verification-only keys")
}

// hatchetAuthenticator 每次本地验证 JWT，缓存仅替代数据库记录读取。
type hatchetAuthenticator struct {
	// manager 使用官方 issuer/audience/server_url 与有效期规则。
	manager authtoken.JWTManager
	// tenants 和 tokens 是未缓存的回源接口，避免叠加两个 TTL。
	tenants v1.TenantRepository
	keys    *verificationKeys
	tokens  v1.APITokenRepository
	// opts 是构造时冻结的验证条件，不接受请求覆盖。
	opts *authtoken.TokenOpts
	// cache 同时验证 token 记录和租户存在性；缓存已核实的身份不重复查询租户。
	cache *cachedTokens
}

// tokenSnapshot 复用官方验证器已经读取的行，避免同一个 RPC 重复查询 PostgreSQL。
type tokenSnapshot struct {
	// row 只用于本次 RPC 的归属校验，不跨请求共享。
	row *sqlcv1.APIToken
}

// tokenSnapshotKey 区分每个请求的记录槽，不共享可修改的验证 context。
type tokenSnapshotKey struct{}

// snapshotRepository 捕获官方验证器读取的同一记录，避免二次查询产生检查窗口。
type snapshotRepository struct {
	v1.APITokenRepository
}

// GetAPITokenById 读取指定 token 记录，缓存与并发合并不得改变记录归属和有效期。
func (r *snapshotRepository) GetAPITokenById(ctx context.Context, id uuid.UUID) (*sqlcv1.APIToken, error) {
	row, err := r.APITokenRepository.GetAPITokenById(ctx, id)
	if err == nil && (row == nil || row.ID != id) {
		return nil, errors.New("token record identity mismatch")
	}
	if err == nil {
		if snapshot, ok := ctx.Value(tokenSnapshotKey{}).(*tokenSnapshot); ok {
			snapshot.row = row
		}
	}
	return row, err
}

// NewHatchetAuthenticator 使用 Hatchet 的公钥格式、issuer/audience/server_url 和官方验证器。
// 额外核对数据库 token 的 tenant，不能接受 JWT subject 与数据库归属不一致的令牌。
func NewHatchetAuthenticator(publicKey []byte, serverURL string, tokens v1.APITokenRepository, tenants v1.TenantRepository) (Authenticator, error) {
	if serverURL == "" || isNil(tokens) || isNil(tenants) {
		return nil, errors.New("Hatchet URL and repositories required")
	}
	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimSpace(string(publicKey)))
	if err != nil {
		return nil, fmt.Errorf("invalid public key encoding: %w", err)
	}
	handle, err := keyset.ReadWithNoSecrets(keyset.NewJSONReader(bytes.NewReader(raw)))
	if err != nil {
		return nil, fmt.Errorf("public verification keys required: %w", err)
	}
	keys := &verificationKeys{handle: handle}
	opts := &authtoken.TokenOpts{
		Issuer:    serverURL,
		Audience:  serverURL,
		ServerURL: serverURL,
	}
	manager, err := authtoken.NewJWTManager(keys, &snapshotRepository{APITokenRepository: tokens}, opts)
	if err != nil {
		return nil, err
	}
	return &hatchetAuthenticator{
		manager: manager,
		tenants: tenants,
		keys:    keys,
		tokens:  tokens,
		opts:    opts,
	}, nil
}

// Authenticate 验证 bearer token 并返回租户身份，不从请求参数读取租户。
func (a *hatchetAuthenticator) Authenticate(ctx context.Context, encoded string) (uuid.UUID, error) {
	if err := ctx.Err(); err != nil {
		return uuid.Nil, err
	}
	snapshot := &tokenSnapshot{}
	ctx = context.WithValue(ctx, tokenSnapshotKey{}, snapshot)
	id, _, err := a.manager.ValidateTenantToken(ctx, encoded)
	if err != nil {
		return uuid.Nil, err
	}
	t := snapshot.row
	if t == nil {
		return uuid.Nil, errors.New("token verification did not read a record")
	}
	if t.Revoked || !t.ExpiresAt.Valid || !t.ExpiresAt.Time.After(time.Now()) {
		return uuid.Nil, errors.New("token no longer valid")
	}
	if t.TenantId == nil || *t.TenantId != id {
		return uuid.Nil, errors.New("token tenant mismatch")
	}
	if a.cache == nil {
		tenant, tenantErr := a.tenants.GetTenantByID(ctx, id)
		if tenantErr != nil {
			if errors.Is(tenantErr, pgx.ErrNoRows) {
				return uuid.Nil, errors.New("unknown tenant")
			}
			return uuid.Nil, fmt.Errorf("%w: %w", authtoken.ErrCouldNotReadTokenFromDatabase, tenantErr)
		}
		if tenant == nil || tenant.ID != id {
			return uuid.Nil, fmt.Errorf("%w: tenant record identity mismatch", authtoken.ErrCouldNotReadTokenFromDatabase)
		}
	}
	return id, nil
}
