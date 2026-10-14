// Command hatchet-extra 以 Hatchet 租户鉴权启动单节点 JetStream gRPC 服务。
package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hatchet-dev/hatchet/internal/services/extra"
	"github.com/hatchet-dev/hatchet/pkg/config/loader"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// uncachedAuth 在 Extra 的内存缓存未命中时读取实时记录，避免叠加数据库层缓存期限。
type uncachedAuth struct {
	v1.APITokenRepository
	v1.TenantRepository
	pool *pgxpool.Pool
}

// GetAPITokenById 读取指定 token 记录，缓存与并发合并不得改变记录归属和有效期。
func (r *uncachedAuth) GetAPITokenById(ctx context.Context, id uuid.UUID) (*sqlcv1.APIToken, error) {
	return sqlcv1.New().GetAPITokenById(ctx, r.pool, id)
}

// GetTenantByID 按持久租户 ID 查询存在性，不根据名称猜测身份。
func (r *uncachedAuth) GetTenantByID(ctx context.Context, id uuid.UUID) (*sqlcv1.Tenant, error) {
	return sqlcv1.New().GetTenantByID(ctx, r.pool, id)
}

// main 启动命令入口，故障退出时输出错误并返回非零退出码。
func main() {
	// 部署辅助命令不初始化数据库或 JetStream，避免健康检查创建额外服务实例。
	if len(os.Args) > 1 && (os.Args[1] == "export-public-key" || os.Args[1] == "healthcheck") {
		if err := deploymentCommand(os.Args[1], os.Args[2:]); err != nil {
			slog.Error("hatchet-extra deployment command failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		slog.Error("hatchet-extra stopped", "error", err)
		os.Exit(1)
	}
}

// run 加载配置和公钥，启动服务并在进程信号到达后按预算关闭。
func run() (result error) {
	listen := flag.String("listen", env("EXTRA_LISTEN_ADDR", "127.0.0.1:7080"), "gRPC listen address")
	store := flag.String("store", env("EXTRA_STORE_DIR", "./data/extra"), "persistent JetStream directory")
	config := flag.String("config", "", "Hatchet database configuration directory")
	public := flag.String("jwt-public-key", os.Getenv("EXTRA_JWT_PUBLIC_KEY_FILE"), "Hatchet unencrypted public JWT keyset file")
	url := flag.String("hatchet-url", env("SERVER_URL", "http://localhost:8080"), "Hatchet server URL / JWT issuer")
	cert := flag.String("tls-cert", os.Getenv("EXTRA_TLS_CERT_FILE"), "gRPC TLS certificate file")
	private := flag.String("tls-key", os.Getenv("EXTRA_TLS_KEY_FILE"), "gRPC TLS private key file")
	grace := flag.Duration("shutdown-timeout", 30*time.Second, "graceful shutdown budget")
	authCacheTTL := flag.Duration("auth-cache-ttl", 5*time.Minute, "Hatchet token memory cache TTL (revocation delay upper bound)")
	maxAuthEntries := flag.Int("auth-cache-entries", 32768, "maximum authentication cache entries (LRU eviction)")
	maxStore := flag.Int64("max-store-bytes", 1<<30, "embedded JetStream disk budget")
	maxMemory := flag.Int64("max-memory-bytes", 256<<20, "embedded JetStream memory budget")
	maxMessage := flag.Int("max-message-bytes", 1<<20, "maximum message/KV value bytes")
	maxFetch := flag.Int("max-fetch-bytes", 4<<20, "maximum bytes in a Fetch batch")
	maxBatch := flag.Int("max-fetch-messages", 64, "maximum messages in a Fetch batch")
	maxPublishing := flag.Int("max-publish-inflight", 64, "global asynchronous publish budget")
	maxRPC := flag.Int("max-concurrent-rpcs", 128, "separate unary/stream admission budgets")
	flag.Parse()
	if *grace <= 0 {
		return errors.New("shutdown timeout must be positive")
	}
	// 私有 ACK 密钥独立于 Hatchet JWT，防止签名用途混用。原始密钥不写日志。
	key, err := base64.StdEncoding.DecodeString(os.Getenv("EXTRA_ACK_KEY"))
	if err != nil || len(key) < 32 {
		return errors.New("EXTRA_ACK_KEY must be base64 encoding of at least 32 random bytes")
	}
	if *public == "" {
		return errors.New("jwt-public-key file required")
	}
	// #nosec G703 -- 路径由运维启动参数指定，不来自 RPC；仅加载并验证公钥。
	pub, err := os.ReadFile(*public)
	if err != nil {
		return fmt.Errorf("read public JWT keys: %w", err)
	}
	var tlsConfig *tls.Config
	if *cert != "" || *private != "" {
		if *cert == "" || *private == "" {
			return errors.New("TLS certificate and key must be supplied together")
		}
		pair, e := tls.LoadX509KeyPair(*cert, *private)
		if e != nil {
			return e
		}
		tlsConfig = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{pair},
		}
	} else {
		host, _, e := net.SplitHostPort(*listen)
		if e != nil {
			return e
		}
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return errors.New("non-loopback listener requires TLS")
		}
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	data, err := loader.NewConfigLoader(*config).InitDataLayer()
	if err != nil {
		return err
	}
	defer func() {
		result = errors.Join(result, data.Disconnect())
	}()
	repositories := &uncachedAuth{
		APITokenRepository: data.V1.APIToken(),
		TenantRepository:   data.V1.Tenant(),
		pool:               data.Pool,
	}
	auth, err := extra.NewHatchetAuthenticator(pub, *url, repositories, repositories)
	if err != nil {
		return err
	}
	server, err := extra.New(extra.Config{
		StoreDir:            *store,
		ACKKey:              key,
		Authenticator:       auth,
		AuthCacheTTL:        *authCacheTTL,
		MaxAuthCacheEntries: *maxAuthEntries,
		TLS:                 tlsConfig,
		MaxStoreBytes:       *maxStore,
		MaxMemoryBytes:      *maxMemory,
		MaxMessageBytes:     *maxMessage,
		MaxFetchBytes:       *maxFetch,
		MaxFetchMessages:    *maxBatch,
		MaxAsyncInFlight:    *maxPublishing,
		MaxConcurrentRPCs:   *maxRPC,
	})
	if err != nil {
		return err
	}
	defer server.Stop()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()
	slog.Info("hatchet-extra ready", "address", listener.Addr().String())
	select {
	case <-ctx.Done():
	case result = <-serveErr:
	}
	budget, stop := context.WithTimeout(context.Background(), *grace)
	defer stop()
	return errors.Join(result, server.Shutdown(budget))
}

// env 读取非空环境配置，未指定时采用明确的默认值。
func env(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
