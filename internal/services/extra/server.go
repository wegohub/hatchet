package extra

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"reflect"
	"sync"
	"time"

	ns "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/stats"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// Config 属于单个 extra 实例。ACKKey 必须持久配置，重启后才能确认之前投递的消息。
type Config struct {
	// StoreDir 保存 JetStream 日志，重启使用同一目录。
	StoreDir string
	// ACKKey 至少 32 字节的持久随机签名密钥。
	ACKKey []byte
	// Authenticator 校验 Hatchet bearer token，并确定租户。
	Authenticator Authenticator
	// AuthCacheTTL 是 Hatchet token 记录的内存缓存期限上限，默认 5 分钟。
	// JWT 每次本地验证；token 撤销最迟在缓存到期后对新 RPC 生效。
	AuthCacheTTL time.Duration
	// MaxAuthCacheEntries 限制鉴权缓存条目数，默认 32768；满时按 LRU 淘汰。
	MaxAuthCacheEntries int
	// TLS 配置网络入口，按标准 tls.Config 约定管理回调和私钥。
	TLS *tls.Config
	// MaxMessageBytes 单条消息/KV value 上限，默认 1 MiB。
	MaxMessageBytes int
	// MaxFetchMessages 单次 pull 的消息数上限，默认 100。
	MaxFetchMessages int
	// MaxPublishBatchMessages 和 MaxPublishBatchBytes 限制原子批次的完整缓冲。
	MaxPublishBatchMessages int
	MaxPublishBatchBytes    int
	// MaxPublishBatchesInFlight 在读取首条请求前限制实例的批次缓冲总数。
	MaxPublishBatchesInFlight int
	// MaxAckBatchMessages 限制单次批量确认的投递条数。
	MaxAckBatchMessages int
	// MaxFetchBytes 单次 pull 的字节预算，默认 4 MiB。
	MaxFetchBytes int
	// MaxAsyncInFlight 实例共享的异步发布预算，默认 64。
	MaxAsyncInFlight int
	// MaxConcurrentRPCs 是兼容预算，仅在新 unary/stream 预算均未设置时为二者赋值。
	MaxConcurrentRPCs int
	// 独立入场预算；旧字段只在两个新预算均未设置时兼容赋值。
	MaxConcurrentUnary      int
	MaxConcurrentStreams    int
	MaxConcurrentReads      int
	BatchCommitTimeout      time.Duration
	BatchAbandonTimeout     time.Duration
	PublishBatchFlowControl int
	// MaxConsumers 请求账户总上限；0 采用默认 10000，-1 显式不限。
	// 单节点原生接口只按 Stream 限制，GetLimits 返回 0 表示未强制账户总上限。
	MaxConsumers int
	// MaxStoreBytes JetStream 磁盘预算，默认 1 GiB。
	MaxStoreBytes int64
	// MaxMemoryBytes memory storage 预算，默认 256 MiB。
	MaxMemoryBytes int64
}

// Service 持有共享连接和不可变配置；业务 RPC 不能改变连接的认证/重连策略。
type Service struct {
	pb.UnimplementedStreamServiceServer
	pb.UnimplementedPublisherServiceServer
	pb.UnimplementedConsumerServiceServer
	pb.UnimplementedKeyValueServiceServer
	// js 与 nc 由同一个实例持有；任何业务 RPC 都不能更改连接配置。
	js jetstream.JetStream
	nc *nats.Conn
	// cfg 在构造时完成快照和校验，运行期间只读。
	cfg Config
	// asyncSlots 是所有 PublishAsync 流共享的在途发布预算。
	asyncSlots chan struct{}
	// batchSlots 限制完整批次缓冲；batchIDs 拒绝同一原生批次并行提交。
	batchSlots chan struct{}
	batchIDs   sync.Map
	// resourceSlots 串行化本实例的配置写入，使 create_only 检查与创建不可被 upsert 穿插。
	resourceSlots chan struct{}
}

// defaults 补齐零值配置；显式非法值交给校验器拒绝。
func defaults(c Config) Config {
	if c.AuthCacheTTL == 0 {
		c.AuthCacheTTL = 5 * time.Minute
	}
	if c.MaxAuthCacheEntries == 0 {
		c.MaxAuthCacheEntries = 32768
	}
	if c.MaxMessageBytes == 0 {
		c.MaxMessageBytes = 1 << 20
	}
	if c.MaxFetchMessages == 0 {
		c.MaxFetchMessages = 100
	}
	if c.MaxPublishBatchMessages == 0 {
		c.MaxPublishBatchMessages = 100
	}
	if c.MaxPublishBatchBytes == 0 {
		c.MaxPublishBatchBytes = 4 << 20
	}
	if c.MaxPublishBatchesInFlight == 0 {
		c.MaxPublishBatchesInFlight = 32
	}
	if c.MaxAckBatchMessages == 0 {
		c.MaxAckBatchMessages = 100
	}
	if c.MaxFetchBytes == 0 {
		c.MaxFetchBytes = 4 << 20
	}
	if c.MaxAsyncInFlight == 0 {
		c.MaxAsyncInFlight = 64
	}
	if c.MaxStoreBytes == 0 {
		c.MaxStoreBytes = 1 << 30
	}
	if c.MaxMemoryBytes == 0 {
		c.MaxMemoryBytes = 256 << 20
	}
	if c.MaxConcurrentUnary == 0 && c.MaxConcurrentStreams == 0 && c.MaxConcurrentRPCs != 0 {
		c.MaxConcurrentUnary, c.MaxConcurrentStreams = c.MaxConcurrentRPCs, c.MaxConcurrentRPCs
	}
	if c.MaxConcurrentRPCs == 0 {
		c.MaxConcurrentRPCs = 128
	}
	if c.MaxConcurrentUnary == 0 {
		c.MaxConcurrentUnary = 128
	}
	if c.MaxConcurrentStreams == 0 {
		c.MaxConcurrentStreams = 1024
	}
	if c.MaxConcurrentReads == 0 {
		c.MaxConcurrentReads = 64
	}
	if c.BatchCommitTimeout == 0 {
		c.BatchCommitTimeout = 8 * time.Second
	}
	if c.BatchAbandonTimeout == 0 {
		c.BatchAbandonTimeout = 10 * time.Second
	}
	if c.PublishBatchFlowControl == 0 {
		c.PublishBatchFlowControl = 16
	}
	if c.MaxConsumers == 0 {
		c.MaxConsumers = 10000
	}
	return c
}

// validateConfig 在启动资源前校验身份、预算和容量，避免无效配置产生副作用。
func validateConfig(c Config) error {
	if c.AuthCacheTTL <= 0 {
		return errors.New("authentication cache TTL must be positive")
	}
	if c.MaxAuthCacheEntries < 1 || c.MaxAuthCacheEntries > 1_000_000 {
		return errors.New("authentication cache capacity must be between 1 and 1000000")
	}
	if isNil(c.Authenticator) {
		return errors.New("authenticator required")
	}
	if len(c.ACKKey) < 32 {
		return errors.New("ACK key must contain at least 32 bytes")
	}
	if c.StoreDir == "" {
		return errors.New("persistent store directory required")
	}
	return validateLimits(c)
}

// ValidateLimits 校验完整数值参数，可在初始化数据库和认证前调用。
func ValidateLimits(c Config) error { return validateLimits(c) }

func validateLimits(c Config) error {
	if c.AuthCacheTTL <= 0 || c.MaxAuthCacheEntries < 1 || c.MaxAuthCacheEntries > 1_000_000 {
		return errors.New("invalid authentication cache limits")
	}
	if c.MaxMessageBytes < 1 ||
		c.MaxMessageBytes > 64<<20 ||
		c.MaxFetchBytes > 256<<20 ||
		c.MaxFetchMessages > 4096 ||
		c.MaxFetchMessages < 1 ||
		c.MaxFetchBytes < c.MaxMessageBytes ||
		c.MaxAsyncInFlight < 1 ||
		c.MaxAsyncInFlight > 4096 {
		return errors.New("invalid resource limits")
	}
	if c.MaxConcurrentRPCs < 0 || c.MaxConcurrentRPCs > 4096 ||
		c.MaxConcurrentUnary < 1 || c.MaxConcurrentUnary > 4096 ||
		c.MaxConcurrentStreams < 1 || c.MaxConcurrentStreams > 4096 ||
		c.MaxConcurrentReads < 1 || c.MaxConcurrentReads > 4096 {
		return errors.New("invalid RPC admission limit")
	}
	if c.MaxPublishBatchMessages < 1 || c.MaxPublishBatchMessages > 1000 ||
		c.MaxPublishBatchBytes < c.MaxMessageBytes+PublishBatchPerMessageOverhead || c.MaxPublishBatchBytes > 256<<20 ||
		c.MaxPublishBatchesInFlight < 1 || c.MaxPublishBatchesInFlight > 1024 ||
		c.MaxAckBatchMessages < 1 || c.MaxAckBatchMessages > 4096 {
		return errors.New("invalid batch limits")
	}
	if c.BatchCommitTimeout <= 0 || c.BatchAbandonTimeout <= c.BatchCommitTimeout {
		return errors.New("batch commit timeout must be positive and less than batch abandon timeout")
	}
	if c.PublishBatchFlowControl < 1 || c.PublishBatchFlowControl > 1000 || c.MaxConsumers < -1 || c.MaxConsumers > 10_000_000 {
		return errors.New("invalid batch flow control or consumer limit")
	}
	if c.MaxStoreBytes < 0 || c.MaxMemoryBytes < 0 {
		return errors.New("invalid storage limits")
	}
	return nil
}

// isNil 在配置边界拒绝 typed nil，避免接口非 nil 却在首个请求上崩溃。
func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// Server 生命周期只有一次；关闭先停止 RPC，再关闭私有 NATS 连接和 embedded server。
type Server struct {
	// service 保存连接与不可变业务预算，关闭后不可重新启用。
	service *Service
	// grpc 与 nats 是实例独占资源，按照传输、连接、引擎的顺序关闭。
	grpc *grpc.Server
	nats *ns.Server
	// once 保证仅一个调用方执行实际关闭。
	once sync.Once
	// forceOnce 和 forced 允许 Stop 或任一调用方的截止时间中断排空。
	forceOnce sync.Once
	forced    chan struct{}
	// forceDone 表示强制停止已关闭传输，清理不能仅依赖通知已经发出。
	forceDone chan struct{}
	// lifecycle 在拦截器边界登记 RPC，排空不依赖 gRPC 的内部 handler 等待锁。
	lifecycle *rpcLifecycle
	// done 关闭前完成资源清理与 shutdownErr 写入，形成读取错误的同步屏障。
	done chan struct{}
	// mu 保护启动与关闭状态；不能持锁执行阻塞的 Serve 或排空。
	mu sync.Mutex
	// listener 在 Serve 接管后属于实例；关闭只停止接收新连接，不打断已有 RPC。
	listener net.Listener
	// serving 在首次接管 listener 时设定，停止后也不允许重启。
	serving bool
	// stopping 让与 Shutdown 并发的 Serve 明确失败。
	stopping bool
	// shutdownErr 只由关闭协程写入，所有调用方在 done 关闭后读取。
	shutdownErr error
}

// rpcLifecycle 统一登记两种入场预算中的调用，关闭后拒绝新 RPC。
type rpcLifecycle struct {
	mu       sync.Mutex
	active   int
	stopping bool
	drained  chan struct{}
}

// rpcState 在每个 RPC 中记录准入状态，End 在响应和状态处理之后才完成登记。
type rpcState struct {
	admitted bool
}

// rpcStateKey 是传输统计与入场拦截器之间的私有状态槽。
type rpcStateKey struct{}

// TagRPC 为本次调用保存独立状态，不在 handler 退出时提前认定响应已经完成。
func (l *rpcLifecycle) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, rpcStateKey{}, &rpcState{})
}

// HandleRPC 在解码前登记调用，在 gRPC 处理响应和终态后释放登记。
func (l *rpcLifecycle) HandleRPC(ctx context.Context, event stats.RPCStats) {
	state, ok := ctx.Value(rpcStateKey{}).(*rpcState)
	if !ok {
		return
	}
	switch event.(type) {
	case *stats.Begin:
		state.admitted = l.begin()
	case *stats.End:
		if state.admitted {
			l.end()
		}
	}
}

// TagConn 不修改连接 context，租户只能在 RPC 鉴权中确定。
func (l *rpcLifecycle) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

// HandleConn 不用 TCP 连接数代替在途 RPC，单连接可以同时有多条流。
func (l *rpcLifecycle) HandleConn(context.Context, stats.ConnStats) {}

// admitted 返回解码前的准入决定，排空期间新请求不能进入业务或鉴权回调。
func admitted(ctx context.Context) bool {
	state, ok := ctx.Value(rpcStateKey{}).(*rpcState)
	return ok && state.admitted
}

// begin 与 stop 共用锁，不能出现排空已结束后又加入请求的竞态。
func (l *rpcLifecycle) begin() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopping {
		return false
	}
	l.active++
	return true
}

// end 在最后一个已登记调用退出时完成排空屏障。
func (l *rpcLifecycle) end() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.active--
	if l.stopping && l.active == 0 {
		close(l.drained)
	}
}

// stop 先停止新增调用，再返回排空屏障；重复调用不会重复关闭 channel。
func (l *rpcLifecycle) stop() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.stopping {
		l.stopping = true
		if l.active == 0 {
			close(l.drained)
		}
	}
	return l.drained
}

// New 启动私有 embedded JetStream；DontListen 禁止未鉴权客户端绕过 gRPC 直接连接。
func New(c Config) (*Server, error) {
	c = defaults(c)
	if err := validateConfig(c); err != nil {
		return nil, err
	}
	c.ACKKey = append([]byte(nil), c.ACKKey...)
	if c.TLS != nil {
		c.TLS = c.TLS.Clone()
	}
	// 鉴权只依赖内存和数据库，不依赖 JetStream 初始化或其内存预算。
	if auth, ok := c.Authenticator.(*hatchetAuthenticator); ok {
		var err error
		c.Authenticator, err = auth.withTokenCache(c.AuthCacheTTL, c.MaxAuthCacheEntries, c.MaxConcurrentUnary)
		if err != nil {
			return nil, err
		}
	}
	n, err := ns.NewServer(&ns.Options{
		JetStream:          true,
		StoreDir:           c.StoreDir,
		DontListen:         true,
		NoSigs:             true,
		NoLog:              true,
		JetStreamMaxStore:  c.MaxStoreBytes,
		JetStreamMaxMemory: c.MaxMemoryBytes,
		JetStreamLimits:    ns.JSLimitOpts{MaxBatchTimeout: c.BatchAbandonTimeout},
		// #nosec G115 -- validateConfig 将 MaxMessageBytes 限定在 64 MiB 以下。
		MaxPayload: int32(c.MaxMessageBytes + 65536),
	})
	if err != nil {
		return nil, err
	}
	n.Start()
	if !n.ReadyForConnections(10 * time.Second) {
		n.Shutdown()
		n.WaitForShutdown()
		return nil, errors.New("embedded NATS startup timed out")
	}
	nc, err := nats.Connect(nats.DefaultURL, nats.InProcessServer(n), nats.Name("hatchet-extra"), nats.MaxReconnects(-1))
	if err != nil {
		n.Shutdown()
		n.WaitForShutdown()
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		n.Shutdown()
		n.WaitForShutdown()
		return nil, err
	}
	s := &Service{
		js:            js,
		nc:            nc,
		cfg:           c,
		asyncSlots:    make(chan struct{}, c.MaxAsyncInFlight),
		batchSlots:    make(chan struct{}, c.MaxPublishBatchesInFlight),
		resourceSlots: make(chan struct{}, 1),
	}
	lifecycle := &rpcLifecycle{drained: make(chan struct{})}
	opts := []grpc.ServerOption{
		grpc.StatsHandler(lifecycle),
		grpc.ChainUnaryInterceptor(unaryAdmission(make(chan struct{}, c.MaxConcurrentUnary)), unaryAuth(c.Authenticator)),
		grpc.ChainStreamInterceptor(streamAdmission(make(chan struct{}, c.MaxConcurrentStreams), make(chan struct{}, c.MaxConcurrentReads)), streamAuth(c.Authenticator)),
		grpc.MaxRecvMsgSize(max(c.MaxMessageBytes+65536, c.MaxAckBatchMessages*4100+65536)),
		grpc.MaxSendMsgSize(c.MaxFetchBytes + 65536),
	}
	if c.TLS != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(c.TLS)))
	}
	g := grpc.NewServer(opts...)
	pb.RegisterStreamServiceServer(g, s)
	pb.RegisterPublisherServiceServer(g, s)
	pb.RegisterConsumerServiceServer(g, s)
	pb.RegisterKeyValueServiceServer(g, s)
	return &Server{
		service:   s,
		grpc:      g,
		nats:      n,
		done:      make(chan struct{}),
		forced:    make(chan struct{}),
		forceDone: make(chan struct{}),
		lifecycle: lifecycle,
	}, nil
}

// Serve 接管 listener 并阻塞处理 RPC；拒绝重复启动时 listener 仍由调用方持有。
func (s *Server) Serve(l net.Listener) error {
	if l == nil {
		return errors.New("listener required")
	}
	s.mu.Lock()
	if s.serving || s.stopping {
		s.mu.Unlock()
		return errors.New("extra server already started or stopped")
	}
	s.serving = true
	s.listener = l
	s.mu.Unlock()
	err := s.grpc.Serve(l)
	s.mu.Lock()
	stopping := s.stopping
	s.mu.Unlock()
	if stopping && errors.Is(err, net.ErrClosed) {
		return nil
	}
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// Shutdown 接受调用方的排空预算；预算耗尽时强制中断长订阅，随后完成独立资源关闭。
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("shutdown context required")
	}
	s.once.Do(func() {
		s.mu.Lock()
		s.stopping = true
		listener := s.listener
		s.mu.Unlock()
		drained := s.lifecycle.stop()
		if listener != nil {
			// 已关闭的 listener 也属于正常停止；已建立的连接仍可完成在途调用。
			_ = listener.Close()
		}
		go func() {
			select {
			case <-drained:
				// 响应已进入传输队列，正常关闭仍须让 gRPC 刷出最后的结果和 trailers。
				flushed := make(chan struct{})
				go func() {
					s.grpc.GracefulStop()
					close(flushed)
				}()
				select {
				case <-flushed:
				case <-ctx.Done():
					s.forceStop()
					<-s.forceDone
				case <-s.forced:
					<-s.forceDone
				}
			case <-ctx.Done():
				s.forceStop()
				<-s.forceDone
			case <-s.forced:
				<-s.forceDone
			}
			if auth, ok := s.service.cfg.Authenticator.(*hatchetAuthenticator); ok && auth.cache != nil {
				auth.cache.close()
			}
			// 排空后先把普通 ACK 等写缓冲刷到 NATS；此步骤不是持久 DoubleAck。
			s.shutdownErr = s.service.nc.FlushTimeout(5 * time.Second)
			s.service.nc.Close()
			s.nats.Shutdown()
			s.nats.WaitForShutdown()
			close(s.done)
		}()
	})
	select {
	case <-s.done:
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), s.shutdownErr)
		}
		return s.shutdownErr
	case <-ctx.Done():
		// 任一调用的更短预算也能打断正在排空的实例。
		s.forceStop()
		<-s.done
		return errors.Join(ctx.Err(), s.shutdownErr)
	}
}

// Stop 强制终止 RPC 并等待实例资源清理，能够中断正在进行的排空。
func (s *Server) Stop() {
	s.forceStop()
	_ = s.Shutdown(context.Background())
}

// forceStop 唤醒排空并关闭传输；实际资源释放仍由统一关闭流程执行一次。
func (s *Server) forceStop() {
	s.forceOnce.Do(func() {
		close(s.forced)
		go func() {
			s.grpc.Stop()
			close(s.forceDone)
		}()
	})
}
