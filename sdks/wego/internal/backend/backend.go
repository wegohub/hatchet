package backend

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	adminpb "github.com/hatchet-dev/hatchet/internal/services/admin/contracts"
	dispatcherpb "github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts"
	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	clientconfig "github.com/hatchet-dev/hatchet/pkg/config/client"
	"github.com/hatchet-dev/hatchet/pkg/config/loader/loaderutils"
	"github.com/hatchet-dev/hatchet/pkg/config/shared"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// Backend 拥有官方执行连接及带预算的协议连接；管理门面只借用它们
type Backend struct {
	// Bind 给管理操作绑定当前实例的调用上下文
	Bind func(context.Context) context.Context
	// raw 满足官方 Worker 的内部依赖接口，不使用其旧工作流定义入口，不向业务暴露
	raw v0.Client
	// rpcConn 为需要精确预算的官方协议调用提供连接，由本实例关闭
	rpcConn *grpc.ClientConn
	// admin 处理普通与批量任务提交，不经过忽略 context 的高层入口
	admin adminpb.WorkflowServiceClient
	// v1admin 处理工作流注册及运行详情
	v1admin v1.AdminServiceClient
	// rpcDispatcher 为每次结果等待建立独立、可取消的订阅
	rpcDispatcher dispatcherpb.DispatcherClient
	// durableStreams 使用正式发布协议，显式控制 producer 和序号
	durableStreams v1.V1StreamsClient
	// features 持有无后台缓存的管理客户端
	features *featureClients
	// observer 跟踪执行、结果上报与注销过程的传输适配器
	observer *transport
	// config 当前实例使用的配置快照
	config spec.Runtime
	// featureMu 保护管理客户端惰性初始化；网络请求不持有此锁
	featureMu sync.Mutex
	// once 确保资源释放或完成通知只执行一次，防止重复关闭 channel
	once sync.Once
	// closeErr 一次关闭过程中保存的错误，重复关闭返回同一结果
	closeErr error
	// embeddedShutdown 嵌入引擎的关闭回调；未启用嵌入模式时为 nil
	embeddedShutdown func(context.Context) error
	// workflowTasks 记录本实例注册的结果形状；访问由 featureMu 保护
	workflowTasks map[string][]string
	// rpcBindings 按正式 workflow version ID 保存已解析约定，由 featureMu 保护
	rpcBindings map[string]rpcBinding
	// StartExecution 开始一次执行的观测钩子，返回派生上下文和结束回调
	StartExecution func(context.Context, string) (context.Context, func(error))
	// ProtocolMetrics 是所属 Engine 的实例指标出口，不使用全局 registry
	ProtocolMetrics func(ports.ProtocolMetric)
	// BeginLogIO 登记所属 Engine 的上报 I/O，使排空与强制停止确认其退出。
	BeginLogIO func(context.Context) (context.Context, func(), error)
}

// New 解析连接配置并创建私有客户端，构造失败时关闭已取得的资源
func New(config spec.Runtime) (result *Backend, err error) {
	// 后端拥有独立配置容器，调用方后续调整投影或标签不会改变已创建实例
	config = config.Clone()
	// embeddedShutdown 嵌入引擎的关闭回调；未启用嵌入模式时为 nil
	var embeddedShutdown func(context.Context) error
	// 先启动实例拥有的嵌入引擎，再使用其解析后的连接地址创建客户端
	if config.Embedded != nil {
		config, embeddedShutdown, err = startEmbedded(config)
		if err != nil {
			return nil, Normalize(err)
		}

		defer func() {
			if err != nil && embeddedShutdown != nil {
				// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
				ctx, cancel := context.WithTimeout(context.Background(), config.CleanupTimeout())
				defer cancel()

				err = errors.Join(err, Normalize(embeddedShutdown(ctx)))
			}
		}()
	}
	// 令牌缺失时尝试明确配置或环境回退；仍缺失则启动失败，不能创建匿名任务后端
	if config.Token == "" {
		config.Token = os.Getenv("HATCHET_CLIENT_TOKEN")
	}
	// 令牌缺失时尝试明确配置或环境回退；仍缺失则启动失败，不能创建匿名任务后端
	if config.Token == "" {
		return nil, fmt.Errorf("wego: token is required")
	}

	// claims, err 接收 loaderutils.GetConfFromJWT 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	claims, err := loaderutils.GetConfFromJWT(config.Token)
	// 当前步骤失败时终止处理：wego: invalid token: %s；不把无效结果交给下一步
	if err != nil {
		return nil, fmt.Errorf("wego: invalid token: %s", err)
	}
	if !claims.ExpiresAt.After(time.Now()) {
		return nil, fmt.Errorf("wego: token has expired")
	}
	// 未显式提供 gRPC 地址时从令牌配置补齐，显式地址不被覆盖
	if config.Address == "" {
		config.Address = claims.GrpcBroadcastAddress
	}
	// 未显式提供 API 地址时从令牌配置补齐
	if config.ServerURL == "" {
		config.ServerURL = claims.ServerURL
	}
	// 未显式提供授权范围身份时从令牌声明补齐
	if config.TenantID == "" {
		config.TenantID = claims.TenantId
	}
	// 仅在应用未选择 TLS 或明文时推导默认传输配置，显式 nil TLS 不能被覆盖
	if !config.TLSSet {
		config.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	// level 当前日志等级的默认值，解析记录或实例配置后再选择实际级别
	level := zerolog.InfoLevel
	if config.Logger.Enabled(context.Background(), slog.LevelDebug) {
		level = zerolog.DebugLevel
	}
	// logger 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理
	logger := zerolog.New(&slogWriter{logger: config.Logger}).Level(level).With().Timestamp().Logger()
	// 连接参数由实例配置显式解析；TLSSet 用来区分默认 TLS 与显式明文连接
	raw, err := v0.NewFromConfig(&clientconfig.ClientConfig{
		Token:                config.Token,
		TenantId:             config.TenantID,
		ServerURL:            config.ServerURL,
		GRPCBroadcastAddress: config.Address,
		TLSConfig:            config.TLS,
		Namespace:            config.Namespace,
		Logger:               shared.LoggerConfigFile{Level: "info", Format: "json"},
	}, v0.WithLogger(&logger))
	if err != nil {
		return nil, Normalize(err)
	}

	// observer 跟踪执行、结果上报与注销过程的传输适配器
	observer := &transport{
		Client:            raw,
		ids:               map[string]string{},
		pending:           map[string]string{},
		listenerDone:      map[string]chan struct{}{},
		cancels:           map[uint64]executionCancel{},
		reports:           map[string]context.Context{},
		reportCancels:     map[string]context.CancelFunc{},
		unregisterBudgets: map[string]context.Context{},
	}
	observer.dispatcher = &dispatcher{DispatcherClient: raw.Dispatcher(), owner: observer}
	// 官方客户端没有公开连接注入入口；执行器借用 raw，带预算的协议调用由 wego 拥有
	// 两条连接有独立用途，均在 Close 中关闭，不通过反射取得官方私有字段
	var creds credentials.TransportCredentials = insecure.NewCredentials()
	if config.TLS != nil {
		creds = credentials.NewTLS(config.TLS)
	}
	// conn 本实例拥有的协议连接，所有成功和失败路径都必须关闭
	conn, err := grpc.NewClient(config.Address, grpc.WithTransportCredentials(creds), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(config.BackendMessageLimit), grpc.MaxCallSendMsgSize(config.BackendMessageLimit)))
	if err != nil {
		_ = raw.Close()
		return nil, Normalize(err)
	}

	// b 当前后端或测试后端对象，在所属实例内管理运行与资源，不经公开 API 暴露
	b := &Backend{
		raw:            raw,
		rpcConn:        conn,
		admin:          adminpb.NewWorkflowServiceClient(conn),
		v1admin:        v1.NewAdminServiceClient(conn),
		rpcDispatcher:  dispatcherpb.NewDispatcherClient(conn),
		durableStreams: v1.NewV1StreamsClient(conn),
		observer:       observer,
		config:         config,

		embeddedShutdown: embeddedShutdown,
	}
	// 后端取得 embedded 关闭权后，构造失败由 b.Close 负责一次清理。
	embeddedShutdown = nil
	observer.backend = b
	b.features = newFeatureClients(raw)
	b.workflowTasks = map[string][]string{}
	return b, nil
}

// Config 返回后端当前使用的连接配置，供内部适配层读取实例设置
func (b *Backend) Config() spec.Runtime {
	return b.config.Clone()
}

// Close 只释放一次私有连接和可选嵌入引擎，管理门面不持有后台缓存
func (b *Backend) Close() error {
	b.once.Do(func() {
		b.closeErr = errors.Join(Normalize(b.rpcConn.Close()), Normalize(b.raw.Close()))
		// 实例拥有嵌入引擎时，在客户端释放后执行其关闭回调
		if b.embeddedShutdown != nil {
			// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
			ctx, cancel := context.WithTimeout(context.Background(), b.config.CleanupTimeout())
			defer cancel()

			b.closeErr = errors.Join(b.closeErr, Normalize(b.embeddedShutdown(ctx)))
		}
	})
	return b.closeErr
}
