package spec

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"math"
	"os"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/telemetry"
)

// ShutdownMode 实例排空策略枚举，区分有限预算与等待完成
type ShutdownMode int

const (
	// DrainWithTimeout 按配置预算排空；例如预算 30 秒后取消剩余业务并清理资源
	DrainWithTimeout ShutdownMode = iota
	// DrainUntilDone 持续等待业务完成；显式强制停止仍可中断等待
	DrainUntilDone
)

// ShutdownConfig 实例关闭方式与资源清理预算
type ShutdownConfig struct {
	// Mode 关闭模式：按预算排空或持续等待业务完成
	Mode ShutdownMode
	// Timeout 关闭资源的预算；默认 30 秒
	Timeout time.Duration
}

// StreamConfig 流协议资源限制；默认输出预取 64 条、4 MiB，单条消息最多 1 MiB
type StreamConfig struct {
	// Window 输出最多预取的业务消息数；默认 64 条
	Window int
	// BufferBytes 输出预取的业务字节预算；默认 4 MiB
	BufferBytes int
	// MaxMessageBytes 单条业务消息的字节上限；默认 1 MiB
	MaxMessageBytes int
}

// EmbeddedConfig 嵌入引擎的数据库、端口与初始化配置
type EmbeddedConfig struct {
	// DatabaseURL 嵌入引擎的 PostgreSQL 连接地址；日志不得输出其中凭证
	DatabaseURL string
	// GRPCPort 嵌入引擎的 gRPC 监听端口
	GRPCPort int
	// APIPort 嵌入引擎的 HTTP API 监听端口
	APIPort int
	// DisableAPI 是否禁止嵌入引擎启动 API
	DisableAPI bool
	// DisableMigrations 是否跳过嵌入数据库迁移；测试使用独立数据库
	DisableMigrations bool
	// LogLevel 嵌入引擎日志等级，例如 info 或 debug
	LogLevel string
}

// Runtime 实例级配置，负责连接、容量和生命周期；禁用 Worker 不禁止 Conn 调用
type Runtime struct {
	// DisableWorker 仅控制 Server 的 Worker 入口，不限制 Conn 调用
	DisableWorker bool

	// Token 后端访问令牌；不写入示例验收报告
	Token string
	// Address 后端 gRPC 地址，例如 localhost:7077
	Address string
	// ServerURL 后端 HTTP API 地址，例如 http://localhost:8080
	ServerURL string
	// TenantID 当前授权范围的身份，由实例配置解析
	TenantID string
	// Namespace 命名作用域，默认空；验收使用唯一值隔离测试资源
	Namespace string
	// BackendMessageLimit 限制自有协议连接的完整 RPC 字节，默认 4 MiB
	BackendMessageLimit int
	// InstanceName 覆盖 Worker 完整展示名称，不参与寻址或执行所有权
	InstanceName string

	// TLSSet 表示应用已显式选择 TLS 或明文，不能只靠 TLS 是否为 nil 判断
	TLS *tls.Config
	// TLSSet 是否显式配置过 TLS；用于区分未设置和明确选择明文
	TLSSet bool

	// 普通和 durable 任务分别使用独立容量
	Slots int
	// DurableSlots durable 任务的独立容量
	DurableSlots int
	// Labels 保存 Worker 附加标签，例如 region；注册时追加最终名称对应的 worker_name
	Labels map[string]any

	// Logger 是后端运行日志配置；WithLogger 应用时同时原子发布 wego log 包共享出口。
	Logger *slog.Logger
	// LogReport 是否向引擎上报任务日志
	LogReport bool
	// LogReportTimeout 单次同步日志上报预算；默认 5 秒，调用 deadline 更早时优先。
	LogReportTimeout time.Duration
	// Middleware 按顺序应用的业务中间件；载荷解码按反向顺序还原
	Middleware []middleware.Option
	// Telemetry 实例观测配置，追踪与指标分别启用
	Telemetry telemetry.Config
	// Shutdown 实例关闭策略和资源清理预算
	Shutdown ShutdownConfig
	// ResultPollInterval 辅助取消状态查询的最小间隔；默认 1 秒，零值使用默认值
	// 例如 100 个长期等待每秒最多发起约 100 次查询，而非每 250ms 全部查询
	ResultPollInterval time.Duration
	// Stream 流方法描述或流配置，由所属类型决定
	Stream StreamConfig

	// StreamMethods 保存各 RPC 方法的输出模式和预算
	StreamMethods map[string]StreamOptions

	// WorkerEventHandler 接收当前实例的定向及广播业务通知；不阻塞内部取消
	WorkerEventHandler func(context.Context, model.WorkerEvent) error
	// WorkerEvents 限定定向及广播通知的资源，不影响任务 slots
	WorkerEvents WorkerEventOptions
	// RoutingDefaults 为每个完整 RPC 方法保存可读的调度字段快照
	RoutingDefaults map[string]map[string]any
	// routingError 保存选项构造时的非法 JSON，启动和提交必须明确拒绝
	routingError error
	// Embedded 嵌入引擎配置；nil 表示连接外部现有引擎
	Embedded *EmbeddedConfig
}

// Defaults 构造默认实例配置；普通槽 100、durable 槽 1000，输出预取 64 条
func Defaults() Runtime {
	return Runtime{
		BackendMessageLimit: 4 << 20,
		Slots:               100,
		DurableSlots:        1000,
		Logger:              slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
		Shutdown:            ShutdownConfig{Timeout: 30 * time.Second},
		ResultPollInterval:  time.Second,
		LogReportTimeout:    5 * time.Second,
		Stream: StreamConfig{
			Window:          64,
			BufferBytes:     4 << 20,
			MaxMessageBytes: 1 << 20,
		},
		RoutingDefaults: map[string]map[string]any{},
		Labels:          map[string]any{},
	}
}

// CleanupTimeout 为资源释放提供独立预算；无限业务排空也必须有有限的连接清理期限。
func (r *Runtime) CleanupTimeout() time.Duration {
	if r.Shutdown.Timeout > 0 {
		return r.Shutdown.Timeout
	}
	return 30 * time.Second
}

// Validate 检查配置组合的有效性，在资源启动前拒绝不合法的容量和预算
func (r *Runtime) Validate() error {
	// options 保存合并后的独立预算，校验不改变实例配置。
	options := r.WorkerEventOptionsFor()
	if err := options.Validate(); err != nil {
		return err
	}
	for method := range r.StreamMethods {
		streamOptions := r.StreamOptionsFor(method)
		if err := streamOptions.Validate(); err != nil {
			return fmt.Errorf("%s: %w", method, err)
		}
	}
	if r.routingError != nil {
		return r.routingError
	}
	if r.ResultPollInterval < 0 {
		return fmt.Errorf("wego: result polling interval cannot be negative")
	}
	if r.BackendMessageLimit < 1 {
		return fmt.Errorf("wego: backend message limit must be positive")
	}
	if r.Logger == nil {
		return fmt.Errorf("wego: logger cannot be nil")
	}
	if r.LogReportTimeout <= 0 {
		return fmt.Errorf("wego: log report timeout must be positive")
	}
	if r.Slots < 1 || r.DurableSlots < 1 || r.Slots > math.MaxInt32 || r.DurableSlots > math.MaxInt32 {
		return fmt.Errorf("wego: worker capacities must be positive")
	}
	if r.Shutdown.Mode == DrainWithTimeout && r.Shutdown.Timeout <= 0 {
		return fmt.Errorf("wego: shutdown timeout must be positive")
	}
	// 同时检查消息数量与字节预算，超限不进入业务缓冲；例如窗口 64 时第 65 条必须等待或失败
	if r.Stream.Window < 1 ||
		r.Stream.MaxMessageBytes < 1 ||
		r.Stream.BufferBytes < r.Stream.MaxMessageBytes {
		return fmt.Errorf("wego: invalid stream limits")
	}

	return nil
}
