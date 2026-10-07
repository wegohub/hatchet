package spec

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/telemetry"
)

// ShutdownMode 实例排空策略枚举，区分有限预算与等待完成。
type ShutdownMode int

const (
	// DrainWithTimeout 按配置预算排空；例如预算 30 秒后取消剩余业务并清理资源。
	DrainWithTimeout ShutdownMode = iota
	// DrainUntilDone 持续等待业务完成；显式强制停止仍可中断等待。
	DrainUntilDone
)

// ShutdownConfig 实例关闭方式与资源清理预算。
type ShutdownConfig struct {
	// Mode 关闭模式：按预算排空或持续等待业务完成。
	Mode ShutdownMode
	// Timeout 关闭资源的预算；默认 30 秒。
	Timeout time.Duration
}

// StreamConfig 流协议资源限制；默认每方向 64 条、4 MiB，单条消息最多 1 MiB。
type StreamConfig struct {
	// Window 每个方向最多允许的未确认消息数；默认 64 条。
	Window int
	// BufferBytes 每个方向缓冲的字节预算；默认 4 MiB。
	BufferBytes int
	// MaxMessageBytes 单条业务消息的字节上限；默认 1 MiB。
	MaxMessageBytes int
	// HandshakeTimeout START 到 OPEN 握手的最长等待时间；默认 10 秒。
	HandshakeTimeout time.Duration
	// InitializationTTL 未打开会话和短期终态记录的保留时间；默认 30 秒。
	InitializationTTL time.Duration
	// CancelTimeout 清理时发送 CANCEL 控制任务的预算，默认 5 秒；零值采用默认值。
	// 此预算不缩短订阅与结果 goroutine 的退出屏障，也不能超过实例清理预算。
	CancelTimeout time.Duration
}

// EmbeddedConfig 嵌入引擎的数据库、端口与初始化配置。
type EmbeddedConfig struct {
	// DatabaseURL 嵌入引擎的 PostgreSQL 连接地址；日志不得输出其中凭证。
	DatabaseURL string
	// GRPCPort 嵌入引擎的 gRPC 监听端口。
	GRPCPort int
	// APIPort 嵌入引擎的 HTTP API 监听端口。
	APIPort int
	// DisableAPI 是否禁止嵌入引擎启动 API。
	DisableAPI bool
	// DisableMigrations 是否跳过嵌入数据库迁移；测试使用独立数据库。
	DisableMigrations bool
	// LogLevel 嵌入引擎日志等级，例如 info 或 debug。
	LogLevel string
}

// Runtime 实例级配置，负责连接、容量和生命周期；禁用 Worker 不禁止 Conn 调用。
type Runtime struct {
	// DisableWorker 仅控制 Server 的 Worker 入口，不限制 Conn 调用。
	DisableWorker bool

	// Token 后端访问令牌；不写入示例验收报告。
	Token string
	// Address 后端 gRPC 地址，例如 localhost:7077。
	Address string
	// ServerURL 后端 HTTP API 地址，例如 http://localhost:8080。
	ServerURL string
	// TenantID 当前授权范围的身份，由实例配置解析。
	TenantID string
	// Namespace 任务名称前缀；每次验收使用唯一值隔离测试资源。
	Namespace string

	// TLSSet 表示应用已显式选择 TLS 或明文，不能只靠 TLS 是否为 nil 判断。
	TLS *tls.Config
	// TLSSet 是否显式配置过 TLS；用于区分未设置和明确选择明文。
	TLSSet bool

	// 普通、durable 和控制任务分别使用独立容量。
	Slots int
	// DurableSlots durable 任务的独立容量。
	DurableSlots int
	// ControlSlots 流控制任务的独立容量，默认 4；业务槽满时仍能处理 ACK。
	ControlSlots int
	// Labels Worker 标签或运行亲和性条件；如 owner 标签要求会话路由到同一实例。
	Labels map[string]any

	// 日志、观测及载荷变换均按实例配置。
	Logger *slog.Logger
	// LogReport 是否向引擎上报任务日志。
	LogReport bool
	// Middleware 按顺序应用的业务中间件；载荷解码按反向顺序还原。
	Middleware []middleware.Option
	// Telemetry 实例观测配置，追踪与指标分别启用。
	Telemetry telemetry.Config
	// Shutdown 实例关闭策略和资源清理预算。
	Shutdown ShutdownConfig
	// ResultPollInterval 辅助取消状态查询的最小间隔；默认 1 秒，零值使用默认值。
	// 例如 100 个长期等待每秒最多发起约 100 次查询，而非每 250ms 全部查询。
	ResultPollInterval time.Duration
	// Stream 流方法描述或流配置，由所属类型决定。
	Stream StreamConfig

	// 完整 RPC 方法名 → routing 键 → protobuf 字段路径。
	Projections map[string]map[string]string
	// Embedded 嵌入引擎配置；nil 表示连接外部现有引擎。
	Embedded *EmbeddedConfig
}

// Defaults 构造默认实例配置；普通槽 100、durable 槽 1000、控制槽 4，流窗口 64。
func Defaults() Runtime {
	return Runtime{
		Slots:              100,
		DurableSlots:       1000,
		ControlSlots:       4,
		Logger:             slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
		Shutdown:           ShutdownConfig{Timeout: 30 * time.Second},
		ResultPollInterval: time.Second,
		Stream: StreamConfig{
			Window:            64,
			BufferBytes:       4 << 20,
			MaxMessageBytes:   1 << 20,
			HandshakeTimeout:  10 * time.Second,
			InitializationTTL: 30 * time.Second,
			CancelTimeout:     5 * time.Second,
		},
		Projections: map[string]map[string]string{},
		Labels:      map[string]any{},
	}
}

// Validate 检查配置组合的有效性，在资源启动前拒绝不合法的容量和预算。
func (r Runtime) Validate() error {
	if r.ResultPollInterval < 0 {
		return fmt.Errorf("wego: result polling interval cannot be negative")
	}
	if r.Logger == nil {
		return fmt.Errorf("wego: logger cannot be nil")
	}
	if r.Slots < 1 || r.DurableSlots < 1 || r.ControlSlots < 1 {
		return fmt.Errorf("wego: worker capacities must be positive")
	}
	if r.Shutdown.Mode == DrainWithTimeout && r.Shutdown.Timeout <= 0 {
		return fmt.Errorf("wego: shutdown timeout must be positive")
	}
	// 同时检查消息数量与字节预算，超限不进入业务缓冲；例如窗口 64 时第 65 条必须等待或失败。
	if r.Stream.Window < 1 ||
		r.Stream.MaxMessageBytes < 1 ||
		r.Stream.BufferBytes < r.Stream.MaxMessageBytes ||
		r.Stream.HandshakeTimeout <= 0 ||
		r.Stream.InitializationTTL <= 0 || r.Stream.CancelTimeout < 0 {
		return fmt.Errorf("wego: invalid stream limits")
	}

	return nil
}
