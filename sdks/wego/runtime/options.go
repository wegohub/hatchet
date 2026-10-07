package runtime

import (
	"crypto/tls"
	"log/slog"
	"maps"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/telemetry"
)

// Option 配置函数或自有配置视图；按传入顺序应用，相同字段后者覆盖前者。
type Option func(*spec.Runtime)

// ShutdownConfig 实例关闭方式与资源清理预算。
type ShutdownConfig = spec.ShutdownConfig

// StreamConfig 流协议资源限制；默认每方向 64 条、4 MiB，单条消息最多 1 MiB。
type StreamConfig = spec.StreamConfig

// EmbeddedConfig 嵌入引擎的数据库、端口与初始化配置。
type EmbeddedConfig = spec.EmbeddedConfig

// WithResultPollInterval 配置结果等待的辅助取消状态查询间隔，零值采用默认 1 秒。
// 例如 2 秒减少长任务的 HTTP 查询数量，但外部取消的发现可能相应延后；结果订阅不受影响。
func WithResultPollInterval(interval time.Duration) Option {
	return func(c *spec.Runtime) { c.ResultPollInterval = interval }
}

const (
	// DrainWithTimeout 按配置预算排空；例如预算 30 秒后取消剩余业务并清理资源。
	DrainWithTimeout = spec.DrainWithTimeout
	// DrainUntilDone 持续等待业务完成；显式强制停止仍可中断等待。
	DrainUntilDone = spec.DrainUntilDone
)

// WithToken 配置 Token：后端访问令牌；不写入示例验收报告。
func WithToken(value string) Option {
	return func(c *spec.Runtime) {
		c.Token = value
	}
}

// WithAddress 配置 Address：后端 gRPC 地址，例如 localhost:7077。
func WithAddress(value string) Option {
	return func(c *spec.Runtime) {
		c.Address = value
	}
}

// WithHostPort 组合后端主机与端口，IPv6 使用合法的方括号地址。
func WithHostPort(host string, port int) Option {
	return WithAddress(fmtHostPort(host, port))
}

// WithServerURL 配置 ServerURL：后端 HTTP API 地址，例如 http://localhost:8080。
func WithServerURL(value string) Option {
	return func(c *spec.Runtime) {
		c.ServerURL = value
	}
}

// WithTenantID 配置 TenantID：当前授权范围的身份，由实例配置解析。
func WithTenantID(value string) Option {
	return func(c *spec.Runtime) {
		c.TenantID = value
	}
}

// WithNamespace 配置 Namespace：任务名称前缀；每次验收使用唯一值隔离测试资源。
func WithNamespace(value string) Option {
	return func(c *spec.Runtime) {
		c.Namespace = value
	}
}

// WithTLSConfig 显式选择 TLS 配置；传入 nil 使用明文连接。
// 例如 WithTLSConfig(nil) 明确选择 localhost:7077 明文连接，后端推导配置不能把它改成 TLS。
func WithTLSConfig(value *tls.Config) Option {
	return func(c *spec.Runtime) {
		if value == nil {
			c.TLS = nil
		} else {
			c.TLS = value.Clone()
		}
		c.TLSSet = true
	}
}

// WithSlots 配置 Slots：普通业务任务容量；流会话业务任务也占用此容量。
func WithSlots(value int) Option {
	return func(c *spec.Runtime) {
		c.Slots = value
	}
}

// WithDurableSlots 配置 DurableSlots：durable 任务的独立容量。
func WithDurableSlots(value int) Option {
	return func(c *spec.Runtime) {
		c.DurableSlots = value
	}
}

// WithControlSlots 配置 ControlSlots：流控制任务的独立容量，默认 4；业务槽满时仍能处理 ACK。
func WithControlSlots(value int) Option {
	return func(c *spec.Runtime) {
		c.ControlSlots = value
	}
}

// WithLabels 配置 Labels：Worker 标签或运行亲和性条件；如 owner 标签要求会话路由到同一实例。
func WithLabels(value map[string]any) Option {
	return func(c *spec.Runtime) {
		c.Labels = maps.Clone(value)
	}
}

// WithLogger 配置 Logger：实例日志记录器；如 slog.LevelDebug 可输出调试日志。
func WithLogger(value *slog.Logger) Option {
	return func(c *spec.Runtime) {
		c.Logger = value
	}
}

// WithLogReport 配置 LogReport：是否向引擎上报任务日志。
func WithLogReport(value bool) Option {
	return func(c *spec.Runtime) {
		c.LogReport = value
	}
}

// WithMiddleware 配置 Middleware：按顺序应用的业务中间件；载荷解码按反向顺序还原。
func WithMiddleware(value ...middleware.Option) Option {
	return func(c *spec.Runtime) {
		c.Middleware = append(c.Middleware, value...)
	}
}

// WithTelemetry 配置 Telemetry：实例观测配置，追踪与指标分别启用。
func WithTelemetry(value telemetry.Config) Option {
	return func(c *spec.Runtime) {
		c.Telemetry = value
	}
}

// WithMetrics 配置实例指标监听，默认关闭；开启时需提供可用地址。
func WithMetrics(value telemetry.MetricsConfig) Option {
	return func(c *spec.Runtime) {
		c.Telemetry.Metrics = value
	}
}

// WithShutdown 设置关闭策略；资源清理预算与调用方业务排空预算分别控制。
func WithShutdown(value ShutdownConfig) Option {
	return func(c *spec.Runtime) {
		c.Shutdown = value
	}
}

// WithStream 配置每个方向的窗口、缓冲和消息上限，例如 64 条、4 MiB、1 MiB。
func WithStream(value StreamConfig) Option {
	return func(c *spec.Runtime) {
		c.Stream = value
	}
}

// WithEmbedded 启用实例拥有的嵌入引擎，启动和迁移必须使用明确的数据库配置。
func WithEmbedded(value EmbeddedConfig) Option {
	return func(c *spec.Runtime) {
		c.Embedded = &value
	}
}

// WithInputProjection 配置 routing 键到 protobuf 字段路径的映射，并复制字段表。
// 客户端与 Worker 必须使用同一份映射，CEL 通过 input.routing.<键> 读取投影。
// 例如 /wego.example.v1.UnaryGreeter/SayHello 配置 {"group":"group_key"}，CEL 使用 input.routing.group。
func WithInputProjection(method string, fields map[string]string) Option {
	return func(c *spec.Runtime) {
		// copied routing 键到 protobuf 字段路径的独立映射，防止调用方修改配置后改变实例投影。
		copied := map[string]string{}
		// 按显式投影映射提取字段，例如 group→group_key 写入 routing.group，不自动暴露其他业务字段。
		for k, v := range fields {
			copied[k] = v
		}
		c.Projections[method] = copied
	}
}

// WithDisableWorker 禁止 Server 启动 Worker 引擎，Conn 仍可发起任务调用。
// 例如 WithGRPC(":9000") 配合此选项提供纯网络服务，不需要 token，也不创建任务后端。
func WithDisableWorker() Option {
	return func(c *spec.Runtime) { c.DisableWorker = true }
}
