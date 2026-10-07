package telemetry

import (
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Config 当前包的配置集合，分离实例资源配置与业务执行策略。
type Config struct {
	// Trace 追踪传播字段或实例追踪配置。
	Trace TraceConfig
	// Metrics 指标配置或管理查询入口；实例指标监听默认关闭。
	Metrics MetricsConfig
}

// TraceConfig 默认向 Hatchet 导出 trace，也可增加由调用方管理的 exporter。
type TraceConfig struct {
	// DisableWorkerExporter 禁止向 Worker 后端导出追踪；其他配置的 exporter 仍可使用。
	DisableWorkerExporter bool
	// Exporters 追加的 span exporter 集合，生命周期遵循配置的所有权规则。
	Exporters []sdktrace.SpanExporter
}

// MetricsConfig 默认关闭；启用时监听实例自己的 HTTP 地址。
type MetricsConfig struct {
	// Enabled 是否启用指标，默认关闭。
	Enabled bool
	// Addr 指标 HTTP 监听地址。
	Addr string
}
