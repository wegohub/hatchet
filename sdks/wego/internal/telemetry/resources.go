package telemetry

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/telemetry"
)

// Resources 实例拥有的 trace provider 和指标 HTTP 资源，关闭时不修改全局 provider
type Resources struct {
	// Provider 本实例拥有的 trace provider，不改变 OTel 全局 provider
	Provider *sdktrace.TracerProvider
	// server 指标 HTTP 服务，关闭时释放端口
	server *http.Server
	// listener 当前对象拥有的网络监听器，停止时先关闭接收入口
	listener net.Listener
	// requests 按方法和状态计数的请求指标
	requests *prometheus.CounterVec
	// duration 请求耗时直方图
	duration *prometheus.HistogramVec
	// protocol 是有限的流和通知指标表；关闭指标时为 nil
	protocol map[string]prometheus.Collector
}

// borrowedExporter 不接管调用方的 exporter 生命周期，避免一个实例关闭其他实例的出口
type borrowedExporter struct {
	// sdktrace.SpanExporter 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	sdktrace.SpanExporter
}

// Shutdown 在传入 context 的预算内排空调用并关闭资源，预算到期后取消剩余工作
func (e *borrowedExporter) Shutdown(context.Context) error {
	return nil
}

// tolerantExporter 对未启用 trace 接收的引擎暂缓发送，避免重复报告 Unimplemented
type tolerantExporter struct {
	// sdktrace.SpanExporter 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	sdktrace.SpanExporter
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁
	mu sync.Mutex
	// retryAt 导出器失败后的下次尝试时间，避免持续故障时密集重试
	retryAt time.Time
}

// ExportSpans 接收完成的 span 批次，按此 exporter 的所有权和失败策略处理
func (e *tolerantExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	// retry 读取本机时钟，用于耗时或超时判断，不参与 durable 重放
	retry := time.Now().Before(e.retryAt)
	e.mu.Unlock()
	if retry {
		return nil
	}

	// err 接收 e.SpanExporter.ExportSpans 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	err := e.SpanExporter.ExportSpans(ctx, spans)
	if status.Code(err) == codes.Unimplemented {
		e.mu.Lock()
		e.retryAt = time.Now().Add(5 * time.Minute)
		e.mu.Unlock()
		return nil
	}

	return err
}

// New 创建实例专属 provider，不调用 OTel 全局设置函数
// 调用方提供的 exporter 由调用方关闭，实例仅负责刷新自己提交的 span
func New(ctx context.Context, config telemetry.Config, address, token string, tlsConfig *tls.Config) (*Resources, error) {
	// options 明确列出本段注册、传输或清理选项，应用顺序决定重复字段的覆盖关系
	options := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewWithAttributes("", attribute.String("service.name", "wego"))),
	}
	if !config.Trace.DisableWorkerExporter {
		// opts 明确列出本段注册、传输或清理选项，应用顺序决定重复字段的覆盖关系
		opts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(address),
			otlptracegrpc.WithHeaders(map[string]string{"authorization": "Bearer " + token}),
		}
		if tlsConfig == nil {
			opts = append(opts, otlptracegrpc.WithInsecure())
		} else {
			opts = append(opts, otlptracegrpc.WithTLSCredentials(credentials.NewTLS(tlsConfig)))
		}
		// exporter, err 接收 otlptracegrpc.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		exporter, err := otlptracegrpc.New(ctx, opts...)
		if err != nil {
			return nil, err
		}

		options = append(options, sdktrace.WithBatcher(&tolerantExporter{SpanExporter: exporter}))
	}
	// 逐项处理 config.Trace.Exporters，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, exporter := range config.Trace.Exporters {
		options = append(options, sdktrace.WithBatcher(&borrowedExporter{SpanExporter: exporter}))
	}
	// r 拥有实例独立 provider 和指标监听器，关闭只释放这一组资源，不改写全局 provider
	r := &Resources{Provider: sdktrace.NewTracerProvider(options...)}
	if config.Metrics.Enabled {
		// registry 创建实例独占的指标注册表，不改写 Prometheus 全局状态
		registry := prometheus.NewRegistry()
		r.protocol = newProtocolMetrics(registry)
		r.requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wego_rpc_calls_total", Help: "Completed RPC calls."}, []string{"method", "side", "code"})
		r.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wego_rpc_duration_seconds",
			Help:    "RPC call duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "side"})
		registry.MustRegister(r.requests, r.duration)
		// address 指标 HTTP 监听地址，空值使用本入口的默认地址
		address := config.Metrics.Addr
		if address == "" {
			address = "127.0.0.1:9464"
		}
		// listener, err 接收 net.Listen 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		listener, err := net.Listen("tcp", address)
		if err != nil {
			_ = r.Provider.Shutdown(ctx)
			return nil, err
		}

		// mux 创建实例独占的指标路由，监听器随实例一起关闭
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
		r.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		r.listener = listener
		go func() {
			_ = r.server.Serve(listener)
		}()
	}
	return r, nil
}

// Observe 记录当前方法的请求次数与耗时，指标关闭时不创建监听资源
func (r *Resources) Observe(method, side string, code codes.Code, duration time.Duration) {
	if r.requests != nil {
		r.requests.WithLabelValues(method, side, code.String()).Inc()
		r.duration.WithLabelValues(method, side).Observe(duration.Seconds())
	}
}

// Close 释放此对象拥有的资源；借用资源必须由所有者关闭
func (r *Resources) Close(ctx context.Context) error {
	// errs 明确列出本段注册、传输或清理选项，应用顺序决定重复字段的覆盖关系
	errs := []error{r.Provider.ForceFlush(ctx), r.Provider.Shutdown(ctx)}
	if r.server != nil {
		// err 保存资源释放结果，清理错误同样必须报告，不能因业务已成功而忽略
		if err := r.server.Shutdown(ctx); err != nil {
			errs = append(errs, err, r.server.Close())
		}
	}
	// 显式关闭监听器，覆盖 Serve goroutine 尚未启动便进入 Shutdown 的竞态
	if r.listener != nil {
		// err 排除已关闭监听器的正常重复关闭，只记录实际资源释放失败
		if err := r.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
