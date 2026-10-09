package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
)

// newProtocolMetrics 声明有限指标表；每个实例有独立 registry，默认关闭时不创建任何 collector
func newProtocolMetrics(registry *prometheus.Registry) map[string]prometheus.Collector {
	collectors := map[string]prometheus.Collector{}
	for _, name := range []string{"wego_stream_publish_total", "wego_stream_claim_conflict_total", "wego_checkpoint_restore_total", "wego_output_messages_total", "wego_worker_event_rejected_total", "wego_frame_encode_total", "wego_frame_decode_total"} {
		collectors[name] = prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: "Instance protocol outcomes."}, []string{"method", "mode", "outcome"})
	}
	for _, name := range []string{"wego_stream_publish_duration_seconds", "wego_stream_claim_duration_seconds", "wego_checkpoint_restore_duration_seconds", "wego_checkpoint_history_messages", "wego_frame_encode_duration_seconds", "wego_frame_decode_duration_seconds"} {
		buckets := prometheus.DefBuckets
		if name == "wego_checkpoint_history_messages" {
			buckets = prometheus.ExponentialBuckets(1, 2, 12)
		}
		collectors[name] = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: "Instance protocol operation distribution.", Buckets: buckets}, []string{"method", "mode", "outcome"})
	}
	for _, name := range []string{"wego_output_prefetch_messages", "wego_output_prefetch_bytes", "wego_worker_event_queue_size"} {
		collectors[name] = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: "Currently retained instance protocol messages."}, []string{"method", "mode", "outcome"})
	}
	for _, collector := range collectors {
		registry.MustRegister(collector)
	}
	return collectors
}

// ObserveProtocol 按固定类型记录增量；不把错误文本、业务 group 或运行身份当作标签
func (r *Resources) ObserveProtocol(metric ports.ProtocolMetric) {
	if r == nil || r.protocol == nil {
		return
	}
	switch collector := r.protocol[metric.Name].(type) {
	case *prometheus.CounterVec:
		if metric.Value >= 0 {
			collector.WithLabelValues(metric.Method, metric.Mode, metric.Outcome).Add(metric.Value)
		}
	case *prometheus.HistogramVec:
		collector.WithLabelValues(metric.Method, metric.Mode, metric.Outcome).Observe(metric.Value)
	case *prometheus.GaugeVec:
		collector.WithLabelValues(metric.Method, metric.Mode, metric.Outcome).Add(metric.Value)
	}
}
