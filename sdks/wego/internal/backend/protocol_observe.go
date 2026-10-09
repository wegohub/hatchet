package backend

import "github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"

// ObserveProtocol 把协议事实交给实例观测资源；backend 不创建另一份指标或全局 provider
func (b *Backend) ObserveProtocol(metric ports.ProtocolMetric) {
	if b != nil && b.ProtocolMetrics != nil {
		b.ProtocolMetrics(metric)
	}
}

// FrameObserver 仅在实例指标启用时提供出口，关闭时编解码不增加计时成本
func (b *Backend) FrameObserver() ports.ProtocolObserver {
	if !b.config.Telemetry.Metrics.Enabled {
		return nil
	}
	return b
}
