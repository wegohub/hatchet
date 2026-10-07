// Package telemetry 管理实例专属的 trace provider 和可选 metrics 服务。
// 实例不修改 OTel 全局 provider，也不接管调用方传入的 exporter。
package telemetry
