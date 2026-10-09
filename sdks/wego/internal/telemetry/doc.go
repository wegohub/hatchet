// Package telemetry 拥有实例级追踪 provider、exporter 与关闭流程，公开 telemetry 包仅声明配置例如两个 Conn 各自创建 provider，关闭其中一个只 flush 自己的 exporter，且不修改 OTel 全局 provider
package telemetry
