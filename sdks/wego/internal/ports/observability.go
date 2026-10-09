package ports

// ProtocolMetric 只使用已注册方法和有限结果标签；运行 UUID、游标及 writer 不进入时间序列
type ProtocolMetric struct {
	// Name 是 SDK 声明的指标名称，未知名称直接拒绝记录
	Name string
	// Method 使用完整已注册 RPC 或固定 WorkerEvents 方法
	Method string
	// Mode 限定 reliable、realtime 或 worker
	Mode string
	// Outcome 限定成功、冲突、拒绝或有限错误类别
	Outcome string
	// Value 是增量、耗时秒数或历史消息数，由对应指标类型解释
	Value float64
}

// ProtocolObserver 让协议组件复用实例指标，不依赖全局 registry 或公开后端对象
type ProtocolObserver interface {
	// ObserveProtocol 记录一个已经确定的协议事实
	ObserveProtocol(ProtocolMetric)
}
