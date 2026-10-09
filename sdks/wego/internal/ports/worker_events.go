package ports

import "github.com/hatchet-dev/hatchet/sdks/wego/model"

// WorkerSendEvent 发布定向或广播业务消息，空 WorkerKey 表示 namespace 广播
type WorkerSendEvent struct {
	// WorkerKey 是定向实例随机身份，不使用 WorkerID 或 hostname
	WorkerKey string
	// Event 使用自有可序列化模型
	Event model.WorkerEvent
}

// featureRequest 将业务通知加入封闭管理命令集合
func (WorkerSendEvent) featureRequest() {}

// WorkerCancelNotice 仅取消明确的旧执行，不能取消整个逻辑 Run
type WorkerCancelNotice struct {
	// WorkerKey 是旧执行的通知地址
	WorkerKey string
	// TaskID 是需要退出的旧任务执行身份
	TaskID string
	// OldEpoch 和 OldWriter 同时约束旧实际执行
	OldEpoch int32
	// OldWriter 是旧实际执行 nonce
	OldWriter string
	// NewEpoch 必须严格大于旧代次
	NewEpoch int32
}

// featureRequest 只允许协议内部发布旧执行通知
func (WorkerCancelNotice) featureRequest() {}
