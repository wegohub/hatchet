package model

import (
	"encoding/json"
	"time"
)

// WorkerEvent 是轻量持久通知，发布成功不代表业务回调已经处理
type WorkerEvent struct {
	// ID 是消息身份，默认生成 UUID；业务重试应复用同一个值
	ID string `json:"id"`
	// Type 是业务事件类型，SDK 内部取消不通过此回调分发
	Type string `json:"type"`
	// Payload 保存独立 JSON 快照，不携带后端类型
	Payload json.RawMessage `json:"payload,omitempty"`
	// SentAt 是客户端首次冻结消息的时间
	SentAt time.Time `json:"sent_at"`
	// ExpiresAt 限制迟到消息，默认 SentAt 后五分钟，可通过实例 TTL 调整
	ExpiresAt time.Time `json:"expires_at"`
	// WorkerKey 在回调中标识当前接收实例，不作为展示名称
	WorkerKey string `json:"worker_key,omitempty"`
}
