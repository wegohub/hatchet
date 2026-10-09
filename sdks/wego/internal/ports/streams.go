package ports

import (
	"context"
	"encoding/json"
	"time"
)

// DurableMessage 是持久 topic 的发布参数；调用方保留 producer 和序号以重试同一次写入
type DurableMessage struct {
	// Namespace 隔离同一租户的不同实例组，允许为空
	Namespace string
	// Topic 是持久日志名称，例如 rpc.<TaskRunID>
	Topic string
	// Producer 标识一个有序发布者，重试不能切换此值
	Producer string
	// Sequence 从零开始；同一序号必须复用相同载荷
	Sequence int64
	// Payload 是 codec 处理后的完整帧字节
	Payload []byte
}

// DurableEntry 保存持久消息和恢复位置；Cursor 不解析、不参与数值比较
type DurableEntry struct {
	// Payload 是持久存储的编码帧，不在后端执行 codec
	Payload []byte
	// Cursor 用于从本消息之后恢复订阅
	Cursor string
	// CreatedAt 是服务端存储时间
	CreatedAt time.Time
}

// DurableSubscription 指定独立日志的消费起点，空 Cursor 表示最早保留位置
type DurableSubscription struct {
	// Namespace 是与发布一致的隔离字段
	Namespace string
	// Topic 是与发布一致的日志名称
	Topic string
	// Cursor 为 nil 时读取历史，非 nil 时从指定位置之后读取
	Cursor *string
}

// DurableStreams 保留正式协议的发布身份和订阅错误，不把 channel 关闭当作任务成功
type DurableStreams interface {
	// PublishDurable 仅发布指定身份的消息，不分配序号、不更换 producer
	PublishDurable(context.Context, DurableMessage) error
	// SubscribeDurable 按持久顺序交付消息；hangup 返回可重连错误，回调错误原样返回
	SubscribeDurable(context.Context, DurableSubscription, func(DurableEntry) error) error
}

// RunInputReader 为幂等恢复核对持久输入，不重新提交任务
type RunInputReader interface {
	// RunInput 返回独立的持久 JSON 快照
	RunInput(context.Context, string) (json.RawMessage, error)
}

// RunLookup 为恢复提供真实 task 身份和观察句柄，不假定 TaskRunID 与 RunID 相同
type RunLookup interface {
	// LookupRun 返回持久 standalone 的唯一任务身份和现有运行句柄
	LookupRun(context.Context, string) (string, Run, error)
}

// SubmissionFinder 只读恢复响应丢失的提交，不通过再次创建任务确认其身份
type SubmissionFinder interface {
	// FindSubmission 按本次实际提交的随机标识查找，并核对方法和原始输入摘要
	FindSubmission(context.Context, string, string, string) (Run, error)
}
