package model

import "google.golang.org/protobuf/proto"

// StreamHistory 是接管时确定的有限输出历史，EOF 表示已经完整恢复；Close 释放订阅
type StreamHistory interface {
	// Next 将下一条历史业务输出解码到调用方提供的 protobuf 消息
	Next(proto.Message) error
	// Close 中止扫描并释放资源，未读完不能继续发布新业务输出
	Close() error
}

// StreamCompletion 为引擎最终接受的流执行携带输出清单，业务错误也保留此身份
type StreamCompletion struct {
	// Version 对应完整帧协议版本
	Version uint32 `json:"version"`
	// TaskID 是实际 task 的身份
	TaskID string `json:"task_run_id"`
	// Epoch 为最终 attempt 的 RetryCount
	Epoch int32 `json:"epoch"`
	// Writer 是此 attempt 获胜的 nonce
	Writer string `json:"writer"`
	// LastOutput 是全部有效业务输出的末条序号
	LastOutput uint64 `json:"last_output"`
	// EndID 指向完整编码后持久发布的结束帧
	EndID string `json:"end_id"`
}

// StreamCheckpoint 是业务可保存的消费断点，仅在一条输出成功交付后推进 Cursor
type StreamCheckpoint struct {
	// Version 防止混用不同日志协议
	Version uint32 `json:"version"`
	// Namespace 与 TenantID 校验恢复连接的作用域
	Namespace string `json:"namespace"`
	// TenantID 是授权租户身份，不包含 token
	TenantID string `json:"tenant_id"`
	// RunID 是现有运行管理身份，恢复不重新提交输入
	RunID string `json:"run_id"`
	// TaskID 是现有 standalone 的任务身份
	TaskID string `json:"task_run_id"`
	// Method 是完整 protobuf 方法名
	Method string `json:"method"`
	// Mode 必须为 reliable，实时流不能生成消费断点
	Mode string `json:"mode"`
	// InputDigest 核对原始调用身份和 routing
	InputDigest string `json:"input_digest"`
	// Cursor 指向最后成功交付的 DATA；空值从最早可验证位置开始
	Cursor string `json:"cursor,omitempty"`
	// Epoch 和 Writer 对应此交付位置的有效 producer
	Epoch int32 `json:"epoch"`
	// Writer 是此位置的获胜实际执行 nonce
	Writer string `json:"writer,omitempty"`
	// WorkerKey 是此位置的通知地址，与展示名称无关
	WorkerKey string `json:"worker_key,omitempty"`
	// OutputSeq 是已经成功交付的累计业务输出序号
	OutputSeq uint64 `json:"output_seq"`
	// Headers 是已经交付的多值原始字节；JSON 使用 base64 保留 -bin
	Headers map[string][][]byte `json:"headers,omitempty"`
	// HeadersSeen 区分空响应头与尚未交付
	HeadersSeen bool `json:"headers_seen"`
}
