// Package client 提供 gRPC 兼容连接、任务执行及管理客户端
// 同一个 Conn 可用于生成的 protobuf 客户端和 Run、RunNoWait、RunMany 调用
// WithRunKey、WithRunSticky、WithRunPriority、WithRunMetadata、WithDesiredWorkerLabels 沿用官方 Go SDK 命名。
// WithRouting 与 WithStreamIdempotencyKey 是 wego 传输扩展；StreamResumeToken 读取 Client 消费进度。
package client
