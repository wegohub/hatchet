// Package session 在任务和事件订阅之上实现 gRPC 流会话。
// 业务任务持有会话容量，控制任务独立处理握手、ACK、半关闭和取消。
package session
