// Package engine 管理一个 wego 实例的连接、Worker、在途调用和观测资源。
// 关闭时先排空执行及控制通道，再刷新观测数据，最后释放底层连接。
package engine
