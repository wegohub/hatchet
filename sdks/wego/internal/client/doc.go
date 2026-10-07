// Package client 实现连接、任务定义与原生 Worker 的内部装配。公开 client 包只提供业务入口，Server 借用连接时不获得共享资源的关闭权。例如两个 Server 共用一个 Conn，停止其中一个只注销其 Worker。
package client
