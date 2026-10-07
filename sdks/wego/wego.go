package wego

import (
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
)

// Conn 连接视图，同时提供标准 gRPC 调用接口和任务管理入口；借用视图不拥有底层资源
type Conn = client.Conn

// Server 共享服务注册表及 Worker、网络双入口生命周期；默认仅启用 Worker
type Server = server.Server

// ServerOption 是 wego 服务配置选项
type ServerOption = server.Option

// NewConn 创建拥有实例资源的连接；例如 pb.NewUnaryGreeterClient(conn) 使用任务传输
func NewConn(options ...client.Option) (*Conn, error) {
	return client.New(options...)
}

// NewServer 采用 gRPC 的单返回值构造形式，启动错误通过 Serve 返回
func NewServer(options ...ServerOption) *Server {
	return server.New(options...)
}
