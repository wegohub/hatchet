package client

import (
	"context"

	"google.golang.org/grpc"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	features "github.com/hatchet-dev/hatchet/sdks/wego/internal/features"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/rpc"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// Option 配置函数或自有配置视图；按传入顺序应用，相同字段后者覆盖前者
type Option func(*spec.Runtime)

// WithRuntime 按传入顺序应用 Runtime 选项，重复字段由后面的设置覆盖
func WithRuntime(options ...runtime.Option) Option {
	return func(c *spec.Runtime) {
		// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序
		for _, o := range options {
			o(c)
		}
	}
}

// Conn 同时实现 gRPC 调用接口和 wego 任务入口
// 从任务上下文借用的连接不拥有底层资源，Close 和 Shutdown 会拒绝关闭它
type Conn struct {
	// engine 本实例的执行引擎，协调调用、Worker 与资源关闭
	engine *engine.Engine
	// borrowed 是否为借用视图；true 时不得关闭共享引擎
	borrowed bool
	// features 复用同一后端的管理客户端集合
	features *features.Clients
}

// New 合并连接配置并创建实例引擎，返回拥有资源的 Conn
func New(options ...Option) (*Conn, error) {
	// config 创建默认配置副本，后续显式选项可以覆盖 0、false 或 nil
	config := spec.Defaults()
	// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序
	for _, o := range options {
		o(&config)
	}
	// e, err 接收 engine.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	e, err := engine.New(config)
	if err != nil {
		return nil, err
	}

	return FromEngine(e, false), nil
}

// FromEngine 为同一实例创建连接视图；borrowed 表示调用方不拥有资源
func FromEngine(e *engine.Engine, borrowed bool) *Conn {
	// c 共享此 Engine 的连接视图，borrowed 控制关闭权限，管理入口复用同一后端
	c := &Conn{engine: e, borrowed: borrowed}
	c.features = features.New(&managedFeatures{Backend: e.Backend, engine: e}, func(ctx context.Context, name string, value any) (string, any, error) {
		return rpc.Input(ctx, e.Config, name, value)
	})
	if !borrowed {
		e.Borrow = func() any {
			return FromEngine(e, true)
		}
	}
	return c
}

// Invoke 实现 grpc.ClientConnInterface 的 unary 调用，将 protobuf 请求提交为任务并把结果解码到 reply；调用上下文控制等待预算
func (c *Conn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	return rpc.Invoke(ctx, c.engine, method, args, reply, opts...)
}

// NewStream 实现 grpc.ClientConnInterface 的流调用，建立 owner 会话并返回标准 gRPC ClientStream
func (c *Conn) NewStream(ctx context.Context, description *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return rpc.NewStream(ctx, c.engine, description, method, opts...)
}

// Close 释放此对象拥有的资源；借用资源必须由所有者关闭
func (c *Conn) Close() error {
	// 借用连接不拥有底层资源，拒绝关闭；例如 handler 中取得的连接须由外层实例统一关闭
	if c.borrowed {
		return model.ErrBorrowedResource
	}

	return c.engine.Close()
}

// Info 读取当前任务或实例的信息；任务身份只存在于 Worker 入口
func (c *Conn) Info(ctx context.Context) (model.InstanceInfo, error) {
	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
	var out model.InstanceInfo
	// err 当前操作产生的错误；nil 表示该步骤成功
	err := (&managedFeatures{Backend: c.engine.Backend, engine: c.engine}).Feature(ctx, ports.RuntimeInfo{}, &out)
	return out, err
}

// Shutdown 在传入 context 的预算内排空调用并关闭资源，预算到期后取消剩余工作
// 例如调用方只有 100ms 预算，超时后取消剩余业务，连接和结果确认另用配置的清理预算
func (c *Conn) Shutdown(ctx context.Context) error {
	// 借用连接不拥有底层资源，拒绝关闭；例如 handler 中取得的连接须由外层实例统一关闭
	if c.borrowed {
		return model.ErrBorrowedResource
	}

	return c.engine.Shutdown(ctx)
}

// Runs 返回当前实例共享后端的 Runs 管理入口，不另建连接
func (c *Conn) Runs() *features.RunsClient {
	return c.features.Runs
}

// Crons 返回当前实例共享后端的 Crons 管理入口，不另建连接
func (c *Conn) Crons() *features.CronsClient {
	return c.features.Crons
}

// Schedules 返回当前实例共享后端的 Schedules 管理入口，不另建连接
func (c *Conn) Schedules() *features.SchedulesClient {
	return c.features.Schedules
}

// Events 返回当前实例共享后端的 Events 管理入口，不另建连接
func (c *Conn) Events() *features.EventsClient {
	return c.features.Events
}

// Filters 返回当前实例共享后端的 Filters 管理入口，不另建连接
func (c *Conn) Filters() *features.FiltersClient {
	return c.features.Filters
}

// Workers 返回当前实例共享后端的 Workers 管理入口，不另建连接
func (c *Conn) Workers() *features.WorkersClient {
	return c.features.Workers
}

// Workflows 返回当前实例共享后端的 Workflows 管理入口，不另建连接
func (c *Conn) Workflows() *features.WorkflowsClient {
	return c.features.Workflows
}

// Logs 返回当前实例共享后端的 Logs 管理入口，不另建连接
func (c *Conn) Logs() *features.LogsClient {
	return c.features.Logs
}

// Webhooks 返回当前实例共享后端的 Webhooks 管理入口，不另建连接
func (c *Conn) Webhooks() *features.WebhooksClient {
	return c.features.Webhooks
}

// Metrics 返回当前实例共享后端的 Metrics 管理入口，不另建连接
func (c *Conn) Metrics() *features.MetricsClient {
	return c.features.Metrics
}

// RateLimits 返回当前实例共享后端的 RateLimits 管理入口，不另建连接
func (c *Conn) RateLimits() *features.RateLimitsClient {
	return c.features.RateLimits
}

// CEL 返回当前实例共享后端的 CEL 管理入口，不另建连接
func (c *Conn) CEL() *features.CELClient {
	return c.features.CEL
}

// Tenant 返回当前实例共享后端的 Tenant 管理入口，不另建连接
func (c *Conn) Tenant() *features.TenantClient {
	return c.features.Tenant
}

// 编译期接口或签名检查，确保适配对象可以被标准 gRPC 或 wego 入口使用
var _ grpc.ClientConnInterface = (*Conn)(nil)

// ResumeStream 根据保存的断点继续消费已有可靠输出，不再提交请求
func (c *Conn) ResumeStream(ctx context.Context, checkpoint model.StreamCheckpoint) (grpc.ClientStream, error) {
	return rpc.ResumeStream(ctx, c.engine, checkpoint)
}
