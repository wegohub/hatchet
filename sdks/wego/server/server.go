package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// Config 当前包的配置集合，分离实例资源配置与业务执行策略
type Config struct {
	// Runtime 实例配置或配置选项集合
	Runtime spec.Runtime
	// Worker Worker 入口的任务执行策略
	Worker worker.Config
	// grpc 可选网络 gRPC 配置；nil 表示仅启用 Worker
	grpc *grpcConfig
}

// grpcConfig 网络入口地址和复制后的原生 gRPC 选项
type grpcConfig struct {
	// addr 网络监听地址，例如 :9000
	addr string
	// opts 当前入口使用的原生 gRPC 选项
	opts []grpc.ServerOption
}

// Option 配置函数或自有配置视图；按传入顺序应用，相同字段后者覆盖前者
type Option func(*Config)

// WithRuntime 按传入顺序应用 Runtime 选项，重复字段由后面的设置覆盖
func WithRuntime(opts ...runtime.Option) Option {
	return func(c *Config) {
		// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序
		for _, o := range opts {
			o(&c.Runtime)
		}
	}
}

// WithWorker 配置同一个 Worker 的任务策略和方法禁用项；入口开关属于 Runtime。
func WithWorker(opts ...worker.Option) Option {
	return func(c *Config) {
		// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序
		for _, o := range opts {
			o(&c.Worker)
		}
	}
}

// WithGRPC 启用网络 gRPC；原生配置仅作用于网络入口
func WithGRPC(addr string, opts ...grpc.ServerOption) Option {
	return func(c *Config) {
		c.grpc = &grpcConfig{addr: addr, opts: append([]grpc.ServerOption(nil), opts...)}
	}
}

// Server 共享服务注册，分别通过网络 gRPC 和 Worker 执行业务
type Server struct {
	// config 当前实例使用的配置快照
	config spec.Runtime
	// policy 任务执行策略，保存重试、并发及调度限制
	policy worker.Config
	// grpcConfig 可选网络入口配置，与 Worker 策略独立
	grpcConfig *grpcConfig
	// services 服务名称到注册描述与 handler 的映射
	services map[string]registration
	// methods 方法名称到 protobuf 绑定的映射
	methods map[string]binding.Method

	// mu 保护生命周期状态及发布给关闭协程的资源
	mu sync.Mutex
	// started 入口是否已经启动；与停止并发时在锁内读写
	started bool
	// stopping 是否已开始停止；为 true 时禁止再次启动或注册
	stopping bool
	// registrationErr 延迟到 Serve 返回的注册错误，保持 RegisterService 无返回值接口
	registrationErr error
	// engine 本实例的执行引擎，协调调用、Worker 与资源关闭
	engine *engine.Engine
	// network 原生网络 gRPC Server，直接执行业务 handler
	network *grpc.Server
	// listener 当前对象拥有的网络监听器，停止时先关闭接收入口
	listener net.Listener
	// networkCalls 网络入口的在途业务记录，包含原生 interceptor 外层工作
	networkCalls networkCalls
	// initialized 初始化屏障；关闭后资源列表不再增加，停止方才能读取并清理
	initialized chan struct{}
	// stopDone Server 停止完成通知；重复 Stop 等待同一结果
	stopDone chan struct{}
	// stopOnce 保证停止流程只启动一次
	stopOnce sync.Once
	// forceOnce 保证强制停止信号只关闭一次
	forceOnce sync.Once
	// force 强制停止信号，可中断正在排空的 GracefulStop
	force chan struct{}
	// startCtx 初始化所用上下文，Stop 可取消正在启动的 Worker
	startCtx context.Context
	// cancelStart 取消初始化过程的函数
	cancelStart context.CancelFunc
	// drainCtx 业务排空上下文；GracefulStop 持续等待，Stop 主动取消
	drainCtx context.Context
	// cancelDrain 中断业务排空的函数，供强制停止使用
	cancelDrain context.CancelFunc
	// errors 异步错误通道，用于把入口故障交给生命周期协调器
	errors chan struct{}
	// faultErr 保存真实入口故障，随后到达的停止信号不能覆盖它。
	faultErr error
	// closeErr 保存真实资源清理失败，stopDone 关闭后才供调用方读取。
	closeErr error
	// background 确认入口和故障观察协程退出，避免 Serve 返回后仍有 SDK 资源。
	background sync.WaitGroup
	// entrancesDone 在入口清理后结束 Worker 错误观察。
	entrancesDone chan struct{}
}

// New 仅收集配置，不建立连接；初始化错误通过 Serve 返回
func New(options ...Option) *Server {
	// c 初始化 Runtime 和方法策略表，后续选项分别控制入口启用与任务执行策略
	c := Config{Runtime: spec.Defaults(), Worker: worker.Config{Methods: map[string]spec.Task{}}}

	// 按配置顺序应用每一项；重复设置的字段以后面的值为准，载荷解码路径则使用反向顺序
	for _, o := range options {
		o(&c)
	}

	// startCtx, cancelStart 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	startCtx, cancelStart := context.WithCancel(context.Background())

	// drainCtx, cancelDrain 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	drainCtx, cancelDrain := context.WithCancel(context.Background())

	return &Server{
		config:        c.Runtime.Clone(),
		policy:        c.Worker,
		grpcConfig:    c.grpc,
		services:      map[string]registration{},
		methods:       map[string]binding.Method{},
		initialized:   make(chan struct{}),
		stopDone:      make(chan struct{}),
		force:         make(chan struct{}),
		startCtx:      startCtx,
		cancelStart:   cancelStart,
		drainCtx:      drainCtx,
		cancelDrain:   cancelDrain,
		errors:        make(chan struct{}, 1),
		entrancesDone: make(chan struct{}),
	}
}

// Serve 自动监听配置地址，启动全部启用入口，并阻塞到停止或运行故障
func (s *Server) Serve() error {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return grpc.ErrServerStopped
	}
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("wego: server already started")
	}
	s.started = true
	err := s.registrationErr
	s.mu.Unlock()
	// 初始化屏障关闭后，停止方才能释放本次启动取得的资源
	if err == nil {
		err = s.prepare()
	}
	close(s.initialized)
	if err != nil {
		if cause := withoutCanceled(err); cause != nil {
			s.recordFault(cause)
		}
		s.Stop()
		return s.result()
	}
	// Serve 保持阻塞；只有显式停止或入口故障才能结束运行。
	select {
	case <-s.stopDone:
	case <-s.errors:
		s.Stop()
	}
	return s.result()
}

// prepare 先检查配置并绑定网络端口，再初始化 Worker，启动失败必须释放已经创建的资源
func (s *Server) prepare() error {
	if s.config.Logger == nil {
		return fmt.Errorf("wego: logger cannot be nil")
	}
	if s.config.Shutdown.Mode != spec.DrainWithTimeout && s.config.Shutdown.Mode != spec.DrainUntilDone {
		return fmt.Errorf("wego: invalid shutdown mode")
	}
	if s.config.Shutdown.Mode == spec.DrainWithTimeout && s.config.Shutdown.Timeout <= 0 {
		return fmt.Errorf("wego: shutdown timeout must be positive")
	}
	if s.config.DisableWorker && s.grpcConfig == nil {
		return fmt.Errorf("wego: no engine enabled")
	}

	if s.grpcConfig != nil {
		if s.grpcConfig.addr == "" {
			return fmt.Errorf("wego: gRPC address cannot be empty")
		}
		// listener, err 接收 net.Listen 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		listener, err := (&net.ListenConfig{}).Listen(s.startCtx, "tcp", s.grpcConfig.addr)
		if err != nil {
			return err
		}
		// opts 明确列出本段注册、传输或清理选项，应用顺序决定重复字段的覆盖关系
		opts := []grpc.ServerOption{
			grpc.StatsHandler(&s.networkCalls),
			grpc.ChainUnaryInterceptor(s.networkCalls.unary),
			grpc.ChainStreamInterceptor(s.networkCalls.stream),
		}
		opts = append(opts, s.grpcConfig.opts...)
		// network 构造共享注册表的 Server，业务只在 Serve 启动后接收调用
		network := grpc.NewServer(opts...)
		// 遍历注册描述，把每个服务或方法绑定到对应入口；注册与调用必须使用同一完整名称
		for _, registration := range s.services {
			network.RegisterService(&registration.description, registration.service)
		}
		s.mu.Lock()
		s.listener, s.network = listener, network
		s.mu.Unlock()
	}

	if !s.config.DisableWorker {
		// err 接收 s.startWorker 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
		if err := s.startWorker(s.startCtx); err != nil {
			return err
		}
	}

	// err 接收 s.startCtx.Err 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
	if err := s.startCtx.Err(); err != nil {
		return err
	}
	if s.network != nil {
		s.background.Go(func() {
			if err := s.network.Serve(s.listener); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, grpc.ErrServerStopped) {
				s.recordFault(err)
			}
		})
	}
	return nil
}

// monitor 保留入口故障直到入口清理完成；正常排空不能提前结束观察。
func (s *Server) monitor(source <-chan error) {
	s.background.Go(func() {
		select {
		case err, ok := <-source:
			if ok {
				s.recordFault(err)
			}
		case <-s.entrancesDone:
			// 清理同时到达时仍检查已交付的错误，不把真实故障吞成正常停止。
			select {
			case err := <-source:
				s.recordFault(err)
			default:
			}
		}
	})
}

// 编译期接口或签名检查，确保适配对象可以被标准 gRPC 或 wego 入口使用
var _ grpc.ServiceRegistrar = (*Server)(nil)
