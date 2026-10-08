package server

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	client "github.com/hatchet-dev/hatchet/sdks/wego/internal/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/rpc"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/session"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/option"
)

// startWorker 将共享服务定义绑定到任务，并启动独立的业务及控制容量。
func (s *Server) startWorker(ctx context.Context) error {
	// 遍历注册描述，把每个服务或方法绑定到对应入口；注册与调用必须使用同一完整名称。
	for _, registration := range s.services {
		// methods, err 接收 binding.Methods 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
		methods, err := binding.Methods(&registration.description, registration.service)
		if err != nil {
			return err
		}
		// 遍历注册描述，把每个服务或方法绑定到对应入口；注册与调用必须使用同一完整名称。
		for _, method := range methods {
			s.methods[method.FullName] = method
		}
	}
	if len(s.methods) == 0 {
		return fmt.Errorf("wego: no methods registered")
	}
	// e, err 接收 engine.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	e, err := engine.New(s.config)
	if err != nil {
		return err
	}
	e.Borrow = func() any { return client.FromEngine(e, true) }
	// 初始化屏障保证关闭操作在钩子和 Worker 全部安装后才访问引擎。
	s.mu.Lock()
	s.engine = e
	s.mu.Unlock()
	// definitions 交给 Worker 注册的任务定义集合。
	definitions := []ports.Definition{}
	// owner 唯一标识本实例，业务和控制任务通过同一个 required label 路由到它。
	owner := randomID()
	// manager 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	manager := session.New(e.Backend, e.Config, owner)
	e.BeginDrain = manager.Drain
	e.StopSessions = manager.Stop
	// hasStreams 是否注册了任一流方法；为 true 时才启动独立流控制 Worker。
	hasStreams := false
	// names 本步骤需要遍历的名称或路径列表，注册、查询和清理使用同一集合以保持对应。
	names := []string{}
	// 遍历注册描述，把每个服务或方法绑定到对应入口；注册与调用必须使用同一完整名称。
	for method := range s.methods {
		names = append(names, method)
	}
	sort.Strings(names)
	// 逐项处理 s.policy.Methods，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for method := range s.policy.Methods {
		// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值。
		if _, ok := s.methods[method]; !ok {
			err = fmt.Errorf("wego: configuration for unknown method %s", method)
		}
	}
	// 逐项处理 names，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, name := range names {
		// m 当前方法的服务描述，包含 handler 或流方向，绑定时与 protobuf 描述进行一致性检查。
		m := s.methods[name]
		// p 合并默认与方法级策略；未设置字段继承，显式 nil 可以清除 CronInput。
		p := spec.Merge(s.policy.Defaults, s.policy.Methods[name])
		if m.Stream != nil {
			if p.Durable {
				err = fmt.Errorf("wego: streaming methods cannot be durable")
				break
			}
			hasStreams = true
			manager.Register(m.FullName, rpc.StreamHandler(e, m))
			// 流会话不重试也不参与 durable 重放，避免重复启动业务 handler。
			p.Retries = option.Some(0)
			p.ScheduleTimeout = option.Some(e.Config.Stream.HandshakeTimeout)
			definitions = append(definitions, ports.Definition{
				Name:   binding.Name(m.FullName) + "-session",
				Policy: p,
				Function: func(ctx context.Context, input any) (any, error) {
					// command 解码后的流控制输入，包含 StreamID、Kind 和编码帧；先校验身份再更新会话。
					var command session.Control
					// err 接收 session.Decode 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
					if err := session.Decode(input, &command); err != nil {
						return nil, err
					}

					return manager.Run(ctx, command)
				},
			})
			continue
		}
		// e 校验当前定义或业务结果，不满足约束时在执行前失败。
		if e := p.Validate(); e != nil {
			err = e
			break
		}
		// definition, e 把标准 unary handler 绑定为任务，业务不接触后端执行类型。
		definition, e := rpc.UnaryDefinition(ctx, e, m, p)
		if e != nil {
			err = e
			break
		}
		definitions = append(definitions, definition)
	}
	if err != nil {
		return err
	}
	if len(definitions) == 0 {
		return fmt.Errorf("wego: no methods registered")
	}

	// businessConfig 业务 Worker 的 Runtime 副本，流 owner 标签在此视图写入，不修改原配置。
	businessConfig := e.Config
	businessConfig.Labels = map[string]any{}
	// 逐项处理 e.Config.Labels，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for key, value := range e.Config.Labels {
		businessConfig.Labels[key] = value
	}
	businessConfig.Labels[session.OwnerLabel] = owner
	// hostname, _ 读取实例主机名，仅用于显示名称，不作为唯一 Worker 身份。
	hostname, _ := os.Hostname()
	// workerName 在同一次启动中生成一次，控制 Worker 追加 -control，便于识别所属业务实例。
	workerName := "wego-" + hostname + "-" + randomID()
	// w, err 接收 e.Worker 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	w, err := e.Worker(ctx, workerName, definitions, businessConfig, s.policy.PanicHandler)
	if err != nil {
		return err
	}
	if err = w.Start(ctx); err != nil {
		return err
	}

	if hasStreams {
		// 独立控制容量保证业务 slots 满载时仍能处理握手、ACK 和取消。
		controlConfig := businessConfig
		controlConfig.Slots = e.Config.ControlSlots
		// control, err 接收 e.Worker 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
		// controlDefinitions 包含公共会话控制和本实例实际支持的方法分配入口。
		controlDefinitions := []ports.Definition{
			{
				Name:   session.ControlName,
				Policy: spec.Task{Retries: option.Some(0)},
				Function: func(ctx context.Context, input any) (any, error) {
					// command 解码后的流控制输入，包含 StreamID、Kind 和编码帧；先校验身份再更新会话。
					var command session.Control
					// err 接收 session.Decode 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块。
					if err := session.Decode(input, &command); err != nil {
						return nil, err
					}

					return manager.Control(ctx, command)
				},
			},
		}
		// START 入口仅由支持该方法的实例注册，独立控制容量避免业务 slots 满载阻塞握手。
		for _, name := range names {
			if s.methods[name].Stream != nil {
				// definition 复制公共控制 handler 的定义，只为当前支持的流方法添加分配入口。
				definition := controlDefinitions[0]
				definition.Name = session.StartName(name)
				controlDefinitions = append(controlDefinitions, definition)
			}
		}
		// control 以独立容量注册会话控制和方法专属 START，业务 slots 满载也必须完成握手。
		control, err := e.Worker(ctx, workerName+"-control", controlDefinitions, controlConfig, nil)
		if err != nil {
			return err
		}
		if err = control.Start(ctx); err != nil {
			return err
		}

		s.monitor(ctx, control.Errors())
		if err = control.WaitReady(ctx); err != nil {
			return err
		}
	}
	if err = w.WaitReady(ctx); err != nil {
		return err
	}

	s.monitor(ctx, w.Errors())
	return nil
}
