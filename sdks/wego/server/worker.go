package server

import (
	"context"
	"fmt"
	"sort"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	client "github.com/hatchet-dev/hatchet/sdks/wego/internal/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/rpc"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// startWorker 将共享服务定义绑定到任务，并启动唯一业务 Worker
func (s *Server) startWorker(ctx context.Context) error {
	// 先筛选并校验调度入口；禁用的方法不绑定 protobuf、不验证任务策略，也不进入后端定义。
	methods, err := s.workerMethods()
	if err != nil {
		return err
	}
	s.methods = methods
	// e, err 接收 engine.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	e, err := engine.New(s.config)
	if err != nil {
		return err
	}
	e.Borrow = func() any { return client.FromEngine(e, true) }
	// 初始化屏障保证关闭操作在钩子和 Worker 全部安装后才访问引擎
	s.mu.Lock()
	s.engine = e
	s.mu.Unlock()
	// definitions 交给 Worker 注册的任务定义集合
	definitions := []ports.Definition{}
	// names 本步骤需要遍历的名称或路径列表，注册、查询和清理使用同一集合以保持对应
	names := []string{}
	// 遍历注册描述，把每个服务或方法绑定到对应入口；注册与调用必须使用同一完整名称
	for method := range s.methods {
		names = append(names, method)
	}
	sort.Strings(names)
	// 逐项处理 names，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, name := range names {
		// m 当前方法的服务描述，包含 handler 或流方向，绑定时与 protobuf 描述进行一致性检查
		m := s.methods[name]
		// p 合并默认与方法级策略；未设置字段继承，显式 nil 可以清除 CronInput
		p := spec.Merge(s.policy.Defaults, s.policy.Methods[name])
		if m.Stream != nil {
			definition, bindingErr := rpc.StreamDefinition(e, m, p, s.policy.PanicHandler)
			if bindingErr != nil {
				err = bindingErr
				break
			}
			definitions = append(definitions, definition)
			continue
		}
		// e 校验当前定义或业务结果，不满足约束时在执行前失败
		if e := p.Validate(); e != nil {
			err = e
			break
		}
		// definition, e 把标准 unary handler 绑定为任务，业务不接触后端执行类型
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

	// 默认展示名称在 backend 与 worker_key 一次生成；Server 不再建立另一个实例身份
	workerName := e.Config.InstanceName
	// w, err 接收 e.Worker 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	w, err := e.Worker(ctx, workerName, definitions, e.Config, s.policy.PanicHandler)
	if err != nil {
		return err
	}
	if err = w.Start(ctx); err != nil {
		return err
	}

	if err = w.WaitReady(ctx); err != nil {
		return err
	}

	s.monitor(w.Errors())
	return nil
}

// workerMethods 从注册快照构造调度方法表，不能裁剪网络入口共享的原始 ServiceDesc。
// 例如禁用 SayHello 后，GetServiceInfo 仍包含它，但 Worker 的任务定义只包含其他方法。
func (s *Server) workerMethods() (map[string]binding.Method, error) {
	registered := make(map[string]struct{})
	for _, registration := range s.services {
		prefix := "/" + registration.description.ServiceName + "/"
		for _, method := range registration.description.Methods {
			registered[prefix+method.MethodName] = struct{}{}
		}
		for _, stream := range registration.description.Streams {
			registered[prefix+stream.StreamName] = struct{}{}
		}
	}

	// 禁用和策略均针对已注册方法；先发现配置错误，避免建立不必要的任务后端连接。
	for method := range s.policy.DisabledMethods {
		if _, ok := registered[method]; !ok {
			return nil, fmt.Errorf("wego: disabling unknown Worker method %q", method)
		}
	}
	for method := range s.policy.Methods {
		if _, ok := registered[method]; !ok {
			return nil, fmt.Errorf("wego: configuration for unknown method %s", method)
		}
	}

	methods := make(map[string]binding.Method)
	for _, registration := range s.services {
		// 独立切片防止过滤覆盖原始方法表；完全禁用的服务无需具备任务入口的 protobuf 描述。
		description := registration.description
		description.Methods = nil
		description.Streams = nil
		prefix := "/" + description.ServiceName + "/"
		for _, method := range registration.description.Methods {
			if _, disabled := s.policy.DisabledMethods[prefix+method.MethodName]; !disabled {
				description.Methods = append(description.Methods, method)
			}
		}
		for _, stream := range registration.description.Streams {
			if _, disabled := s.policy.DisabledMethods[prefix+stream.StreamName]; !disabled {
				description.Streams = append(description.Streams, stream)
			}
		}
		if len(description.Methods)+len(description.Streams) == 0 {
			continue
		}
		bound, err := binding.Methods(&description, registration.service)
		if err != nil {
			return nil, err
		}
		for _, method := range bound {
			methods[method.FullName] = method
		}
	}
	if len(methods) == 0 {
		return nil, fmt.Errorf("wego: no methods enabled for Worker; use runtime.WithDisableWorker for a network-only Server")
	}
	return methods, nil
}
