package server

import (
	"errors"
	"fmt"
	"reflect"

	"google.golang.org/grpc"
)

// registration 服务描述与 handler 的注册快照，两个入口共享业务实现
type registration struct {
	// description gRPC 服务或流描述，决定 handler 与请求类型的绑定
	description grpc.ServiceDesc
	// service 两个入口共享的业务 handler 实例
	service any
}

// RegisterService 接受生成桩；错误保留到 Serve 返回，避免注册错误终止进程
func (s *Server) RegisterService(desc *grpc.ServiceDesc, impl any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// fail 记录首个注册错误；服务已启动时通知 Serve 触发统一停止，缓冲满时不阻塞注册调用
	fail := func(err error) {
		if s.registrationErr == nil {
			s.registrationErr = err
		}
		if s.started && !s.stopping {
			s.faultErr = errors.Join(s.faultErr, err)
			// 根据已经就绪的通知选择下一步，不通过轮询固定延迟猜测执行时机
			select {
			case s.errors <- struct{}{}:
			default:
			}
		}
	}
	if s.started || s.stopping {
		fail(fmt.Errorf("wego: registration after start or stop"))
		return
	}
	if desc == nil || desc.ServiceName == "" {
		fail(fmt.Errorf("wego: invalid service description"))
		return
	}
	// handlerType 取得类型描述用于接口及签名核对，不执行真实网络调用
	handlerType := reflect.TypeOf(desc.HandlerType)
	if handlerType == nil || handlerType.Kind() != reflect.Pointer || handlerType.Elem().Kind() != reflect.Interface {
		fail(fmt.Errorf("wego: HandlerType must point to an interface"))
		return
	}
	if impl != nil && !reflect.TypeOf(impl).Implements(handlerType.Elem()) {
		fail(fmt.Errorf("wego: handler does not implement %s", handlerType.Elem()))
		return
	}
	// exists 检查重复注册；同一服务名重复出现时保存错误并由 Serve 返回
	if _, exists := s.services[desc.ServiceName]; exists {
		fail(fmt.Errorf("wego: duplicate service %s", desc.ServiceName))
		return
	}
	// 复制方法表，注册后调用方修改切片不能改变服务路由
	copy := *desc
	copy.Methods = append([]grpc.MethodDesc(nil), desc.Methods...)
	copy.Streams = append([]grpc.StreamDesc(nil), desc.Streams...)
	// names 名称去重表，服务注册时检查重复方法并保存已经使用的键
	names := map[string]bool{}
	// 逐项处理 copy.Methods，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, method := range copy.Methods {
		if method.MethodName == "" || method.Handler == nil || names[method.MethodName] {
			fail(fmt.Errorf("wego: invalid or duplicate method %s", method.MethodName))
			return
		}
		names[method.MethodName] = true
	}
	// 逐项处理 copy.Streams，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, stream := range copy.Streams {
		if stream.StreamName == "" || stream.Handler == nil || names[stream.StreamName] {
			fail(fmt.Errorf("wego: invalid or duplicate stream %s", stream.StreamName))
			return
		}
		names[stream.StreamName] = true
	}
	s.services[copy.ServiceName] = registration{description: copy, service: impl}
}

// GetServiceInfo 返回独立的方法列表； Metadata 与原生 gRPC 一样视为只读值
func (s *Server) GetServiceInfo() map[string]grpc.ServiceInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	// out 初始化空映射，按键收集当前步骤的数据，避免向 nil map 写入
	out := make(map[string]grpc.ServiceInfo, len(s.services))
	// 遍历注册描述，把每个服务或方法绑定到对应入口；注册与调用必须使用同一完整名称
	for name, registration := range s.services {
		// info 构造当前步骤的数据对象，字段在提交、编码或断言之前一次性初始化
		info := grpc.ServiceInfo{Metadata: registration.description.Metadata}
		// 逐项处理 registration.description.Methods，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for _, method := range registration.description.Methods {
			info.Methods = append(info.Methods, grpc.MethodInfo{Name: method.MethodName})
		}
		// 逐项处理 registration.description.Streams，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for _, stream := range registration.description.Streams {
			info.Methods = append(info.Methods, grpc.MethodInfo{
				Name: stream.StreamName, IsClientStream: stream.ClientStreams, IsServerStream: stream.ServerStreams,
			})
		}
		out[name] = info
	}
	return out
}
