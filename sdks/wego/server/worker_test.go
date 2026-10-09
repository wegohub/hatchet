package server

import (
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// TestWorkerMethodSelection 覆盖各 RPC 方向、禁用优先级和误配置，筛选不改变完整服务注册。
func TestWorkerMethodSelection(t *testing.T) {
	all := []string{
		pb.Greeter_SayHello_FullMethodName,
		pb.Greeter_WaitHello_FullMethodName,
		pb.Greeter_ChildHello_FullMethodName,
		pb.Greeter_UploadHellos_FullMethodName,
		pb.Greeter_WatchHellos_FullMethodName,
		pb.Greeter_ChatHellos_FullMethodName,
	}
	// 全部禁用时不创建空 Worker；纯网络服务应显式使用 Runtime 的 Worker 开关。
	disableAll := []worker.Option{worker.WithDisableMethod(all...)}
	// 禁用项独立于策略保存；策略先后顺序都不能重新启用被禁用的 SayHello。
	cases := []struct {
		// name 标识方法方向或配置边界，失败信息不依赖业务环境。
		name string
		// options 当前 Server 的 Worker 配置，允许组合普通 / durable 策略与禁用项。
		options []worker.Option
		// disabled 本场景预期未生成调度绑定的方法。
		disabled []string
		// wantErr 配置错误的可诊断信息；为空表示应成功筛选。
		wantErr string
	}{
		{name: "default"},
		{name: "no_arguments", options: []worker.Option{worker.WithDisableMethod()}},
		{name: "batch", options: []worker.Option{worker.WithDisableMethod(all[0], all[3], all[4], all[5])}, disabled: []string{all[0], all[3], all[4], all[5]}},
		{name: "unary", options: []worker.Option{worker.WithDisableMethod(all[0])}, disabled: all[:1]},
		{name: "client_stream", options: []worker.Option{worker.WithDisableMethod(all[3])}, disabled: all[3:4]},
		{name: "server_stream", options: []worker.Option{worker.WithDisableMethod(all[4])}, disabled: all[4:5]},
		{name: "bidi_stream", options: []worker.Option{worker.WithDisableMethod(all[5])}, disabled: all[5:]},
		{name: "repeated", options: []worker.Option{worker.WithDisableMethod(all[0]), worker.WithDisableMethod(all[0])}, disabled: all[:1]},
		{name: "disable_before_policy", options: []worker.Option{worker.WithDisableMethod(all[0]), worker.WithTask(all[0], task.WithExecutionTimeout(-time.Second))}, disabled: all[:1]},
		{name: "disable_after_durable_policy", options: []worker.Option{worker.WithDurableTask(all[0]), worker.WithDisableMethod(all[0])}, disabled: all[:1]},
		{name: "unknown_disable", options: []worker.Option{worker.WithDisableMethod("/unknown.Service/Method")}, wantErr: "disabling unknown Worker method"},
		{name: "batch_unknown_disable", options: []worker.Option{worker.WithDisableMethod(all[0], "/unknown.Service/Method")}, wantErr: "disabling unknown Worker method"},
		{name: "empty_disable", options: []worker.Option{worker.WithDisableMethod("")}, wantErr: "disabling unknown Worker method"},
		{name: "unknown_policy", options: []worker.Option{worker.WithTask("/unknown.Service/Method")}, wantErr: "configuration for unknown method"},
		{name: "all_disabled", options: disableAll, wantErr: "no methods enabled for Worker"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := New(WithWorker(tc.options...))
			defer srv.Stop()
			pb.RegisterGreeterServer(srv, &greeter{})
			methods, err := srv.workerMethods()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("selection error: %v", err)
				}
				// Serve 必须先返回配置错误，不能先尝试连接没有 token 的任务后端。
				if err := srv.Serve(); err == nil || !strings.Contains(err.Error(), tc.wantErr) || srv.engine != nil {
					t.Fatalf("startup error=%v engine=%v", err, srv.engine)
				}
				return
			}
			if err != nil || len(methods) != len(all)-len(tc.disabled) {
				t.Fatalf("selection count=%d error=%v", len(methods), err)
			}
			for _, method := range all {
				_, enabled := methods[method]
				if enabled == slices.Contains(tc.disabled, method) {
					t.Errorf("unexpected Worker binding for %s", method)
				}
			}
			if info := srv.GetServiceInfo()[pb.Greeter_ServiceDesc.ServiceName]; len(info.Methods) != len(all) {
				t.Fatal("Worker filtering changed network service info")
			}
		})
	}
}

// TestDisabledMethodsDoNotRequireProtoBindings 禁用的手写网络方法和服务无需任务 protobuf 描述。
func TestDisabledMethodsDoNotRequireProtoBindings(t *testing.T) {
	srv := New(WithWorker(
		worker.WithDisableMethod("/wego.example.v1.Greeter/NetworkOnly", "/manual.Greeter/SayHello"),
	))
	defer srv.Stop()
	// 独立追加 NetworkOnly，不改写生成代码中的 ServiceDesc。
	desc := pb.Greeter_ServiceDesc
	desc.Methods = append(slices.Clone(desc.Methods), grpc.MethodDesc{MethodName: "NetworkOnly", Handler: desc.Methods[0].Handler})
	srv.RegisterService(&desc, &greeter{})
	manual := pb.Greeter_ServiceDesc
	manual.ServiceName = "manual.Greeter"
	manual.Methods = slices.Clone(manual.Methods[:1])
	manual.Streams = nil
	srv.RegisterService(&manual, &greeter{})
	methods, err := srv.workerMethods()
	if err != nil || len(methods) != 6 {
		t.Fatalf("enabled protobuf bindings=%d error=%v", len(methods), err)
	}
	if len(srv.GetServiceInfo()[desc.ServiceName].Methods) != 7 || len(srv.GetServiceInfo()[manual.ServiceName].Methods) != 1 {
		t.Fatal("network-only descriptions were mutated")
	}
}
