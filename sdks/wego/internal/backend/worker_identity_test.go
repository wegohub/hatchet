package backend

import (
	"context"
	"testing"

	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"github.com/rs/zerolog"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// workerLabelClient 仅提供构造 Worker 所需能力，不建立连接或注册实际消费者
type workerLabelClient struct {
	// Client 补齐测试未使用的方法；若构造意外调用它们，测试会直接失败
	v0.Client
}

// Dispatcher 只允许构造阶段查询版本，标签持久注册另由真实引擎验证
func (*workerLabelClient) Dispatcher() v0.DispatcherClient { return &workerLabelDispatcher{} }

// Admin 返回未使用的管理能力；此测试没有 workflow 定义，不应提交注册请求
func (*workerLabelClient) Admin() v0.AdminClient { return nil }

// Logger 使用无输出日志器，使配置回归不打印实例信息
func (*workerLabelClient) Logger() *zerolog.Logger {
	logger := zerolog.Nop()
	return &logger
}

// workerLabelDispatcher 只提供构造时的版本查询，其他调度方法不应被调用
type workerLabelDispatcher struct {
	// DispatcherClient 补齐未使用的接口，不创建监听器
	v0.DispatcherClient
}

// GetVersion 返回支持普通和 durable 配置的正式版本，不触发网络 I/O
func (*workerLabelDispatcher) GetVersion(context.Context) (string, error) { return "v0.110.5", nil }

// TestWorkerNameLabelIsInstanceScoped 验证共享配置生成不同实例标签，不能污染调用方或丢失整数标签类型
func TestWorkerNameLabelIsInstanceScoped(t *testing.T) {
	// config 与 backend 指向同一配置来源；Worker 构造只能改写独占的标签副本
	config := spec.Defaults()
	config.Labels = map[string]any{"region": "zone-a", "capacity": int(8), "worker_name": "configured"}
	b := &Backend{config: config, raw: &workerLabelClient{}}
	// 默认、原生 Worker 显式名、Runtime 展示名都应在解析名称后写入相同标签
	for _, requested := range []string{"", "first", "second"} {
		w, err := b.Worker(context.Background(), requested, nil, config, nil)
		if err != nil {
			t.Fatal(err)
		}
		instance := w.(*worker)
		if name := instance.config.Labels["worker_name"]; name != instance.InstanceName() {
			t.Fatalf("registered name label = %v, instance name = %s", name, instance.InstanceName())
		}
		if instance.config.Labels["capacity"] != int(8) || instance.config.Labels["region"] != "zone-a" {
			t.Fatal("custom labels lost or integer type changed")
		}
		// 改写一个实例不能影响共享配置；下一实例仍从原始业务标签开始构造
		instance.config.Labels["region"] = "instance-only"
		if config.Labels["region"] != "zone-a" || config.Labels["worker_name"] != "configured" || b.config.Labels["worker_name"] != "configured" {
			t.Fatal("worker labels leaked into shared runtime")
		}
	}
	config.InstanceName = "stable-display"
	w, err := b.Worker(context.Background(), "ignored", nil, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	if w.(*worker).config.Labels["worker_name"] != "stable-display" {
		t.Fatal("instance-name override missing from labels")
	}
	// nil 标签配置仍须生成 worker_name；不要求调用方预先分配 map
	config.Labels = nil
	w, err = b.Worker(context.Background(), "", nil, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	if w.(*worker).config.Labels["worker_name"] != "stable-display" || config.Labels != nil {
		t.Fatal("nil labels not isolated")
	}
}

// 评审专用：同名 Worker 拥有各自监听器，注销不得共享 channel
type reviewListener struct{ v0.WorkerActionListener }

// Unregister 模拟无网络副作用的注销，仅观察监听器自身完成通知的所有权
func (*reviewListener) Unregister() error { return nil }

// TestReviewSameNameListenersCanBothClose 两个监听器分别注销并关闭自身通知通道，同名覆盖不得引发 panic
func TestReviewSameNameListenersCanBothClose(t *testing.T) {
	// owner 保存本实例的完成或故障通知，关闭通道不代表业务成功
	owner := &transport{listenerDone: map[string]chan struct{}{"worker": make(chan struct{})}, unregisterBudgets: map[string]context.Context{}}
	// first 保存首次注册的独占通知；后续名称映射变化不能改变它的所有权
	first := &actionListener{WorkerActionListener: &reviewListener{}, owner: owner, name: "worker", done: owner.listenerDone["worker"]}
	owner.listenerDone["worker"] = make(chan struct{})
	// second 保存自己的结束通知，即使显示名称相同也不能引用第一个监听器的 channel
	second := &actionListener{WorkerActionListener: &reviewListener{}, owner: owner, name: "worker", done: owner.listenerDone["worker"]}
	defer func() {
		// r 捕获当前执行的 panic，边界应返回可诊断错误并继续资源清理
		if r := recover(); r != nil {
			t.Errorf("two same-name workers panic on close: %v", r)
		}
	}()
	// err 结束本实例的监听注册，两个同名 Worker 不得关闭同一个通知通道
	if err := first.Unregister(); err != nil {
		t.Fatal(err)
	}
	// err 结束本实例的监听注册，两个同名 Worker 不得关闭同一个通知通道
	if err := second.Unregister(); err != nil {
		t.Fatal(err)
	}
}

// TestSubmissionQueueHonorsCancellation 前一子调用提交阻塞时，后一个调用的 deadline 仍须生效
func TestSubmissionQueueHonorsCancellation(t *testing.T) {
	// gate 为 execution 使用的零值提交门禁，第一项先占用唯一 token
	var gate submissionGate
	// err 检查首次占用是否成功，后续断言只能在 token 已持有时执行
	if err := gate.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer gate.Unlock()
	// ctx 在排队前取消，不能等待持有者手动释放
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// err 必须保留调用方的取消原因，而非等待或占用稳定提交序号
	if err := gate.Lock(ctx); err != context.Canceled {
		t.Fatal(err)
	}
}

// TestAffinityIntegerBounds 允许协议整数类型，拒绝 JSON 浮点和 int32 溢出
func TestAffinityIntegerBounds(t *testing.T) {
	// value 包含各支持的数值表示及协议两端边界，不能因为 Go 平台字长不同而截断
	for _, value := range []any{int(8), int32(8), int64(8), int64(-2147483648), int64(2147483647)} {
		// converted 与 err 检查值和类型，真实 dispatcher 只识别 int32 及 string
		converted, err := affinityValue(value)
		if err != nil {
			t.Fatal(err)
		}
		// ok 确认输出没有退化成 float64，避免底层忽略整数亲和条件
		if _, ok := converted.(int32); !ok {
			t.Fatalf("integer type lost: %T", converted)
		}
	}
	// value 包含越界整数、浮点和非数值；每项必须在发送请求之前报错
	for _, value := range []any{int64(-2147483649), int64(2147483648), float64(8), true, nil} {
		// err 必须显式拒绝非法动态值，不能静默丢弃 Required 条件
		if _, err := affinityValue(value); err == nil {
			t.Fatalf("invalid affinity accepted: %v", value)
		}
	}
}
