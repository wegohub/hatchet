package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/rpc"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// 原生 JSON 字符串必须保持类型，不能按其内容再次解释为 JSON。
func TestSyncReviewJSONStringOutput(t *testing.T) {
	// value 覆盖容易被误判为数字、布尔、null 和对象的合法 JSON 字符串。
	for _, value := range []string{"7", "true", "null", `{"a":1}`} {
		t.Run(value, func(t *testing.T) {
			// r 是受控协议请求或自有结果视图，测试只检查其当前入口的行为。
			r := &Result{outputs: map[string]any{"task": value}}
			// out 要求字符串类型，内容为 null 也不能被转换为空值。
			var out string
			// err 检查当前操作结果，失败时不能使用未解码或未交付的输出。
			if err := r.Into(&out); err != nil || out != value {
				t.Fatalf("string %q changed to %q: %v", value, out, err)
			}
		})
	}
}

// TestSyncReviewSelectedNullOutput 验证已存在的 null 与缺失输出键分别处理。
func TestSyncReviewSelectedNullOutput(t *testing.T) {
	// r 是受控协议请求或自有结果视图，测试只检查其当前入口的行为。
	r := &Result{outputs: map[string]any{"task": nil}}
	// out 允许检查 JSON 真实类型，合法 null 解码后为 nil。
	var out any
	// err 检查当前操作结果，失败时不能使用未解码或未交付的输出。
	if err := r.TaskOutput("task").Into(&out); err != nil {
		t.Fatalf("present JSON null is treated as absent: %v", err)
	}
}

// TestSyncReviewDecodeJSONDoesNotChangeJSONString 验证字符串 7 保持 string 类型。
func TestSyncReviewDecodeJSONDoesNotChangeJSONString(t *testing.T) {
	// out 允许检查 JSON 真实类型，合法 null 解码后为 nil。
	var out any
	// err 检查当前操作结果，失败时不能使用未解码或未交付的输出。
	if err := rpc.DecodeJSON("7", &out); err != nil {
		t.Fatal(err)
	}
	// ok 检查动态结果的实际类型，字符串不能被解释成数字。
	if _, ok := out.(string); !ok {
		t.Fatalf("native string became %T: %#v", out, out)
	}
}

// 创建失败的 Worker 也不能改变 Conn 的路由配置。
func TestSyncReviewWorkerProjectionIsolation(t *testing.T) {
	// config 基于合法默认配置，只覆盖当前测试所需的投影、载荷或握手预算。
	config := spec.Defaults()
	config.Projections["/fixture/Call"] = map[string]string{"group": "group_key"}
	// c 是 Conn 或流控制请求的自有视图，不持有独立后端连接。
	c := &Conn{engine: &engine.Engine{Config: config}}
	// err 保留调用的失败原因；本场景只验证错误或合并详情，不使用普通返回值。
	_, err := c.NewWorker("worker", WithWorkerRuntime(runtime.WithInputProjection("/fixture/Call", map[string]string{"group": "account"})))
	if err == nil {
		t.Fatal("fixture expected Worker runtime conflict")
	}
	// got 读取 Conn 的原投影，Worker 创建失败后仍须为 group_key。
	if got := c.engine.Config.Projections["/fixture/Call"]["group"]; got != "group_key" {
		t.Fatalf("failed worker creation mutated Conn projection: %s", got)
	}
}

// syncDecodePayload 用受控通道模拟外部载荷读取，以验证取消和资源归属。
type syncDecodePayload struct {
	// entered 发布 Decode 所收到的上下文，用于检查取消和 deadline。
	entered chan context.Context
	// release 控制受控 I/O 退出，清理前必须释放等待者。
	release chan struct{}
}

// Encode 保留业务字节，此测试仅在 Decode 阶段模拟外部 I/O。
func (*syncDecodePayload) Encode(_ context.Context, _ string, data []byte) ([]byte, error) {
	return data, nil
}

// Decode 等待释放或上下文取消，模拟可取消的对象存储下载。
func (p *syncDecodePayload) Decode(ctx context.Context, _ string, data []byte) ([]byte, error) {
	p.entered <- ctx
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.release:
		return data, nil
	}
}

// TestResultDecodeBudgetsAndShutdown 验证对象下载受调用预算和 Conn 关闭控制。
// 每种模式都等到 Decode 真正开始后再取消，不能只检查收到的 context 类型。
func TestResultDecodeBudgetsAndShutdown(t *testing.T) {
	// mode 分别验证显式取消、deadline 和实例强制排空。
	for _, mode := range []string{"cancel", "deadline", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			// payload 发布 I/O 进入通知，退出仅由预算控制。
			payload := &syncDecodePayload{entered: make(chan context.Context, 1), release: make(chan struct{})}
			// config 为对象下载登记实例生命周期，观测不连接外部出口。
			config := spec.Defaults()
			config.Middleware = []middleware.Option{middleware.WithPayload(payload)}
			// instance 拥有完整在途表，测试结束会重复关闭验证幂等。
			instance := resultEngine(t, config)
			// envelope 是合法业务输出，不含损坏协议或错误状态。
			envelope, err := wire.Encode(context.Background(), "/fixture/Call", &pb.Reply{}, nil, nil, 1024)
			if err != nil {
				t.Fatal(err)
			}
			// result 复用已取得的业务结果，解码阶段拥有新的调用登记。
			result := &Result{outputs: map[string]any{"output": envelope}, engine: instance, method: "/fixture/Call"}
			// budget 与 cancel 专属于本次解码，不能重用已经完成的 Result 等待预算。
			budget, cancel := context.WithCancel(context.Background())
			if mode == "deadline" {
				cancel()
				budget, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			}
			defer cancel()
			// done 发布 I/O 的真实退出原因，断言后不存在剩余受控 goroutine。
			done := make(chan error, 1)
			// 解码错误通过独立通道交付，收到它即确认载荷 I/O 已退出。
			go func() { done <- result.IntoContext(budget, &pb.Reply{}) }()
			// entered 确认 Decode 使用可取消预算，再触发指定的失败路径。
			select {
			// entered 是实际载荷 I/O 上下文，不能使用无取消通知的后台上下文。
			case entered := <-payload.entered:
				if entered.Done() == nil {
					t.Fatal("Decode has no cancellation")
				}
			case <-time.After(time.Second):
				t.Fatal("Decode fixture not reached")
			}
			if mode == "cancel" {
				cancel()
			}
			if mode == "shutdown" {
				// shutdown 已到期，只取消实例在途调用；资源清理拥有独立有限预算。
				shutdown, stop := context.WithCancel(context.Background())
				stop()
				// closeErr 必须保留排空取消，不可伪造正常排空成功。
				closeErr := instance.Shutdown(shutdown)
				if !errors.Is(closeErr, context.Canceled) {
					t.Fatalf("shutdown: %v", closeErr)
				}
			}
			// expected 区分业务预算到期与显式/实例取消。
			expected := context.Canceled
			if mode == "deadline" {
				expected = context.DeadlineExceeded
			}
			// decodeErr 必须反映真实预算原因，下载不能在关闭后继续占用资源。
			select {
			// decodeErr 是 I/O 真正退出的结果，收到后才完成清理断言。
			case decodeErr := <-done:
				// decodeErr 应保留 deadline 或取消身份，不能吞掉对象下载失败。
				if !errors.Is(decodeErr, expected) {
					t.Fatalf("Decode: %v", decodeErr)
				}
			case <-time.After(time.Second):
				t.Fatal("Decode did not exit")
			}
		})
	}
}

// resultBackend 不拥有网络资源，关闭计数由 Engine 生命周期管理。
type resultBackend struct{ ports.Backend }

// Close 释放受控后端，测试不会连接真实任务引擎。
func (resultBackend) Close() error { return nil }

// resultEngine 为载荷 I/O 构造完整在途表和观测资源，关闭时确认全部操作退出。
func resultEngine(t *testing.T, config spec.Runtime) *engine.Engine {
	t.Helper()
	config.Telemetry.Trace.DisableWorkerExporter = true
	// e 与 err 构造完整 Engine，观测及在途调用均由同一生命周期管理。
	e, err := engine.NewWithBackend(config, resultBackend{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// 关闭受控实例并检查资源回收错误，测试不能忽略清理失败。
		if err := e.Close(); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	})
	return e
}

// TestWorkerRuntimeBoundary 验证原生 Worker 局部选项只能改变任务运行资源。
func TestWorkerRuntimeBoundary(t *testing.T) {
	// base 包含非空 protobuf 投影与已配置拦截器，合法容量覆盖不能因能力是函数而失败。
	base := spec.Defaults()
	base.Namespace = "first"
	base.Projections["/fixture/Call"] = map[string]string{"group": "group_key"}
	base.Middleware = []middleware.Option{{UnaryClient: func(context.Context, string, any, any, *grpc.ClientConn, grpc.UnaryInvoker, ...grpc.CallOption) error {
		return nil
	}}}
	// local 继承能力对象，只有容量与标签归当前 Worker 所有。
	local := base.Clone()
	local.Slots = 2
	local.DurableSlots = 3
	local.Labels["owner"] = "worker-1"
	// err 合法局部覆盖不会改变实例配置，也不会丢弃拦截器。
	if err := validateWorkerRuntime(base, local, local.TLS); err != nil {
		t.Fatal(err)
	}
	local.Namespace = "second"
	// err 必须明确拒绝更换 namespace，不能返回成功后忽略此选项。
	if err := validateWorkerRuntime(base, local, local.TLS); err == nil {
		t.Fatal("namespace override accepted")
	}
	local = base.Clone()
	local.Shutdown.Timeout = time.Second
	// err 关闭预算由 Conn 统一拥有，Worker 不允许静默覆盖。
	if err := validateWorkerRuntime(base, local, local.TLS); err == nil {
		t.Fatal("shutdown override accepted")
	}
	local = base.Clone()
	local.ResultPollInterval = 2 * time.Second
	// err 轮询间隔属于共享后端，Worker 局部设置不能被接受后静默忽略。
	if err := validateWorkerRuntime(base, local, local.TLS); err == nil {
		t.Fatal("result polling override accepted")
	}
}
