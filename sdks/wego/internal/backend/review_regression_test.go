package backend_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	admin "github.com/hatchet-dev/hatchet/internal/services/admin/contracts"
	dispatcher "github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts"
	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/backend"
	client "github.com/hatchet-dev/hatchet/sdks/wego/internal/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	wruntime "github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// 以下是评审专用回归测试：断言设计所要求的行为，失败即留下可复现证据。
func TestReviewBinaryMetadata(t *testing.T) {
	// want 生成当前数据的文本表示，用于名称、业务比较或报告，不包含凭证。
	want := string([]byte{0xff, 0, 0xfe})
	// ctx 携带 token-bin 的 ff00fe 原始字节，protobuf 和 JSON 都不得按 UTF-8 替换。
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("token-bin", want))
	// env, err 按协议编码业务 protobuf；先投影调度字段，再执行载荷变换。
	env, err := wire.Encode(ctx, "method", &pb.Request{}, nil, nil, 1024)
	if err != nil {
		t.Fatal(err)
	}
	// data, _ 编码待传输的数据，错误时不能发送不完整的载荷。
	data, _ := json.Marshal(env)
	// decoded, err 还原 envelope 元数据及字节载荷，协议版本随后必须校验。
	decoded, err := wire.AsEnvelope(dataToMap(t, data))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Metadata["token-bin"][0] != want {
		t.Errorf("binary metadata corrupted: got %x, want %x", decoded.Metadata["token-bin"][0], want)
	}
	// encoded 经 protobuf/base64 转发多值二进制响应头。
	encoded, err := wire.EncodeFrame(&wire.Frame{Version: wire.Version, StreamId: "review", Kind: "HEADER", Metadata: wire.Metadata(metadata.Pairs("token-bin", want, "token-bin", "\x00\xff"))})
	if err != nil {
		t.Fatal(err)
	}
	// frame 必须完整恢复两项字节，不能只检查编码成功。
	frame, err := wire.DecodeFrame(encoded, 1024)
	if err != nil || !bytes.Equal(frame.Metadata["token-bin"].Values[0], []byte(want)) || !bytes.Equal(frame.Metadata["token-bin"].Values[1], []byte{0, 0xff}) {
		t.Fatalf("stream metadata changed: %v, %v", frame, err)
	}
	// rpcError 在错误通道中同样传输 binary headers 和 trailers。
	rpcError := &model.RPCError{Err: status.Error(codes.Aborted, "business failure"), Headers: env.Metadata, Trailers: env.Metadata}
	// restored 只使用 wego 错误链，任意字节不混入业务 details。
	var restored *model.RPCError
	if !errors.As(wire.DecodeError(wire.EncodeError(rpcError)), &restored) || restored.Headers["token-bin"][0] != want || restored.Trailers["token-bin"][0] != want {
		t.Fatal("error metadata changed")
	}
}

// dataToMap 模拟任务引擎的 JSON map 出口，验证任意字节 metadata 经 JSON 仍可恢复。
func dataToMap(t *testing.T, data []byte) map[string]any {
	t.Helper()
	// v 模拟 JSON 出口的独立 map，与编码前的 Envelope 不共享内存。
	var v map[string]any
	// err 完整解析输入再交付业务，损坏数据不能推进执行状态。
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// TestReviewCronInputOverride 验证仅设置方法输入时继承默认 Cron 表达式，并独立覆盖业务输入。
func TestReviewCronInputOverride(t *testing.T) {
	// base 的默认 Cron 输入为 default，方法级只改输入后应得到 per-method。
	base := spec.Task{Cron: []string{"* * * * *"}, CronInput: "default"}
	// got 合并默认与方法级策略；未设置字段继承，显式 nil 可以清除 CronInput。
	got := spec.Merge(base, spec.Task{CronInput: "per-method"})
	if got.CronInput != "per-method" || got.Cron[0] != "* * * * *" {
		t.Fatalf("method override lost: got %v", got)
	}
	// expressionOnly 改为每两分钟，未设置输入时必须继续使用 default。
	expressionOnly := spec.Merge(base, spec.Task{Cron: []string{"*/2 * * * *"}})
	if expressionOnly.CronInput != "default" {
		t.Fatal("cron expression unexpectedly cleared input")
	}
	// cleared 显式 nil 清空输入，空策略则继承默认输入。
	cleared := spec.Merge(base, spec.Task{CronInputSet: true})
	if cleared.CronInput != nil || spec.Merge(base, spec.Task{}).CronInput != "default" {
		t.Fatal("explicit clear and inheritance conflated")
	}
}

// blockedAdmin 提交阶段阻塞的受控管理服务，测试真实 gRPC 取消预算。
type blockedAdmin struct {
	// admin.UnimplementedWorkflowServiceServer 委托未覆盖的 gRPC 方法，只有此 fixture 的目标 RPC 由受控实现处理。
	admin.UnimplementedWorkflowServiceServer
	// entered 真实请求进入 fixture 的通知，断言取消发生在实际 I/O 阶段。
	entered chan *v1.TriggerWorkflowRequest
	// release 显式释放 fixture 的清理信号，业务断言不得依赖它制造成功。
	release chan struct{}
}

// TriggerWorkflow 阻塞真实提交 RPC，只有测试释放或实际取消预算能结束它。
func (s *blockedAdmin) TriggerWorkflow(ctx context.Context, in *v1.TriggerWorkflowRequest) (*admin.TriggerWorkflowResponse, error) {
	s.entered <- in
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return nil, status.Error(codes.InvalidArgument, "review fixture end")
}

// blockedRegistration 启动注册阶段阻塞的受控服务，模拟引擎迟迟不返回注册结果。
type blockedRegistration struct {
	// v1.UnimplementedAdminServiceServer 委托未覆盖的 gRPC 方法，只有此 fixture 的目标 RPC 由受控实现处理。
	v1.UnimplementedAdminServiceServer
	// entered 真实请求进入 fixture 的通知，断言取消发生在实际 I/O 阶段。
	entered chan struct{}
	// release 显式释放 fixture 的清理信号，业务断言不得依赖它制造成功。
	release chan struct{}
	// once 只发布一次注册进入通知，重试不能重复关闭 channel。
	once sync.Once
}

// PutWorkflow 阻塞注册 RPC，Stop 必须通过 context 取消真实请求。
func (s *blockedRegistration) PutWorkflow(ctx context.Context, in *v1.CreateWorkflowVersionRequest) (*v1.CreateWorkflowVersionResponse, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return nil, status.Error(codes.InvalidArgument, "review fixture end")
}

// reviewDispatcher 返回支持的固定引擎版本，避免无关能力探测影响取消测试。
type reviewDispatcher struct {
	// dispatcher.UnimplementedDispatcherServer 委托未覆盖的 gRPC 方法，只有此 fixture 的目标 RPC 由受控实现处理。
	dispatcher.UnimplementedDispatcherServer
}

// GetVersion 返回受控支持版本，使注册测试进入目标 RPC 而非旧引擎分支。
func (*reviewDispatcher) GetVersion(context.Context, *dispatcher.GetVersionRequest) (*dispatcher.GetVersionResponse, error) {
	return &dispatcher.GetVersionResponse{Version: "v0.107.0"}, nil
}

// Register 模拟建立监听阶段的真实注册阻塞，只由传输取消结束，不依赖测试放行。
func (*reviewDispatcher) Register(ctx context.Context, _ *dispatcher.WorkerRegisterRequest) (*dispatcher.WorkerRegisterResponse, error) {
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

// fixture 建立隔离 gRPC fixture 与有效配置，凭证仅用于测试且不打印。
func fixture(t *testing.T, a *blockedAdmin, r *blockedRegistration) spec.Runtime {
	t.Helper()
	// l, err 先绑定可用端口，启动失败由创建方释放监听器。
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// g 提供受控的 gRPC 后端，测试可阻塞提交和注册并观察取消。
	g := grpc.NewServer()
	dispatcher.RegisterDispatcherServer(g, &reviewDispatcher{})
	if a != nil {
		admin.RegisterWorkflowServiceServer(g, a)
	}
	if r != nil {
		v1.RegisterAdminServiceServer(g, r)
	}
	go g.Serve(l)
	t.Cleanup(g.Stop)
	// c 创建默认配置副本，后续显式选项可以覆盖 0、false 或 nil。
	c := spec.Defaults()
	c.Address = l.Addr().String()
	c.ServerURL = "http://127.0.0.1:1"
	c.TenantID = "00000000-0000-4000-8000-000000000001"
	c.TLSSet = true
	c.TLS = nil
	c.Shutdown.Timeout = 100 * time.Millisecond
	c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	c.Telemetry.Trace.DisableWorkerExporter = true
	// claims, _ 编码待传输的数据，错误时不能发送不完整的载荷。
	claims, _ := json.Marshal(map[string]any{"sub": c.TenantID, "exp": time.Now().Add(time.Hour).Unix(), "server_url": c.ServerURL, "grpc_broadcast_address": c.Address})
	c.Token = "e30." + base64.RawURLEncoding.EncodeToString(claims) + ".fixture"
	return c
}

// TestReviewTriggerDeadline 验证提交 RPC 已开始后 deadline 仍能结束提交，不能依靠手工释放。
func TestReviewTriggerDeadline(t *testing.T) {
	// a 保存本实例的完成或故障通知，关闭通道不代表业务成功。
	a := &blockedAdmin{entered: make(chan *v1.TriggerWorkflowRequest, 1), release: make(chan struct{})}
	// c 启动受控引擎传输服务，测试只观察实际 RPC，不依赖生产实例。
	c := fixture(t, a, nil)
	// b, err 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	b, err := backend.New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	// ctx, cancel 为本段 I/O 或等待分配有限预算，退出时释放取消函数。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	// done 分配本段逻辑独占的容器或通知通道，避免与其他调用共享可变状态。
	done := make(chan error, 1)
	// go func() { _, err 提交并等待业务结果，失败和取消不得作为有效输出继续使用。
	go func() { _, err := b.Run(ctx, "review", nil, model.RunOptions{}); done <- err }()
	select {
	case <-a.entered:
	case <-time.After(time.Second):
		t.Fatal("request did not enter fixture")
	}
	select {
	case <-done:
	case <-time.After(150 * time.Millisecond):
		t.Error("Run is still blocked 120ms past caller deadline")
	}
	close(a.release)
	select {
	case <-done:
	case <-time.After(time.Second):
	}
}

// TestReviewNumericAffinity 检查真实提交请求包含 capacity=8 的整数值及 Required 约束。
func TestReviewNumericAffinity(t *testing.T) {
	// a 保存本实例的完成或故障通知，关闭通道不代表业务成功。
	a := &blockedAdmin{entered: make(chan *v1.TriggerWorkflowRequest, 1), release: make(chan struct{})}
	// c 启动受控引擎传输服务，测试只观察实际 RPC，不依赖生产实例。
	c := fixture(t, a, nil)
	// b, err 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	b, err := backend.New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	defer close(a.release)
	// done 分配本段逻辑独占的容器或通知通道，避免与其他调用共享可变状态。
	done := make(chan struct{})
	// comparator 验证数字比较器与必需性、权重一起保留到真实请求。
	comparator := model.LabelGreaterEqual
	go func() {
		defer close(done)
		_, _ = b.Run(context.Background(), "review", nil, model.RunOptions{Labels: map[string]*model.DesiredWorkerLabel{"capacity": {Value: int32(8), Required: true, Weight: 7, Comparator: &comparator}}})
	}()
	select {
	// req 从实际操作完成通道读取结果，失败时不能继续后续成功断言。
	case req := <-a.entered:
		// label 是真实提交请求中的 affinity 条件，capacity=8 必须是整数而非丢失的动态值。
		label := req.DesiredWorkerLabels["capacity"]
		if label == nil || label.IntValue == nil || *label.IntValue != 8 || !label.GetRequired() || label.GetWeight() != 7 || int32(label.GetComparator()) != int32(model.LabelGreaterEqual) {
			t.Errorf("numeric affinity value missing from dispatched request: %v", label)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not enter fixture")
	}
}

// TestReviewServerStopDuringRegistration 验证启动注册被阻塞时 Stop 在关闭预算内结束，而非等待服务端放行。
func TestReviewServerStopDuringRegistration(t *testing.T) {
	// r 保存本实例的完成或故障通知，关闭通道不代表业务成功。
	r := &blockedRegistration{entered: make(chan struct{}), release: make(chan struct{})}
	// c 启动受控引擎传输服务，测试只观察实际 RPC，不依赖生产实例。
	c := fixture(t, nil, r)
	// s 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	s := server.New(server.WithRuntime(wruntime.WithShutdown(wruntime.ShutdownConfig{Mode: wruntime.DrainWithTimeout, Timeout: 50 * time.Millisecond}), wruntime.WithToken(c.Token), wruntime.WithAddress(c.Address), wruntime.WithTLSConfig(nil), wruntime.WithLogger(c.Logger), wruntime.WithTelemetry(telemetry.Config{Trace: telemetry.TraceConfig{DisableWorkerExporter: true}})))
	pb.RegisterUnaryGreeterServer(s, &pb.UnimplementedUnaryGreeterServer{})
	// serve 分配本段逻辑独占的容器或通知通道，避免与其他调用共享可变状态。
	serve := make(chan error, 1)
	go func() { serve <- s.Serve() }()
	select {
	case <-r.entered:
	// err 从实际操作完成通道读取结果，失败时不能继续后续成功断言。
	case err := <-serve:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("registration did not enter fixture")
	}
	// stopped 分配本段逻辑独占的容器或通知通道，避免与其他调用共享可变状态。
	stopped := make(chan struct{})
	go func() { s.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(200 * time.Millisecond):
		t.Error("Stop cannot cancel in-flight registration")
	}
	close(r.release)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not finish after fixture released")
	}
	<-serve
}

// cacheGoroutines 只统计 SDK TTL cache 的 goroutine，避免把无关运行时任务当作泄漏。
func cacheGoroutines() int {
	// b 捕获 goroutine 堆栈的缓冲区，仅匹配 TTL cache 的特定执行函数。
	var b bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	return strings.Count(b.String(), "internal/cache.NewTTL[")
}

// TestReviewClosedConnectionsLeaveNoCacheGoroutines 反复构造并关闭实例，特定缓存 goroutine 数量不能增长。
func TestReviewClosedConnectionsLeaveNoCacheGoroutines(t *testing.T) {
	// c 启动受控引擎传输服务，测试只观察实际 RPC，不依赖生产实例。
	c := fixture(t, nil, nil)
	// before 统计 TTL 缓存的特定 goroutine 堆栈，重复关闭后不得增长。
	before := cacheGoroutines()
	for range 3 {
		// b, err 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
		b, err := backend.New(c)
		if err != nil {
			t.Fatal(err)
		}
		// 初始化 Metrics 的共享缓存；即便 HTTP 失败，关闭仍须回收全部缓存资源。
		_ = b.Feature(context.Background(), ports.MetricsGetQueueMetrics{Query: model.Query{}}, nil)
		if err = b.Close(); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(30 * time.Millisecond)
	// after 统计 TTL 缓存的特定 goroutine 堆栈，重复关闭后不得增长。
	after := cacheGoroutines()
	if after > before {
		t.Fatalf("cache goroutine stack entries grew after 3 closed connections: before=%d after=%d", before, after)
	}
}

// TestReviewInvalidTenantDoesNotPanic 错误租户配置必须返回错误，不允许 UUID 解析 panic 穿过公开边界。
func TestReviewInvalidTenantDoesNotPanic(t *testing.T) {
	// c 启动受控引擎传输服务，测试只观察实际 RPC，不依赖生产实例。
	c := fixture(t, nil, nil)
	c.TenantID = "invalid"
	defer func() {
		// r 捕获当前执行的 panic，边界应返回可诊断错误并继续资源清理。
		if r := recover(); r != nil {
			t.Errorf("invalid configuration panicked instead of returning error: %v", r)
		}
	}()
	// b, err 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	b, err := backend.New(c)
	if b != nil {
		_ = b.Close()
	}
	if err == nil {
		t.Error("invalid tenant accepted")
	}
}

// 评审专用：客户端拦截器追加的原生 Header 选项必须传至底层 invoker。
type immediateBackend struct {
	// Backend 委托资源关闭，提交响应由本 fixture 控制。
	ports.Backend
	// failure 非 nil 时通过 Wait 返回业务错误，验证错误分支的 metadata。
	failure error
}

// Run 构造受控 protobuf 响应及真实 metadata 形状，供 invoker 选项传递测试使用。
func (b immediateBackend) Run(ctx context.Context, name string, input any, opts model.RunOptions) (ports.Run, error) {
	// env, err 按协议编码业务 protobuf；先投影调度字段，再执行载荷变换。
	env, err := wire.Encode(ctx, "method", &pb.Reply{Message: "response"}, nil, nil, 1024)
	if err != nil {
		return ports.Run{}, err
	}
	env.Headers = map[string][]string{"header": {"value"}}
	env.Trailers = map[string][]string{"trailer": {"value"}}
	return ports.Run{ID: "fixture", Wait: func(context.Context) (ports.Result, error) {
		if b.failure != nil {
			return ports.Result{}, &model.RPCError{Err: b.failure, Headers: env.Headers, Trailers: env.Trailers}
		}
		return ports.Result{RunID: "fixture", Outputs: map[string]any{"output": env}}, nil
	}}, nil
}

// TestReviewInterceptorCallOptionsReachInvoker 拦截器追加 Header 后，真实 invoker 必须填写新增的目标对象。
func TestReviewInterceptorCallOptionsReachInvoker(t *testing.T) {
	// cfg 启动受控引擎传输服务，测试只观察实际 RPC，不依赖生产实例。
	cfg := fixture(t, nil, nil)
	// header 接收拦截器添加 Header 选项的目标，响应后必须非空。
	var header metadata.MD
	cfg.Middleware = []middleware.Option{middleware.WithUnaryClient(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, next grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return next(ctx, method, req, reply, cc, append(opts, grpc.Header(&header))...)
	})}
	// e, err 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	e, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.Backend = immediateBackend{Backend: e.Backend}
	// c 拥有当前测试引擎，关闭时释放其连接、缓存及在途调用。
	c := client.FromEngine(e, false)
	defer c.Close()
	_, err = pb.NewUnaryGreeterClient(c).SayHello(context.Background(), &pb.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if len(header.Get("header")) != 1 {
		t.Fatalf("interceptor-added Header option discarded: %v", header)
	}
}

// 评审专用：事件载荷转换也必须受发布调用的取消预算控制。
type reviewPayload struct{ sawCancelled bool }

// Encode 记录 payload 编码看到的取消状态，测试发布预算是否覆盖对象 I/O。
func (p *reviewPayload) Encode(ctx context.Context, _ string, data []byte) ([]byte, error) {
	p.sawCancelled = ctx.Err() != nil
	return nil, status.Error(codes.Canceled, "review fixture end")
}

// Decode 此 fixture 不执行反向变换；编码取消断言不应被解码路径影响。
func (*reviewPayload) Decode(context.Context, string, []byte) ([]byte, error) { return nil, nil }

// TestReviewEventPayloadHonorsCancellation 发布 context 已取消时，payload 编码也必须收到取消信号。
func TestReviewEventPayloadHonorsCancellation(t *testing.T) {
	// cfg 启动受控引擎传输服务，测试只观察实际 RPC，不依赖生产实例。
	cfg := fixture(t, nil, nil)
	// transform 记录实际 payload.Encode 是否看见发布 context 的取消信号。
	transform := &reviewPayload{}
	cfg.Middleware = []middleware.Option{middleware.WithPayload(transform)}
	// e, err 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	e, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// c 拥有当前测试引擎，关闭时释放其连接、缓存及在途调用。
	c := client.FromEngine(e, false)
	defer c.Close()
	// ctx, cancel 创建独立取消视图，退出时释放后台等待，不改写父执行 context。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = c.Events().Push(ctx, "review", model.RPCInput{Method: pb.UnaryGreeter_SayHello_FullMethodName, Message: &pb.Request{}})
	if !transform.sawCancelled {
		t.Error("event payload Encode received uncancellable context despite caller cancellation")
	}
}

// 评审专用：同一连接上的管理方法应使用同一命名空间解析规则。
func TestReviewWorkflowMetricsUsesNamespace(t *testing.T) {
	// requested 分配本段逻辑独占的容器或通知通道，避免与其他调用共享可变状态。
	requested := make(chan string, 1)
	// api 提供受控 HTTP 响应，记录实际查询的名称与删除后的资源状态。
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested <- r.URL.Query().Get("name")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"rows":[]}`)
	}))
	defer api.Close()
	// cfg 启动受控引擎传输服务，测试只观察实际 RPC，不依赖生产实例。
	cfg := fixture(t, nil, nil)
	cfg.ServerURL = api.URL
	cfg.Namespace = "review_"
	// b, err 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	b, err := backend.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	// out 业务断言的解码目标，返回错误时不得使用旧的有效值。
	var out model.Resource
	_ = b.Feature(context.Background(), ports.MetricsGetWorkflowMetrics{Name: "task", Query: nil}, &out)
	// got 从实际操作完成通道读取结果，失败时不能继续后续成功断言。
	if got := <-requested; got != "review_task" {
		t.Errorf("metrics requested unqualified name: got %q want review_task", got)
	}
}

// 评审专用：删除成功后，同一个连接不得继续从缓存返回已删除资源。
func TestReviewWorkflowDeleteInvalidatesLookup(t *testing.T) {
	// mu 保护 HTTP fixture 的删除状态，Get 与 Delete 可以并发请求。
	var mu sync.Mutex
	// deleted 是受锁保护的 HTTP fixture 状态，删除后查询应返回空列表而非缓存旧值。
	deleted := false
	// identity 是同名资源重建后的身份，旧 ID 不可从失效缓存中复活。
	identity := "00000000-0000-4000-8000-000000000002"
	// api 提供受控 HTTP 响应，记录实际查询的名称与删除后的资源状态。
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "DELETE" {
			deleted = true
			w.WriteHeader(204)
			return
		}
		if deleted {
			io.WriteString(w, `{"rows":[]}`)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"rows": []any{map[string]any{"metadata": map[string]any{"id": identity}, "name": "review_task"}}})
	}))
	defer api.Close()
	// cfg 启动受控引擎传输服务，测试只观察实际 RPC，不依赖生产实例。
	cfg := fixture(t, nil, nil)
	cfg.ServerURL = api.URL
	cfg.Namespace = "review_"
	// b, err 创建此调用所需的实例对象，拥有的连接、监听器或缓存由创建方清理。
	b, err := backend.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	// out 业务断言的解码目标，返回错误时不得使用旧的有效值。
	var out model.Resource
	if err = b.Feature(context.Background(), ports.WorkflowsGet{ID: "task"}, &out); err != nil {
		t.Fatal(err)
	}
	if err = b.Feature(context.Background(), ports.WorkflowsDelete{ID: "task"}, nil); err != nil {
		t.Fatal(err)
	}
	if err = b.Feature(context.Background(), ports.WorkflowsGet{ID: "task"}, &out); err == nil {
		t.Errorf("deleted workflow still returned by same client's cache: %v", out["name"])
	}
	mu.Lock()
	deleted = false
	identity = "00000000-0000-4000-8000-000000000003"
	mu.Unlock()
	if err = b.Feature(context.Background(), ports.WorkflowsGet{ID: "task"}, &out); err != nil || out.ID() != identity {
		t.Fatalf("recreated workflow identity not refreshed: %v, %v", out, err)
	}
}

// BulkTriggerWorkflow 将批量 RPC 同样阻塞到预算结束，证明 RunMany 不使用后台 context。
func (s *blockedAdmin) BulkTriggerWorkflow(ctx context.Context, in *admin.BulkTriggerWorkflowRequest) (*admin.BulkTriggerWorkflowResponse, error) {
	// request 使测试能观察到真正进入传输的第一项，不能用外层超时伪造取消。
	for _, request := range in.Workflows {
		select {
		case s.entered <- request:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return nil, status.FromContextError(ctx.Err()).Err()
}

// TestBulkSubmissionCancellation 覆盖提交前取消和提交中到期；失败时 fixture 始终可被清理。
func TestBulkSubmissionCancellation(t *testing.T) {
	// before 为 true 时请求不应到达服务端，为 false 时必须在进入 RPC 后结束。
	for _, before := range []bool{true, false} {
		t.Run(map[bool]string{true: "before_submission", false: "during_submission"}[before], func(t *testing.T) {
			// admin 的记录通道有缓冲，取消断言不依赖测试 goroutine 及时消费。
			a := &blockedAdmin{entered: make(chan *v1.TriggerWorkflowRequest, 4), release: make(chan struct{})}
			// b 是实际后端，测试经过 protobuf gRPC 序列化链。
			b, err := backend.New(fixture(t, a, nil))
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			defer close(a.release)
			// ctx 的 50ms 预算必须进入 BulkTriggerWorkflow；本测试不释放 fixture 来制造成功。
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if before {
				cancel()
			}
			// done 防止预算传递回归使整个测试进程无限等待。
			done := make(chan error, 1)
			go func() {
				// _, e 逐条提交并保留输入顺序，结果句柄与每项输入一一对应。
				_, e := b.RunMany(ctx, "bulk", []model.RunManyInput{{Input: map[string]any{"id": 1}}})
				done <- e
			}()
			if !before {
				select {
				case <-a.entered:
				case <-time.After(time.Second):
					t.Fatal("bulk RPC did not start")
				}
			}
			select {
			// err 必须体现取消或超时，而不是 fixture 人工放行后的业务错误。
			case err := <-done:
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && status.Code(err) != codes.Canceled && status.Code(err) != codes.DeadlineExceeded {
					t.Fatalf("submission budget: %v", err)
				}
			case <-time.After(250 * time.Millisecond):
				t.Fatal("bulk submission ignored cancellation")
			}
			if before && len(a.entered) != 0 {
				t.Fatal("cancelled request reached engine")
			}
		})
	}
}

// TestBinaryErrorMetadata 检查错误结果与成功 envelope 使用相同字节协议，多值不合并。
func TestBinaryErrorMetadata(t *testing.T) {
	// binary 包含非法 UTF-8，但属于合法 gRPC -bin metadata。
	binary := string([]byte{0xff, 0, 0xfe})
	// encoded 模拟任务失败后引擎传递的错误文本。
	encoded := wire.EncodeError(&model.RPCError{Err: status.Error(codes.PermissionDenied, "denied"), Headers: map[string][]string{"token-bin": {binary, "two"}}, Trailers: map[string][]string{"token-bin": {binary}}})
	// decoded 必须保留业务 status 和两种响应 metadata。
	decoded := wire.DecodeError(encoded)
	// transport 是 wego 自有错误，不能保留 Hatchet 错误实例。
	transport, ok := decoded.(*model.RPCError)
	if !ok || status.Code(decoded) != codes.PermissionDenied {
		t.Fatalf("error status: %v", decoded)
	}
	if transport.Headers["token-bin"][0] != binary || len(transport.Headers["token-bin"]) != 2 || transport.Trailers["token-bin"][0] != binary {
		t.Fatalf("binary metadata damaged: %x", transport.Headers)
	}
}

// blockingPayload 模拟对象存储上传，I/O 只在实际调用 context 被取消后返回。
type blockingPayload struct {
	// entered 发布上传已开始的通知，测试不能在编码之前关闭连接掩盖缺陷。
	entered chan struct{}
}

// Encode 阻塞在真实预算上，不用 fixture 的额外释放通道制造取消效果。
func (p *blockingPayload) Encode(ctx context.Context, _ string, _ []byte) ([]byte, error) {
	close(p.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}

// Decode 不参与上传生命周期测试。
func (*blockingPayload) Decode(_ context.Context, _ string, data []byte) ([]byte, error) {
	return data, nil
}

// TestTriggerEncodingParticipatesInClose 验证编码阶段已登记在途调用，Conn.Close 能结束阻塞上传。
func TestTriggerEncodingParticipatesInClose(t *testing.T) {
	// cfg 将关闭预算缩短到 50ms，模拟上传期间退出应用。
	cfg := fixture(t, nil, nil)
	cfg.Shutdown.Timeout = 50 * time.Millisecond
	// payload 使用自身的 entered 通知上传开始。
	payload := &blockingPayload{entered: make(chan struct{})}
	cfg.Middleware = []middleware.Option{middleware.WithPayload(payload)}
	// e 和 conn 拥有本轮资源，关闭操作必须取消发布编码而非只关闭后端连接。
	e, err := engine.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// conn 由内部工厂装配，仅用于验证实例生命周期边界。
	conn := client.FromEngine(e, false)
	defer conn.Close()
	// completed 保存当前上传结束的实际错误。
	completed := make(chan error, 1)
	go func() {
		completed <- conn.Events().Push(context.Background(), "future", model.RPCInput{Method: pb.UnaryGreeter_SayHello_FullMethodName, Message: &pb.Request{}})
	}()
	select {
	case <-payload.entered:
	case <-time.After(time.Second):
		t.Fatal("upload did not start")
	}
	_ = conn.Close()
	select {
	// err 必须来自上传看到的取消，不能在函数之外伪装提前返回。
	case err := <-completed:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Close left payload upload alive")
	}
}

// TestInterceptorReplacementOptionsAndReply 验证替换 CallOption 和响应对象，以及业务错误的 headers/trailers。
func TestInterceptorReplacementOptionsAndReply(t *testing.T) {
	// failure 分别覆盖成功和带 status 的业务错误，不能只验证正常输出。
	for _, failure := range []error{nil, status.Error(codes.FailedPrecondition, "fixture rejected")} {
		// cfg 与引擎资源属于当前迭代，保证错误测试也关闭缓存和连接。
		cfg := fixture(t, nil, nil)
		// headers、trailers 接收拦截器替换后的选项；discarded 对应被替换掉的调用方选项。
		var headers, trailers, discarded metadata.MD
		cfg.Middleware = []middleware.Option{middleware.WithUnaryClient(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, next grpc.UnaryInvoker, _ ...grpc.CallOption) error {
			// replacement 是实际 invoker 的解码目标，不能仍然写入外层捕获的 reply。
			replacement := &pb.Reply{}
			// err 同时覆盖成功和业务错误分支，响应 metadata 不依赖成功结果。
			err := next(ctx, method, req, replacement, cc, grpc.Header(&headers), grpc.Trailer(&trailers))
			if err == nil {
				reply.(*pb.Reply).Message = replacement.Message
			}
			return err
		})}
		// e 是当前断言拥有的引擎，后端仅替换业务响应。
		e, err := engine.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		e.Backend = immediateBackend{Backend: e.Backend, failure: failure}
		// c 拥有测试引擎，退出时释放实例资源。
		c := client.FromEngine(e, false)
		// out 与 err 是真实生成桩的输出，成功时必须读到 replacement 的业务字段。
		out, err := pb.NewUnaryGreeterClient(c).SayHello(context.Background(), &pb.Request{}, grpc.Header(&discarded))
		if status.Code(err) != status.Code(failure) || len(headers.Get("header")) != 1 || len(trailers.Get("trailer")) != 1 || len(discarded) != 0 || (failure == nil && out.Message != "response") {
			t.Errorf("interceptor replacement failed: %v, %v, %v, %v", out, err, headers, trailers)
		}
		if err = c.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestWorkerStartupDeadlineHonorsOriginalCause 实际 Register 取消后仍应报告调用方 DeadlineExceeded。
func TestWorkerStartupDeadlineHonorsOriginalCause(t *testing.T) {
	// cfg 与 b 使用真实 gRPC fixture，版本探测先完成，Start 才进入阻塞注册。
	cfg := fixture(t, nil, nil)
	// b 拥有当前连接；无业务任务的 Worker 只验证初始化传输。
	b, err := backend.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	// w 使用与 Server 相同的 backend 生命周期入口。
	w, err := b.Worker(context.Background(), "startup-budget", nil, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(context.Background())
	// ctx 在真实注册尚未响应时耗尽，监听生命周期桥接不能把 deadline 变成普通取消。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	// err 必须来自请求预算，fixture 不主动解除阻塞。
	if err = w.Start(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
