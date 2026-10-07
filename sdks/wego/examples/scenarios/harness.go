package scenarios

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/telemetry"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// Record 单条已通过断言的证据，记录场景和运行身份，不记录凭证。
type Record struct {
	// Scenario 验收场景名称，用于把断言关联到 manifest。
	Scenario string `json:"scenario"`
	// TestScenario 细分测试场景标识。
	TestScenario string `json:"test_scenario,omitempty"`
	// Namespace 任务名称前缀；每次验收使用唯一值隔离测试资源。
	Namespace string `json:"namespace"`
	// Assertion 本条验收记录已通过的具体断言。
	Assertion string `json:"assertion"`
	// AssertionID 使用内容身份关联 manifest，记录只在对应业务断言已通过时追加。
	AssertionID string `json:"assertion_id"`
	// RunID 工作流运行身份，例如 run-001，用于等待、查询和取消。
	RunID string `json:"run_id,omitempty"`
	// WorkerID 本次执行所属 Worker 的注册身份。
	WorkerID string `json:"worker_id,omitempty"`
	// TraceID 与本次执行关联的追踪身份。
	TraceID string `json:"trace_id,omitempty"`
	// At 断言记录的时间。
	At time.Time `json:"at"`
}

// Report 并发安全的验收记录集合。
type Report struct {
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁。
	mu sync.Mutex
	// Records 已经确认的验收记录集合，不保存令牌等凭证。
	Records []Record
}

// Count 读取当前报告记录数，用于标记一个场景新增断言的范围。
func (r *Report) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.Records)
}

// AttributeSince 为指定位置之后的记录关联测试场景，便于报告归组。
func (r *Report) AttributeSince(index int, scenario string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
	for i := index; i < len(r.Records); i++ {
		r.Records[i].TestScenario = scenario
	}
}

// Add 追加已经通过的断言和运行身份，写报告时不包含访问凭证。
func (r *Report) Add(h *Harness, assertion string, reply *pb.Reply) {
	r.AddTrace(h, assertion, reply, "")
}

// AddTrace 追加成功断言及其 trace 身份，与普通断言使用同一稳定 ID 生成规则。
func (r *Report) AddTrace(h *Harness, assertion string, reply *pb.Reply, traceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// record 仅在对应断言已经成功时创建，记录执行身份和稳定断言 ID，不记录凭证。
	record := Record{
		Scenario:    h.Scenario,
		Namespace:   h.Namespace,
		Assertion:   assertion,
		AssertionID: fmt.Sprintf("%x", sha256.Sum256([]byte(assertion)))[:24],
		TraceID:     traceID,
		At:          time.Now().UTC(),
	}
	if reply != nil {
		record.RunID = reply.RunId
		record.WorkerID = reply.WorkerId
	}
	r.Records = append(r.Records, record)
}

// UnaryHandler 标准 protobuf unary 业务函数，参数仍是 context.Context。
type UnaryHandler func(context.Context, *pb.Request) (*pb.Reply, error)

// Service 可注入三个 unary handler 的验收服务。
type Service struct {
	// pb.UnimplementedUnaryGreeterServer 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则。
	pb.UnimplementedUnaryGreeterServer
	// Say, Wait, Child 三个 unary handler 的可替换实现，供不同场景注入行为。
	Say, Wait, Child UnaryHandler
}

// SayHello 处理 unary 文本请求，返回可用于断言的响应。
func (s *Service) SayHello(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	if s.Say != nil {
		return s.Say(ctx, in)
	}

	return Reply(ctx, in), nil
}

// WaitHello 处理可等待或 durable 的请求，用于验证执行预算与等待能力。
func (s *Service) WaitHello(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	if s.Wait != nil {
		return s.Wait(ctx, in)
	}

	return Reply(ctx, in), nil
}

// ChildHello 通过任务上下文提交子调用，并返回父子运行身份。
func (s *Service) ChildHello(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	if s.Child != nil {
		return s.Child(ctx, in)
	}

	return Reply(ctx, in), nil
}

// Reply 构造业务响应并附带当前任务身份，便于断言 Worker 入口确实经过引擎。
func Reply(ctx context.Context, in *pb.Request) *pb.Reply {
	// info, _ 读取实际执行身份，例如 RunID 与 WorkerID；普通网络 context 没有任务身份。
	info, _ := task.Info(ctx)
	// out 是响应消息，复制业务文本、数量与真实任务身份；后续据此区分 Worker 和直接网络调用。
	out := &pb.Reply{
		Message:         in.Message,
		Count:           in.Count,
		RunId:           info.RunID,
		WorkerId:        info.WorkerID,
		RetryCount:      int32(info.RetryCount),
		InvocationCount: info.InvocationCount,
	}
	if info.ParentRunID != nil {
		out.ParentRunId = *info.ParentRunID
	}
	return out
}

// Harness 将每个验收场景的实例、命名空间和资源清理放在一起管理。
type Harness struct {
	// Scenario, Namespace 验收场景与隔离前缀；同一运行中的资源使用同一前缀。
	Scenario, Namespace string
	// Conn 通过任务引擎调用的 wego 连接。
	Conn *client.Conn
	// Server 共享服务注册和入口生命周期的 Server。
	Server *server.Server
	// RPC 由生成代码创建的业务客户端。
	RPC pb.UnaryGreeterClient
	// Report 结果报告或日志上报回调，由所属类型决定。
	Report *Report
	// Runtime 实例配置或配置选项集合。
	Runtime []runtime.Option
	// serve Serve 的退出结果通道，关闭验收时确认没有后台入口残留。
	serve chan error
	// NativeWorker 原生任务或 batch 场景单独注册的 Worker。
	NativeWorker *client.Worker
	// Names 本场景注册的任务名称集合，用于就绪检查。
	Names []string
}

// Runtime 返回客户端和 Worker 共用的配置，确保投影和载荷协议一致。
func Runtime(namespace string) []runtime.Option {
	// address 读取本机显式配置；未提供时由紧接着的默认分支解析。
	address := os.Getenv("WEGO_GRPC_ADDRESS")
	if address == "" {
		address = "localhost:7077"
	}
	// api 读取本机显式配置；未提供时由紧接着的默认分支解析。
	api := os.Getenv("WEGO_API_URL")
	if api == "" {
		api = "http://localhost:8080"
	}
	return []runtime.Option{
		runtime.WithToken(os.Getenv("HATCHET_CLIENT_TOKEN")),
		runtime.WithAddress(address),
		runtime.WithServerURL(api),
		runtime.WithTLSConfig(nil),
		runtime.WithNamespace(namespace),
		runtime.WithSlots(10),
		runtime.WithDurableSlots(4),
		runtime.WithInputProjection(pb.UnaryGreeter_SayHello_FullMethodName, map[string]string{
			"group":   "group_key",
			"account": "account",
			"tier":    "tier",
			"user_id": "user_id",
			"id":      "id",
			"count":   "count",
		}),
		runtime.WithInputProjection(pb.UnaryGreeter_WaitHello_FullMethodName, map[string]string{
			"group":   "group_key",
			"account": "account",
			"tier":    "tier",
			"id":      "id",
		}),
	}
}

// Start 启动 Worker 入口；运行期间的异步错误通过错误通道报告。
func Start(
	ctx context.Context,
	scenario string,
	report *Report,
	service *Service,
	runtimeOptions []runtime.Option,
	policies ...worker.Option,
) (*Harness, error) {
	return StartRegistered(ctx, scenario, report, func(registrar grpc.ServiceRegistrar) {
		pb.RegisterUnaryGreeterServer(registrar, service)
	}, nil, runtimeOptions, policies...)
}

// StartRegistered 为每次运行创建唯一命名空间，并在真实消费就绪后创建业务客户端。
func StartRegistered(
	ctx context.Context,
	scenario string,
	report *Report,
	register func(grpc.ServiceRegistrar),
	names []string,
	runtimeOptions []runtime.Option,
	policies ...worker.Option,
) (*Harness, error) {
	// namespace 生成当前数据的文本表示，用于名称、业务比较或报告，不包含凭证。
	namespace := fmt.Sprintf("wego_accept_%s_%d_", scenario, time.Now().UnixNano())
	// opts 复制或扩展当前列表，避免把内部可变容器直接交给调用方修改。
	opts := append(Runtime(namespace), runtimeOptions...)
	// srv 构造共享注册表的 Server，业务只在 Serve 启动后接收调用。
	srv := wego.NewServer(server.WithRuntime(opts...), server.WithWorker(policies...))

	register(srv)
	// h 当前场景的生命周期夹具，集中保存 Server、Conn、隔离前缀和真实验收报告。
	h := &Harness{
		Scenario:  scenario,
		Namespace: namespace,
		Server:    srv,
		Report:    report,
		Runtime:   opts,
		Names:     names,
		serve:     make(chan error, 1),
	}
	go func() {
		h.serve <- srv.Serve()
	}()

	// clientOpts 复制或扩展当前列表，避免把内部可变容器直接交给调用方修改。
	clientOpts := append(append([]runtime.Option{}, opts...), runtime.WithMetrics(telemetry.MetricsConfig{}))
	// conn, err 接收 wego.NewConn 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	conn, err := wego.NewConn(client.WithRuntime(clientOpts...))
	if err != nil {
		_ = support.StopServer(context.Background(), srv)
		return nil, err
	}

	// minimum 当前步骤的初始计数 1，后续根据实际执行或数据量更新。
	minimum := 1
	// 逐项处理 srv.GetServiceInfo()，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, info := range srv.GetServiceInfo() {
		// 逐项处理 info.Methods，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
		for _, method := range info.Methods {
			if method.IsClientStream || method.IsServerStream {
				minimum = 2
			}
		}
	}
	// err 保存注册和消费就绪检查结果；未就绪时不得开始提交本场景业务。
	if err := support.WaitWorkers(ctx, conn, namespace, minimum, h.serve); err != nil {
		srv.Stop()
		_ = conn.Close()
		return nil, err
	}
	h.Conn = conn
	h.RPC = pb.NewUnaryGreeterClient(h.Conn)
	return h, nil
}

// Close 释放此对象拥有的资源；借用资源必须由所有者关闭。
func (h *Harness) Close() error {
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// 删除工作流会一并移除调度和过滤资源；运行历史按唯一验收命名空间保留在报告中。
	var errs []error
	if h.Server != nil {
		errs = append(errs, support.StopServer(ctx, h.Server))
	}
	if h.NativeWorker != nil {
		errs = append(errs, h.NativeWorker.Shutdown(ctx))
	}
	// names 本步骤需要遍历的名称或路径列表，注册、查询和清理使用同一集合以保持对应。
	names := h.Names
	if names == nil {
		// 逐项处理 []string{ pb.UnaryGreeter_SayHello_FullMethodName, pb.UnaryGreeter_WaitHello_FullMethodName, pb.UnaryGreeter_ChildHello_FullMethodName, }，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
		for _, method := range []string{
			pb.UnaryGreeter_SayHello_FullMethodName,
			pb.UnaryGreeter_WaitHello_FullMethodName,
			pb.UnaryGreeter_ChildHello_FullMethodName,
		} {
			names = append(names, TaskName(method))
		}
	}
	// 逐项处理 names，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, name := range names {
		// _, err 接收 h.Conn.Workflows 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
		_, err := h.Conn.Workflows().Delete(ctx, name)
		errs = append(errs, err)
	}
	errs = append(errs, h.Conn.Shutdown(ctx))
	// result 合并各步骤错误，不能因前一步失败遗漏后续清理错误。
	result := errors.Join(errs...)
	if result == nil {
		h.Report.Add(h, "workflow resources deleted; run history retained", nil)
	}
	return result
}

// expect 检验业务断言，不满足时返回明确失败，避免把只注册成功视为验收通过。
func expect(ok bool, message string) error {
	// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值。
	if !ok {
		return fmt.Errorf("assertion failed: %s", message)
	}

	return nil
}

// pointer 返回值的指针，供必须区分未设置与零值的测试配置使用。
func pointer[T any](v T) *T {
	return &v
}

// Scenario 可独立或由统一运行器调用的真实引擎验收函数。
type Scenario func(context.Context, *Report) error

// Registry 可执行验收场景注册表；manifest 中的场景必须能在此找到真实实现。
var Registry = map[string]Scenario{
	"dual-entry":       DualEntry,
	"simple":           Basic,
	"stubs":            Stubs,
	"sdk-migration":    Basic,
	"sdk-migration-v1": Events,
	"child-workflows":  Children,
	"retries":          Retries,
	"durable-sleep":    DurableSleep,
	"events":           Events,
	"on-event":         Filters,
	"logs":             Logs,
	"panic-handler":    Panic,
	"idempotency":      Idempotency,
	"batch":            Batch,
	"durable-event":    DurableEvents,
	"eviction":         Eviction,
	"concurrency":      Concurrency,
	"slot-cost":        SlotCost,
	"rate-limiting":    RateLimits,
	"runtime-affinity": Affinity,
	"sticky-workers":   Sticky,
	"cron":             Cron,
	"webhooks":         Webhooks,
	"mergent":          Mergent,
	"temporal":         Temporal,
	"observability":    Observability,
	"grpc-streams":     GRPCStreams,
	"streaming":        Streaming,
	"middleware":       Middleware,
	"shutdown":         Shutdown,
}

// Run 提交任务并按此入口的结果类型等待或返回执行结果；输入和执行策略共同决定调度。
func Run(ctx context.Context, name string, report *Report) error {
	// fn, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值。
	fn, ok := Registry[name]
	// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值。
	if !ok {
		return fmt.Errorf("wego: acceptance scenario %s is not implemented", name)
	}

	return fn(ctx, report)
}

// Main 运行单个示例场景并打印结果，失败以非零退出码结束。
func Main(name string) {
	// budget 此 Worker 注销时使用的明确预算，不能用未受限的默认上下文等待。
	budget := 3 * time.Minute
	if name == "temporal" {
		budget = 6 * time.Minute
	}
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	// report 并发安全的验收记录收集器，成功断言与清理结果逐条写入，不保存凭证。
	report := &Report{}
	// err 当前操作产生的错误；nil 表示该步骤成功。
	err := Run(ctx, name, report)
	// data, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
	data, _ := json.MarshalIndent(report.Records, "", "  ")
	fmt.Println(string(data))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
