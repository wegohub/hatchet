//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	"google.golang.org/grpc"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/engine"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/session"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// streamCostBackend 仅计数客户端实际提交，不改写真实 Worker 传输。
// 输出发布和辅助 HTTP 查询不计入此值，因此它只表示任务提交放大率。
type streamCostBackend struct {
	// Backend 借用真实后端；关闭权属于 Engine。
	ports.Backend
	// mu 保护后台取消控制与前台业务发送共同写入的计数。
	mu sync.Mutex
	// counts 按 START/RUN/DATA/ACK 等协议类型累计真实提交次数。
	counts map[string]int
}

// Run 计数已实际提交的任务，失败提交不能伪装为已执行任务。
func (b *streamCostBackend) Run(ctx context.Context, name string, input any, opts model.RunOptions) (ports.Run, error) {
	// ref 与 err 来自真实后端，不使用合成成功身份。
	ref, err := b.Backend.Run(ctx, name, input, opts)
	if err == nil {
		// control 区分会话协议任务与普通任务，协议类型由自有 DTO 提供。
		if control, ok := input.(session.Control); ok {
			b.mu.Lock()
			b.counts[control.Kind]++
			b.mu.Unlock()
		}
	}
	return ref, err
}

// TestReviewStreamingCost 测量本机单会话串行 bidi 的延迟与任务提交放大率。
// 该受控样本没有业务等待；只提供本机基线，不据此宣称高频流性能最优。
func TestReviewStreamingCost(t *testing.T) {
	testStreamingCost(t, 1, 8)
}

// TestReviewConcurrentStreamingCost 验证四个真实会话并发收发并记录调度成本，不设未经测量的性能门槛。
func TestReviewConcurrentStreamingCost(t *testing.T) {
	testStreamingCost(t, 4, 3)
}

// testStreamingCost 共用同一真实 echo 服务，计数 DATA/ACK 及会话初始化任务。
func testStreamingCost(t *testing.T, concurrency, messages int) {
	t.Helper()
	preflight(t)
	// ctx 限制真实注册、握手、八次收发及资源清理的总预算。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	// names 包含业务会话和独立控制入口，就绪须确认全部定义。
	names := []string{session.ControlName}
	// description 使用同一生成服务描述，避免测试任务名与标准客户端不一致。
	for _, description := range pb.Greeter_ServiceDesc.Streams {
		names = append(names, scenarios.TaskName("/wego.example.v1.Greeter/"+description.StreamName)+"-session", session.StartName("/wego.example.v1.Greeter/"+description.StreamName))
	}
	// harness 注册真实 echo handler，清理只作用于本次 namespace。
	harness, err := scenarios.StartRegistered(ctx, "stream-cost", &scenarios.Report{}, func(registrar grpc.ServiceRegistrar) {
		pb.RegisterGreeterServer(registrar, &faultService{})
	}, names, nil)
	if err != nil {
		t.Fatal(err)
	}
	// cleaned 只在本测试内写入，成功清理后 defer 不再重复删除已关闭连接的资源。
	cleaned := false
	defer func() {
		if cleaned {
			return
		}
		// closeErr 保留真实资源清理失败，不能因延迟测量成功而忽略它。
		if closeErr := harness.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	// config 与 Worker 共用载荷和连接设置，客户端另外拥有本次测试连接。
	config := spec.Defaults()
	// option 逐项恢复共享配置，不能只取默认地址或缺省 namespace。
	for _, option := range harness.Runtime {
		option(&config)
	}
	// instance 使用内部工厂，只为计数层装配真实 backend，不暴露到公开 SDK。
	// 本性能样本使用显式 20 秒握手预算，SDK 默认值仍为 10 秒。
	config.Stream.HandshakeTimeout = 20 * time.Second
	// instance 取得与 Worker 相同配置的真实后端，测量仍经由官方引擎。
	instance, err := engine.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	// backend 为每种协议任务保存计数，不改变执行、取消或订阅行为。
	backend := &streamCostBackend{Backend: instance.Backend, counts: map[string]int{}}
	// client 使用标准业务桩，计数仅插在内部 Worker 传输边界。
	client := pb.NewGreeterClient(&faultConn{backend: backend, config: config, engine: instance})
	// started 计量所有会话包括初始化的总墙钟时间，不作为纯消息吞吐基准。
	started := time.Now()
	// measurements 每个会话只写一次不可变结果，合并不与 goroutine 并发修改切片。
	measurements := make(chan bidiMeasurement, concurrency)
	// index 为每个流分配独立的序号范围，检测会话间消息串线。
	for index := 0; index < concurrency; index++ {
		go func() { measurements <- measureBidi(ctx, client, index, messages) }()
	}
	// latencies 保存实际串行往返样本，单位毫秒，允许不同流同时推进。
	latencies := make([]float64, 0, concurrency*messages)
	// handshakes 保存每个流独立握手时间，不将总墙钟时间作为单流握手时间。
	handshakes := make([]float64, 0, concurrency)
	// runIDs 用于确认每个会话拥有独立任务身份。
	runIDs := make(map[string]bool)
	// failures 累计全部会话失败并先回收资源，不能在收集完 goroutine 前 Fatal。
	var failures []error
	// index 将所有结果收齐，任何会话都不能被当成缺失或 Skip。
	for index := 0; index < concurrency; index++ {
		// measurement 来自一次完整握手、多消息与半关闭周期。
		measurement := <-measurements
		if measurement.err != nil {
			failures = append(failures, measurement.err)
		}
		latencies = append(latencies, measurement.latencies...)
		handshakes = append(handshakes, measurement.handshake)
		runIDs[measurement.runID] = true
	}
	// elapsed 包含并发会话初始化和全部收发，用于描述本样本的总完成速度。
	elapsed := time.Since(started)
	if len(failures) > 0 || len(runIDs) != concurrency {
		t.Fatalf("concurrent sessions: failures=%v identities=%d", failures, len(runIDs))
	}
	if err = instance.Close(); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	// counts 是传输退出后的独立快照，后台取消控制不能再改写报告。
	counts := maps.Clone(backend.counts)
	backend.mu.Unlock()
	if counts["DATA"] != concurrency*messages || counts["ACK"] != concurrency*messages || counts["RUN"] != concurrency || counts["START"] != concurrency || counts["OPEN"] != concurrency || counts["END"] != concurrency {
		t.Fatalf("protocol amplification: %v", counts)
	}
	// info 记录实际引擎版本，不将客户端发行依赖当作服务端版本。
	info, err := harness.Conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// evidence 保存命令、运行规模、延迟、任务提交数和清理结果，不含凭证。
	evidence := map[string]any{"status": "PASSED", "sdk_version": wego.Version, "server_version": info.Version,
		"mq": "postgresql", "namespace": harness.Namespace, "messages": concurrency * messages, "messages_per_session": messages, "payload_bytes": 256, "concurrency": concurrency,
		"handshake_ms": handshakes[0], "handshakes_ms": handshakes, "elapsed_seconds": elapsed.Seconds(), "messages_per_second_including_handshake": float64(concurrency*messages) / elapsed.Seconds(), "handshake_budget_ms": config.Stream.HandshakeTimeout.Milliseconds(), "round_trip_ms": latencies, "submitted_tasks": counts,
		"command": os.Getenv("WEGO_TEST_COMMAND"), "scope": "local concurrent bidi sessions with sequential messages within each; elapsed includes handshake; task submissions exclude output publication and HTTP polling"}
	if err = harness.Close(); err != nil {
		t.Fatal(err)
	}
	cleaned = true
	evidence["cleanup"] = "workflow definitions deleted; owned transports closed; run and worker history retained"
	// data 将实际断言证据持久化，报告失败也必须使测试失败。
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	// destination 位于忽略的本机证据目录，汇总器只读取本轮版本的成功报告。
	prefix := "stream-cost-"
	if concurrency > 1 {
		prefix = "stream-cost-concurrent-"
	}
	// destination 区分并发和串行样本，不让并发结果覆盖串行基线。
	destination := filepath.Join("..", "..", ".test-results", prefix+harness.Namespace+".json")
	if err = os.WriteFile(destination, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("concurrency=%d handshakes_ms=%v round_trips_ms=%v submitted_tasks=%v", concurrency, handshakes, latencies, counts)
}

// bidiMeasurement 是一次完整会话的实测结果，发布后不可变。
type bidiMeasurement struct {
	// handshake 为单流初始化时间，单位毫秒。
	handshake float64
	// latencies 保存每条消息的实际往返时间，包含发送和接收确认。
	latencies []float64
	// runID 为该会话的真实任务身份，检查不同会话没有串线。
	runID string
	// err 保存首个失败，失败测量不能成为成功证据。
	err error
}

// measureBidi 在一个流内串行收发，多个调用之间可并发；不从后台 goroutine 调用 t.Fatal。
func measureBidi(ctx context.Context, client pb.GreeterClient, index, messages int) (out bidiMeasurement) {
	// ctx 独立拥有当前流的预算，内容断言失败也会取消会话并回收远端容量。
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// started 计量独立握手，客户端任务仍走真实 Hatchet 引擎。
	started := time.Now()
	// stream 初始化失败时保留原错误，不生成虚构身份。
	stream, err := client.ChatHellos(ctx)
	out.handshake = float64(time.Since(started)) / float64(time.Millisecond)
	if err != nil {
		out.err = err
		return out
	}
	// payload 固定 256 字节，每个流的 Count 范围互不重叠。
	payload := strings.Repeat("x", 256)
	// message 对照每个实际请求，不能只数收到多少响应。
	for message := 0; message < messages; message++ {
		// count 与 started 分别标识内容和该消息往返起点。
		count, started := int32(index*100+message), time.Now()
		// err 是实际发送失败，记录该流失败并由派生预算取消会话。
		if err := stream.Send(&pb.Request{Message: payload, Count: count}); err != nil {
			out.err = err
			return out
		}
		// reply 必须包含完整业务值和稳定会话身份。
		reply, err := stream.Recv()
		if err != nil || reply == nil || reply.Count != count || reply.Message != payload || reply.RunId == "" || (out.runID != "" && reply.RunId != out.runID) {
			out.err = fmt.Errorf("session %d message %d: reply=%v error=%v", index, message, reply, err)
			return out
		}
		out.runID = reply.RunId
		out.latencies = append(out.latencies, float64(time.Since(started))/float64(time.Millisecond))
	}
	// err 检查半关闭成功后再读取最终状态，不能以已发送消息数推断成功。
	if err := stream.CloseSend(); err != nil {
		out.err = err
		return out
	}
	// err 只有收齐业务输出和成功终态才是 EOF，其他状态不得记录为成功。
	_, err = stream.Recv()
	if err != io.EOF {
		out.err = fmt.Errorf("session %d terminal: %v", index, err)
	}
	return out
}
