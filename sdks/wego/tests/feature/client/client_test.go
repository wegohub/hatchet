package client

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware/payload"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

var (
	// conn 手动验收共享的任务连接，同时提供原生 Run 与生成客户端两种调用入口。
	conn *wego.Conn
	// closeCodec 在全部手动测试结束后释放 MinIO HTTP 空闲连接。
	closeCodec func()
	// c 基于任务连接创建的标准桩，调用通过 Hatchet 调度执行。
	c pb.GreeterClient
)

// init 从本机私有配置创建验收连接；运行测试前需另行启动 feature Worker。
func init() {
	loadDotEnv()

	// key 与 Worker 使用同一份私有配置，32 字节密钥不写入日志。
	key, err := hex.DecodeString(os.Getenv("WEGO_CODEC_AES_KEY"))
	if err != nil || len(key) != 32 {
		panic("WEGO_CODEC_AES_KEY requires a 32-byte hex key")
	}
	// encrypted 使用随机 nonce 和 nil AAD，接收端通过 CodecID 查找解密组件。
	encrypted, err := payload.NewAESGCM(key)
	if err != nil {
		panic(err)
	}
	// objects 通过 AWS S3 SDK 连接本机 MinIO；与 Worker 共用桶和稳定前缀。
	objects, err := payload.NewS3(context.Background(), payload.ConfigFromEnv(), "feature/", payload.WithThreshold(1))
	if err != nil {
		panic(err)
	}
	// compressed 在组件构造时配置压缩阈值，注册后发送策略不可变。
	compressed, err := payload.NewZstd(payload.WithThreshold(1))
	if err != nil {
		objects.Close()
		panic(err)
	}
	// gzipCodec 显式配置组件阈值，当前仅注册为历史消息的解码能力。
	gzipCodec, err := payload.NewGzip(payload.WithThreshold(1))
	if err != nil {
		objects.Close()
		panic(err)
	}
	closeCodec = objects.Close
	conn, err = wego.NewConn(client.WithRuntime(
		runtime.WithToken(os.Getenv("HATCHET_CLIENT_TOKEN")),
		runtime.WithAddress(os.Getenv("HATCHET_BROKER_ADDR")),
		runtime.WithServerURL(os.Getenv("HATCHET_SERVER_ADDR")),
		runtime.WithTLSConfig(nil),
		// 发送按 zstd→加密→S3 执行，接收依据消息 ID 逆序还原。
		runtime.WithMiddleware(
			middleware.WithPayload(gzipCodec),
			middleware.WithPayload(compressed),
			middleware.WithPayload(encrypted),
			middleware.WithPayload(objects),
		),
		// 显式选择 zstd→AES-GCM→S3；gzip 仅作为已注册的解码能力。
		runtime.WithEncodingCodecs(
			payload.ZstdCodecID,
			payload.AESGCMCodecID,
			payload.S3ObjectCodecID,
		),
	))
	if err != nil {
		closeCodec()
		panic(err)
	}
	c = pb.NewGreeterClient(conn)
}

// TestMain 等全部调用结束再释放连接与 codec，保留已上传的持久对象。
func TestMain(m *testing.M) {
	// code 保留测试退出状态，关闭错误也必须让本次运行失败。
	code := m.Run()
	if err := conn.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	closeCodec()
	os.Exit(code)
}

// loadDotEnv 读取手动验收的私有配置；已设置的变量优先，CI 可以独立注入凭证
func loadDotEnv() {
	// 测试目录和编译源文件目录都可定位配置，独立运行测试二进制时不依赖工作目录
	paths := []string{filepath.Join("..", ".env")}
	if _, file, _, ok := goruntime.Caller(0); ok {
		paths = append(paths, filepath.Join(filepath.Dir(file), "..", ".env"))
	}

	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}

			key, value, ok := strings.Cut(strings.TrimPrefix(line, "export "), "=")
			if !ok {
				continue
			}

			key = strings.TrimSpace(key)
			// 只去除配置文件的外层引号，凭证值不写入日志
			value = strings.Trim(strings.TrimSpace(value), `"'`)

			if _, exists := os.LookupEnv(key); !exists {
				if err := os.Setenv(key, value); err != nil {
					panic(err)
				}
			}
		}

		return
	}
}

// Test_UnaryRPC 需要显式启动 feature Worker；任务等待有预算，测试拥有并关闭连接
func Test_UnaryRPC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rsp, err := c.SayHello(ctx, &pb.Request{
		Message: "say hello",
	}, client.WithRunOptions(
		// 供CEL使用
		client.WithRouting(map[string]any{
			"worker_name": "feature-0",
		}),
		// worker亲和性配置
		client.WithDesiredWorkerLabels(map[string]*model.DesiredWorkerLabel{
			"worker_name": {
				Value:      "feature-2",
				Weight:     1,
				Comparator: model.LabelEqual,
				Required:   true, // 硬亲和， false为软亲和
			},
		}),
		//
		client.WithRunMetadata(map[string]string{
			"key": "value",
		}),
	))
	if err != nil {
		t.Fatalf("invoke error: %s", err.Error())
	}

	marshal, _ := protojson.Marshal(rsp)
	t.Log(string(marshal))
}

// TestUnaryRPCRun 通过原生 Run 提交 protobuf RPC，并读取 RunID 与解码后的业务响应。
func Test_UnaryRPCRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rsp, err := conn.Run(ctx, pb.Greeter_SayHello_FullMethodName, &pb.Request{
		Message: "say hello run",
	})
	if err != nil {
		t.Fatalf("run error: %s", err.Error())
	}

	t.Log(fmt.Sprintf("run_id: %s", rsp.RunID))

	result := &pb.Reply{}
	err = rsp.Into(result)
	if err != nil {
		t.Fatalf("into error: %s", err.Error())
	}

	marshal, _ := protojson.Marshal(result)
	t.Log(string(marshal))
}

// Test_UnaryRunProxy 演示原生 StandaloneTask 的定义入口，不提交业务执行。
func Test_UnaryRunProxy(t *testing.T) {
	conn.NewStandaloneTask("test", func() {})
}

// 创建计划运行，未到时间出现在 Scheduled Runs 中
func Test_Scheduled(t *testing.T) {
	res, err := conn.Schedules().Create(
		context.Background(),
		pb.Greeter_SayHello_FullMethodName,
		model.CreateScheduledRunTrigger{
			TriggerAt: time.Now().Add(time.Second * 30),
			Priority:  2, // 直接设置值；0 表示未指定，1–3 分别为低、中、高优先级。
			Input: &pb.Request{
				Message: "hello scheduled",
			},
		},
	)
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Logf("scheduled id: %v", res.ID())
}

// Test_Cron 演示带显式优先级的周期触发器创建，需要本机 Worker 已注册对应 RPC。
func Test_Cron(t *testing.T) {
	res, err := conn.Crons().Create(
		context.Background(),
		pb.Greeter_SayHello_FullMethodName,
		model.CreateCronTrigger{
			Name:       "say hello",
			Expression: "* * * * *",
			Priority:   3, // 不需要取地址；省略或填 0 时使用服务端默认优先级。
			Input: &pb.Request{
				Message: "hello crontab",
			},
			// 会出现在任务的metadata中
			AdditionalMetadata: map[string]interface{}{
				"description": "Daily cleanup and maintenance tasks",
			},
		},
	)
	if err != nil {
		panic(err)
	}
	t.Logf("cron id: %s", res.ID())
}

// Test_RunMany 按 Test_Concurrency 的规则提交十项、三个分组，验证部分成功及七次幂等冲突。
// 需要启动当前 feature Worker：以 routing.group 做 STATUS 幂等键，每组 MaxRuns=1。
func Test_RunMany(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// 每次使用独立分组前缀，避免另一轮事件或批量调用占用同一组的幂等键。
	prefix := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	inputs := make([]client.RunManyInput, 10)
	for i := range inputs {
		inputs[i] = client.RunManyInput{
			Input: model.Input{
				Value: &pb.Request{Message: "test concurrency"},
				Routing: map[string]any{
					"group":   fmt.Sprintf("%s-group-%d", prefix, i%3),
					"message": "hello-filter",
				},
			},
		}
	}

	// RunMany 直接触发 RPC workflow，不传事件 Filter Scope；仍保留相同的 routing 数据。
	refs, err := conn.RunMany(ctx, pb.Greeter_SayHello_FullMethodName, inputs)
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		// 失败时只取消本次已确认的运行，清理预算独立于已经超时的等待 context。
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for i := range refs {
			if cancelErr := refs[i].Cancel(cleanupCtx); cancelErr != nil {
				t.Errorf("cancel run %s: %v", refs[i].RunID, cancelErr)
			}
		}
	})

	// 与事件发布不同，批量直接提交显式返回冲突，同时保留成功创建的三个 RunRef。
	var collision *model.BulkIdempotencyCollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("expected bulk idempotency collision, got %d runs and error %v", len(refs), err)
	}
	if len(refs) != 3 || len(collision.SuccessfulRunIDs) != 3 || len(collision.Collisions) != 7 {
		t.Fatalf("expected 3 runs and 7 collisions, got %d refs, %d successful IDs and %d collisions",
			len(refs), len(collision.SuccessfulRunIDs), len(collision.Collisions))
	}
	accepted := make(map[string]bool, len(refs))
	for _, ref := range refs {
		if ref.RunID == "" || accepted[ref.RunID] || ref.InputIndex != -1 {
			t.Fatalf("invalid partial-success identity: run_id=%q input_index=%d", ref.RunID, ref.InputIndex)
		}
		// 官方批量冲突响应不提供输入位置，不能把返回顺序当作原始下标。
		accepted[ref.RunID] = true
		t.Logf("accepted run_id=%s input_index=%d", ref.RunID, ref.InputIndex)
	}
	// 错误详情和成功句柄必须是同一个不重复的运行集合。
	confirmed := make(map[string]bool, len(refs))
	for _, runID := range collision.SuccessfulRunIDs {
		if !accepted[runID] || confirmed[runID] {
			t.Fatalf("successful run %q is missing or duplicated", runID)
		}
		confirmed[runID] = true
	}
	for _, conflict := range collision.Collisions {
		if conflict == nil || !accepted[conflict.ExistingRunID] {
			t.Fatalf("collision does not reference an accepted run: %+v", conflict)
		}
	}

	// 提交成功不代表业务完成；逐个读取并解码结果，核对业务响应和执行身份。
	for i := range refs {
		result, err := refs[i].Result(ctx)
		if err != nil {
			t.Fatalf("run %s result: %v", refs[i].RunID, err)
		}
		reply := &pb.Reply{}
		if err := result.IntoContext(ctx, reply); err != nil {
			t.Fatalf("run %s decode: %v", refs[i].RunID, err)
		}
		if reply.RunId != refs[i].RunID || reply.WorkerId == "" || !strings.HasPrefix(reply.Message, "Processed: test concurrency| ") {
			t.Fatalf("unexpected response for run %s: %v", refs[i].RunID, reply)
		}
		t.Logf("completed run_id=%s worker_id=%s retry_count=%d message=%q",
			reply.RunId, reply.WorkerId, reply.RetryCount, reply.Message)
	}
	t.Logf("submitted=%d accepted=%d idempotency_collisions=%d", len(inputs), len(refs), len(collision.Collisions))
}

// 事件运行
func Test_Event(t *testing.T) {
	scop := "test-feature"
	err := conn.Events().Push(
		context.Background(),
		pb.Greeter_SayHello_FullMethodName,
		model.Input{
			Value: &pb.Request{
				//Message: "hello event",
				Message: "test error",
			},
			Routing: map[string]any{
				"message": "hello-filter",
			},
		}, scop)
	if err != nil {
		panic(err)
	}
}

// 并发策略测试
func Test_Concurrency(t *testing.T) {
	for i := range 10 {
		err := conn.Events().Push(
			context.Background(),
			pb.Greeter_SayHello_FullMethodName,
			model.Input{
				Value: &pb.Request{
					Message: "test concurrency",
				},
				Routing: map[string]any{
					"group":   fmt.Sprintf("group-%d", i%3),
					"message": "hello-filter",
				},
			},
			"test-feature",
		)
		if err != nil {
			panic(err)
		}
	}
}

// Test_ChildSpawning 调用 durable 父任务，验证随机数量、完整子结果及真实父子运行关系。
// 当前 Worker 为每个子任务生成独立调度分组，3–10 个输入应全部执行而非互相去重。
func Test_ChildSpawning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	request := &pb.Request{Message: "test child spawning"}
	reply, err := c.WaitHello(ctx, request)
	if err != nil {
		t.Fatalf("spawn children: %v", err)
	}
	if !reply.Ok || reply.RunId == "" || reply.WorkerId == "" || reply.Message != "Collected: "+request.Message {
		t.Fatalf("unexpected parent response: %v", reply)
	}
	if reply.Count < 3 || reply.Count > 10 || len(reply.ChildRunIds) != int(reply.Count) || len(reply.ChildResults) != int(reply.Count) {
		t.Fatalf("expected 3–10 complete child results, got count=%d IDs=%d results=%d",
			reply.Count, len(reply.ChildRunIds), len(reply.ChildResults))
	}
	if _, err := time.Parse(time.RFC3339Nano, reply.ProcessedAt); err != nil {
		t.Fatalf("durable seed timestamp: %v", err)
	}

	// 每项结果与对应 RunID 一一匹配；子 RunID 必须唯一且不能等于父 RunID。
	seen := make(map[string]bool, reply.Count)
	for i, child := range reply.ChildResults {
		if child == nil {
			t.Fatalf("child %d result is missing", i)
		}
		if child.RunId == "" || child.RunId != reply.ChildRunIds[i] || child.RunId == reply.RunId || seen[child.RunId] {
			t.Fatalf("child %d has an invalid or duplicate run identity: %v", i, child)
		}
		seen[child.RunId] = true
		if child.ParentRunId != reply.RunId || child.WorkerId == "" {
			t.Fatalf("child %d parent/worker identity mismatch: %v", i, child)
		}
		expected := fmt.Sprintf("Processed: %s child-%d| ", request.Message, i+1)
		if !strings.HasPrefix(child.Message, expected) {
			t.Fatalf("child %d output mismatch: got %q, expected prefix %q", i, child.Message, expected)
		}
		t.Logf("child index=%d run_id=%s parent_run_id=%s worker_id=%s message=%q",
			i, child.RunId, child.ParentRunId, child.WorkerId, child.Message)
	}
	t.Logf("parent run_id=%s child_count=%d invocation_count=%d",
		reply.RunId, reply.Count, reply.InvocationCount)
}
