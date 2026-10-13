package client

import (
	"context"
	"encoding/hex"
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
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/codec/payload"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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

// 批量运行
func Test_RunMany(t *testing.T) {

}

// 事件运行
func Test_Event(t *testing.T) {
	scop := "test-feature"
	err := conn.Events().Push(
		context.Background(),
		pb.Greeter_SayHello_FullMethodName,
		model.Input{
			Value: &pb.Request{
				Message: "hello event",
			},
			Routing: map[string]any{
				"message": "hello-filter",
			},
		}, scop)
	if err != nil {
		panic(err)
	}
}
