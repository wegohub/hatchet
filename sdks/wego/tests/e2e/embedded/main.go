//go:build wego_embedded

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/support"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
)

// freePort 临时监听回环地址获取空闲端口，立即释放后交给嵌入引擎启动
func freePort(t *checks) int {
	t.Helper()
	// l, err 接收 net.Listen 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// port 保存实际绑定地址，测试使用空闲端口避免固定端口冲突
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// TestEmbeddedChild 在独立子进程的测试库中启动、迁移、调用并关闭嵌入引擎
func TestEmbeddedChild(t *checks) {
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// grpcPort, apiPort 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值
	grpcPort, apiPort := freePort(t), freePort(t)
	// ns 生成当前数据的文本表示，用于名称、业务比较或报告，不包含凭证
	ns := fmt.Sprintf("wego_embedded_rpc_%d_", time.Now().UnixNano())
	// config 使用测试独占数据库和空闲端口，迁移、调用、关闭与删库均有独立断言
	config := runtime.EmbeddedConfig{
		DatabaseURL: os.Getenv("WEGO_EMBEDDED_DATABASE_URL"),
		GRPCPort:    grpcPort,
		APIPort:     apiPort,
		LogLevel:    "warn",
	}
	// 先同步启动并迁移独立引擎，环境初始化完成后才创建其他连接和 Worker
	conn, err := client.New(client.WithRuntime(runtime.WithEmbedded(config), runtime.WithNamespace(ns)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	info, err := conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 测试部署显式初始化权限；SDK 不读写 entitlement 数据表
	database, err := pgx.Connect(ctx, config.DatabaseURL)
	if err != nil {
		t.Fatal("embedded entitlement fixture connection failed")
	}
	_, err = database.Exec(ctx, `INSERT INTO tenant_entitlement (tenant_id, audit_logs, prometheus_metrics, strict_additional_metadata_filters, dag_operator, durable_streams)
        VALUES ($1::uuid, false, false, false, false, true)
        ON CONFLICT (tenant_id) DO UPDATE SET durable_streams = true, updated_at = NOW()`, info.TenantID)
	database.Close(ctx)
	if err != nil {
		t.Fatal("embedded entitlement fixture initialization failed")
	}
	// Server 只拥有自己的 Worker 连接，嵌入引擎由 conn 最后关闭
	srv := server.New(server.WithRuntime(
		runtime.WithToken(os.Getenv("HATCHET_CLIENT_TOKEN")),
		runtime.WithAddress(fmt.Sprintf("127.0.0.1:%d", grpcPort)),
		runtime.WithServerURL(fmt.Sprintf("http://localhost:%d", apiPort)),
		runtime.WithTLSConfig(nil), runtime.WithNamespace(ns),
	))
	defer srv.Stop()
	pb.RegisterUnaryGreeterServer(srv, &scenarios.Service{})
	done := make(chan error, 1)
	go func() { done <- srv.Serve() }()
	// err 保存注册和消费就绪检查结果；未就绪时不得开始提交本场景业务
	if err := support.WaitWorkers(ctx, conn, ns, 1, done); err != nil {
		t.Fatal(err)
	}

	// out, err 接收 pb.NewUnaryGreeterClient 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	out, err := pb.NewUnaryGreeterClient(conn).SayHello(ctx, &pb.Request{Message: "embedded protobuf", Count: 42})
	if err != nil {
		t.Fatal(err)
	}
	if out.Message != "embedded protobuf" || out.Count != 42 || out.RunId == "" || out.WorkerId == "" {
		t.Fatalf("embedded result: %v", out)
	}
	// 逐项处理 pb.UnaryGreeter_ServiceDesc.Methods，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, d := range pb.UnaryGreeter_ServiceDesc.Methods {
		if _, err = conn.Workflows().Delete(ctx, "/wego.example.v1.UnaryGreeter/"+d.MethodName); err != nil {
			t.Fatal(err)
		}
	}
	// closeCtx, stop 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏
	closeCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()

	if err = support.StopServer(closeCtx, srv); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = conn.Shutdown(closeCtx); err != nil {
		t.Fatal(err)
	}
	// 逐项处理 []int{grpcPort, apiPort}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, port := range []int{grpcPort, apiPort} {
		// connection, e 在有限预算内确认服务端口可连接，不以健康端口代替业务断言
		connection, e := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if e == nil {
			connection.Close()
			t.Fatal("embedded listener remained open")
		}
	}
	// report 记录独立嵌入验收的版本、运行身份、PostgreSQL MQ 和断言；不包含数据库凭证或访问 token
	report := map[string]any{
		"scenario":       "embedded",
		"status":         "PASSED",
		"server_version": info.Version,
		"mq":             "postgresql",
		"run_id":         out.RunId,
		"worker_id":      out.WorkerId,
		"assertions": []string{
			"independent process and PostgreSQL database",
			"migrations and registration",
			"standard protobuf RPC result",
			"engine and API listeners closed",
		},
		"cleanup": "workflow definitions deleted; parent drops independent database",
	}
	// path 读取本机显式配置；未提供时由紧接着的默认分支解析
	path := os.Getenv("WEGO_REPORT_PATH")
	if path == "" {
		path = filepath.Join(os.Getenv("WEGO_REPORT_DIRECTORY"), fmt.Sprintf("embedded-%d.json", time.Now().UnixNano()))
	}
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	// data, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷
	data, _ := json.MarshalIndent(report, "", "  ")
	if err = os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}

// checks 嵌入独立进程使用的断言适配器，将测试失败转换为明确退出
type checks struct{}

// Helper 实现测试断言适配接口，不改变业务执行
func (*checks) Helper() {}

// Fatal 报告不可继续的测试失败并结束当前验收进程
func (*checks) Fatal(args ...any) {
	panic(fmt.Sprint(args...))
}

// Fatalf 格式化不可继续的测试失败并结束当前验收进程
func (*checks) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// Error 返回供日志或调用方读取的错误文本，不额外暴露后端错误对象
func (*checks) Error(args ...any) {
	panic(fmt.Sprint(args...))
}

// main 组织此示例或测试进程的初始化、执行和清理；错误以日志或非零退出码报告，不把启动成功代替业务成功
func main() {
	defer func() {
		// err 接收 recover 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
		if err := recover(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}()

	TestEmbeddedChild(&checks{})
}
