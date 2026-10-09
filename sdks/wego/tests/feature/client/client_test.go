package client

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	conn *wego.Conn
	c    pb.GreeterClient
)

func init() {
	loadDotEnv()

	var err error
	conn, err = wego.NewConn(client.WithRuntime(
		runtime.WithToken(os.Getenv("HATCHET_CLIENT_TOKEN")),
		runtime.WithAddress(os.Getenv("HATCHET_BROKER_ADDR")),
		runtime.WithServerURL(os.Getenv("HATCHET_SERVER_ADDR")),
		runtime.WithTLSConfig(nil),
	))
	if err != nil {
		panic(err)
	}
	c = pb.NewGreeterClient(conn)
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

	ctx = client.WithRouting(ctx, map[string]any{
		"worker_name": "feature-0",
	})

	rsp, err := c.SayHello(ctx, &pb.Request{
		Message: "say hello",
	})
	if err != nil {
		t.Fatalf("invoke error: %s", err.Error())
	}

	marshal, _ := protojson.Marshal(rsp)
	t.Log(string(marshal))
}

func TestUnaryRPCRun(t *testing.T) {
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
