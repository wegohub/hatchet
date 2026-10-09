//go:build e2e

package e2e

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
)

// TestReviewStreamingCost 验证单次 bidi 的多条输入只提交一个任务
func TestReviewStreamingCost(t *testing.T) { testStreamingCost(t, 1, 8) }

// TestReviewConcurrentStreamingCost 验证并发调用各自只有一个任务及独立运行身份
func TestReviewConcurrentStreamingCost(t *testing.T) { testStreamingCost(t, 4, 3) }

// testStreamingCost 测量实际调度成本，不用会话或 ACK 任务放大消息数量
func testStreamingCost(t *testing.T, concurrency, messages int) {
	t.Helper()
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	harness, err := scenarios.StartRegistered(ctx, "stream-cost", &scenarios.Report{}, func(registrar grpc.ServiceRegistrar) { pb.RegisterGreeterServer(registrar, &faultService{}) }, streamNames(), nil)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			if err := harness.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	conn, proxy := faultConn(t, harness.Namespace, "none")
	// failures 保存每个并发调用的错误，任何一个失败都不能按均值掩盖
	failures := make(chan error, concurrency)
	// completed 确认所有标准桩已经读取完整结果
	var completed sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		completed.Add(1)
		go func() {
			defer completed.Done()
			chat, err := pb.NewGreeterClient(conn).ChatHellos(ctx)
			if err != nil {
				failures <- err
				return
			}
			for j := 0; j < messages; j++ {
				if err := chat.Send(&pb.Request{Count: int32(j)}); err != nil {
					failures <- err
					return
				}
			}
			if err := chat.CloseSend(); err != nil {
				failures <- err
				return
			}
			for j := 0; j < messages; j++ {
				reply, err := chat.Recv()
				if err != nil {
					failures <- err
					return
				}
				if reply.Count != int32(j) {
					failures <- io.ErrUnexpectedEOF
					return
				}
			}
			if _, err := chat.Recv(); err != io.EOF {
				failures <- err
			}
		}()
	}
	completed.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if actual := proxy.calls.Load(); actual != int32(concurrency) {
		t.Fatalf("tasks=%d, calls=%d, messages=%d", actual, concurrency, concurrency*messages)
	}
	proxy.mu.Lock()
	ids := append([]string(nil), proxy.runIDs...)
	proxy.mu.Unlock()
	if t.Failed() {
		return
	}
	info, err := conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	prefix := "stream-cost"
	if concurrency > 1 {
		prefix = "stream-cost-concurrent"
	}
	writeStreamEvidence(t, prefix, map[string]any{"server_version": info.Version, "namespace": harness.Namespace, "concurrency": concurrency, "messages": concurrency * messages, "submitted_tasks": proxy.calls.Load(), "run_ids": ids, "assertions": []string{"one task per RPC", "ordered protobuf messages", "complete authoritative EOF"}, "cleanup": "workers and connections stopped; definitions deleted; history retained"})
}
