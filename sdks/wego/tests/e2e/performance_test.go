//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	wegoRuntime "github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// performanceService 发布有序消息及真实执行身份，避免只测空 handler 或注册路径
type performanceService struct {
	// UnimplementedGreeterServer 满足标准生成接口，其他方法不在负载场景调用
	pb.UnimplementedGreeterServer
}

// WatchHellos 每条响应都有独立业务序号，成功 EOF 必须等权威结果与最终输出清单对账
func (*performanceService) WatchHellos(in *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	for i := int32(0); i < in.Count; i++ {
		if err := stream.Send(scenarios.Reply(stream.Context(), &pb.Request{Count: i, Message: in.Message})); err != nil {
			return err
		}
	}
	return nil
}

// performanceSetting 允许独立复现更长负载，非法配置失败而非静默降低规模
func performanceSetting(t *testing.T, name string, fallback, maximum int) int {
	t.Helper()
	if value := os.Getenv(name); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > maximum {
			t.Fatalf("%s must be in [1,%d]", name, maximum)
		}
		return n
	}
	return fallback
}

// TestStreamPerformanceBaseline 用真实持久流测量调度到完整消费的延迟、吞吐与进程资源
// 默认 100 路同时调用，每路 10 条 1 KiB 输出；数值是观测结果，不假设本机达到固定 QPS
func TestStreamPerformanceBaseline(t *testing.T) {
	preflight(t)
	calls := performanceSetting(t, "WEGO_PERF_CALLS", 100, 100000)
	concurrency := performanceSetting(t, "WEGO_PERF_CONCURRENCY", 100, 1000)
	messages := performanceSetting(t, "WEGO_PERF_MESSAGES", 10, 1000)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h, err := scenarios.StartRegistered(ctx, "performance", &scenarios.Report{}, func(r grpc.ServiceRegistrar) { pb.RegisterGreeterServer(r, &performanceService{}) }, streamNames(), []wegoRuntime.Option{wegoRuntime.WithSlots(32)})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			if err := h.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	info, err := h.Conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 采样只读取本进程资源，不能把 Go heap 冒充 Hatchet/PostgreSQL 的整体内存
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	peakHeap, peakGoroutines := before.HeapAlloc, runtime.NumGoroutine()
	stopSampling, sampled := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
				// current 为完整进程快照，峰值只由此采样线程写入
				var current runtime.MemStats
				runtime.ReadMemStats(&current)
				peakHeap = max(peakHeap, current.HeapAlloc)
				peakGoroutines = max(peakGoroutines, runtime.NumGoroutine())
			}
		}
	}()
	// 每个输入槽只由对应调用写入，Wait 后再读取汇总；不共享可变 protobuf 响应
	latencies := make([]float64, calls)
	runIDs := make([]string, calls)
	failures := make([]error, calls)
	jobs := make(chan int)
	// workers 是客户端调用退出屏障，与引擎业务 slots 独立
	var workers sync.WaitGroup
	started := time.Now()
	for i := 0; i < min(calls, concurrency); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				callStarted := time.Now()
				output, err := pb.NewGreeterClient(h.Conn).WatchHellos(ctx, &pb.Request{Count: int32(messages), Message: strings.Repeat("x", 1024)})
				if err == nil {
					for sequence := 0; sequence < messages; sequence++ {
						reply, recvErr := output.Recv()
						if recvErr != nil {
							err = recvErr
							break
						}
						if reply.Count != int32(sequence) || len(reply.Message) != 1024 || reply.RunId == "" {
							err = fmt.Errorf("output order/identity mismatch at %d", sequence)
							break
						}
						if runIDs[index] != "" && runIDs[index] != reply.RunId {
							err = fmt.Errorf("output changed run identity")
							break
						}
						runIDs[index] = reply.RunId
					}
					if err == nil {
						checkpoint, checkpointErr := client.StreamCheckpoint(output.Context())
						if checkpointErr != nil {
							err = checkpointErr
						} else if checkpoint.OutputSeq != uint64(messages) {
							err = fmt.Errorf("delivered checkpoint mismatch")
						}
					}
					if err == nil {
						if _, recvErr := output.Recv(); recvErr != io.EOF {
							err = fmt.Errorf("expected authoritative EOF, got %v", recvErr)
						}
					}
				}
				latencies[index], failures[index] = time.Since(callStarted).Seconds(), err
			}
		}()
	}
	for index := 0; index < calls; index++ {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	elapsed := time.Since(started).Seconds()
	close(stopSampling)
	<-sampled
	runtime.ReadMemStats(&after)
	unique := map[string]bool{}
	for i, err := range failures {
		if err != nil {
			t.Fatalf("call %d failed: %v", i, err)
		}
		if unique[runIDs[i]] {
			t.Fatal("independent calls reused run identity")
		}
		unique[runIDs[i]] = true
	}
	sort.Float64s(latencies)
	percentile := func(p float64) float64 { return latencies[max(0, int(math.Ceil(float64(calls)*p))-1)] }
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	writeStreamEvidence(t, "performance", map[string]any{"server_version": info.Version, "namespace": h.Namespace, "calls": calls, "concurrency": concurrency, "worker_slots": 32, "messages_per_call": messages, "payload_bytes": 1024, "elapsed_seconds": elapsed, "calls_per_second": float64(calls) / elapsed, "messages_per_second": float64(calls*messages) / elapsed, "latency_seconds": map[string]float64{"p50": percentile(.5), "p95": percentile(.95), "p99": percentile(.99)}, "process_resources": map[string]any{"heap_before_bytes": before.HeapAlloc, "heap_after_bytes": after.HeapAlloc, "sampled_peak_heap_bytes": peakHeap, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "gc_cycles": after.NumGC - before.NumGC, "sampled_peak_goroutines": peakGoroutines}, "run_ids": runIDs, "assertions": []string{"all independent runs succeeded", "all outputs ordered and complete", "checkpoint advanced only through delivered output", "authority reconciled before EOF"}, "cleanup": "workers/connections stopped; test definitions deleted; run/topic history retained"})
}
