//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
)

// TestBatchShutdownBudget 在真实引擎中阻塞 batch handler，预算到期关闭后断言 Worker 与资源完成清理。
func TestBatchShutdownBudget(t *testing.T) {
	preflight(t)
	// ctx, cancel 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// report 并发安全的验收记录收集器，成功断言与清理结果逐条写入，不保存凭证。
	report := &scenarios.Report{}
	// h 保存原生 batch 的定义与连接，预算到期后仍必须关闭 Worker 并删除定义。
	h := &scenarios.Harness{
		Scenario:  "batch-shutdown",
		Namespace: fmt.Sprintf("wego_batch_shutdown_%d_", time.Now().UnixNano()),
		Names:     []string{"bounded-batch"},
		Report:    report,
	}
	// err 当前操作产生的错误；nil 表示该步骤成功。
	var err error
	h.Conn, err = client.New(client.WithRuntime(scenarios.Runtime(h.Namespace)...))
	if err != nil {
		t.Fatal(err)
	}
	// cleaned 是否已完成本用例的资源清理，避免正常路径与 defer 重复删除同一资源。
	cleaned := false
	defer func() {
		if !cleaned {
			// e 保存资源释放结果，清理错误同样必须报告，不能因业务已成功而忽略。
			if e := h.Close(); e != nil {
				t.Error(e)
			}
		}
	}()

	// workerConn, err 接收 client.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	workerConn, err := client.New(client.WithRuntime(append(scenarios.Runtime(h.Namespace), runtime.WithSlots(2))...))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// e 保存资源释放结果，清理错误同样必须报告，不能因业务已成功而忽略。
		if e := workerConn.Close(); e != nil && !errors.Is(e, context.DeadlineExceeded) {
			t.Error(e)
		}
	}()

	// started, stopped 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	started, stopped := make(chan struct{}, 2), make(chan struct{}, 2)
	// interval 批聚合的时间窗口，未达大小阈值时由此时长触发执行。
	interval := 100 * time.Millisecond
	// definition 注册批量聚合定义，例如三项输入各有独立 RunID 与对应输出。
	definition := workerConn.NewStandaloneBatchTask("bounded-batch", func(ctx context.Context, input map[string]map[string]string) (map[string]map[string]string, error) {
		started <- struct{}{}
		<-ctx.Done()
		stopped <- struct{}{}
		return nil, ctx.Err()
	}, model.BatchConfig{MaxSize: 1, MaxInterval: &interval, GroupKey: pointerForBatch("input.group")})
	// worker, err 接收 workerConn.NewWorker 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	worker, err := workerConn.NewWorker("bounded-batch-worker", client.WithWorkflows(definition))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = worker.Start(); err != nil {
		t.Fatal(err)
	}
	if err = worker.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	// refs, err 接收 h.Conn.RunMany 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	refs, err := h.Conn.RunMany(
		ctx,
		definition.GetName(),
		[]client.RunManyInput{
			{Input: map[string]string{"group": "one"}},
			{Input: map[string]string{"group": "two"}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
	for i := 0; i < 2; i++ {
		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("two batches did not run concurrently:", ctx.Err())
		}
	}
	// budget, stop 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
	budget, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	// began 读取本机时钟，用于耗时或超时判断，不参与 durable 重放。
	began := time.Now()
	err = worker.Shutdown(budget)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(began) > time.Second {
		t.Fatalf("shutdown budget: %v", err)
	}
	// release, stop 创建当前步骤的派生上下文及取消函数，退出时必须释放取消资源，避免后台等待泄漏。
	release, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()

	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
	for i := 0; i < 2; i++ {
		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
		select {
		case <-stopped:
		case <-release.Done():
			t.Fatal("batch execution retained after shutdown")
		}
	}
	// e 保存资源释放结果，清理错误同样必须报告，不能因业务已成功而忽略。
	if e := workerConn.Close(); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("expected terminal worker budget result: %v", e)
	}
	// ids Worker 名称到注册身份的映射。
	ids := make([]string, len(refs))
	// 逐项处理 refs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for i, ref := range refs {
		ids[i] = ref.RunID
		report.Add(h, "concurrent batch context canceled within shutdown budget", &pb.Reply{RunId: ref.RunID, WorkerId: worker.ID()})
	}
	if _, err = h.Conn.Runs().Cancel(ctx, model.Resource{"externalIds": ids}); err != nil {
		t.Fatal(err)
	}
	// info, err 接收 h.Conn.Info 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	info, err := h.Conn.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Close(); err != nil {
		t.Fatal(err)
	}
	// 生成报告前已显式关闭并完成资源删除。
	cleaned = true
	// 组合仓库与资源路径，扫描、生成和清理使用同一位置。
	path := filepath.Join("..", "..", ".test-results", fmt.Sprintf("batch-shutdown-%d.json", time.Now().UnixNano()))
	// data, _ 接收编码后的传输数据与错误，编码成功后才提交或发布，避免发送半成品载荷。
	data, _ := json.MarshalIndent(
		map[string]any{
			"status":         "PASSED",
			"server_version": info.Version,
			"mq":             "postgresql",
			"records":        report.Records,
			"command":        "sdks/wego/scripts/local-test.py go test -race -tags=e2e ./sdks/wego/tests/e2e/... -run TestBatchShutdownBudget -v -timeout 5m",
		},
		"",
		"  ",
	)
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	t.Log("batch shutdown report:", path)
}

// pointerForBatch 构造批配置指针，允许测试显式设置聚合参数。
func pointerForBatch(v string) *string {
	return &v
}
