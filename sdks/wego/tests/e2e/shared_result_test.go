//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// TestIndependentReviewDurableSharedResult 验证公开 RunRef 在真实引擎中的并发读取。
// 子任务模拟一秒业务处理，三个结果观察者在同一未完成 RunID 上等待。
func TestIndependentReviewDurableSharedResult(t *testing.T) {
	preflight(t)
	// ctx 和 cancel 为测试设置有限预算，退出时取消全部受控等待。
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	// service 的普通 handler 延迟完成，durable handler 并发观察同一真实子运行。
	service := &scenarios.Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// 受控业务延迟使多个观察者有机会同时等待尚未完成的真实子任务。
			timer := time.NewTimer(time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
				return scenarios.Reply(ctx, in), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		Wait: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// 借用连接保留父执行的 durable 账本，三个观察者共享同一 RunRef。
			conn, err := task.Client(ctx)
			if err != nil {
				return nil, err
			}
			// ref 与 err 提交唯一子运行，后续三个观察者均使用同一 RunID。
			ref, err := conn.RunNoWait(ctx, pb.UnaryGreeter_SayHello_FullMethodName, in)
			if err != nil {
				return nil, err
			}
			// results 为三个观察者各保留一个结果，任何取消或失败均进入最终断言。
			results := make(chan error, 3)
			for range 3 {
				go func() {
					// budget 与 stop 给每个观察者独立的 4 秒预算，不共享取消函数。
					budget, stop := context.WithTimeout(ctx, 4*time.Second)
					defer stop()
					// result 与 err 等待同一子任务，成功后继续验证 protobuf 解码。
					result, err := ref.Result(budget)
					if err == nil {
						// reply 验证每个观察者均能解码同一 protobuf 子结果。
						var reply pb.Reply
						err = result.Into(&reply)
					}
					results <- err
				}()
			}
			// 将每个观察者的结果作为父任务输出，失败观察者不触发父任务重试。
			out := scenarios.Reply(ctx, in)
			// failures 记录每个失败观察者的原因，父任务输出不能掩盖部分成功。
			failures := []string{}
			for range 3 {
				// err 等待该观察者真实结束，任何取消或失败都进入断言。
				if err := <-results; err != nil {
					failures = append(failures, err.Error())
				} else {
					out.Count++
				}
			}
			out.Message = fmt.Sprintf("child=%s errors=%s", ref.RunID, strings.Join(failures, "; "))
			return out, nil
		},
	}
	// h 与 err 启动唯一 namespace 的真实 Worker，退出时清理已注册资源。
	h, err := scenarios.Start(ctx, "review-shared-result", &scenarios.Report{}, service, nil,
		worker.WithDurableTask(pb.UnaryGreeter_WaitHello_FullMethodName))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// 关闭受控实例并检查资源回收错误，测试不能忽略清理失败。
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	}()
	// out 与 err 保存真实父任务结果，Count 必须等于三个观察者的成功数。
	out, err := h.RPC.WaitHello(ctx, &pb.Request{Message: "shared-result"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("namespace=%s parent=%s worker=%s %s", h.Namespace, out.RunId, out.WorkerId, out.Message)
	if out.Count != 3 {
		t.Fatalf("one completed durable child: want 3 successful observers, got %d; %s", out.Count, out.Message)
	}
}
