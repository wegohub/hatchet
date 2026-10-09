//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// durableRemoteOutput 保留多个业务字段，防止将字段映射误当成任务结果映射
type durableRemoteOutput struct {
	// Count 是子任务的实际计算结果，例如输入 3 返回 6
	Count int
	// ParentRunID 由子执行读取，用于验证真实父子身份
	ParentRunID string
	// ChildRunID 来自子执行，不能由父 handler 伪造
	ChildRunID string
}

// TestReviewDurableRemoteChildOutput 验证只有远端 Worker 注册的子任务仍可恢复输出形状
func TestReviewDurableRemoteChildOutput(t *testing.T) {
	preflight(t)
	// ctx 限制本轮注册、业务执行与清理，不允许缺少结果时永久等待
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	// namespace 隔离两个独立后端，它们共享调度空间但不共享本地定义缓存
	namespace := fmt.Sprintf("wego_remote_child_%d_", time.Now().UnixNano())
	// parentConn 只注册父任务；它无法通过本地注册信息推断远端子任务的字段
	parentConn, err := client.New(client.WithRuntime(scenarios.Runtime(namespace)...))
	if err != nil {
		t.Fatal(err)
	}
	defer parentConn.Close()
	// childConn 拥有另一个 Worker 和连接，注册信息不能回填到 parentConn
	childConn, err := client.New(client.WithRuntime(scenarios.Runtime(namespace)...))
	if err != nil {
		t.Fatal(err)
	}
	defer childConn.Close()
	defer func() {
		// cleanup 不继承已耗尽的业务预算，实际删除两个定义后才能报告成功
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		// name 是本轮创建的两项定义，运行历史按验收规则保留
		for _, name := range []string{"remote-parent", "remote-child"} {
			// err 验证实际资源删除，不用关闭连接替代删除断言
			if _, err := parentConn.Workflows().Delete(cleanup, name); err != nil {
				t.Error(err)
			}
		}
	}()
	// child 只在 childConn 上注册，业务返回三个字段来暴露错误的结果层级
	child := childConn.NewStandaloneTask("remote-child", func(ctx context.Context, in map[string]int) (durableRemoteOutput, error) {
		// info 来自真实执行，返回的身份必须与引擎分配一致
		info, _ := task.Info(ctx)
		if info.ParentRunID == nil {
			return durableRemoteOutput{}, fmt.Errorf("remote child missing parent identity")
		}
		return durableRemoteOutput{Count: 2 * in["count"], ParentRunID: *info.ParentRunID, ChildRunID: info.RunID}, nil
	})
	// parent 沿用自己的 durable listener 提交子任务，不能借用 childConn 的本地定义
	parent := parentConn.NewStandaloneDurableTask("remote-parent", func(ctx context.Context, in map[string]int) (durableRemoteOutput, error) {
		// borrowed 属于当前执行，其配置和父身份均来自 parentConn
		borrowed, err := task.Client(ctx)
		if err != nil {
			return durableRemoteOutput{}, err
		}
		// result 必须完整保留远端业务对象，不能把 Count 当成一个任务的输出
		result, err := borrowed.Run(ctx, "remote-child", in)
		if err != nil {
			return durableRemoteOutput{}, err
		}
		// out 通过公开 Into 解码，结果读取不能依赖内部缓存或后端类型
		var out durableRemoteOutput
		err = result.Into(&out)
		return out, err
	})
	// workers 定义两个独立实例，父实例没有注册子任务
	workers := []struct {
		// conn 是本实例的连接与注册表所有者
		conn *client.Conn
		// definition 是本实例唯一注册的任务
		definition client.Definition
	}{{parentConn, parent}, {childConn, child}}
	// item 在各自连接上启动消费，不通过共享 Worker 偷渡定义信息
	for _, item := range workers {
		// worker 使用正式启动和就绪确认，业务请求不会依赖猜测的 sleep
		worker, err := item.conn.NewWorker("remote-worker", client.WithWorkflows(item.definition))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = worker.Start(); err != nil {
			t.Fatal(err)
		}
		if err = worker.WaitReady(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// result 是父任务的实际终态，失败时不能靠子任务已经执行证明成功
	result, err := parent.Run(ctx, map[string]int{"count": 3})
	if err != nil {
		t.Fatal(err)
	}
	// out 验证计算值、父身份和独立子 RunID 三个属性
	var out durableRemoteOutput
	if err = result.Into(&out); err != nil || out.Count != 6 || out.ParentRunID != result.RunID || out.ChildRunID == "" || out.ChildRunID == result.RunID {
		t.Fatalf("remote child result mismatch: %+v, err=%v", out, err)
	}
	t.Logf("namespace=%s parent_run_id=%s child_run_id=%s count=%d", namespace, result.RunID, out.ChildRunID, out.Count)
}
