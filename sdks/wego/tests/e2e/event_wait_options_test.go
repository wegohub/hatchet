//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// TestEventWaitOptions 验证两种入口从历史事件中按显式空范围或具体范围还原业务结果。
func TestEventWaitOptions(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	namespace := fmt.Sprintf("wego_event_options_%d_", time.Now().UnixNano())
	conn, err := client.New(client.WithRuntime(scenarios.Runtime(namespace)...))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// definitions 共用同一组参数，仅业务结果解码入口不同。
	definitions := make([]client.Definition, 0, 2)
	for _, into := range []bool{false, true} {
		name := fmt.Sprintf("wait-%v", into)
		definition := conn.NewStandaloneDurableTask(name, func(ctx context.Context, input map[string]string) (*pb.Reply, error) {
			now, err := task.Now(ctx)
			if err != nil {
				return nil, err
			}
			opts := []task.EventWaitOption{
				task.WithEventScope(input["scope"]), task.WithConsiderEventsSince(now.Add(-time.Minute)),
			}
			if into {
				request := new(pb.Request)
				if err := task.WaitForEventInto(ctx, input["key"], "true", request, opts...); err != nil {
					return nil, err
				}
				return &pb.Reply{Message: request.Message}, nil
			}
			event, err := task.WaitForEvent(ctx, input["key"], "true", opts...)
			if err != nil {
				return nil, err
			}
			return &pb.Reply{Message: fmt.Sprint(event["message"])}, nil
		})
		definitions = append(definitions, definition)
	}
	worker, err := conn.NewWorker("", client.WithWorkflows(definitions...))
	if err != nil {
		t.Fatal(err)
	}
	// 清理使用独立预算；先停止消费，再删除本轮定义，不影响其他 namespace。
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := worker.Shutdown(cleanup); err != nil {
			t.Error(err)
		}
		for _, definition := range definitions {
			if _, err := conn.Workflows().Delete(cleanup, definition.GetName()); err != nil {
				t.Error(err)
			}
		}
	}()
	if _, err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	if err := worker.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"wait-false", "wait-true"} {
		for _, scope := range []string{"", "target"} {
			key := name + ":" + scope
			// 先发布其他范围再发布匹配范围，等待尚未登记；只能依靠历史查询完成。
			if err := conn.Events().Push(ctx, key, &pb.Request{Message: "wrong"}, "other"); err != nil {
				t.Fatal(err)
			}
			if err := conn.Events().Push(ctx, key, &pb.Request{Message: "matched"}, scope); err != nil {
				t.Fatal(err)
			}
			ref, err := conn.RunNoWait(ctx, name, map[string]string{"key": key, "scope": scope})
			if err != nil {
				t.Fatal(err)
			}
			result, err := ref.Result(ctx)
			if err != nil {
				t.Fatal(err)
			}
			out := new(pb.Reply)
			if err := result.IntoContext(ctx, out); err != nil || out.Message != "matched" {
				t.Fatalf("name=%s scope=%q: reply=%v error=%v", name, scope, out, err)
			}
			t.Logf("name=%s scope=%q run_id=%s history matched", name, scope, ref.RunID)
		}
	}
}
