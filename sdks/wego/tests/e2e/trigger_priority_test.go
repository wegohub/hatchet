//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/scenarios"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestTriggerPriorityPresence 读取真实服务端资源，确认 0 使用默认 1、1–3 原值保存。
func TestTriggerPriorityPresence(t *testing.T) {
	preflight(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	harness, err := scenarios.Start(ctx, "trigger-priority", &scenarios.Report{}, &scenarios.Service{}, nil)
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
	// 本组只读取未来触发配置；真实分钟 Cron 和一次性调度执行由 TestExamples/cron 覆盖。
	for priority := int32(0); priority <= 3; priority++ {
		for _, kind := range []string{"cron", "schedule"} {
			t.Run(fmt.Sprintf("%s/%d", kind, priority), func(t *testing.T) {
				// created 与 stored 分别保存创建资源和随后查询的持久配置。
				var created, stored model.Resource
				// err 记录创建或查询失败；缺少配置不能作为默认值正确的证据。
				var err error
				if kind == "cron" {
					created, err = harness.Conn.Crons().Create(ctx, pb.UnaryGreeter_SayHello_FullMethodName, model.CreateCronTrigger{
						Name: harness.Namespace + fmt.Sprint(priority), Expression: "0 0 1 1 *",
						Input: &pb.Request{Message: "future"}, Priority: priority,
					})
					if err == nil {
						stored, err = harness.Conn.Crons().Get(ctx, created.ID())
					}
				} else {
					created, err = harness.Conn.Schedules().Create(ctx, pb.UnaryGreeter_SayHello_FullMethodName, model.CreateScheduledRunTrigger{
						TriggerAt: time.Now().Add(time.Hour), Input: &pb.Request{Message: "future"}, Priority: priority,
					})
					if err == nil {
						stored, err = harness.Conn.Schedules().Get(ctx, created.ID())
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				expected := priority
				if expected == 0 {
					expected = 1
				}
				if created.ID() == "" || stored["priority"] != float64(expected) {
					t.Fatalf("stored priority mismatch: wanted %d, resource=%v", expected, stored)
				}
				harness.Report.Add(harness, fmt.Sprintf("%s priority %d stored as %d; id=%s", kind, priority, expected, created.ID()), nil)
			})
		}
	}
	// 必须确认本组 workflow 和未触发资源已经删除后才能写入成功证据。
	err = harness.Close()
	closed = true
	if err != nil {
		t.Fatal(err)
	}
	writeStreamEvidence(t, "trigger-priority", map[string]any{"namespace": harness.Namespace, "records": harness.Report.Records, "cleanup": "workflows and future triggers deleted"})
}
