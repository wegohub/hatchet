package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	f "github.com/hatchet-dev/hatchet/sdks/go/features"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// TestTriggerPriorityHTTP 检查实际官方 HTTP 请求；0 必须省略，1–3 必须传递而不能在 DTO 中丢失。
func TestTriggerPriorityHTTP(t *testing.T) {
	for _, kind := range []string{"cron", "schedule"} {
		for priority := int32(0); priority <= 3; priority++ {
			t.Run(fmt.Sprintf("%s/%d", kind, priority), func(t *testing.T) {
				// 回应合法资源并捕获请求体，兼顾官方指针 DTO 转换和最终 HTTP 序列化。
				bodies := make(chan map[string]json.RawMessage, 1)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodPost {
						t.Errorf("unexpected method %s", r.Method)
					}
					// body 保存这一请求的 JSON 字段；通过通道交付，避免服务端与测试并发读写。
					var body map[string]json.RawMessage
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					bodies <- body
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprint(w, `{"metadata":{"id":"00000000-0000-0000-0000-000000000001"}}`)
				}))
				defer server.Close()
				api, err := rest.NewClientWithResponses(server.URL)
				if err != nil {
					t.Fatal(err)
				}
				tenant := uuid.New()
				// call 绑定实际创建方法，覆盖 wego 值字段到官方可选 DTO 的转换。
				var call featureCall
				// trigger 保持自有模型类型，不能提前转换后绕过真实适配路径。
				var trigger any
				if kind == "cron" {
					client := &cronClient{api: api, tenant: tenant}
					call = feature2[string, f.CreateCronTrigger](client.Create)
					trigger = model.CreateCronTrigger{Name: "fixture", Expression: "* * * * *", Priority: priority}
				} else {
					client := f.NewSchedulesClient(api, tenant.String(), nil)
					call = feature2[string, f.CreateScheduledRunTrigger](client.Create)
					trigger = model.CreateScheduledRunTrigger{TriggerAt: time.Unix(0, 0).UTC(), Priority: priority}
				}
				if _, err := call(context.Background(), []any{"fixture", trigger}); err != nil {
					t.Fatal(err)
				}
				body := <-bodies
				actual, present := body["priority"]
				if present != (priority != 0) || (present && string(actual) != fmt.Sprint(priority)) {
					t.Fatalf("HTTP priority mismatch: %s", body)
				}
			})
		}
	}
}
