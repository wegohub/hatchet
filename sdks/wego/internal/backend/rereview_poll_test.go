package backend

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	"github.com/rs/zerolog"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// rereviewAPIClient 只提供实际 REST 查询路径；结果订阅由独立完成信号控制
type rereviewAPIClient struct {
	// v0.Client 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障
	v0.Client
	// api 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障
	api *rest.ClientWithResponses
}

// API 返回此 fixture 拥有的 HTTP 客户端
func (c *rereviewAPIClient) API() *rest.ClientWithResponses { return c.api }

// Logger 提供 RunsClient 初始化所需的无输出日志器
func (c *rereviewAPIClient) Logger() *zerolog.Logger { logger := zerolog.Nop(); return &logger }

// TenantId 使用独立的有效测试身份，不包含运行环境数据
func (c *rereviewAPIClient) TenantId() string { return "00000000-0000-4000-8000-000000000001" }

// TestReReviewRESTPollDoesNotBlockCompletedResult 完成订阅已交付成功后，辅助状态查询不得继续阻塞它
func TestReReviewRESTPollDoesNotBlockCompletedResult(t *testing.T) {
	// entered 与 release 控制真实 HTTP 请求；completed 代表独立 gRPC 结果已交付
	entered, release := make(chan struct{}, 1), make(chan struct{})
	// completed 取得 make 的结果，确认成功后才进入下一处理阶段
	completed := make(chan struct{})
	// srv 取得 httptest.NewServer 的结果，确认成功后才进入下一处理阶段
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`"RUNNING"`))
	}))
	defer srv.Close()
	// api, err 取得 rest.NewClientWithResponses 的结果，确认成功后才进入下一处理阶段
	api, err := rest.NewClientWithResponses(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	// b 隔离后端实例，连接及监听器由创建方统一关闭
	b := &Backend{config: spec.Runtime{ResultPollInterval: 20 * time.Millisecond}, features: newFeatureClients(&rereviewAPIClient{api: api})}
	// ctx 当前操作预算，用于传输取消；不得替换为无预算的 Background
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// done 本次 goroutine 的退出确认，关闭函数返回前需完成资源回收
	done := make(chan error, 1)
	go func() {
		// _, err 取得 b.await 的结果，确认成功后才进入下一处理阶段
		_, err := b.await(ctx, "00000000-0000-4000-8000-000000000002", func(context.Context) (ports.Result, error) {
			<-completed
			return ports.Result{RunID: "complete"}, nil
		})
		done <- err
	}()
	select {
	case <-entered:
	// err 当前操作错误，失败时不继续使用对应结果
	case err := <-done:
		close(completed)
		t.Fatalf("fixture failed before status request: %v", err)
	case <-ctx.Done():
		close(completed)
		t.Fatal("HTTP fixture was not reached")
	}
	close(completed)
	select {
	// err 当前操作错误，失败时不继续使用对应结果
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
		close(release)
		return
	case <-time.After(250 * time.Millisecond):
		t.Error("successful subscription result is blocked behind the auxiliary HTTP status request")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("result did not finish after HTTP fixture release")
	}
}
