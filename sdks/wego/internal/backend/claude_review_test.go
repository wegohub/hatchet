package backend

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"github.com/hatchet-dev/hatchet/pkg/client/rest"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// TestMemoFaultProtocolMatchesOfficialSchema 核对透明故障 fixture 使用的字段号，升级不能静默改变注入目标
func TestMemoFaultProtocolMatchesOfficialSchema(t *testing.T) {
	// fields 将消息描述与字段名绑定，只有 backend 测试读取官方类型
	fields := []struct {
		// descriptor 是官方版本实际编译的消息描述
		descriptor protoreflect.MessageDescriptor
		// name 为固定协议字段名，不依赖字段顺序
		name protoreflect.Name
		// number 对应不导入官方类型的透明代理读法
		number protoreflect.FieldNumber
	}{
		{(&v1.DurableTaskRequest{}).ProtoReflect().Descriptor(), "complete_memo", 7},
		{(&v1.DurableTaskCompleteMemoRequest{}).ProtoReflect().Descriptor(), "memo_key", 3},
		{(&v1.DurableTaskResponse{}).ProtoReflect().Descriptor(), "memo_ack", 2},
		{(&v1.DurableTaskEventMemoAckResponse{}).ProtoReflect().Descriptor(), "memo_already_existed", 2},
		{(&v1.DurableTaskEventMemoAckResponse{}).ProtoReflect().Descriptor(), "memo_result_payload", 3},
	}
	// field 校验每个实际描述，缺失也必须报错
	for _, field := range fields {
		// actual 来自官方描述符；这个测试不替代真实服务端 pending ACK 验收
		actual := field.descriptor.Fields().ByName(field.name)
		if actual == nil || actual.Number() != field.number {
			t.Fatalf("memo fixture field changed: %s.%s", field.descriptor.FullName(), field.name)
		}
	}
}

// TestCancelledAckExitsWithInvocation 验证无 ACK 时的三种退出信号都能回收协调等待
// 活跃 invocation 的观察者取消仍不归还槽，根退出或官方清理后才允许释放
func TestCancelledAckExitsWithInvocation(t *testing.T) {
	// mode 分别覆盖官方 listener 停止、根账本清理和根 context 退出
	for _, mode := range []string{"listener-stop", "task-cleanup", "root-cancel"} {
		t.Run(mode, func(t *testing.T) {
			// execution 与 peer 使用真实官方 listener，丢弃已发送请求的 ACK
			execution, peer, root := syncExecution(t)
			// lifetime 允许独立结束根执行，不依赖测试函数清理才唤醒后台接收
			lifetime, cancelRoot := context.WithCancel(root)
			defer cancelRoot()
			execution.lifetime = lifetime
			// observer 先取消单次等待；此时根执行依然存活
			observer, cancelObserver := context.WithCancel(root)
			// result 交付前台取消，后台仍隔离未返回的确认
			result := make(chan error, 1)
			go func() { result <- execution.Sleep(observer, time.Second) }()
			nextRequest(t, peer, "wait")
			cancelObserver()
			// err 来自前台取消，后台确认隔离不延迟业务返回
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			switch mode {
			case "listener-stop":
				execution.durableListener().Stop()
			case "task-cleanup":
				execution.durableListener().CleanupTaskState("memo-task", 1)
			case "root-cancel":
				cancelRoot()
			}
			// budget 检查协调 goroutine 已实际归还登记权，不靠运行时 goroutine 数量猜测
			budget, stop := context.WithTimeout(root, time.Second)
			defer stop()
			// err 证明后台接收已归还确认槽，超时表示存在等待泄漏
			if err := execution.ackGate.Lock(budget); err != nil {
				t.Fatal("ACK coordinator did not exit:", err)
			}
			execution.ackGate.Unlock()
			if mode == "root-cancel" {
				// err 确认槽虽已释放，退出的根执行仍不能再次发送或接收确认
				_, err := execution.Now(root)
				if !errors.Is(err, context.Canceled) {
					t.Fatal("terminated invocation accepted a memo:", err)
				}
			}
		})
	}
}

// TestResultPollingInterval 验证配置的查询间隔生效，订阅完成后查询不会继续运行
func TestResultPollingInterval(t *testing.T) {
	// requests 保存真实 HTTP 查询开始时间，不记录 headers 或授权信息
	requests := make(chan time.Time, 4)
	// apiServer 只返回 RUNNING；完成事件来自独立结果订阅
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- time.Now()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`"RUNNING"`))
	}))
	defer apiServer.Close()
	// api 使用官方 REST 客户端验证真正查询路径，与生产转换共用同一实现
	api, err := rest.NewClientWithResponses(apiServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	// backend 的 80ms 间隔只用于确定性测试，生产默认值为 1 秒
	backend := &Backend{config: spec.Runtime{ResultPollInterval: 80 * time.Millisecond}, features: newFeatureClients(&rereviewAPIClient{api: api})}
	// ctx 为订阅和 HTTP 查询共同提供总预算，错误不能被静默忽略
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// completed 控制独立订阅的完成时点，轮询必须随后退出
	completed := make(chan struct{})
	// done 确认生产 await 包括两个后台操作均已回收
	done := make(chan error, 1)
	go func() {
		// err 来自完整等待路径，HTTP 不得把成功结果变成错误
		_, err := backend.await(ctx, "00000000-0000-4000-8000-000000000002", func(budget context.Context) (ports.Result, error) {
			select {
			case <-completed:
				return ports.Result{}, nil
			case <-budget.Done():
				return ports.Result{}, budget.Err()
			}
		})
		done <- err
	}()
	// first 与 second 是查询实际开始时间，不能在已完成慢查询后立刻补发
	first := <-requests
	// second 必须至少晚一个完整间隔，留少量定时器精度容差
	second := <-requests
	if second.Sub(first) < 70*time.Millisecond {
		t.Errorf("poll interval ignored: %s", second.Sub(first))
	}
	close(completed)
	// err 是所有等待和轮询退出后的结果，不允许掩盖传输错误
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case <-requests:
		t.Fatal("polling outlived result subscription")
	case <-time.After(100 * time.Millisecond):
	}
}
