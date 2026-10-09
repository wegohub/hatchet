package backend

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	eventpb "github.com/hatchet-dev/hatchet/internal/services/ingestor/contracts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// logIngestor 保存完整正式协议请求，阻塞模式用于验证预算与 I/O 注销。
type logIngestor struct {
	// UnimplementedEventsServiceServer 满足官方注册描述，无需修改上游代码。
	eventpb.UnimplementedEventsServiceServer
	// requests 保存实际收到的请求，不含有效 token。
	requests chan *eventpb.PutLogRequest
	// block 让服务端等待 context 取消，不人为返回成功。
	block bool
}

// PutLog 收到请求后记录，阻塞时实际观察传输取消。
func (s *logIngestor) PutLog(ctx context.Context, req *eventpb.PutLogRequest) (*eventpb.PutLogResponse, error) {
	s.requests <- req
	if s.block {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &eventpb.PutLogResponse{}, nil
}

// logBackend 建立受控协议服务和实例连接，自动关闭服务、连接及监听。
func logBackend(t *testing.T, block bool) (*Backend, *logIngestor) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	fixture := &logIngestor{requests: make(chan *eventpb.PutLogRequest, 8), block: block}
	eventpb.RegisterEventsServiceServer(srv, fixture)
	go func() { _ = srv.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///fixture", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); srv.Stop(); listener.Close() })
	return &Backend{rpcConn: conn, config: spec.Defaults()}, fixture
}

// TestLogRecordProtocol 验证正式字段与请求时间，metadata 不混入 message。
func TestLogRecordProtocol(t *testing.T) {
	b, fixture := logBackend(t, false)
	when := time.Date(2026, 1, 2, 3, 4, 5, 123, time.UTC)
	record := slog.NewRecord(when, slog.LevelWarn, "batch completed", 0)
	record.AddAttrs(slog.Int("count", 3), slog.Any("error", errors.New("fixture failure")), slog.Group("request", slog.String("id", "one")))
	if err := b.logRecord(context.Background(), "task-1", 2, time.Second, record); err != nil {
		t.Fatal(err)
	}
	req := <-fixture.requests
	// metadata 保存协议中的 JSON 属性，消息文本保持独立。
	var metadata map[string]any
	if err := json.Unmarshal([]byte(req.Metadata), &metadata); err != nil {
		t.Fatal(err)
	}
	if req.Message != record.Message || req.GetLevel() != "WARN" || req.GetTaskRetryCount() != 2 || !req.CreatedAt.AsTime().Equal(when) || metadata["count"] != float64(3) || metadata["error"] != "fixture failure" {
		t.Fatal(req.Message, req.Level, req.TaskRetryCount, metadata)
	}
}

// TestLogRecordValidation 错误必须在提交前返回，不能丢弃字段或截断消息。
func TestLogRecordValidation(t *testing.T) {
	b, fixture := logBackend(t, false)
	for _, record := range []slog.Record{
		slog.NewRecord(time.Now(), slog.LevelInfo, "", 0),
		slog.NewRecord(time.Now(), slog.LevelInfo, strings.Repeat("x", 10001), 0),
	} {
		if err := b.logRecord(context.Background(), "task-1", 0, time.Second, record); status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
	}
	record := slog.NewRecord(time.Now(), slog.LevelInfo, "fixture", 0)
	record.AddAttrs(slog.Any("unsupported", make(chan int)))
	if err := b.logRecord(context.Background(), "task-1", 0, time.Second, record); err == nil {
		t.Fatal("unsupported value silently discarded")
	}
	record = slog.NewRecord(time.Now(), slog.LevelInfo, "fixture", 0)
	record.AddAttrs(slog.Group("nested", slog.String("first", "ok")), slog.String("later", "\x00"))
	if err := b.logRecord(context.Background(), "task-1", 0, time.Second, record); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	b.config.BackendMessageLimit = 10
	if err := b.logRecord(context.Background(), "task-1", 0, time.Second, slog.NewRecord(time.Now(), slog.LevelInfo, "fixture", 0)); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	select {
	case <-fixture.requests:
		t.Fatal("invalid request reached backend")
	default:
	}
}

// TestLogRecordBudgetAndTracking 验证独立 5 秒配置可收窄，早于上报预算的调用 deadline 优先。
func TestLogRecordBudgetAndTracking(t *testing.T) {
	b, _ := logBackend(t, true)
	// active 跟踪未注销的日志 I/O，超时后必须归零。
	var active atomic.Int32
	b.BeginLogIO = func(ctx context.Context) (context.Context, func(), error) {
		active.Add(1)
		return ctx, func() { active.Add(-1) }, nil
	}
	record := slog.NewRecord(time.Now(), slog.LevelInfo, "fixture", 0)
	if err := b.logRecord(context.Background(), "task-1", 0, 30*time.Millisecond, record); !errors.Is(err, context.DeadlineExceeded) && status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := b.logRecord(ctx, "task-1", 0, time.Second, record); !errors.Is(err, context.DeadlineExceeded) && status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(err)
	}
	if active.Load() != 0 {
		t.Fatal("log I/O registration leaked")
	}
}
