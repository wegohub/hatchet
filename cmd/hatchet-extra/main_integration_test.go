//go:build integration

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/internal/services/extra/proto/v1"
)

// TestLiveProcess 使用真实 Hatchet 数据库/token 与命令进程，测试未配置时明确失败。
func TestLiveProcess(t *testing.T) {
	for _, name := range []string{
		"EXTRA_TEST_BINARY",
		"EXTRA_JWT_PUBLIC_KEY_FILE",
		"EXTRA_TEST_HATCHET_TOKEN",
		"DATABASE_URL",
		"EXTRA_ACK_KEY",
	} {
		if os.Getenv(name) == "" {
			t.Fatalf("%s required", name)
		}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	check(t, err)
	address := l.Addr().String()
	check(t, l.Close())
	dir := t.TempDir()
	log, err := os.Create(filepath.Join(dir, "process.log"))
	check(t, err)
	defer log.Close()
	cmd := exec.Command(os.Getenv("EXTRA_TEST_BINARY"), "--listen", address, "--store", filepath.Join(dir, "store"), "--shutdown-timeout", "2s")
	cmd.Env = append(os.Environ(), "SERVER_SECURITY_CHECK_ENABLED=false")
	cmd.Stdout = log
	cmd.Stderr = log
	check(t, cmd.Start())
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	exited := false
	t.Cleanup(func() {
		if exited {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	check(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+os.Getenv("EXTRA_TEST_HATCHET_TOKEN")))
	verifyRPCs(t, conn, ctx)
	check(t, cmd.Process.Signal(syscall.SIGTERM))
	select {
	case e := <-done:
		exited = true
		check(t, e)
	case <-time.After(5 * time.Second):
		t.Fatal("signal shutdown did not complete")
	}
}

// verifyRPCs 在真实服务上逐一执行 24 个 RPC，只删除本轮独立 namespace 的资源。
func verifyRPCs(t *testing.T, conn *grpc.ClientConn, ctx context.Context) {
	t.Helper()
	var err error
	s := pb.NewStreamServiceClient(conn)
	p := pb.NewPublisherServiceClient(conn)
	c := pb.NewConsumerServiceClient(conn)
	kv := pb.NewKeyValueServiceClient(conn)
	scope := &pb.Scope{Namespace: "acceptance-" + uuid.NewString()[:12]}
	sr := &pb.StreamRef{
		Scope:  scope,
		Stream: "messages",
	}
	// RPC 就绪通过完整的鉴权查询确认，连接建立不等于服务可用。
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		probe, stop := context.WithTimeout(ctx, 200*time.Millisecond)
		_, e := s.GetStream(probe, &pb.StreamInfoRequest{Stream: sr})
		stop()
		if status.Code(e) == codes.NotFound {
			break
		}
		if e != nil && status.Code(e) != codes.Unavailable && status.Code(e) != codes.DeadlineExceeded {
			t.Fatalf("readiness: %v", e)
		}
		select {
		case <-ctx.Done():
			t.Fatal("startup deadline")
		case <-ticker.C:
		}
	}
	_, err = s.CreateOrUpdateStream(ctx, &pb.StreamConfigRequest{
		Scope: scope,
		Config: &pb.NatsStreamConfig{
			Name:     point("messages"),
			Subjects: []string{"assets.>"},
		},
	})
	check(t, err)
	_, err = p.Publish(ctx, &pb.PublishRequest{
		Scope: scope,
		Message: &pb.Message{
			Subject: "assets.one",
			Data:    []byte("one"),
		},
	})
	check(t, err)
	pipeline, err := p.PublishAsync(ctx)
	check(t, err)
	check(t, pipeline.Send(&pb.PublishRequest{
		Scope:         scope,
		CorrelationId: "two",
		Message: &pb.Message{
			Subject: "assets.one",
			Data:    []byte("two"),
		},
	}))
	check(t, pipeline.CloseSend())
	published, err := pipeline.Recv()
	check(t, err)
	if published.GetAck() == nil {
		t.Fatal(published)
	}
	_, err = pipeline.Recv()
	end(t, err)
	stored, err := s.GetMessage(ctx, &pb.GetMessageRequest{
		Stream:   sr,
		Selector: &pb.GetMessageRequest_Sequence{Sequence: 1},
	})
	check(t, err)
	if string(stored.Data) != "one" {
		t.Fatal("stored value mismatch")
	}
	_, err = s.GetStream(ctx, &pb.StreamInfoRequest{Stream: sr})
	check(t, err)
	list, err := s.ListStreams(ctx, &pb.StreamListRequest{Scope: scope})
	check(t, err)
	_, err = list.Recv()
	check(t, err)
	_, err = list.Recv()
	end(t, err)
	cr := &pb.ConsumerRef{
		Stream:   sr,
		Consumer: "reader",
	}
	_, err = c.CreateOrUpdateConsumer(ctx, &pb.ConsumerConfigRequest{
		Stream: sr,
		Config: &pb.NatsConsumerConfig{Durable: point("reader")},
	})
	check(t, err)
	_, err = c.GetConsumer(ctx, cr)
	check(t, err)
	consumers, err := c.ListConsumers(ctx, sr)
	check(t, err)
	_, err = consumers.Recv()
	check(t, err)
	_, err = consumers.Recv()
	end(t, err)
	fetch, err := c.Fetch(ctx, &pb.FetchRequest{
		Consumer: cr,
		Batch:    &pb.FetchRequest_MaxMessages{MaxMessages: 2},
		NoWait:   true,
	})
	check(t, err)
	for i := 0; i < 2; i++ {
		m, e := fetch.Recv()
		check(t, e)
		_, e = c.Ack(ctx, &pb.AckRequest{
			AckToken: m.AckToken,
			Confirm:  true,
		})
		check(t, e)
	}
	_, err = fetch.Recv()
	end(t, err)
	br := &pb.BucketRef{
		Scope:  scope,
		Bucket: "state",
	}
	kr := &pb.KeyRef{
		Bucket: br,
		Key:    "cursor",
	}
	_, err = kv.CreateOrUpdateBucket(ctx, &pb.KeyValueConfigRequest{
		Scope: scope,
		Config: &pb.NatsKeyValueConfig{
			Bucket:  point("state"),
			History: point(uint32(10)),
		},
	})
	check(t, err)
	_, err = kv.GetBucket(ctx, br)
	check(t, err)
	buckets, err := kv.ListBuckets(ctx, scope)
	check(t, err)
	_, err = buckets.Recv()
	check(t, err)
	_, err = buckets.Recv()
	end(t, err)
	created, err := kv.Create(ctx, &pb.KeyValueCreateRequest{
		Key:   kr,
		Value: []byte("1"),
	})
	check(t, err)
	_, err = kv.Get(ctx, &pb.KeyValueGetRequest{Key: kr})
	check(t, err)
	_, err = kv.Update(ctx, &pb.KeyValueUpdateRequest{
		Key:              kr,
		ExpectedRevision: created.Revision,
		Value:            []byte("2"),
	})
	check(t, err)
	_, err = kv.Put(ctx, &pb.KeyValuePutRequest{
		Key:   kr,
		Value: []byte("3"),
	})
	check(t, err)
	history, err := kv.History(ctx, &pb.KeyValueHistoryRequest{Key: kr})
	check(t, err)
	historyCount := 0
	for {
		_, e := history.Recv()
		if errors.Is(e, io.EOF) {
			break
		}
		check(t, e)
		historyCount++
	}
	if historyCount != 3 {
		t.Fatalf("history=%d", historyCount)
	}
	watchCtx, stopWatch := context.WithCancel(ctx)
	watch, err := kv.Watch(watchCtx, &pb.KeyValueWatchRequest{Bucket: br})
	check(t, err)
	for {
		e, e2 := watch.Recv()
		check(t, e2)
		if e.GetInitialized() != nil {
			break
		}
	}
	_, err = kv.Delete(ctx, &pb.KeyValueDeleteRequest{Key: kr})
	check(t, err)
	deleted, err := watch.Recv()
	check(t, err)
	if deleted.GetEntry().Operation != pb.KeyValueEntry_DELETE {
		t.Fatal("delete not observed")
	}
	stopWatch()
	_, err = watch.Recv()
	if status.Code(err) != codes.Canceled {
		t.Fatalf("watch cancel: %v", err)
	}
	_, err = kv.DeleteBucket(ctx, br)
	check(t, err)
	_, err = c.DeleteConsumer(ctx, cr)
	check(t, err)
	_, err = s.DeleteStream(ctx, sr)
	check(t, err)
}

// point 为验收请求保存可选值，避免引用循环中的可变变量。
func point[T any](v T) *T {
	return &v
}

// check 在真实进程验收中立即报告错误，不跳过必要能力。
func check(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}

// end 要求服务正常结束时返回 EOF，其他状态必须报错。
func end(t *testing.T, e error) {
	t.Helper()
	if e != io.EOF {
		t.Fatalf("expected EOF: %v", e)
	}
}
