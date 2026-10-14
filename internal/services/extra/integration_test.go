package extra

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/internal/services/extra/proto/v1"
)

// fixture 每个测试启动真实 embedded NATS 与 TCP gRPC，不依赖外部服务或固定端口。
type fixture struct {
	listener  net.Listener
	server    *Server
	conn      *grpc.ClientConn
	streams   pb.StreamServiceClient
	publisher pb.PublisherServiceClient
	consumers pb.ConsumerServiceClient
	kv        pb.KeyValueServiceClient
	ctx       context.Context
	scope     *pb.Scope
	tenant    uuid.UUID
}

// openFixture 为测试创建独立存储、端口和取消预算，清理时等待 Serve 完成。
func openFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	t.Setenv("SERVER_SECURITY_CHECK_ENABLED", "false")
	if cfg.StoreDir == "" {
		cfg.StoreDir = t.TempDir()
	}
	if cfg.ACKKey == nil {
		cfg.ACKKey = make([]byte, 32)
		_, err := rand.Read(cfg.ACKKey)
		requireOK(t, err)
	}
	if cfg.Authenticator == nil {
		cfg.Authenticator = AuthenticatorFunc(func(ctx context.Context, t string) (uuid.UUID, error) {
			return uuid.Parse(t)
		})
	}
	server, err := New(cfg)
	requireOK(t, err)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	requireOK(t, err)
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(l)
	}()
	transport := grpc.WithTransportCredentials(insecure.NewCredentials())
	if cfg.TLS != nil {
		cert, e := x509.ParseCertificate(cfg.TLS.Certificates[0].Certificate[0])
		requireOK(t, e)
		roots := x509.NewCertPool()
		roots.AddCert(cert)
		transport = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs:    roots,
			MinVersion: tls.VersionTLS12,
		}))
	}
	conn, err := grpc.NewClient(l.Addr().String(), transport)
	requireOK(t, err)
	id := uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+id.String()))
	f := &fixture{
		listener:  l,
		server:    server,
		conn:      conn,
		streams:   pb.NewStreamServiceClient(conn),
		publisher: pb.NewPublisherServiceClient(conn),
		consumers: pb.NewConsumerServiceClient(conn),
		kv:        pb.NewKeyValueServiceClient(conn),
		ctx:       ctx,
		scope:     &pb.Scope{Namespace: "isolated"},
		tenant:    id,
	}
	t.Cleanup(func() {
		cancel()
		server.Stop()
		_ = conn.Close()
		select {
		case err := <-done:
			requireOK(t, err)
		case <-time.After(5 * time.Second):
			t.Error("Serve did not exit")
		}
	})
	return f
}

// requireOK 立即报告操作失败，避免后续断言掩盖首个错误。
func requireOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// requireCode 核对 gRPC 状态码，同时保留实际错误用于诊断。
func requireCode(t *testing.T, err error, c codes.Code) {
	t.Helper()
	if status.Code(err) != c {
		t.Fatalf("code=%v, want=%v; error=%v", status.Code(err), c, err)
	}
}

// streamRef 构造本轮测试作用域内的 stream 引用。
func (f *fixture) streamRef() *pb.StreamRef {
	return &pb.StreamRef{
		Scope:  f.scope,
		Stream: "messages",
	}
}

// consumerRef 构造指定测试 stream 内的消费者引用。
func (f *fixture) consumerRef() *pb.ConsumerRef {
	return &pb.ConsumerRef{
		Stream:   f.streamRef(),
		Consumer: "reader",
	}
}

// bucketRef 构造本轮测试作用域内的 KV 桶引用。
func (f *fixture) bucketRef() *pb.BucketRef {
	return &pb.BucketRef{
		Scope:  f.scope,
		Bucket: "state",
	}
}

// keyRef 构造测试桶中的业务 key 引用。
func (f *fixture) keyRef(k string) *pb.KeyRef {
	return &pb.KeyRef{
		Bucket: f.bucketRef(),
		Key:    k,
	}
}

// createStream 注册测试专用 stream，开启有限期限的发布去重。
func (f *fixture) createStream(t *testing.T) {
	t.Helper()
	_, err := f.streams.CreateOrUpdateStream(f.ctx, &pb.StreamConfigRequest{
		Scope: f.scope,
		Config: &pb.NatsStreamConfig{
			Name:       ptr("messages"),
			Subjects:   []string{"assets.>"},
			Duplicates: ptr(int64(time.Minute)),
		},
	})
	requireOK(t, err)
}

// createConsumer 注册显式 ACK 消费者，等待期限由测试场景控制。
func (f *fixture) createConsumer(t *testing.T, wait time.Duration) {
	t.Helper()
	_, err := f.consumers.CreateOrUpdateConsumer(f.ctx, &pb.ConsumerConfigRequest{
		Stream: f.streamRef(),
		Config: &pb.NatsConsumerConfig{
			Durable:       ptr("reader"),
			AckWait:       ptr(int64(wait)),
			FilterSubject: ptr("assets.>"),
		},
	})
	requireOK(t, err)
}

// publish 发布测试消息并返回持久确认，复用指定操作 ID 验证去重。
func (f *fixture) publish(t *testing.T, data, id string) *pb.NatsPubAck {
	t.Helper()
	r, err := f.publisher.Publish(f.ctx, &pb.PublishRequest{
		Scope: f.scope,
		Message: &pb.Message{
			Subject: "assets.one",
			Data:    []byte(data),
		},
		Options: &pb.PublishOptions{MessageId: ptr(id)},
	})
	requireOK(t, err)
	return r
}

// fetch 收集一个有界 pull 批次，必须明确收到 EOF 或故障。
func (f *fixture) fetch(t *testing.T, n uint32, noWait bool) []*pb.Delivery {
	t.Helper()
	r := &pb.FetchRequest{
		Consumer: f.consumerRef(),
		Batch:    &pb.FetchRequest_MaxMessages{MaxMessages: n},
		NoWait:   noWait,
	}
	if !noWait {
		r.Options = &pb.FetchOptions{MaxWaitNs: ptr(int64(time.Second))}
	}
	stream, err := f.consumers.Fetch(f.ctx, r)
	requireOK(t, err)
	var all []*pb.Delivery
	for {
		m, e := stream.Recv()
		if e == io.EOF {
			break
		}
		requireOK(t, e)
		all = append(all, m)
	}
	return all
}

// ack 执行测试指定的确认方式，不在读取时隐式确认。
func (f *fixture) ack(t *testing.T, m *pb.Delivery, kind pb.AckRequest_Kind, confirm bool) {
	t.Helper()
	_, err := f.consumers.Ack(f.ctx, &pb.AckRequest{
		AckToken: m.AckToken,
		Kind:     kind,
		Confirm:  confirm,
	})
	requireOK(t, err)
}

// createBucket 创建保留历史的测试 KV 桶，允许单 key TTL。
func (f *fixture) createBucket(t *testing.T) {
	t.Helper()
	_, err := f.kv.CreateOrUpdateBucket(f.ctx, &pb.KeyValueConfigRequest{
		Scope: f.scope,
		Config: &pb.NatsKeyValueConfig{
			Bucket:         ptr("state"),
			History:        ptr(uint32(10)),
			LimitMarkerTtl: ptr(int64(time.Second)),
		},
	})
	requireOK(t, err)
}

// TestStreamAndPublisher 验证五个 Stream RPC 与两种发布方式的真实存储结果。
func TestStreamAndPublisher(t *testing.T) {
	f := openFixture(t, Config{})
	f.createStream(t)
	ack := f.publish(t, "first", "op-1")
	if ack.GetStream() != "messages" || ack.GetSequence() != 1 || ack.GetDuplicate() {
		t.Fatalf("unexpected ack: %v", ack)
	}
	dup := f.publish(t, "ignored", "op-1")
	if !dup.GetDuplicate() || dup.GetSequence() != 1 {
		t.Fatalf("dedup failed: %v", dup)
	}
	stored, err := f.streams.GetMessage(f.ctx, &pb.GetMessageRequest{
		Stream:   f.streamRef(),
		Selector: &pb.GetMessageRequest_Sequence{Sequence: 1},
	})
	requireOK(t, err)
	if string(stored.Data) != "first" || stored.GetSubject() != "assets.one" {
		t.Fatalf("stored message=%v", stored)
	}
	last, err := f.streams.GetMessage(f.ctx, &pb.GetMessageRequest{
		Stream:   f.streamRef(),
		Selector: &pb.GetMessageRequest_LastSubject{LastSubject: "assets.one"},
	})
	requireOK(t, err)
	if last.GetSequence() != 1 {
		t.Fatal(last)
	}
	// 双向流水线逐条关联确认；不依赖消息完成的顺序。
	stream, err := f.publisher.PublishAsync(f.ctx)
	requireOK(t, err)
	for i := 0; i < 20; i++ {
		requireOK(t, stream.Send(&pb.PublishRequest{
			Scope:         f.scope,
			CorrelationId: strconv.Itoa(i),
			Message: &pb.Message{
				Subject: "assets.one",
				Data:    []byte("chunk"),
			},
		}))
	}
	requireOK(t, stream.CloseSend())
	seen := map[string]bool{}
	for {
		r, e := stream.Recv()
		if e == io.EOF {
			break
		}
		requireOK(t, e)
		if r.GetError() != nil || r.GetAck() == nil || seen[r.CorrelationId] {
			t.Fatalf("invalid pipeline result: %v", r)
		}
		seen[r.CorrelationId] = true
	}
	if len(seen) != 20 {
		t.Fatalf("ack count=%d", len(seen))
	}
	info, err := f.streams.GetStream(f.ctx, &pb.StreamInfoRequest{
		Stream:        f.streamRef(),
		SubjectFilter: ptr("assets.>"),
	})
	requireOK(t, err)
	if info.Config.GetName() != "messages" || info.State.GetMsgs() != 21 || info.State.Subjects["assets.one"] != 21 {
		t.Fatalf("info=%v", info)
	}
	list, err := f.streams.ListStreams(f.ctx, &pb.StreamListRequest{Scope: f.scope})
	requireOK(t, err)
	listed, err := list.Recv()
	requireOK(t, err)
	if listed.Config.GetName() != "messages" {
		t.Fatal(listed)
	}
	_, err = list.Recv()
	if err != io.EOF {
		t.Fatalf("list end=%v", err)
	}
	_, err = f.streams.DeleteStream(f.ctx, f.streamRef())
	requireOK(t, err)
	_, err = f.streams.GetStream(f.ctx, &pb.StreamInfoRequest{Stream: f.streamRef()})
	requireCode(t, err, codes.NotFound)
}

// TestConsumerACK 验证持久状态、逐条确认、拒绝重投、进度续期和终止。
func TestConsumerACK(t *testing.T) {
	f := openFixture(t, Config{})
	f.createStream(t)
	f.createConsumer(t, time.Second)
	for i := 0; i < 3; i++ {
		f.publish(t, strconv.Itoa(i), "op-"+strconv.Itoa(i))
	}
	list, err := f.consumers.ListConsumers(f.ctx, f.streamRef())
	requireOK(t, err)
	info, err := list.Recv()
	requireOK(t, err)
	if info.GetName() != "reader" {
		t.Fatal(info)
	}
	_, err = list.Recv()
	if err != io.EOF {
		t.Fatalf("list=%v", err)
	}
	messages := f.fetch(t, 3, false)
	if len(messages) != 3 {
		t.Fatalf("delivery count=%d", len(messages))
	}
	f.ack(t, messages[1], pb.AckRequest_ACK, true)
	f.ack(t, messages[0], pb.AckRequest_NAK, false)
	f.ack(t, messages[2], pb.AckRequest_IN_PROGRESS, false)
	redelivered := f.fetch(t, 1, false)
	if len(redelivered) != 1 ||
		redelivered[0].Metadata.Sequence.GetStream() != messages[0].Metadata.Sequence.GetStream() ||
		redelivered[0].Metadata.GetNumDelivered() < 2 {
		t.Fatalf("redelivery=%v", redelivered)
	}
	f.ack(t, redelivered[0], pb.AckRequest_ACK, true)
	f.ack(t, messages[2], pb.AckRequest_TERM, false)
	eventually(t, func() bool {
		info, e := f.consumers.GetConsumer(f.ctx, f.consumerRef())
		return e == nil && info.GetNumAckPending() == 0
	})
	if rest := f.fetch(t, 1, true); len(rest) != 0 {
		t.Fatalf("unexpected delivery=%v", rest)
	}
	_, err = f.consumers.DeleteConsumer(f.ctx, f.consumerRef())
	requireOK(t, err)
	_, err = f.consumers.GetConsumer(f.ctx, f.consumerRef())
	requireCode(t, err, codes.NotFound)
}

// eventually 以有截止时间的轮询等待状态断言，不用固定 sleep 推测操作完成。
func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if fn() {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("condition did not become true")
		}
	}
}

// TestKVOperations 验证完整 KV 首版链路，包括空字节、revision 与删除历史。
func TestKVOperations(t *testing.T) {
	f := openFixture(t, Config{})
	f.createBucket(t)
	st, err := f.kv.GetBucket(f.ctx, f.bucketRef())
	requireOK(t, err)
	if st.Bucket != "state" || st.History != 10 || st.Config.GetBucket() != "state" {
		t.Fatal(st)
	}
	buckets, err := f.kv.ListBuckets(f.ctx, f.scope)
	requireOK(t, err)
	b, err := buckets.Recv()
	requireOK(t, err)
	if b.Bucket != "state" {
		t.Fatal(b)
	}
	_, err = buckets.Recv()
	if err != io.EOF {
		t.Fatal(err)
	}
	first, err := f.kv.Create(f.ctx, &pb.KeyValueCreateRequest{
		Key:   f.keyRef("asset"),
		Value: []byte("a"),
	})
	requireOK(t, err)
	_, err = f.kv.Create(f.ctx, &pb.KeyValueCreateRequest{
		Key:   f.keyRef("asset"),
		Value: []byte("other"),
	})
	requireCode(t, err, codes.AlreadyExists)
	second, err := f.kv.Update(f.ctx, &pb.KeyValueUpdateRequest{
		Key:              f.keyRef("asset"),
		Value:            []byte("b"),
		ExpectedRevision: first.Revision,
	})
	requireOK(t, err)
	_, err = f.kv.Update(f.ctx, &pb.KeyValueUpdateRequest{
		Key:              f.keyRef("asset"),
		Value:            []byte("c"),
		ExpectedRevision: first.Revision,
	})
	requireCode(t, err, codes.Aborted)
	old, err := f.kv.Get(f.ctx, &pb.KeyValueGetRequest{
		Key:      f.keyRef("asset"),
		Revision: ptr(first.Revision),
	})
	requireOK(t, err)
	if string(old.Value) != "a" {
		t.Fatal(old)
	}
	current, err := f.kv.Get(f.ctx, &pb.KeyValueGetRequest{Key: f.keyRef("asset")})
	requireOK(t, err)
	if string(current.Value) != "b" || current.Revision != second.Revision {
		t.Fatal(current)
	}
	_, err = f.kv.Put(f.ctx, &pb.KeyValuePutRequest{
		Key:   f.keyRef("empty"),
		Value: []byte{},
	})
	requireOK(t, err)
	empty, err := f.kv.Get(f.ctx, &pb.KeyValueGetRequest{Key: f.keyRef("empty")})
	requireOK(t, err)
	if len(empty.Value) != 0 {
		t.Fatal(empty)
	}
	history, err := f.kv.History(f.ctx, &pb.KeyValueHistoryRequest{Key: f.keyRef("asset")})
	requireOK(t, err)
	count := 0
	for {
		_, e := history.Recv()
		if e == io.EOF {
			break
		}
		requireOK(t, e)
		count++
	}
	if count != 2 {
		t.Fatalf("history count=%d", count)
	}
	_, err = f.kv.Delete(f.ctx, &pb.KeyValueDeleteRequest{
		Key:              f.keyRef("asset"),
		ExpectedRevision: ptr(first.Revision),
	})
	requireCode(t, err, codes.Aborted)
	_, err = f.kv.Delete(f.ctx, &pb.KeyValueDeleteRequest{
		Key:              f.keyRef("asset"),
		ExpectedRevision: ptr(second.Revision),
	})
	requireOK(t, err)
	_, err = f.kv.Get(f.ctx, &pb.KeyValueGetRequest{Key: f.keyRef("asset")})
	requireCode(t, err, codes.NotFound)
	_, err = f.kv.DeleteBucket(f.ctx, f.bucketRef())
	requireOK(t, err)
	_, err = f.kv.GetBucket(f.ctx, f.bucketRef())
	requireCode(t, err, codes.NotFound)
}

// TestWatchResumeAndTTL 以初始化标记建立屏障，不用固定 sleep 推测 watcher 就绪。
func TestWatchResumeAndTTL(t *testing.T) {
	f := openFixture(t, Config{})
	f.createBucket(t)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	w, err := f.kv.Watch(ctx, &pb.KeyValueWatchRequest{
		Bucket: f.bucketRef(),
		Keys:   []string{"asset.*"},
	})
	requireOK(t, err)
	ready, err := w.Recv()
	requireOK(t, err)
	if ready.GetInitialized() == nil {
		t.Fatal(ready)
	}
	r, err := f.kv.Put(f.ctx, &pb.KeyValuePutRequest{
		Key:   f.keyRef("asset.one"),
		Value: []byte("first"),
	})
	requireOK(t, err)
	e, err := w.Recv()
	requireOK(t, err)
	if e.GetEntry().Revision != r.Revision {
		t.Fatal(e)
	}
	cancel()
	_, err = w.Recv()
	requireCode(t, err, codes.Canceled)
	next, err := f.kv.Put(f.ctx, &pb.KeyValuePutRequest{
		Key:   f.keyRef("asset.one"),
		Value: []byte("second"),
	})
	requireOK(t, err)
	ctx2, cancel2 := context.WithCancel(f.ctx)
	defer cancel2()
	resumed, err := f.kv.Watch(ctx2, &pb.KeyValueWatchRequest{
		Bucket:  f.bucketRef(),
		Keys:    []string{"asset.*"},
		Options: &pb.WatchOptions{ResumeFromRevision: ptr(r.Revision + 1)},
	})
	requireOK(t, err)
	got, err := resumed.Recv()
	requireOK(t, err)
	if got.GetEntry().Revision != next.Revision {
		t.Fatal(got)
	}
	// per-key TTL 需要 bucket 的 LimitMarkerTTL；到期结果必须是 NotFound。
	_, err = f.kv.Create(f.ctx, &pb.KeyValueCreateRequest{
		Key:   f.keyRef("temporary"),
		Value: []byte("v"),
		TtlNs: ptr(int64(time.Second)),
	})
	requireOK(t, err)
	eventually(t, func() bool {
		_, e := f.kv.Get(f.ctx, &pb.KeyValueGetRequest{Key: f.keyRef("temporary")})
		return status.Code(e) == codes.NotFound
	})
}

// TestTenantAndNamespaceIsolation 覆盖查询、列表、发布、ACK token 与 KV 的双重隔离。
func TestTenantAndNamespaceIsolation(t *testing.T) {
	f := openFixture(t, Config{})
	f.createStream(t)
	f.createConsumer(t, time.Second)
	f.createBucket(t)
	f.publish(t, "v", "one")
	m := f.fetch(t, 1, false)[0]
	other := metadata.NewOutgoingContext(f.ctx, metadata.Pairs("authorization", "Bearer "+uuid.NewString()))
	_, err := f.streams.GetStream(other, &pb.StreamInfoRequest{Stream: f.streamRef()})
	requireCode(t, err, codes.NotFound)
	_, err = f.consumers.Ack(other, &pb.AckRequest{
		AckToken: m.AckToken,
		Confirm:  true,
	})
	requireCode(t, err, codes.PermissionDenied)
	_, err = f.kv.GetBucket(other, f.bucketRef())
	requireCode(t, err, codes.NotFound)
	list, err := f.streams.ListStreams(other, &pb.StreamListRequest{Scope: f.scope})
	requireOK(t, err)
	_, err = list.Recv()
	if err != io.EOF {
		t.Fatal(err)
	}
	r := &pb.StreamRef{
		Scope:  &pb.Scope{Namespace: "other"},
		Stream: "messages",
	}
	_, err = f.streams.GetStream(f.ctx, &pb.StreamInfoRequest{Stream: r})
	requireCode(t, err, codes.NotFound)
	_, err = f.publisher.Publish(other, &pb.PublishRequest{
		Scope: f.scope,
		Message: &pb.Message{
			Subject: "assets.one",
			Data:    []byte("other"),
		},
	})
	requireCode(t, err, codes.Unavailable)
	corrupted := append([]byte(nil), m.AckToken...)
	corrupted[len(corrupted)-1] ^= 1
	_, err = f.consumers.Ack(f.ctx, &pb.AckRequest{AckToken: corrupted})
	requireCode(t, err, codes.PermissionDenied)
	unauth := context.Background()
	_, err = f.kv.GetBucket(unauth, f.bucketRef())
	requireCode(t, err, codes.Unauthenticated)
}

// TestPersistentRecovery 重启整个 embedded server，验证磁盘消息、消费者 ACK 与旧 token 的可用性。
func TestPersistentRecovery(t *testing.T) {
	cfg := Config{
		StoreDir: t.TempDir(),
		ACKKey:   make([]byte, 32),
	}
	_, err := rand.Read(cfg.ACKKey)
	requireOK(t, err)
	f := openFixture(t, cfg)
	f.createStream(t)
	f.createConsumer(t, time.Second)
	f.createBucket(t)
	f.publish(t, "first", "one")
	f.publish(t, "second", "two")
	messages := f.fetch(t, 2, false)
	f.ack(t, messages[0], pb.AckRequest_ACK, true)
	_, err = f.kv.Put(f.ctx, &pb.KeyValuePutRequest{
		Key:   f.keyRef("cursor"),
		Value: []byte("1"),
	})
	requireOK(t, err)
	f.server.Stop()
	resumed := openFixture(t, cfg)
	resumed.ctx = metadata.NewOutgoingContext(resumed.ctx, metadata.Pairs("authorization", "Bearer "+f.tenant.String()))
	_, err = resumed.consumers.Ack(resumed.ctx, &pb.AckRequest{
		AckToken: messages[1].AckToken,
		Confirm:  true,
	})
	requireOK(t, err)
	info, err := resumed.consumers.GetConsumer(resumed.ctx, resumed.consumerRef())
	requireOK(t, err)
	if info.Delivered.GetStream() != 2 || info.AckFloor.GetStream() != 2 || info.GetNumAckPending() != 0 {
		t.Fatalf("recovered consumer=%v", info)
	}
	kv, err := resumed.kv.Get(resumed.ctx, &pb.KeyValueGetRequest{Key: resumed.keyRef("cursor")})
	requireOK(t, err)
	if string(kv.Value) != "1" {
		t.Fatal(kv)
	}
	stored, err := resumed.streams.GetMessage(resumed.ctx, &pb.GetMessageRequest{
		Stream:   resumed.streamRef(),
		Selector: &pb.GetMessageRequest_Sequence{Sequence: 1},
	})
	requireOK(t, err)
	if string(stored.Data) != "first" {
		t.Fatal(stored)
	}
}

// TestConcurrentCASAndShutdown 验证同一 revision 只有一个成功写入，并发关闭能够中断长 Watch。
func TestConcurrentCASAndShutdown(t *testing.T) {
	f := openFixture(t, Config{})
	f.createBucket(t)
	rev, err := f.kv.Create(f.ctx, &pb.KeyValueCreateRequest{
		Key:   f.keyRef("shared"),
		Value: []byte("0"),
	})
	requireOK(t, err)
	var wg sync.WaitGroup
	result := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := f.kv.Update(f.ctx, &pb.KeyValueUpdateRequest{
				Key:              f.keyRef("shared"),
				Value:            []byte(strconv.Itoa(i)),
				ExpectedRevision: rev.Revision,
			})
			result <- e
		}(i)
	}
	wg.Wait()
	close(result)
	success := 0
	for e := range result {
		if e == nil {
			success++
		} else {
			requireCode(t, e, codes.Aborted)
		}
	}
	if success != 1 {
		t.Fatalf("CAS winners=%d", success)
	}
	watch, err := f.kv.Watch(f.ctx, &pb.KeyValueWatchRequest{Bucket: f.bucketRef()})
	requireOK(t, err)
	for {
		e, e2 := watch.Recv()
		requireOK(t, e2)
		if e.GetInitialized() != nil {
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- f.server.Shutdown(context.Background())
	}()
	err = f.server.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown=%v", err)
	}
	requireOK(t, <-done)
	_, err = watch.Recv()
	if err == nil {
		t.Fatal("Watch remained open")
	}
	f.server.Stop()
}

// TestFetchBudgetsAndDelayedNAK 验证 byte 批次、空批次、deadline、延迟拒绝与发布前置条件。
func TestFetchBudgetsAndDelayedNAK(t *testing.T) {
	f := openFixture(t, Config{})
	f.createStream(t)
	f.createConsumer(t, 5*time.Second)
	f.publish(t, "one", "one")
	_, err := f.publisher.Publish(f.ctx, &pb.PublishRequest{
		Scope: f.scope,
		Message: &pb.Message{
			Subject: "assets.one",
			Data:    []byte("bad"),
		},
		Options: &pb.PublishOptions{ExpectedLastSequence: ptr(uint64(99))},
	})
	requireCode(t, err, codes.FailedPrecondition)
	stream, err := f.consumers.Fetch(f.ctx, &pb.FetchRequest{
		Consumer: f.consumerRef(),
		Batch:    &pb.FetchRequest_MaxBytes{MaxBytes: 4096},
		NoWait:   true,
	})
	requireOK(t, err)
	m, err := stream.Recv()
	requireOK(t, err)
	_, err = stream.Recv()
	if err != io.EOF {
		t.Fatal(err)
	}
	_, err = f.consumers.Ack(f.ctx, &pb.AckRequest{
		AckToken:   m.AckToken,
		Kind:       pb.AckRequest_NAK,
		NakDelayNs: ptr(int64(200 * time.Millisecond)),
	})
	requireOK(t, err)
	if data := f.fetch(t, 1, true); len(data) != 0 {
		t.Fatal("delayed NAK delivered immediately")
	}
	later := f.fetch(t, 1, false)
	if len(later) != 1 || later[0].Metadata.GetNumDelivered() < 2 {
		t.Fatal(later)
	}
	_, err = f.consumers.Ack(f.ctx, &pb.AckRequest{
		AckToken:   later[0].AckToken,
		Kind:       pb.AckRequest_TERM,
		TermReason: ptr("completed elsewhere"),
	})
	requireOK(t, err)
	eventually(t, func() bool {
		i, e := f.consumers.GetConsumer(f.ctx, f.consumerRef())
		return e == nil && i.GetNumAckPending() == 0
	})
	ctx, cancel := context.WithTimeout(f.ctx, 50*time.Millisecond)
	defer cancel()
	blocked, err := f.consumers.Fetch(ctx, &pb.FetchRequest{
		Consumer: f.consumerRef(),
		Batch:    &pb.FetchRequest_MaxMessages{MaxMessages: 1},
	})
	requireOK(t, err)
	_, err = blocked.Recv()
	requireCode(t, err, codes.DeadlineExceeded)
	// 一个批次超过消息数/字节上限必须明确失败。
	for _, request := range []*pb.FetchRequest{
		{
			Consumer: f.consumerRef(),
			Batch:    &pb.FetchRequest_MaxMessages{MaxMessages: 65},
		},
		{
			Consumer: f.consumerRef(),
			Batch:    &pb.FetchRequest_MaxBytes{MaxBytes: 1 << 30},
		},
		{
			Consumer: f.consumerRef(),
			Batch:    &pb.FetchRequest_MaxMessages{MaxMessages: 1},
			Options:  &pb.FetchOptions{HeartbeatNs: ptr(int64(1 << 62))},
		},
	} {
		call, e := f.consumers.Fetch(f.ctx, request)
		requireOK(t, e)
		_, e = call.Recv()
		requireCode(t, e, codes.InvalidArgument)
	}
}

// TestPipelineErrorsAndAdmission 同时覆盖单条错误、全局在途预算及长期订阅与普通 RPC 的容量隔离。
func TestPipelineErrorsAndAdmission(t *testing.T) {
	f := openFixture(t, Config{
		MaxAsyncInFlight:  1,
		MaxConcurrentRPCs: 1,
	})
	f.createStream(t)
	f.createBucket(t)
	stream, err := f.publisher.PublishAsync(f.ctx)
	requireOK(t, err)
	requireOK(t, stream.Send(&pb.PublishRequest{
		Scope:         f.scope,
		CorrelationId: "bad",
		Message:       &pb.Message{Subject: "$SYS"},
	}))
	requireOK(t, stream.Send(&pb.PublishRequest{
		Scope:         f.scope,
		CorrelationId: "ok",
		Message: &pb.Message{
			Subject: "assets.one",
			Data:    []byte("ok"),
		},
	}))
	requireOK(t, stream.CloseSend())
	results := map[string]*pb.PublishResult{}
	for {
		r, e := stream.Recv()
		if e == io.EOF {
			break
		}
		requireOK(t, e)
		results[r.CorrelationId] = r
	}
	if results["bad"].GetError().GrpcCode != int32(codes.InvalidArgument) || results["ok"].GetAck() == nil {
		t.Fatal(results)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	watch, err := f.kv.Watch(ctx, &pb.KeyValueWatchRequest{Bucket: f.bucketRef()})
	requireOK(t, err)
	_, err = watch.Recv()
	requireOK(t, err)
	second, err := f.kv.Watch(f.ctx, &pb.KeyValueWatchRequest{Bucket: f.bucketRef()})
	requireOK(t, err)
	_, err = second.Recv()
	requireCode(t, err, codes.ResourceExhausted)
	// Watch 已占满 streaming 预算，普通查询仍可执行。
	_, err = f.kv.GetBucket(f.ctx, f.bucketRef())
	requireOK(t, err)
	cancel()
	_, err = watch.Recv()
	requireCode(t, err, codes.Canceled)
}

// TestStartupAndCloseFailures 验证失败启动、停止先于启动、重复启动和多调用方并发停止。
func TestStartupAndCloseFailures(t *testing.T) {
	key := make([]byte, 32)
	auth := AuthenticatorFunc(func(context.Context, string) (uuid.UUID, error) {
		return uuid.New(), nil
	})
	_, err := New(Config{
		StoreDir:      "unused",
		ACKKey:        key,
		Authenticator: AuthenticatorFunc(nil),
	})
	if err == nil {
		t.Fatal("accepted typed nil authenticator")
	}
	f := openFixture(t, Config{})
	// 必须先确认首个 Serve 已接收 RPC；goroutine 被创建不代表已取得启动所有权。
	f.createBucket(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	requireOK(t, err)
	defer l.Close()
	if err = f.server.Serve(l); err == nil {
		t.Fatal("accepted second Serve")
	}
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	requireOK(t, err)
	defer l2.Close()
	s, err := New(Config{
		StoreDir:      t.TempDir(),
		ACKKey:        key,
		Authenticator: auth,
	})
	requireOK(t, err)
	s.Stop()
	if err = s.Serve(l2); err == nil {
		t.Fatal("accepted Serve after Stop")
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.server.Stop()
		}()
	}
	wg.Wait()
}
