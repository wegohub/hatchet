package extra

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc/codes"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// TestGracefulShutdownPreservesResponse 排空拒绝新增 RPC，但最后一个响应和 trailers 必须完整送达。
func TestGracefulShutdownPreservesResponse(t *testing.T) {
	var hold atomic.Bool
	var entered sync.Once
	started, release := make(chan struct{}), make(chan struct{})
	f := openFixture(t, Config{Authenticator: AuthenticatorFunc(func(ctx context.Context, token string) (uuid.UUID, error) {
		if hold.Load() {
			entered.Do(func() { close(started) })
			select {
			case <-ctx.Done():
				return uuid.Nil, ctx.Err()
			case <-release:
			}
		}
		return uuid.Parse(token)
	})})
	f.createBucket(t)
	hold.Store(true)
	done := make(chan error, 1)
	go func() { _, err := f.kv.GetBucket(f.ctx, f.bucketRef()); done <- err }()
	select {
	case <-started:
	case <-f.ctx.Done():
		t.Fatal("request did not enter authentication")
	}
	closed := make(chan error, 1)
	go func() { closed <- f.server.Shutdown(f.ctx) }()
	eventually(t, func() bool {
		f.server.lifecycle.mu.Lock()
		defer f.server.lifecycle.mu.Unlock()
		return f.server.lifecycle.stopping
	})
	_, err := f.kv.GetBucket(f.ctx, f.bucketRef())
	requireCode(t, err, codes.Unavailable)
	close(release)
	requireOK(t, <-done)
	requireOK(t, <-closed)
}

// nilTokenRecords 模拟异常数据库适配，空记录必须在官方验证器解引用前拒绝。
type nilTokenRecords struct {
	v1.APITokenRepository
}

// GetAPITokenById 返回受控空记录，不代表合法的数据库查询结果。
func (r *nilTokenRecords) GetAPITokenById(context.Context, uuid.UUID) (*sqlcv1.APIToken, error) {
	return nil, nil
}

// TestAuthRepositoryNilGuards typed nil 在构造时失败，异常空记录在验证器调用前失败。
func TestAuthRepositoryNilGuards(t *testing.T) {
	_, records, _, _ := cachedAuthFixture(t, 0, 2)
	var missing *nilTokenRecords
	_, err := NewHatchetAuthenticator(nil, "http://localhost:8080", missing, records)
	if err == nil || !strings.Contains(err.Error(), "repositories required") {
		t.Fatal("typed nil repository reached key parsing")
	}
	snapshot := &snapshotRepository{APITokenRepository: &nilTokenRecords{}}
	_, err = snapshot.GetAPITokenById(context.Background(), uuid.New())
	if err == nil {
		t.Fatal("nil token record accepted")
	}
}

// TestKVAndConsumerValidationBeforeIO 非法 key、截断的 history 和 pinned 需本地拒绝。
func TestKVAndConsumerValidationBeforeIO(t *testing.T) {
	s := &Service{cfg: defaults(Config{})}
	ctx := contextWithTenant(context.Background(), uuid.New())
	_, err := s.Get(ctx, &pb.KeyValueGetRequest{Key: &pb.KeyRef{
		Bucket: &pb.BucketRef{Bucket: "state"},
		Key:    "asset..id",
	}})
	requireCode(t, err, codes.InvalidArgument)
	_, err = s.CreateOrUpdateBucket(ctx, &pb.KeyValueConfigRequest{Config: &pb.NatsKeyValueConfig{
		Bucket:  ptr("state"),
		History: ptr(uint32(256)),
	}})
	requireCode(t, err, codes.InvalidArgument)
	_, err = s.CreateOrUpdateConsumer(ctx, &pb.ConsumerConfigRequest{
		Stream: &pb.StreamRef{Stream: "messages"},
		Config: &pb.NatsConsumerConfig{
			Durable:        ptr("reader"),
			PriorityPolicy: ptr(int64(jetstream.PriorityPolicyPinned)),
		},
	})
	requireCode(t, err, codes.InvalidArgument)
}

// TestSourceInfoLogicalSubjects mirror/source 的运行状态也必须使用逻辑 subject。
func TestSourceInfoLogicalSubjects(t *testing.T) {
	a, err := addressFor(contextWithTenant(context.Background(), uuid.New()), &pb.Scope{Namespace: "isolated"})
	requireOK(t, err)
	transform := jetstream.SubjectTransformConfig{
		Source:      a.subjects + "assets.*",
		Destination: a.subjects + "forward.*",
	}
	source := &jetstream.StreamSourceInfo{
		Name:              a.names + "origin",
		SubjectTransforms: []jetstream.SubjectTransformConfig{transform},
	}
	info := a.streamInfo(&jetstream.StreamInfo{
		Config:  jetstream.StreamConfig{Name: a.names + "copy"},
		Mirror:  source,
		Sources: []*jetstream.StreamSourceInfo{source},
	})
	for _, result := range []*pb.NatsStreamSourceInfo{
		info.Mirror,
		info.Sources[0],
	} {
		if result.GetName() != "origin" || result.SubjectTransforms[0].GetSource() != "assets.*" || result.SubjectTransforms[0].GetDestination() != "forward.*" {
			t.Fatal("physical subject exposed in source state")
		}
	}
}

// TestShutdownDoesNotWaitForUnresponsiveAuth 强制停止可打断排空，不等待不响应取消的外部回调。
func TestShutdownDoesNotWaitForUnresponsiveAuth(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var entered sync.Once
	f := openFixture(t, Config{Authenticator: AuthenticatorFunc(func(context.Context, string) (uuid.UUID, error) {
		entered.Do(func() {
			close(started)
		})
		<-release
		return uuid.New(), nil
	})})
	defer close(release)
	requestDone := make(chan error, 1)
	go func() {
		_, err := f.kv.GetBucket(f.ctx, f.bucketRef())
		requestDone <- err
	}()
	select {
	case <-started:
	case <-f.ctx.Done():
		t.Fatal("authentication callback did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() {
		closed <- f.server.Shutdown(ctx)
	}()
	select {
	case err := <-closed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown error=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("forced shutdown waited for external callback")
	}
	if err := <-requestDone; err == nil {
		t.Fatal("interrupted request succeeded")
	}
}

// TestCacheLoadBudget 客户端取消不能绕过实际回源预算，同一实例最多一个 SQL 时第二个键被拒绝。
func TestCacheLoadBudget(t *testing.T) {
	ctx, records, token, auth := cachedAuthFixture(t, 0, 2)
	auth.cache.loadSlots = make(chan struct{}, 1)
	blocked := &blockingTokens{
		APITokenRepository: records,
		started:            make(chan struct{}),
		release:            make(chan struct{}),
	}
	auth.cache.APITokenRepository = blocked
	done := make(chan error, 1)
	go func() {
		_, err := auth.Authenticate(ctx, token.Token)
		done <- err
	}()
	select {
	case <-blocked.started:
	case <-ctx.Done():
		t.Fatal("database read did not start")
	}
	_, err := auth.cache.GetAPITokenById(ctx, uuid.New())
	if err == nil {
		t.Fatal("query capacity exceeded")
	}
	close(blocked.release)
	requireOK(t, <-done)
	if records.tokenReads.Load() != 1 {
		t.Fatal("excess query reached database")
	}
}

// TestFetchBusinessStatusHeaders 业务 Status 字段不能被误识别为 pull 控制帧，包括空业务值。
func TestFetchBusinessStatusHeaders(t *testing.T) {
	f := openFixture(t, Config{})
	f.createStream(t)
	f.createConsumer(t, time.Second)
	values := []string{
		"100",
		"404",
		"408",
		"409",
		"503",
	}
	for _, value := range values {
		_, err := f.publisher.Publish(f.ctx, &pb.PublishRequest{
			Scope: f.scope,
			Message: &pb.Message{
				Subject: "assets.one",
				Headers: map[string]*pb.StringValues{"Status": {Values: []string{value}}},
			},
		})
		requireOK(t, err)
	}
	messages := f.fetch(t, uint32(len(values)), true)
	if len(messages) != len(values) {
		t.Fatalf("business deliveries=%d, want %d", len(messages), len(values))
	}
	for i, message := range messages {
		if message.Message.Headers["Status"].GetValues()[0] != values[i] || len(message.Message.Data) != 0 {
			t.Fatal("business header/value changed")
		}
		f.ack(t, message, pb.AckRequest_ACK, true)
	}
}

// TestPublishHeaderOptionsBeforeIO typed options 不得绕过业务头校验注入 NATS 控制字段。
func TestPublishHeaderOptionsBeforeIO(t *testing.T) {
	s := &Service{cfg: defaults(Config{})}
	ctx := contextWithTenant(context.Background(), uuid.New())
	for _, options := range []*pb.PublishOptions{
		{MessageId: ptr("id\r\nNats-Schedule-Target: foreign")},
		{ExpectedLastMessageId: ptr("id\nNats-Expected-Stream: foreign")},
		{MessageId: ptr("id\x00")},
		{Schedule: &pb.ScheduleOptions{
			TargetSubject: "assets.one",
			Schedule:      &pb.ScheduleOptions_Cron{Cron: "* * * * *\r\nNats-Schedule-Target: foreign"},
		}},
		{StallWaitNs: ptr(int64(time.Second))},
	} {
		_, err := s.Publish(ctx, &pb.PublishRequest{
			Message: &pb.Message{Subject: "assets.one"},
			Options: options,
		})
		requireCode(t, err, codes.InvalidArgument)
	}
}

// TestDuplicateServeKeepsListener 拒绝重复 Serve 不能关闭已被第一次 Serve 接管的同一 listener。
func TestDuplicateServeKeepsListener(t *testing.T) {
	f := openFixture(t, Config{})
	f.createBucket(t)
	if err := f.server.Serve(f.listener); err == nil {
		t.Fatal("duplicate Serve accepted")
	}
	_, err := f.kv.GetBucket(f.ctx, f.bucketRef())
	requireOK(t, err)
}

// TestACKSignedIdentityMismatch 校验签名以后仍须核对描述身份与原生回复地址，跨租户地址禁止确认。
func TestACKSignedIdentityMismatch(t *testing.T) {
	f := openFixture(t, Config{})
	ctx := contextWithTenant(context.Background(), f.tenant)
	a, err := addressFor(ctx, f.scope)
	requireOK(t, err)
	stream := a.names + "messages"
	subject := "$JS.ACK." + stream + ".reader.1.1.1." + strconv.FormatInt(time.Now().UnixNano(), 10) + ".0"
	token, err := f.server.service.signACK(ctx, stream, "reader", subject)
	requireOK(t, err)
	_, err = f.server.service.verifyACK(ctx, token)
	requireOK(t, err)
	for _, identity := range [][2]string{
		{stream + "other", "reader"},
		{stream, "other"},
	} {
		token, err := f.server.service.signACK(ctx, identity[0], identity[1], subject)
		requireOK(t, err)
		_, err = f.server.service.verifyACK(ctx, token)
		requireCode(t, err, codes.PermissionDenied)
	}
	other, err := addressFor(contextWithTenant(ctx, uuid.New()), f.scope)
	requireOK(t, err)
	foreign := other.names + "messages"
	subject = "$JS.ACK." + foreign + ".reader.1.1.1." + strconv.FormatInt(time.Now().UnixNano(), 10) + ".0"
	token, err = f.server.service.signACK(ctx, foreign, "reader", subject)
	requireOK(t, err)
	_, err = f.server.service.verifyACK(ctx, token)
	requireCode(t, err, codes.PermissionDenied)
}

// TestCacheLeaderCancellation 首个请求取消只停止自己的等待，不取消共享回源。
func TestCacheLeaderCancellation(t *testing.T) {
	ctx, records, token, auth := cachedAuthFixture(t, 0, 2)
	blocked := &blockingTokens{
		APITokenRepository: records,
		started:            make(chan struct{}),
		release:            make(chan struct{}),
	}
	auth.cache.APITokenRepository = blocked
	leader, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := auth.Authenticate(leader, token.Token)
		done <- err
	}()
	select {
	case <-blocked.started:
	case <-ctx.Done():
		t.Fatal("database read did not start")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader cancellation lost: %v", err)
	}
	waiter, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	_, err := auth.Authenticate(waiter, token.Token)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("leader canceled shared query: %v", err)
	}
	close(blocked.release)
	_, err = auth.Authenticate(ctx, token.Token)
	requireOK(t, err)
	if records.tokenReads.Load() != 1 {
		t.Fatal("shared query restarted")
	}
}

// TestCacheShutdownCancelsLoads 关闭取消并等待共享回源，释放缓存后拒绝新回源。
func TestCacheShutdownCancelsLoads(t *testing.T) {
	ctx, records, token, auth := cachedAuthFixture(t, 0, 2)
	blocked := &blockingTokens{
		APITokenRepository: records,
		started:            make(chan struct{}),
		release:            make(chan struct{}),
	}
	auth.cache.APITokenRepository = blocked
	done := make(chan error, 1)
	go func() {
		_, err := auth.Authenticate(ctx, token.Token)
		done <- err
	}()
	select {
	case <-blocked.started:
	case <-ctx.Done():
		t.Fatal("database read did not start")
	}
	auth.cache.close()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("query remained active: %v", err)
	}
	_, err := auth.Authenticate(ctx, token.Token)
	if err == nil || records.tokenReads.Load() != 0 || len(auth.cache.loadSlots) != 0 {
		t.Fatal("closed cache admitted a query or retained a reservation")
	}
}

// TestRepublishTenantIsolation 同名 stream/subject 在不同租户隔离，转发仍落在本租户。
func TestRepublishTenantIsolation(t *testing.T) {
	f := openFixture(t, Config{})
	other := contextWithTenant(context.Background(), uuid.New())
	create := func(ctx context.Context, republish bool) {
		var forwarding *pb.NatsRePublish
		if republish {
			forwarding = &pb.NatsRePublish{
				Source:      ptr("assets.>"),
				Destination: ptr("forward.>"),
			}
		}
		_, err := f.server.service.CreateOrUpdateStream(ctx, &pb.StreamConfigRequest{
			Scope: f.scope,
			Config: &pb.NatsStreamConfig{
				Name:      ptr("origin"),
				Subjects:  []string{"assets.>"},
				RePublish: forwarding,
			},
		})
		requireOK(t, err)
		_, err = f.server.service.CreateOrUpdateStream(ctx, &pb.StreamConfigRequest{
			Scope: f.scope,
			Config: &pb.NatsStreamConfig{
				Name:     ptr("copy"),
				Subjects: []string{"forward.>"},
			},
		})
		requireOK(t, err)
	}
	own := contextWithTenant(context.Background(), f.tenant)
	create(own, true)
	create(other, true)
	_, err := f.server.service.Publish(own, &pb.PublishRequest{
		Scope: f.scope,
		Message: &pb.Message{
			Subject: "assets.one",
			Data:    []byte("value"),
		},
	})
	requireOK(t, err)
	request := &pb.StreamInfoRequest{Stream: &pb.StreamRef{
		Scope:  f.scope,
		Stream: "copy",
	}}
	eventually(t, func() bool {
		info, err := f.server.service.GetStream(own, request)
		return err == nil && info.State.GetMsgs() == 1
	})
	info, err := f.server.service.GetStream(other, request)
	requireOK(t, err)
	if info.State.GetMsgs() != 0 {
		t.Fatal("republish crossed tenant boundary")
	}
}
