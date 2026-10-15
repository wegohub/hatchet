package extra

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/hatchet-dev/hatchet/pkg/extra/v1"
)

// TestValidationBeforeIO 使用空后端，证明非法输入在下载/发布等外部 I/O 前被拒绝。
func TestValidationBeforeIO(t *testing.T) {
	ctx := contextWithTenant(context.Background(), uuid.New())
	s := &Service{cfg: defaults(Config{})}
	tests := []struct {
		name string
		call func() error
	}{
		{"nil_publish", func() error {
			_, err := s.Publish(ctx, nil)
			return err
		}},
		{"empty_subject", func() error {
			_, err := s.Publish(ctx, &pb.PublishRequest{Message: &pb.Message{}})
			return err
		}},
		{"reserved_subject", func() error {
			_, err := s.Publish(ctx, &pb.PublishRequest{Message: &pb.Message{Subject: "$SYS.>"}})
			return err
		}},
		{"wildcard_publish", func() error {
			_, err := s.Publish(ctx, &pb.PublishRequest{Message: &pb.Message{Subject: "assets.*"}})
			return err
		}},
		{"reserved_header", func() error {
			_, err := s.Publish(ctx, &pb.PublishRequest{Message: &pb.Message{
				Subject: "assets.one",
				Headers: map[string]*pb.StringValues{"nAtS-Expected-Stream": {Values: []string{"foreign"}}},
			}})
			return err
		}},
		{"newline_header", func() error {
			_, err := s.Publish(ctx, &pb.PublishRequest{Message: &pb.Message{
				Subject: "assets.one",
				Headers: map[string]*pb.StringValues{"label": {Values: []string{"a\r\nb"}}},
			}})
			return err
		}},
		{"missing_expected_sequence", func() error {
			_, err := s.Publish(ctx, &pb.PublishRequest{
				Message: &pb.Message{Subject: "assets.one"},
				Options: &pb.PublishOptions{ExpectedLastSubject: ptr("assets.one")},
			})
			return err
		}},
		{"negative_ttl", func() error {
			_, err := s.Publish(ctx, &pb.PublishRequest{
				Message: &pb.Message{Subject: "assets.one"},
				Options: &pb.PublishOptions{MessageTtlNs: ptr(int64(-1))},
			})
			return err
		}},
		{"empty_message_id", func() error {
			_, err := s.Publish(ctx, &pb.PublishRequest{
				Message: &pb.Message{Subject: "assets.one"},
				Options: &pb.PublishOptions{MessageId: ptr("")},
			})
			return err
		}},
		{"too_many_retries", func() error {
			_, err := s.Publish(ctx, &pb.PublishRequest{
				Message: &pb.Message{Subject: "assets.one"},
				Options: &pb.PublishOptions{RetryAttempts: ptr(uint32(11))},
			})
			return err
		}},
		{"missing_schedule", func() error {
			_, err := s.Publish(ctx, &pb.PublishRequest{
				Message: &pb.Message{Subject: "assets.one"},
				Options: &pb.PublishOptions{Schedule: &pb.ScheduleOptions{TargetSubject: "assets.one"}},
			})
			return err
		}},
		{"invalid_schedule_time", func() error {
			_, err := s.Publish(ctx, &pb.PublishRequest{
				Message: &pb.Message{Subject: "assets.one"},
				Options: &pb.PublishOptions{Schedule: &pb.ScheduleOptions{
					TargetSubject: "assets.one",
					Schedule:      &pb.ScheduleOptions_At{At: &timestamppb.Timestamp{Seconds: math.MaxInt64}},
				}},
			})
			return err
		}},
		{"missing_message_selector", func() error {
			_, err := s.GetMessage(ctx, &pb.GetMessageRequest{Stream: &pb.StreamRef{Stream: "messages"}})
			return err
		}},
		{"invalid_query_subject", func() error {
			_, err := s.GetStream(ctx, &pb.StreamInfoRequest{
				Stream:        &pb.StreamRef{Stream: "messages"},
				SubjectFilter: ptr("$SYS.>"),
			})
			return err
		}},
		{"nil_stream", func() error {
			_, err := s.GetStream(ctx, &pb.StreamInfoRequest{})
			return err
		}},
		{"nil_kv_key", func() error {
			_, err := s.Get(ctx, &pb.KeyValueGetRequest{})
			return err
		}},
		{"zero_cas_revision", func() error {
			_, err := s.Update(ctx, &pb.KeyValueUpdateRequest{})
			return err
		}},
		{"zero_read_revision", func() error {
			_, err := s.Get(ctx, &pb.KeyValueGetRequest{Revision: ptr(uint64(0))})
			return err
		}},
		{"delete_purge_option", func() error {
			_, err := s.Delete(ctx, &pb.KeyValueDeleteRequest{PurgeTtlNs: ptr(int64(time.Second))})
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requireCode(t, tc.call(), codes.InvalidArgument)
		})
	}
	_, _, _, err := s.preparePublish(ctx, &pb.PublishRequest{Message: &pb.Message{
		Subject: "assets.one",
		Data:    make([]byte, s.cfg.MaxMessageBytes+1),
	}})
	requireCode(t, err, codes.ResourceExhausted)
}

// TestConfigAndAddressValidation 验证预算与寻址边界，不同 namespace 必须得到不同地址。
func TestConfigAndAddressValidation(t *testing.T) {
	cfg := defaults(Config{
		StoreDir: "persistent",
		ACKKey:   make([]byte, 32),
		Authenticator: AuthenticatorFunc(func(context.Context, string) (uuid.UUID, error) {
			return uuid.New(), nil
		}),
	})
	requireOK(t, validateConfig(cfg))
	cases := []func(*Config){
		func(c *Config) {
			c.ACKKey = nil
		}, func(c *Config) {
			c.Authenticator = nil
		}, func(c *Config) {
			c.StoreDir = ""
		},
		func(c *Config) {
			c.MaxMessageBytes = -1
		}, func(c *Config) {
			c.MaxFetchMessages = -1
		}, func(c *Config) {
			c.MaxAsyncInFlight = 4097
		},
		func(c *Config) {
			c.MaxFetchBytes = 1
		}, func(c *Config) {
			c.MaxStoreBytes = -1
		},
		func(c *Config) {
			c.AuthCacheTTL = -time.Minute
		},
		func(c *Config) {
			c.MaxAuthCacheEntries = -1
		}, func(c *Config) {
			c.MaxAuthCacheEntries = 1_000_001
		},
	}
	for i, mutate := range cases {
		c := cfg
		mutate(&c)
		if validateConfig(c) == nil {
			t.Fatalf("accepted invalid config %d", i)
		}
	}
	for _, s := range []string{
		"",
		"a.b",
		"$SYS",
		"has space",
		"name/with/slash",
	} {
		if validName(s) {
			t.Fatalf("validName accepted %q", s)
		}
	}
	for _, s := range []string{
		"assets.one",
		"assets/one",
		"assets:one",
	} {
		if !validSubject(s, false) {
			t.Fatalf("rejected subject %q", s)
		}
	}
	for _, s := range []string{
		"assets.*",
		"assets.>",
	} {
		if !validSubject(s, true) || validSubject(s, false) {
			t.Fatalf("wrong wildcard validation %q", s)
		}
	}
	for _, s := range []string{
		"assets.>.one",
		"assets..one",
		"$JS.API",
		"one\nother",
		"foo*",
	} {
		if validSubject(s, true) {
			t.Fatalf("accepted subject %q", s)
		}
	}
	_, err := addressFor(context.Background(), nil)
	requireCode(t, err, codes.Unauthenticated)
	ctx := contextWithTenant(context.Background(), uuid.New())
	a, err := addressFor(ctx, &pb.Scope{Namespace: "one"})
	requireOK(t, err)
	b, err := addressFor(ctx, &pb.Scope{Namespace: "two"})
	requireOK(t, err)
	if a.names == b.names || a.subjects == b.subjects {
		t.Fatal("namespace alias")
	}
	_, err = addressFor(ctx, &pb.Scope{Namespace: "../other"})
	requireCode(t, err, codes.InvalidArgument)
}

// TestImmutableConversion 验证 map、切片和嵌套转换都生成独立快照。
func TestImmutableConversion(t *testing.T) {
	p := &pb.NatsStreamConfig{
		Name:      ptr("messages"),
		Subjects:  []string{"assets.>"},
		Metadata:  map[string]string{"label": "one"},
		Placement: &pb.NatsPlacement{Tags: []string{"zone"}},
	}
	c := decodeStreamConfig(p)
	c.Subjects[0] = "other"
	c.Metadata["label"] = "two"
	c.Placement.Tags[0] = "other"
	if p.Subjects[0] != "assets.>" || p.Metadata["label"] != "one" || p.Placement.Tags[0] != "zone" {
		t.Fatal("native config aliases protobuf request")
	}
	q := encodeStreamConfig(&c)
	q.Subjects[0] = "again"
	q.Metadata["label"] = "again"
	if c.Subjects[0] != "other" || c.Metadata["label"] != "two" {
		t.Fatal("encoded response aliases native state")
	}
}

// TestScopedNestedConfig 验证嵌套 source/mirror 的租户映射与禁止组合。
func TestScopedNestedConfig(t *testing.T) {
	a, err := addressFor(contextWithTenant(context.Background(), uuid.New()), nil)
	requireOK(t, err)
	c := jetstream.StreamConfig{
		Name: "copy",
		Mirror: &jetstream.StreamSource{
			Name:          "source",
			FilterSubject: "assets.>",
		},
	}
	requireOK(t, a.mapStreamConfig(&c))
	if c.Mirror.Name != a.names+"source" || c.Mirror.FilterSubject != a.subjects+"assets.>" {
		t.Fatalf("unmapped source=%v", c.Mirror)
	}
	c = jetstream.StreamConfig{
		Name: "unsafe",
		Sources: []*jetstream.StreamSource{{
			Name:     "source",
			External: &jetstream.ExternalStream{APIPrefix: "$JS.API"},
		}},
	}
	requireCode(t, a.mapStreamConfig(&c), codes.InvalidArgument)
	_, err = watchOptions(&pb.WatchOptions{
		UpdatesOnly:    true,
		IncludeHistory: true,
	})
	requireCode(t, err, codes.InvalidArgument)
	_, err = watchOptions(&pb.WatchOptions{ResumeFromRevision: ptr(uint64(0))})
	requireCode(t, err, codes.InvalidArgument)
}

// TestAuthHeaders 验证重复、缺失和非法 Bearer header，以及认证租户传递。
func TestAuthHeaders(t *testing.T) {
	id := uuid.New()
	good := AuthenticatorFunc(func(context.Context, string) (uuid.UUID, error) {
		return id, nil
	})
	for _, values := range [][]string{
		nil,
		{""},
		{"token"},
		{"Bearer"},
		{"Bearer "},
		{"Basic abc"},
		{"Bearer a b"},
		{"Bearer one", "Bearer two"},
	} {
		md := metadata.MD{}
		md["authorization"] = values
		_, err := authenticate(metadata.NewIncomingContext(context.Background(), md), good)
		requireCode(t, err, codes.Unauthenticated)
	}
	ctx, err := authenticate(metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "bEaReR token")), good)
	requireOK(t, err)
	got, err := tenant(ctx)
	requireOK(t, err)
	if got != id {
		t.Fatal("tenant lost")
	}
	bad := AuthenticatorFunc(func(context.Context, string) (uuid.UUID, error) {
		return uuid.Nil, errors.New("invalid")
	})
	_, err = authenticate(metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer token")), bad)
	requireCode(t, err, codes.Unauthenticated)
}

// TestErrorMapping 验证取消、超时、缺失资源和版本冲突的错误码。
func TestErrorMapping(t *testing.T) {
	tests := []struct {
		err  error
		code codes.Code
	}{
		{context.Canceled, codes.Canceled}, {context.DeadlineExceeded, codes.DeadlineExceeded},
		{jetstream.ErrStreamNotFound, codes.NotFound}, {jetstream.ErrConsumerNotFound, codes.NotFound},
		{jetstream.ErrKeyNotFound, codes.NotFound}, {jetstream.ErrBucketNotFound, codes.NotFound},
		{&jetstream.APIError{Code: 503}, codes.Unavailable},
		{&jetstream.APIError{Code: 429, ErrorCode: 10210}, codes.ResourceExhausted},
	}
	for _, tc := range tests {
		requireCode(t, rpcError(tc.err), tc.code)
	}
	requireCode(t, casError(jetstream.ErrKeyExists), codes.Aborted)
}

// FuzzSubjectAndACK 对非法 subject 和 ACK token 做模糊测试，解析失败不能崩溃。
func FuzzSubjectAndACK(f *testing.F) {
	f.Add("assets.one", []byte("invalid"))
	key := make([]byte, 32)
	s := &Service{cfg: Config{ACKKey: key}}
	ctx := contextWithTenant(context.Background(), uuid.New())
	f.Fuzz(func(t *testing.T, sub string, token []byte) {
		if validSubject(sub, false) && !validSubject(sub, true) {
			t.Fatal("literal subject rejected as filter")
		}
		_, _ = s.verifyACK(ctx, token)
	})
}
