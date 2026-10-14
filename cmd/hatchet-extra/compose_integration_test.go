//go:build integration

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	pb "github.com/hatchet-dev/hatchet/internal/services/extra/proto/v1"
)

// deployedConnection 连接已部署容器，凭证只读取环境、不写入测试日志。
func deployedConnection(t *testing.T) (*grpc.ClientConn, context.Context) {
	t.Helper()
	address := os.Getenv("EXTRA_TEST_ADDRESS")
	token := os.Getenv("EXTRA_TEST_HATCHET_TOKEN")
	ca := os.Getenv("EXTRA_TEST_CA_FILE")
	if address == "" || token == "" || ca == "" {
		t.Fatal("EXTRA_TEST_ADDRESS, EXTRA_TEST_HATCHET_TOKEN and EXTRA_TEST_CA_FILE required")
	}
	cert, err := os.ReadFile(ca)
	check(t, err)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cert) {
		t.Fatal("invalid CA certificate")
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
	})))
	check(t, err)
	t.Cleanup(func() {
		check(t, conn.Close())
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return conn, metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

// TestDeployedService 验证 TLS Compose 服务的完整 24 RPC 链路与真实 Hatchet token。
func TestDeployedService(t *testing.T) {
	conn, ctx := deployedConnection(t)
	verifyRPCs(t, conn, ctx)
}

// TestDeploymentPersistence 分两阶段运行，中间重启容器，验证消息、ACK 进度、KV 与旧 ACK token。
func TestDeploymentPersistence(t *testing.T) {
	conn, ctx := deployedConnection(t)
	namespace := os.Getenv("EXTRA_TEST_NAMESPACE")
	phase := os.Getenv("EXTRA_TEST_PHASE")
	tokenFile := os.Getenv("EXTRA_TEST_ACK_FILE")
	if namespace == "" || tokenFile == "" || (phase != "write" && phase != "read") {
		t.Fatal("namespace, ACK file and write/read phase required")
	}
	scope := &pb.Scope{Namespace: namespace}
	s := pb.NewStreamServiceClient(conn)
	p := pb.NewPublisherServiceClient(conn)
	c := pb.NewConsumerServiceClient(conn)
	kv := pb.NewKeyValueServiceClient(conn)
	sr := &pb.StreamRef{
		Scope:  scope,
		Stream: "messages",
	}
	cr := &pb.ConsumerRef{
		Stream:   sr,
		Consumer: "reader",
	}
	br := &pb.BucketRef{
		Scope:  scope,
		Bucket: "state",
	}
	kr := &pb.KeyRef{
		Bucket: br,
		Key:    "cursor",
	}
	if phase == "write" {
		_, err := s.CreateOrUpdateStream(ctx, &pb.StreamConfigRequest{
			Scope: scope,
			Config: &pb.NatsStreamConfig{
				Name:     point("messages"),
				Subjects: []string{"assets.>"},
			},
		})
		check(t, err)
		_, err = c.CreateOrUpdateConsumer(ctx, &pb.ConsumerConfigRequest{
			Stream: sr,
			Config: &pb.NatsConsumerConfig{Durable: point("reader")},
		})
		check(t, err)
		for _, value := range []string{
			"confirmed",
			"pending",
		} {
			_, err = p.Publish(ctx, &pb.PublishRequest{
				Scope: scope,
				Message: &pb.Message{
					Subject: "assets.one",
					Data:    []byte(value),
				},
			})
			check(t, err)
		}
		fetch, err := c.Fetch(ctx, &pb.FetchRequest{
			Consumer: cr,
			Batch:    &pb.FetchRequest_MaxMessages{MaxMessages: 2},
			NoWait:   true,
		})
		check(t, err)
		first, err := fetch.Recv()
		check(t, err)
		_, err = c.Ack(ctx, &pb.AckRequest{
			AckToken: first.AckToken,
			Confirm:  true,
		})
		check(t, err)
		second, err := fetch.Recv()
		check(t, err)
		check(t, os.WriteFile(tokenFile, second.AckToken, 0600))
		_, err = fetch.Recv()
		end(t, err)
		_, err = kv.CreateOrUpdateBucket(ctx, &pb.KeyValueConfigRequest{
			Scope:  scope,
			Config: &pb.NatsKeyValueConfig{Bucket: point("state")},
		})
		check(t, err)
		_, err = kv.Put(ctx, &pb.KeyValuePutRequest{
			Key:   kr,
			Value: []byte("cursor-2"),
		})
		check(t, err)
		return
	}
	info, err := c.GetConsumer(ctx, cr)
	check(t, err)
	if info.GetAckFloor().GetStream() != 1 || info.GetNumAckPending() != 1 {
		t.Fatalf("unexpected consumer progress: %v", info)
	}
	stored, err := s.GetMessage(ctx, &pb.GetMessageRequest{
		Stream:   sr,
		Selector: &pb.GetMessageRequest_Sequence{Sequence: 2},
	})
	check(t, err)
	if string(stored.Data) != "pending" {
		t.Fatal("message did not survive restart")
	}
	value, err := kv.Get(ctx, &pb.KeyValueGetRequest{Key: kr})
	check(t, err)
	if string(value.Value) != "cursor-2" {
		t.Fatal("KV did not survive restart")
	}
	ack, err := os.ReadFile(tokenFile)
	check(t, err)
	_, err = c.Ack(ctx, &pb.AckRequest{
		AckToken: ack,
		Confirm:  true,
	})
	check(t, err)
	info, err = c.GetConsumer(ctx, cr)
	check(t, err)
	if info.GetNumAckPending() != 0 || info.GetAckFloor().GetStream() != 2 {
		t.Fatalf("old ACK token not applied: %v", info)
	}
	_, err = kv.DeleteBucket(ctx, br)
	check(t, err)
	_, err = s.DeleteStream(ctx, sr)
	check(t, err)
}
