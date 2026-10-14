package main

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/internal/services/extra/proto/v1"
)

// probeService 模拟鉴权屏障，用于证明探针确实经过 TLS 和 RPC，而非仅检查端口。
type probeService struct {
	pb.UnimplementedStreamServiceServer
}

// GetStream 拒绝无 token 的探针，不返回任何租户业务数据。
func (*probeService) GetStream(context.Context, *pb.StreamInfoRequest) (*pb.NatsStreamInfo, error) {
	return nil, status.Error(codes.Unauthenticated, "token required")
}

// TestDeploymentHealth 验证可信 TLS、无 token 就绪挑战和不合法信任配置。
func TestDeploymentHealth(t *testing.T) {
	fixture := httptest.NewTLSServer(nil)
	pair := fixture.TLS.Certificates[0]
	fixture.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{pair},
	})))
	pb.RegisterStreamServiceServer(server, &probeService{})
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	ca := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: pair.Certificate[0],
	}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := deploymentCommand("healthcheck", []string{
		"--address=" + listener.Addr().String(),
		"--ca=" + ca,
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ca, []byte("invalid certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := checkHealth(listener.Addr().String(), ca); err == nil {
		t.Fatal("invalid certificate accepted")
	}
}

// TestDeploymentArguments 拒绝缺少文件和多余参数，避免初始化误写目标。
func TestDeploymentArguments(t *testing.T) {
	for _, command := range []string{
		"healthcheck",
		"export-public-key",
		"unknown",
	} {
		if err := deploymentCommand(command, nil); err == nil {
			t.Fatalf("%s accepted incomplete arguments", command)
		}
	}
	if err := exportPublicKey(filepath.Join(t.TempDir(), "missing.yaml"), filepath.Join(t.TempDir(), "public.key")); err == nil {
		t.Fatal("missing configuration accepted")
	}
}
