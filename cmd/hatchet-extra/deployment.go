package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tink-crypto/tink-go/keyset"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/internal/services/extra/proto/v1"
	"github.com/hatchet-dev/hatchet/pkg/config/loader"
)

// deploymentCommand 提供公钥导出和就绪探针，不启动第二个内嵌 NATS。
func deploymentCommand(command string, args []string) error {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	if command == "export-public-key" {
		config := flags.String("config", "", "Hatchet server.yaml file")
		output := flags.String("output", "", "public keyset destination")
		if err := flags.Parse(args); err != nil {
			return err
		}
		if *config == "" || *output == "" || flags.NArg() != 0 {
			return errors.New("config and output files required")
		}
		return exportPublicKey(*config, *output)
	}
	if command != "healthcheck" {
		return errors.New("unknown deployment command")
	}
	address := flags.String("address", "localhost:7080", "TLS gRPC endpoint")
	ca := flags.String("ca", "", "trusted TLS certificate file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *ca == "" || flags.NArg() != 0 {
		return errors.New("CA certificate required")
	}
	return checkHealth(*address, *ca)
}

// exportPublicKey 仅向目标写入无秘密的验签 keyset，运行服务无需接触 Hatchet 私钥。
func exportPublicKey(configPath, outputPath string) error {
	// #nosec G703 -- 配置路径由部署管理员指定，不来自 RPC。
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read Hatchet configuration: %w", err)
	}
	config, err := loader.LoadServerConfigFile(raw)
	if err != nil {
		return errors.New("load Hatchet configuration failed")
	}
	encryption, err := loader.LoadEncryptionSvc(config)
	if err != nil {
		// 配置解析错误可能包含密钥内容，部署输出只说明失败阶段。
		return errors.New("load Hatchet encryption keys failed")
	}
	var buf bytes.Buffer
	if err := encryption.GetPublicJWTHandle().WriteWithNoSecrets(keyset.NewJSONWriter(&buf)); err != nil {
		return fmt.Errorf("export public JWT keyset: %w", err)
	}
	// #nosec G703 -- 目标路径由部署管理员指定；内容仅包含公钥。
	return os.WriteFile(outputPath, []byte(base64.RawStdEncoding.EncodeToString(buf.Bytes())), 0600)
}

// checkHealth 验证 TLS、HTTP/2 和鉴权拦截器可用；探针不携带 token、不访问租户数据。
func checkHealth(address, caPath string) error {
	// #nosec G703 -- CA 路径来自启动配置，不来自 RPC。
	cert, err := os.ReadFile(caPath)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cert) {
		return errors.New("invalid CA certificate")
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
	})))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = pb.NewStreamServiceClient(conn).GetStream(ctx, &pb.StreamInfoRequest{})
	if status.Code(err) != codes.Unauthenticated {
		return fmt.Errorf("expected authentication challenge, got %s", status.Code(err))
	}
	return nil
}
