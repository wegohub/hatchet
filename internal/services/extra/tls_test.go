package extra

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	pb "github.com/hatchet-dev/hatchet/internal/services/extra/proto/v1"
)

// TestTLS 验证标准 gRPC TLS 传输可以承载普通调用和 server streaming。
func TestTLS(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	requireOK(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	cert, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	requireOK(t, err)
	raw, err := x509.MarshalPKCS8PrivateKey(key)
	requireOK(t, err)
	pair, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert,
	}), pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: raw,
	}))
	requireOK(t, err)
	f := openFixture(t, Config{TLS: &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{pair},
	}})
	f.createStream(t)
	f.publish(t, "encrypted transport", "one")
	streams, err := f.streams.ListStreams(f.ctx, &pb.StreamListRequest{Scope: f.scope})
	requireOK(t, err)
	info, err := streams.Recv()
	requireOK(t, err)
	if info.Config.GetName() != "messages" {
		t.Fatal(info)
	}
}
