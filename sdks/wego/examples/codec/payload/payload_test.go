package payload

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestTransformsBoundedAndAuthenticated 检验实际风险：压缩膨胀、密文篡改和跨方法解密
func TestTransformsBoundedAndAuthenticated(t *testing.T) {
	ctx := context.Background()
	compressed, err := (&Gzip{}).Encode(ctx, "/service/Read", bytes.Repeat([]byte("a"), 2<<20))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (&Gzip{}).DecodeLimit(ctx, "/service/Read", compressed, 1<<20); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("gzip expansion bypassed budget: %v", err)
	}
	encrypted, err := NewAESGCM(bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encrypted.Encode(ctx, "/service/Read", []byte("message"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encrypted.Decode(ctx, "/service/Write", encoded); status.Code(err) != codes.DataLoss {
		t.Fatalf("method authentication bypassed: %v", err)
	}
	encoded[len(encoded)-1] ^= 1
	if _, err := encrypted.Decode(ctx, "/service/Read", encoded); status.Code(err) != codes.DataLoss {
		t.Fatalf("tampering accepted: %v", err)
	}
}

// TestS3SignedScopedAndBounded 使用受控 HTTP S3 响应检查签名、目录边界和下载预算
// 真实 MinIO 的兼容性、RPC 与恢复由 TestMinIOCodec 验收，单元测试不依赖 Docker
func TestS3SignedScopedAndBounded(t *testing.T) {
	// stored 模拟一个对象；mu 保护 HTTP 服务端和故障注入方的并发访问
	var mu sync.Mutex
	// stored 是服务端当前返回的实际内容，故障注入可以让它与引用声明不同
	var stored []byte
	// reads 证明非法引用在请求 S3 之前被拒绝，unsigned 记录签名缺失
	var reads atomic.Int32
	// unsigned 标记任何没有使用 SigV4 的请求，供测试结束时检查
	var unsigned atomic.Bool
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
			unsigned.Store(true)
		}
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/fixture-bucket/codec-test/") {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			stored, _ = io.ReadAll(r.Body)
		case http.MethodGet:
			reads.Add(1)
			_, _ = w.Write(stored)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer fixture.Close()
	objects, err := NewS3(context.Background(), Config{
		Endpoint: fixture.URL, Region: "us-east-1", Bucket: "fixture-bucket",
		AccessKey: "fixture-access", SecretKey: "fixture-secret",
	}, "codec-test/")
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	ref, err := objects.Encode(context.Background(), "/service/Read", []byte("ten bytes!"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := objects.Decode(context.Background(), "/service/Read", ref)
	if err != nil || string(got) != "ten bytes!" || unsigned.Load() {
		t.Fatalf("signed roundtrip: %q, unsigned=%t, error=%v", got, unsigned.Load(), err)
	}
	before := reads.Load()
	if _, err := objects.DecodeLimit(context.Background(), "/service/Read", ref, 4); status.Code(err) != codes.ResourceExhausted || reads.Load() != before {
		t.Fatalf("reference budget caused I/O: %v", err)
	}
	// malformed 指向当前目录之外；允许的摘要和大小也不能绕过目录限制
	var malformed reference
	if err := json.Unmarshal(ref, &malformed); err != nil {
		t.Fatal(err)
	}
	malformed.Key = "other/" + malformed.SHA256
	bad, _ := json.Marshal(malformed)
	if _, err := objects.Decode(context.Background(), "/service/Read", bad); status.Code(err) != codes.DataLoss || reads.Load() != before {
		t.Fatalf("cross-prefix request escaped: %v", err)
	}
	// 后端谎报引用大小：实际响应大于预算必须在读取中拒绝，不靠元数据检查
	mu.Lock()
	stored = bytes.Repeat([]byte{1}, 64)
	mu.Unlock()
	if _, err := objects.DecodeLimit(context.Background(), "/service/Read", ref, 16); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("actual download bypassed budget: %v", err)
	}
	// 同样大小的坏内容不会触发大小限制，但 SHA256 必须识别它
	mu.Lock()
	stored = bytes.Repeat([]byte{1}, 10)
	mu.Unlock()
	if _, err := objects.Decode(context.Background(), "/service/Read", ref); status.Code(err) != codes.DataLoss {
		t.Fatalf("corrupt object accepted: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := objects.Decode(canceled, "/service/Read", ref); !errors.Is(err, context.Canceled) {
		t.Fatalf("S3 did not preserve cancellation: %v", err)
	}
}

// TestReferenceMalformed 拒绝尾随 JSON、未知字段和不支持的格式，不让它们进入网络层
func TestReferenceMalformed(t *testing.T) {
	objects := &Objects{prefix: "codec-test/"}
	for _, input := range []string{
		"{}", "{", "{} {}", `{"v":1,"url":"http://other/object"}`,
		strings.Repeat("x", 1025),
	} {
		t.Run(input[:min(len(input), 40)], func(t *testing.T) {
			if _, err := objects.parseReference([]byte(input), 128); status.Code(err) != codes.DataLoss {
				t.Fatalf("malformed reference accepted: %v", err)
			}
		})
	}
}
