package payload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ioBudget 给单次 S3 操作独立上限；外层 context 的更短截止时间仍然优先
const ioBudget = 10 * time.Second

// prefixPattern 只允许显式测试目录，禁止通过 ../ 或绝对路径读取其他对象
var prefixPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9/_-]{0,120}/$`)

// Config 保存 S3 连接配置；示例不打印此结构，避免泄露凭证
type Config struct {
	// Endpoint 使用完整 URL，例如 http://127.0.0.1:9000；只用于初始化客户端
	Endpoint string
	// Region 参与 SigV4 签名，本机 MinIO 使用 us-east-1
	Region string
	// Bucket 是所有引用允许访问的唯一存储桶，不从消息读取
	Bucket string
	// AccessKey, SecretKey 来自调用方环境，不写入对象引用和验收报告
	AccessKey, SecretKey string
}

// ConfigFromEnv 读取标准 S3 配置；凭证不设公共默认值，缺失时构造明确失败
func ConfigFromEnv() Config {
	return Config{
		Endpoint:  envDefault("WEGO_S3_ENDPOINT", "http://127.0.0.1:9000"),
		Region:    envDefault("AWS_REGION", "us-east-1"),
		Bucket:    envDefault("WEGO_S3_BUCKET", "wego-codec"),
		AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"), SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}
}

// envDefault 只为公开地址和资源配置默认值，不处理令牌或密钥
func envDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// Objects 将变换后的字节上传 S3 并返回有界引用，流控制帧和业务帧使用同一实现
type Objects struct {
	// client 使用 AWS S3 SDK；endpoint 与 bucket 固定，不信任载荷中的外部地址
	client *s3.Client
	// httpClient 只属于此示例，结束时释放其空闲连接
	httpClient *http.Client
	// bucket, prefix 限制此轮允许访问的对象范围，例如 codec-<UUID>/
	bucket, prefix string
	// uploaded, downloaded 统计成功的实际 S3 往返，可被并发 handler 安全读取
	uploaded, downloaded atomic.Int64
}

// reference 是持久引用格式；不包含 URL、凭证、任意 bucket 或可执行指令
type reference struct {
	// Version 校验引用格式，拒绝把未知格式当作原始业务数据
	Version int `json:"v"`
	// Key 只能是本实例 prefix 下的内容摘要对象名
	Key string `json:"key"`
	// Size 限制下载预算并检查截断；仍需对实际读取应用硬上限
	Size int `json:"size"`
	// SHA256 校验实际对象字节，避免被篡改后进入下一层 codec
	SHA256 string `json:"sha256"`
}

// NewS3 使用 BaseEndpoint 和路径寻址连接 MinIO，检查或创建专用示例桶
// 桶创建仅是示例初始化；实际应用可预先建桶并移除此处的创建权限
func NewS3(ctx context.Context, config Config, prefix string) (*Objects, error) {
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint == nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return nil, status.Error(codes.InvalidArgument, "codec: invalid S3 endpoint")
	}
	if config.AccessKey == "" || config.SecretKey == "" || config.Bucket == "" || config.Region != "us-east-1" || !prefixPattern.MatchString(prefix) {
		return nil, status.Error(codes.InvalidArgument, "codec: S3 credentials, bucket, us-east-1 region and scoped prefix required")
	}
	// transport 独立克隆，关闭此示例不会影响进程中其他 HTTP 客户端
	transport := &http.Transport{
		Proxy:             http.ProxyFromEnvironment,
		DialContext:       (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, MaxIdleConns: 32, MaxIdleConnsPerHost: 16,
		IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 5 * time.Second,
	}
	httpClient := &http.Client{Transport: transport, Timeout: ioBudget}
	client := s3.NewFromConfig(aws.Config{
		Region: config.Region, Credentials: credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, ""),
		HTTPClient: httpClient, RetryMaxAttempts: 3,
	}, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(config.Endpoint)
		options.UsePathStyle = true
	})
	objects := &Objects{client: client, httpClient: httpClient, bucket: config.Bucket, prefix: prefix}
	budget, cancel := context.WithTimeout(ctx, ioBudget)
	defer cancel()
	if _, err := client.HeadBucket(budget, &s3.HeadBucketInput{Bucket: &config.Bucket}); err != nil {
		// 仅不存在时创建；鉴权失败或连接错误不能伪装成首次建桶
		var apiError smithy.APIError
		if !errors.As(err, &apiError) || (apiError.ErrorCode() != "NotFound" && apiError.ErrorCode() != "NoSuchBucket") {
			objects.Close()
			return nil, fmt.Errorf("codec: S3 bucket check: %w", err)
		}
		_, err = client.CreateBucket(budget, &s3.CreateBucketInput{Bucket: &config.Bucket})
		if err != nil && (!errors.As(err, &apiError) || apiError.ErrorCode() != "BucketAlreadyOwnedByYou") {
			objects.Close()
			return nil, fmt.Errorf("codec: S3 bucket creation: %w", err)
		}
	}
	return objects, nil
}

// Close 释放连接池；不删除仍被持久日志引用的对象
func (p *Objects) Close() { p.httpClient.CloseIdleConnections() }

// CodecID 标识引用格式，不包含随机对象名或凭证
func (*Objects) CodecID() string { return "example.s3-object.sha256.v1" }

// Encode 使用内容摘要命名对象并校验上传；同一冻结密文重复上传指向同一对象
func (p *Objects) Encode(ctx context.Context, _ string, data []byte) ([]byte, error) {
	if len(data) > maxObjectBytes {
		return nil, status.Error(codes.ResourceExhausted, "codec: object exceeds limit")
	}
	digest := sha256.Sum256(data)
	ref := reference{Version: 1, Key: p.prefix + hex.EncodeToString(digest[:]), Size: len(data), SHA256: hex.EncodeToString(digest[:])}
	budget, cancel := context.WithTimeout(ctx, ioBudget)
	defer cancel()
	_, err := p.client.PutObject(budget, &s3.PutObjectInput{
		Bucket: &p.bucket, Key: &ref.Key, Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data))),
		ContentType: aws.String("application/octet-stream"), ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(digest[:])),
	})
	if err != nil {
		return nil, fmt.Errorf("codec: S3 upload: %w", err)
	}
	p.uploaded.Add(1)
	return json.Marshal(ref)
}

// Decode 以绝对预算下载对象；流协议调用 DecodeLimit 使用当前完整帧预算
func (p *Objects) Decode(ctx context.Context, method string, data []byte) ([]byte, error) {
	return p.DecodeLimit(ctx, method, data, maxObjectBytes)
}

// parseReference 拒绝越界、未知字段和尾随 JSON；对象 key 必须和摘要严格对应
func (p *Objects) parseReference(data []byte, limit int) (reference, error) {
	// ref 在全部验证通过后才用于访问 S3，避免坏引用发起额外 I/O
	var ref reference
	if len(data) > 1024 {
		return ref, status.Error(codes.DataLoss, "codec: oversized object reference")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ref); err != nil {
		return ref, status.Error(codes.DataLoss, "codec: malformed object reference")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return ref, status.Error(codes.DataLoss, "codec: trailing object reference")
	}
	digest, err := hex.DecodeString(ref.SHA256)
	if ref.Version != 1 || err != nil || len(digest) != sha256.Size || ref.SHA256 != hex.EncodeToString(digest) || ref.Key != p.prefix+ref.SHA256 || ref.Size < 0 {
		return ref, status.Error(codes.DataLoss, "codec: invalid object identity")
	}
	if limit < 1 || ref.Size > min(limit, maxObjectBytes) {
		return ref, status.Error(codes.ResourceExhausted, "codec: object exceeds decode budget")
	}
	return ref, nil
}

// DecodeLimit 对实际下载也做限制，再检查长度和 SHA256；不存在的历史引用不能当作空历史
func (p *Objects) DecodeLimit(ctx context.Context, _ string, data []byte, limit int) ([]byte, error) {
	ref, err := p.parseReference(data, limit)
	if err != nil {
		return nil, err
	}
	budget, cancel := context.WithTimeout(ctx, ioBudget)
	defer cancel()
	output, err := p.client.GetObject(budget, &s3.GetObjectInput{Bucket: &p.bucket, Key: &ref.Key})
	if err != nil {
		// apiError 保留 S3 的结构化错误码，仅 NoSuchKey 表示历史对象已缺失
		var apiError smithy.APIError
		if errors.As(err, &apiError) && apiError.ErrorCode() == "NoSuchKey" {
			return nil, status.Error(codes.DataLoss, "codec: historical object missing")
		}
		return nil, fmt.Errorf("codec: S3 download: %w", err)
	}
	defer output.Body.Close()
	plain, err := readLimit(budget, output.Body, limit)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(plain)
	if len(plain) != ref.Size || hex.EncodeToString(digest[:]) != ref.SHA256 {
		return nil, status.Error(codes.DataLoss, "codec: object integrity mismatch")
	}
	p.downloaded.Add(1)
	return plain, nil
}

// Counts 返回成功上传与还原次数，例子用它证明并非只执行了本地压缩
func (p *Objects) Counts() (uploaded, downloaded int64) {
	return p.uploaded.Load(), p.downloaded.Load()
}

// ObjectCount 分页统计本轮 prefix 的真实对象，不遍历其他测试或应用目录
func (p *Objects) ObjectCount(ctx context.Context) (int, error) {
	budget, cancel := context.WithTimeout(ctx, ioBudget)
	defer cancel()
	pages := s3.NewListObjectsV2Paginator(p.client, &s3.ListObjectsV2Input{Bucket: &p.bucket, Prefix: &p.prefix})
	count := 0
	for pages.HasMorePages() {
		page, err := pages.NextPage(budget)
		if err != nil {
			return 0, err
		}
		count += len(page.Contents)
	}
	return count, nil
}

// VerifyFailureCases 验证真实 S3 缺失对象、引用越界和预算拒绝；只清理独立探针对象
func (p *Objects) VerifyFailureCases(ctx context.Context) (err error) {
	probe, err := p.Encode(ctx, "probe", []byte("scoped-s3-budget-probe"))
	if err != nil {
		return err
	}
	ref, err := p.parseReference(probe, maxObjectBytes)
	if err != nil {
		return err
	}
	// 此 key 不进入任何任务或日志；清理不会破坏持久恢复引用
	defer func() {
		budget, cancel := context.WithTimeout(context.WithoutCancel(ctx), ioBudget)
		defer cancel()
		_, cleanupErr := p.client.DeleteObject(budget, &s3.DeleteObjectInput{Bucket: &p.bucket, Key: &ref.Key})
		err = errors.Join(err, cleanupErr)
	}()
	if _, err := p.DecodeLimit(ctx, "probe", probe, 4); status.Code(err) != codes.ResourceExhausted {
		return fmt.Errorf("codec: expected bounded download refusal: %v", err)
	}
	// 坏引用的目录不被接受，即使该对象碰巧在同一个桶中存在
	outside := ref
	outside.Key = "other/" + ref.SHA256
	invalid, _ := json.Marshal(outside)
	if _, err := p.Decode(ctx, "probe", invalid); status.Code(err) != codes.DataLoss {
		return fmt.Errorf("codec: expected scoped reference refusal: %v", err)
	}
	// 生成确定不存在的合法引用，GET 应返回历史缺失，而不是成功的零字节消息
	missing := reference{Version: 1, Key: p.prefix + strings.Repeat("0", 64), SHA256: strings.Repeat("0", 64), Size: 1}
	missingBytes, _ := json.Marshal(missing)
	if _, err := p.Decode(ctx, "probe", missingBytes); status.Code(err) != codes.DataLoss {
		return fmt.Errorf("codec: expected historical object missing: %v", err)
	}
	return nil
}
