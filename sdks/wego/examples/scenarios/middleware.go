package scenarios

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// gzipPayload gzip 业务载荷变换，仅用于验证压缩往返
type gzipPayload struct{}

// Encode 压缩业务或完整协议字节，例如重复文本压缩后仍通过 Decode 完整还原
func (*gzipPayload) Encode(_ context.Context, _ string, data []byte) ([]byte, error) {
	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
	var out bytes.Buffer
	// w 创建 gzip 业务载荷编码器，关闭后才可使用完整压缩结果
	w := gzip.NewWriter(&out)
	// _, err 接收 w.Write 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	// err 保存资源释放结果，清理错误同样必须报告，不能因业务已成功而忽略
	if err := w.Close(); err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}

// Decode 还原 gzip 载荷并关闭 reader，向上层返回原始字节
func (p *gzipPayload) Decode(ctx context.Context, method string, data []byte) ([]byte, error) {
	return p.DecodeLimit(ctx, method, data, 4<<20)
}

// CodecID 固定压缩格式标识；随机 nonce 或对象地址不进入此标识
func (*gzipPayload) CodecID() string { return "gzip.v1" }

// DecodeLimit 在解压过程中最多读取 limit+1 字节，例如 5 MiB 解压结果在 4 MiB 预算下明确失败
func (*gzipPayload) DecodeLimit(ctx context.Context, _ string, data []byte, limit int) ([]byte, error) {
	// r, err 接收 gzip.NewReader 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	defer r.Close()

	return readPayloadLimit(ctx, r, limit)
}

// encryptedPayload 使用 AEAD 的业务载荷变换，验证加密还原和完整性校验
type encryptedPayload struct {
	// cipher.AEAD 嵌入的接口或实现，委托未覆盖的方法；所属对象只额外实现当前适配规则
	cipher.AEAD
}

// Encode 生成 nonce 并使用 AEAD 加密载荷，nonce 随密文保存
func (p *encryptedPayload) Encode(_ context.Context, method string, data []byte) ([]byte, error) {
	// nonce 分配本段逻辑独占的容器或通知通道，避免与其他调用共享可变状态
	nonce := make([]byte, p.NonceSize())
	// _, err 保存随机字节读取的数量与错误；随机源失败时不得使用未填满的 nonce 或会话身份
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	return p.Seal(nonce, nonce, data, []byte(method)), nil
}

// Decode 按 AEAD 的 nonce 长度拆分密文并校验完整性，验证失败不交给 protobuf 解码
func (p *encryptedPayload) Decode(ctx context.Context, method string, data []byte) ([]byte, error) {
	return p.DecodeLimit(ctx, method, data, 4<<20)
}

// CodecID 描述 AEAD 格式，不包含每条消息随机生成的 nonce
func (*encryptedPayload) CodecID() string { return "aes-gcm.v1" }

// DecodeLimit 在分配明文前根据 AEAD 长度检查预算，并验证当前方法作为附加认证数据
func (p *encryptedPayload) DecodeLimit(ctx context.Context, method string, data []byte, limit int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(data)-p.NonceSize()-p.Overhead() > limit {
		return nil, status.Error(codes.ResourceExhausted, "fixture: decrypted payload exceeds limit")
	}
	if len(data) < p.NonceSize() {
		return nil, fmt.Errorf("encrypted payload truncated")
	}

	return p.Open(nil, data[:p.NonceSize()], data[p.NonceSize():], []byte(method))
}

// objectPayload 受控对象存储卸载变换，把字节存入 fixture 并通过引用恢复
type objectPayload struct {
	// url 受控对象存储或 HTTP fixture 的地址
	url string
	// encoded, decoded 载荷编码与解码次数，用于断言变换确实执行
	encoded, decoded atomic.Int32
}

// Encode 把业务字节交给受控对象 fixture，返回可用于恢复的引用
func (p *objectPayload) Encode(ctx context.Context, method string, data []byte) ([]byte, error) {
	// id 当前资源的唯一身份，用于查找对应运行或会话
	id := make([]byte, 16)
	// _, err 保存随机字节读取的数量与错误；随机源失败时不得使用未填满的 nonce 或会话身份
	if _, err := rand.Read(id); err != nil {
		return nil, err
	}

	// key 把原始字节转换为传输或诊断文本，不改变字节内容
	key := hex.EncodeToString(id)
	// req, err 接收 http.NewRequestWithContext 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, p.url+"/"+key, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	// response, err 接收 http.DefaultClient.Do 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	response.Body.Close()
	if response.StatusCode != 201 {
		return nil, fmt.Errorf("fixture PUT: %d", response.StatusCode)
	}

	p.encoded.Add(1)
	return []byte(key), nil
}

// Decode 根据对象引用读取原始字节，验证卸载后的业务载荷可以往返
func (p *objectPayload) Decode(ctx context.Context, method string, data []byte) ([]byte, error) {
	return p.DecodeLimit(ctx, method, data, 4<<20)
}

// CodecID 稳定标识引用格式，具体对象 key 在编码载荷中保存
func (*objectPayload) CodecID() string { return "fixture-object.v1" }

// DecodeLimit 对对象下载使用当前 context 和字节预算，不先无限下载再检查大小
func (p *objectPayload) DecodeLimit(ctx context.Context, method string, data []byte, limit int) ([]byte, error) {
	if len(data) != 32 {
		return nil, fmt.Errorf("invalid fixture object key")
	}

	// req, err 接收 http.NewRequestWithContext 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url+"/"+string(data), nil)
	if err != nil {
		return nil, err
	}

	// response, err 接收 http.DefaultClient.Do 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	if response.StatusCode != 200 {
		return nil, fmt.Errorf("fixture GET: %d", response.StatusCode)
	}

	p.decoded.Add(1)
	return readPayloadLimit(ctx, response.Body, limit)
}

// readPayloadLimit 读取一个预算内结果；超限字节仅用于判定，不交给后续 codec
func readPayloadLimit(ctx context.Context, input io.Reader, limit int) ([]byte, error) {
	if limit < 1 {
		return nil, status.Error(codes.ResourceExhausted, "fixture: invalid decode budget")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(input, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, status.Error(codes.ResourceExhausted, "fixture: restored payload exceeds limit")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

// Middleware 验证压缩、加密、对象卸载的往返，同时检查调度routing保持可读取 每次使用唯一 namespace，成功断言写入报告后清理可删除资源
func Middleware(ctx context.Context, report *Report) (err error) {
	// storeMu 保护当前 fixture 或缓存，测试并发路径也必须遵守锁规则
	var storeMu sync.Mutex
	// objects 受控对象存储的引用到字节映射，压缩和加密后的业务载荷在此卸载及恢复
	objects := map[string][]byte{}
	// fixture 是受控 HTTP 对象存储服务，按请求保存或读取字节；测试关闭时一并释放监听端口
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		storeMu.Lock()
		defer storeMu.Unlock()

		// 按协议、输入类型或状态分派处理；每个分支只应用与该类型匹配的转换和状态更新
		switch r.Method {
		case http.MethodPut:
			// data, e 读取受控 HTTP 响应字节，用于比较转发数据及顺序
			data, e := io.ReadAll(io.LimitReader(r.Body, (4<<20)+1))
			if e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			if len(data) > 4<<20 {
				http.Error(w, "fixture object exceeds limit", http.StatusRequestEntityTooLarge)
				return
			}
			objects[r.URL.Path] = data
			w.WriteHeader(201)
		case http.MethodGet:
			// data, ok 读取可选值及存在标记，必须在 ok 为 true 时使用该值
			data, ok := objects[r.URL.Path]
			// 缺少所需的可选值、类型或上下文能力，走此入口定义的回退或失败路径，不使用无效值
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(data)
		default:
			w.WriteHeader(405)
		}
	}))
	defer fixture.Close()

	// key 分配当前步骤的结果列表，容量只用于聚合，不代表业务已经成功完成
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		return err
	}

	// block, err 接收 aes.NewCipher 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}

	// gcm, err 接收 cipher.NewGCM 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	// objectsMiddleware 引用当前 HTTP fixture 的卸载中间件，记录真实编码与解码次数
	objectsMiddleware := &objectPayload{url: fixture.URL}
	// clientCalls, serverCalls, clientStreams, serverStreams 并发安全计数器，供测试或观测断言实际执行次数
	var clientCalls, serverCalls, clientStreams, serverStreams atomic.Int32
	// chain 明确列出本段注册、传输或清理选项，应用顺序决定重复字段的覆盖关系
	chain := []middleware.Option{
		middleware.WithPayload(&gzipPayload{}),
		middleware.WithPayload(&encryptedPayload{gcm}),
		middleware.WithPayload(objectsMiddleware),
		middleware.WithUnaryClient(func(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, next grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			clientCalls.Add(1)
			return next(ctx, method, req, reply, conn, opts...)
		}),
		middleware.WithUnaryServer(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			serverCalls.Add(1)
			return next(ctx, req)
		}),
		middleware.WithStreamClient(func(
			ctx context.Context,
			desc *grpc.StreamDesc,
			conn *grpc.ClientConn,
			method string,
			next grpc.Streamer,
			opts ...grpc.CallOption,
		) (grpc.ClientStream, error) {
			clientStreams.Add(1)
			return next(ctx, desc, conn, method, opts...)
		}),
		middleware.WithStreamServer(func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
			serverStreams.Add(1)
			return next(service, stream)
		}),
	}
	// active, peak 并发安全计数器，供测试或观测断言实际执行次数
	var active, peak atomic.Int32
	// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份
	service := &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// n 是此刻实际进入 handler 的并发数，峰值用它验证调度分组仍可读取routing
			n := active.Add(1)
			defer active.Add(-1)

			// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
			for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(75 * time.Millisecond):
			}
			return Reply(ctx, in), nil
		},
	}
	// strategy 本场景需要验证的并发策略，实际执行峰值和终态用于判断其是否生效
	strategy := model.GroupRoundRobin
	// h, err 接收 runtime.WithMiddleware 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	h, err := Start(
		ctx,
		"middleware",
		report,
		service,
		[]runtime.Option{runtime.WithMiddleware(chain...)},
		worker.WithTask(
			pb.UnaryGreeter_SayHello_FullMethodName,
			task.WithConcurrency(model.Concurrency{
				Expression:    "input.routing.group",
				MaxRuns:       pointer(int32(1)),
				LimitStrategy: &strategy,
			}),
		),
	)
	if err != nil {
		return err
	}

	// closed 是否已关闭或禁止接收新工作；已有工作按关闭策略处理
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, h.Close())
		}
	}()

	// wg 等待本组并发步骤结束，资源关闭前必须确认所有已启动步骤退出
	var wg sync.WaitGroup
	// errs 分配当前步骤的结果列表，容量只用于聚合，不代表业务已经成功完成
	errs := make([]error, 4)
	// 逐项处理 errs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// out, e 通过标准生成桩执行 unary，请求和响应使用 protobuf 载荷
			out, e := h.RPC.SayHello(ctx, &pb.Request{
				GroupKey: "same-projected-group",
				Message:  strings.Repeat("compressible protobuf ", 100),
				Count:    int32(i),
			})
			if e == nil && (out.Count != int32(i) || out.Message != strings.Repeat("compressible protobuf ", 100)) {
				e = fmt.Errorf("payload roundtrip mismatch")
			}
			errs[i] = e
			if e == nil {
				report.Add(h, "gzip/AES-GCM/object storage roundtrip; scheduling routing retained", out)
			}
		}()
	}
	wg.Wait()
	if err = errors.Join(errs...); err != nil {
		return err
	}
	if peak.Load() != 1 || clientCalls.Load() != 4 || serverCalls.Load() != 4 {
		return fmt.Errorf("routing/interceptor assertions: peak=%d client=%d server=%d", peak.Load(), clientCalls.Load(), serverCalls.Load())
	}

	closed = true
	if err = h.Close(); err != nil {
		return err
	}
	if err = grpcStreams(ctx, report, []runtime.Option{runtime.WithMiddleware(chain...)}); err != nil {
		return err
	}
	if clientStreams.Load() != 9 || serverStreams.Load() != 8 || objectsMiddleware.encoded.Load() < 42 || objectsMiddleware.decoded.Load() < 42 {
		return fmt.Errorf(
			"stream middleware counters: %d/%d encode/decode %d/%d",
			clientStreams.Load(),
			serverStreams.Load(),
			objectsMiddleware.encoded.Load(),
			objectsMiddleware.decoded.Load(),
		)
	}

	return nil
}
