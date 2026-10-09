package scenarios

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	"github.com/hatchet-dev/hatchet/sdks/wego/examples/codec/payload"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// codecRecoveryService 复用标准 client/bidi handler，并让 server stream 用真实对象恢复历史
type codecRecoveryService struct {
	// streamService 提供 UploadHellos 和 ChatHellos 的标准生成桩实现
	streamService
	// attempts 统计实际执行，客户端恢复不能使它增加
	attempts atomic.Int32
}

// WatchHellos 首次发布两条后触发重试；恢复后从历史末条继续，不重复发送已有业务输出
func (s *codecRecoveryService) WatchHellos(in *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	s.attempts.Add(1)
	history, err := task.Checkpoint(stream.Context())
	if err != nil {
		return err
	}
	// next 从经过完整 codec 链还原的有限历史计算，例如已有 0、1 时从 2 继续
	next := int32(0)
	for {
		previous := &pb.Reply{}
		if err := history.Next(previous); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if previous.Count != next || previous.Message != in.Message {
			return status.Error(codes.DataLoss, "codec: restored history differs from business input")
		}
		next++
	}
	if err := stream.SendHeader(metadata.Pairs("codec", "gzip-aes-s3", "proof-bin", string([]byte{0, 255}))); err != nil {
		return err
	}
	info, ok := task.Info(stream.Context())
	if !ok {
		return model.ErrTaskContext
	}
	for i := next; i < in.Count; i++ {
		if err := stream.Send(Reply(stream.Context(), &pb.Request{Message: in.Message, Count: i})); err != nil {
			return err
		}
		if info.RetryCount == 0 && i == 1 {
			return status.Error(codes.Unavailable, "codec: retry after two persisted outputs")
		}
	}
	stream.SetTrailer(metadata.Pairs("recovered", "true"))
	return nil
}

// codecChain 显式组合正向压缩→加密→上传；反向为下载→认证解密→有界解压
func codecChain(key []byte, objects *payload.Objects) ([]middleware.Option, error) {
	encrypted, err := payload.NewAESGCM(key)
	if err != nil {
		return nil, err
	}
	return []middleware.Option{
		middleware.WithPayload(&payload.Gzip{}),
		middleware.WithPayload(encrypted),
		middleware.WithPayload(objects),
	}, nil
}

// MinIOCodec 通过 AWS S3 SDK 验证真实对象卸载，包含四种 RPC、调度、控制帧及两端恢复
// 不加入默认无外部存储的场景集；专用示例和 TestMinIOCodec 共用此入口，不静默 Skip
func MinIOCodec(ctx context.Context, report *Report) (err error) {
	key, err := hex.DecodeString(os.Getenv("WEGO_CODEC_AES_KEY"))
	if err != nil || len(key) != 32 {
		return fmt.Errorf("codec: WEGO_CODEC_AES_KEY must contain a 32-byte hex key")
	}
	config := payload.ConfigFromEnv()
	prefix := "codec-" + uuid.NewString() + "/"
	objects, err := payload.NewS3(ctx, config, prefix)
	if err != nil {
		return err
	}
	defer objects.Close()
	chain, err := codecChain(key, objects)
	if err != nil {
		return err
	}
	// active/peak 检查三次同组并发调用：业务载荷被卸载，但 CEL routing 必须仍然可读
	var active, peak atomic.Int32
	service := &Service{Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(75 * time.Millisecond):
			return Reply(ctx, in), nil
		}
	}}
	streamService := &codecRecoveryService{}
	events := make(chan model.WorkerEvent, 1)
	names := []string{
		pb.UnaryGreeter_SayHello_FullMethodName, pb.UnaryGreeter_WaitHello_FullMethodName, pb.UnaryGreeter_ChildHello_FullMethodName,
	}
	for _, method := range pb.Greeter_ServiceDesc.Methods {
		names = append(names, "/"+pb.Greeter_ServiceDesc.ServiceName+"/"+method.MethodName)
	}
	for _, method := range pb.Greeter_ServiceDesc.Streams {
		names = append(names, "/"+pb.Greeter_ServiceDesc.ServiceName+"/"+method.StreamName)
	}
	strategy := model.GroupRoundRobin
	h, err := StartRegistered(ctx, "minio-codec", report, func(registrar grpc.ServiceRegistrar) {
		pb.RegisterUnaryGreeterServer(registrar, service)
		pb.RegisterGreeterServer(registrar, streamService)
	}, names, []runtime.Option{
		runtime.WithMiddleware(chain...),
		runtime.WithWorkerEventHandler(func(ctx context.Context, event model.WorkerEvent) error {
			select {
			case events <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
	}, worker.WithTask(pb.UnaryGreeter_SayHello_FullMethodName, task.WithConcurrency(model.Concurrency{
		Expression: "input.routing.group", MaxRuns: pointer(int32(1)), LimitStrategy: &strategy,
	})), worker.WithTask(pb.Greeter_WatchHellos_FullMethodName, task.WithRetries(1)))
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, h.Close())
		}
	}()
	text := strings.Repeat("compress-encrypt-offload ", 200)
	// 每次调用提供同一分组，三个 goroutine 真正并发提交，但引擎最多执行一个 handler
	var group sync.WaitGroup
	failures := make([]error, 3)
	for i := range failures {
		group.Add(1)
		go func() {
			defer group.Done()
			out, callErr := h.RPC.SayHello(client.WithRouting(ctx, map[string]any{"group": "codec-group"}), &pb.Request{Message: text, Count: int32(i)})
			if callErr == nil && (out.Message != text || out.Count != int32(i) || out.RunId == "") {
				callErr = fmt.Errorf("codec: unary payload or execution identity mismatch")
			}
			failures[i] = callErr
			if callErr == nil {
				report.Add(h, "S3 unary roundtrip; per-call routing retained under group concurrency", out)
			}
		}()
	}
	group.Wait()
	if err := errors.Join(failures...); err != nil {
		return err
	}
	if peak.Load() != 1 {
		return fmt.Errorf("codec: CEL group concurrency peak=%d", peak.Load())
	}
	rpc := pb.NewGreeterClient(h.Conn)
	// client stream 的三条输入先编码冻结，CloseAndRecv 仅提交一次整批任务
	upload, err := rpc.UploadHellos(ctx)
	if err != nil {
		return err
	}
	for i := int32(0); i < 3; i++ {
		if err := upload.Send(&pb.Request{Message: text, Count: i}); err != nil {
			return err
		}
	}
	response, err := upload.CloseAndRecv()
	if err != nil || response.Count != 3 {
		return fmt.Errorf("codec: upload result %v: %v", response, err)
	}
	report.Add(h, "S3 client stream batch input and terminal response", response)
	// bidi 输入先关闭，再逐条验证通过完整帧 codec 还原的输出
	chat, err := rpc.ChatHellos(ctx)
	if err != nil {
		return err
	}
	for i := int32(0); i < 3; i++ {
		if err := chat.Send(&pb.Request{Message: text, Count: i}); err != nil {
			return err
		}
	}
	if err := chat.CloseSend(); err != nil {
		return err
	}
	for i := int32(0); i < 3; i++ {
		out, err := chat.Recv()
		if err != nil || out.Message != text || out.Count != i {
			return fmt.Errorf("codec: bidi output %d differs: %v", i, err)
		}
		report.Add(h, "S3 bidi batch input and ordered output", out)
	}
	if _, err := chat.Recv(); err != io.EOF {
		return fmt.Errorf("codec: bidi terminal %v", err)
	}
	if err := verifyCodecRecovery(ctx, h, config, prefix, key, text, streamService); err != nil {
		return err
	}
	// 广播和 JOIN 也经过相同的完整帧 codec；无控制 Worker 或额外任务调度
	if err := h.Conn.Workers().BroadcastEvent(ctx, model.WorkerEvent{Type: "codec-probe", Payload: json.RawMessage(`{"storage":"s3"}`)}); err != nil {
		return err
	}
	select {
	case event := <-events:
		if event.Type != "codec-probe" || string(event.Payload) != `{"storage":"s3"}` {
			return fmt.Errorf("codec: broadcast payload mismatch")
		}
		report.Add(h, "S3 JOIN and broadcast EVENT frames decoded by Worker", nil)
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := objects.VerifyFailureCases(ctx); err != nil {
		return err
	}
	report.Add(h, "S3 rejects missing history, cross-prefix references and oversized downloads; probe object deleted", nil)
	// 复用完整标准流矩阵，额外验证零输入、零输出、慢消费、提前返回和业务错误
	if err := grpcStreams(ctx, report, []runtime.Option{runtime.WithMiddleware(chain...)}); err != nil {
		return err
	}
	// 先关闭全部使用者再统计；持久日志引用仍然有效，不能随 Worker 停止删除对象
	closed = true
	if err := h.Close(); err != nil {
		return err
	}
	count, err := objects.ObjectCount(ctx)
	if err != nil {
		return err
	}
	uploaded, downloaded := objects.Counts()
	if count < 20 || uploaded < 20 || downloaded < 20 {
		return fmt.Errorf("codec: real S3 operations insufficient: objects=%d PUT=%d GET=%d", count, uploaded, downloaded)
	}
	report.Add(h, fmt.Sprintf("S3 bucket=%s prefix=%s objects=%d PUT=%d GET=%d; objects retained with run/topic history", config.Bucket, prefix, count, uploaded, downloaded), nil)
	return nil
}

// verifyCodecRecovery 分别验证 Worker 历史恢复与独立客户端断点消费，二者不共享内存输出
func verifyCodecRecovery(ctx context.Context, h *Harness, config payload.Config, prefix string, key []byte, text string, service *codecRecoveryService) (err error) {
	watch, err := pb.NewGreeterClient(h.Conn).WatchHellos(client.WithIdempotencyKey(ctx, "codec-recover"), &pb.Request{Count: 4, Message: text})
	if err != nil {
		return err
	}
	first, err := watch.Recv()
	if err != nil || first.Count != 0 || first.Message != text {
		return fmt.Errorf("codec: first persisted output %v", err)
	}
	checkpoint, err := client.StreamCheckpoint(watch.Context())
	if err != nil {
		return err
	}
	serialized, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	// saved 模拟写入本地存储并重新读取；不依赖原流的 metadata 指针
	var saved model.StreamCheckpoint
	if err := json.Unmarshal(serialized, &saved); err != nil {
		return err
	}
	for i := int32(1); i < 4; i++ {
		out, err := watch.Recv()
		if err != nil || out.Count != i || out.Message != text || out.RunId != saved.RunID {
			return fmt.Errorf("codec: restored attempt output %d: %v", i, err)
		}
	}
	if _, err := watch.Recv(); err != io.EOF || service.attempts.Load() != 2 {
		return fmt.Errorf("codec: retry terminal/attempts=%d: %v", service.attempts.Load(), err)
	}
	header, err := watch.Header()
	if err != nil || len(header.Get("proof-bin")) != 1 || header.Get("proof-bin")[0] != string([]byte{0, 255}) || len(watch.Trailer().Get("recovered")) != 1 {
		return fmt.Errorf("codec: headers/trailers not restored: %v", err)
	}
	report := h.Report
	report.Add(h, "S3 Worker checkpoint restores two historical outputs and completes on second attempt; binary headers/trailers preserved", first)
	// 新建 S3 客户端和 codec 实例，排除复用原连接或对象缓存造成的假恢复
	objects, err := payload.NewS3(ctx, config, prefix)
	if err != nil {
		return err
	}
	defer objects.Close()
	chain, err := codecChain(key, objects)
	if err != nil {
		return err
	}
	options := append(Runtime(h.Namespace), runtime.WithMiddleware(chain...))
	conn, err := client.New(client.WithRuntime(options...))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	stream, err := conn.ResumeStream(ctx, saved)
	if err != nil {
		return err
	}
	for i := int32(1); i < 4; i++ {
		out := &pb.Reply{}
		if err := stream.RecvMsg(out); err != nil || out.Count != i || out.Message != text || out.RunId != saved.RunID {
			return fmt.Errorf("codec: independent resume output %d: %v", i, err)
		}
	}
	if err := stream.RecvMsg(&pb.Reply{}); err != io.EOF || service.attempts.Load() != 2 {
		return fmt.Errorf("codec: resume terminal/attempts=%d: %v", service.attempts.Load(), err)
	}
	report.Add(h, "S3 independent Conn and codec resume serialized checkpoint without another task or handler execution", first)
	return nil
}
