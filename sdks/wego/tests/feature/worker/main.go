package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/log"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/hatchet-dev/hatchet/sdks/wego"
	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware"
	"github.com/hatchet-dev/hatchet/sdks/wego/middleware/payload"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/server"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// NewGreeter 实例化greeter服务
func NewGreeter(instanceName string) pb.GreeterServer {
	return &greeter{
		instanceName: instanceName,
	}
}

// greeter 示例或测试业务服务，用标准 gRPC 注册方式共享 handler
type greeter struct {
	// UnimplementedGreeterServer 按值嵌入，满足生成器的私有兼容性标记
	pb.UnimplementedGreeterServer
	// instanceName 当前实例展示名称，响应据此区分同进程中的多个 Worker。
	instanceName string
}

// SayHello 返回请求响应及实际执行身份，供本机 Worker 调用检查
func (obj *greeter) SayHello(ctx context.Context, request *pb.Request) (*pb.Reply, error) {
	// 自动刷新超时时间
	return task.WithRefreshTimeout(ctx, time.Second*2, func(ctx context.Context) (*pb.Reply, error) {
		_ = log.RInfo(ctx, "start")
		// info, _ 读取实际执行身份，例如 RunID 与 WorkerID；普通网络 context 没有任务身份
		info, _ := task.Info(ctx)

		// 指定消息才触发失败，正常请求仍能验证响应与本次执行身份。
		if request.Message == "test error" {
			return nil, errors.New("test error")
		}

		// 用派生 context 等待，续期失败或本次执行到期时立即结束模拟工作。
		select {
		case <-ctx.Done():
			_ = log.RInfo(ctx, "DONE")
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
		}

		_ = log.RInfo(ctx, "end")

		reply := &pb.Reply{
			Message:    "Processed: " + request.Message + "| " + obj.instanceName,
			RunId:      info.RunID,
			WorkerId:   info.WorkerID,
			RetryCount: int32(info.RetryCount),
		}
		// 子调用返回真实父运行身份；顶层任务与网络入口不伪造父任务。
		if info.ParentRunID != nil {
			reply.ParentRunId = *info.ParentRunID
		}
		return reply, nil
	})
}

// WaitHello 创建 3–10 个普通子任务，持久等待全部结果后返回完整响应集合。
// 子任务数量由可重放时间生成固定随机种子，恢复后不重新抽取数量。
func (obj *greeter) WaitHello(ctx context.Context, request *pb.Request) (*pb.Reply, error) {
	conn, err := task.Conn(ctx)
	if err != nil {
		return nil, err
	}

	// Now 的时间由引擎 memo 保存；本地随机生成器每次重放都读取同一个种子。
	recordedAt, err := task.Now(ctx)
	if err != nil {
		return nil, err
	}
	generator := rand.New(rand.NewPCG(uint64(recordedAt.UnixNano()), 0))
	count := 3 + generator.IntN(8)
	info, _ := task.Info(ctx)

	children := make([]*pb.Reply, count)
	childRunIDs := make([]string, count)

	/*
		原生批量入口示例；与下方生成客户端方式二选一。
		// child-0 等键绑定父运行中的稳定位置；独立 group 避免 SayHello 的幂等策略合并不同子任务。
		inputs := make([]client.RunManyInput, count)
		for i := range inputs {
			key := fmt.Sprintf("child-%d", i)
			inputs[i] = client.RunManyInput{
				Input: &pb.Request{Message: fmt.Sprintf("%s child-%d", request.Message, i+1)},
				Options: []client.RunOption{
					client.WithRunKey(key),
					client.WithRouting(map[string]any{"group": info.RunID + ":" + key}),
				},
			}
		}
		refs, err := conn.RunMany(ctx, pb.Greeter_SayHello_FullMethodName, inputs)
		if err != nil {
			return nil, fmt.Errorf("spawn children: %w", err)
		}
		log.Info(ctx, "children submitted", "count", count)

		// 先批量提交再逐个等待，子任务可并行执行；durable 等待在重放时复用原结果。
		for i := range refs {
			result, err := refs[i].Result(ctx)
			if err != nil {
				return nil, fmt.Errorf("child %s: %w", refs[i].RunID, err)
			}
			children[i] = &pb.Reply{}
			if err := result.IntoContext(ctx, children[i]); err != nil {
				return nil, fmt.Errorf("decode child %s: %w", refs[i].RunID, err)
			}
			childRunIDs[i] = refs[i].RunID
		}
	*/

	// 生成客户端通过 wego Conn 调度子任务；例如 child-0 对应第一个输入，重放复用同一运行。
	childClient := pb.NewGreeterClient(conn)
	childErrors := make([]error, count)
	// group 等待全部子调用结束；每个 goroutine 只写自己的下标，Wait 后才能读取完整集合。
	var group sync.WaitGroup
	log.Info(ctx, "spawning children", "count", count)
	for i := range count {
		group.Go(func() {
			key := fmt.Sprintf("child-%d", i)
			children[i], childErrors[i] = childClient.SayHello(
				ctx,
				&pb.Request{Message: fmt.Sprintf("%s child-%d", request.Message, i+1)},
				client.WithRunOptions(
					client.WithRunKey(key),
					// 独立 group 避免 SayHello 的幂等策略合并不同子任务。
					client.WithRouting(map[string]any{"group": info.RunID + ":" + key}),
				),
			)
			if childErrors[i] != nil {
				childErrors[i] = fmt.Errorf("child %d: %w", i, childErrors[i])
				return
			}
			childRunIDs[i] = children[i].RunId
		})
	}
	group.Wait()
	// 等待所有已发起调用，保留每项错误；单个子任务失败不提前遗留其他结果等待。
	if err := errors.Join(childErrors...); err != nil {
		return nil, err
	}
	log.Info(ctx, "children completed", "count", count)
	return &pb.Reply{
		Message:         "Collected: " + request.Message,
		RunId:           info.RunID,
		WorkerId:        info.WorkerID,
		RetryCount:      int32(info.RetryCount),
		InvocationCount: info.InvocationCount,
		ProcessedAt:     recordedAt.Format(time.RFC3339Nano),
		Count:           int32(len(children)),
		ChildRunIds:     childRunIDs,
		ChildResults:    children,
		Ok:              true,
	}, nil
}

// ChildHello 借用执行连接并以稳定子调用键 greeting 调用 unary 服务。
func (obj *greeter) ChildHello(ctx context.Context, request *pb.Request) (*pb.Reply, error) {
	// conn, err 接收 task.Conn 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	conn, err := task.Conn(ctx)
	if err != nil {
		return nil, err
	}

	return pb.NewUnaryGreeterClient(conn).SayHello(task.WithChildKey(ctx, "greeting"), request)
}

// UploadHellos 收完输入后返回数量；例如两条请求产生 Count=2 和对应 trailer。
func (obj *greeter) UploadHellos(stream grpc.ClientStreamingServer[pb.Request, pb.Reply]) error {
	return task.WithRefreshStreamTimeout(stream.Context(), 5*time.Second, func(ctx context.Context) error {
		// 两条请求得到 Count=2；Recv 阻塞期间仍由原始流 context 管理传输。
		var count int32
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			_, err := stream.Recv()
			if err == io.EOF {
				stream.SetTrailer(metadata.Pairs("count", strconv.Itoa(int(count))))
				return stream.SendAndClose(&pb.Reply{Count: count})
			}
			if err != nil {
				return err
			}
			count++
		}
	})
}

// WatchHellos 连续输出 Count 条响应；例如 Count=3 的输出序号为 0、1、2。
func (obj *greeter) WatchHellos(request *pb.Request, stream grpc.ServerStreamingServer[pb.Reply]) error {
	return task.WithRefreshStreamTimeout(stream.Context(), 5*time.Second, func(ctx context.Context) error {
		if err := stream.SendHeader(metadata.Pairs("mode", "server")); err != nil {
			return err
		}
		for i := int32(0); i < request.Count; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := stream.Send(&pb.Reply{Message: request.Message, Count: i}); err != nil {
				return err
			}
		}
		stream.SetTrailer(metadata.Pairs("count", strconv.Itoa(int(request.Count))))
		return nil
	})
}

// ChatHellos 逐条收发，用同一标准 handler 验证任务与网络流入口
func (obj *greeter) ChatHellos(stream grpc.BidiStreamingServer[pb.Request, pb.Reply]) error {
	return task.WithRefreshStreamTimeout(stream.Context(), 5*time.Second, func(ctx context.Context) error {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			req, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err = stream.Send(&pb.Reply{Message: req.Message, Count: req.Count}); err != nil {
				return err
			}
		}
	})
}

// runSingle 阻塞运行指定展示名称的 Server，上下文取消后排空当前实例。
func runSingle(ctx context.Context, instanceName string) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	// key 与 Conn 使用同一份私有环境配置；启动前验证密钥长度。
	key, keyErr := hex.DecodeString(os.Getenv("WEGO_CODEC_AES_KEY"))
	if keyErr != nil || len(key) != 32 {
		log.Error(ctx, "WEGO_CODEC_AES_KEY requires a 32-byte hex key")
		return
	}
	// encrypted 只处理 value，使用随机 nonce 和 nil AAD。
	encrypted, codecErr := payload.NewAESGCM(key)
	if codecErr != nil {
		log.Error(ctx, "initialize AES codec", "error", codecErr)
		return
	}
	// objects 通过 AWS S3 SDK 连接本机 MinIO；每个 Server 独立拥有 HTTP 连接。
	objects, codecErr := payload.NewS3(ctx, payload.ConfigFromEnv(), "feature/", payload.WithThreshold(1))
	if codecErr != nil {
		log.Error(ctx, "initialize S3 codec", "error", codecErr)
		return
	}
	// compressed 在组件构造时配置压缩阈值，注册后发送策略不可变。
	compressed, codecErr := payload.NewZstd(payload.WithThreshold(1))
	if codecErr != nil {
		objects.Close()
		logger.ErrorContext(ctx, "initialize zstd codec", "error", codecErr)
		return
	}
	// gzipCodec 显式配置组件阈值，当前仅注册为历史消息的解码能力。
	gzipCodec, codecErr := payload.NewGzip(payload.WithThreshold(1))
	if codecErr != nil {
		objects.Close()
		log.Error(ctx, "initialize gzip codec", "error", codecErr)
		return
	}
	// 排空结束后只关闭连接，历史结果和流仍可能引用已上传对象。
	defer objects.Close()
	// 实例化server
	s := wego.NewServer(
		server.WithRuntime(
			runtime.WithLogger(logger),
			// 业务值压缩、加密后上传 MinIO；routing 保持明文供 CEL 调度。
			runtime.WithMiddleware(
				middleware.WithPayload(gzipCodec),
				middleware.WithPayload(compressed),
				middleware.WithPayload(encrypted),
				middleware.WithPayload(objects),
			),
			// 显式选择 zstd→AES-GCM→S3；gzip 仅作为已注册的解码能力
			runtime.WithEncodingCodecs(
				payload.ZstdCodecID,
				payload.AESGCMCodecID,
				payload.S3ObjectCodecID,
			),

			runtime.WithToken(os.Getenv("HATCHET_CLIENT_TOKEN")),
			runtime.WithAddress(os.Getenv("HATCHET_BROKER_ADDR")),
			runtime.WithServerURL(os.Getenv("HATCHET_SERVER_ADDR")),
			runtime.WithTLSConfig(nil),
			runtime.WithSlots(10),
			runtime.WithDurableSlots(10),
			runtime.WithInstanceName(instanceName),
			runtime.WithLogReport(true),
		),
		server.WithWorker(
			worker.WithTask(
				pb.Greeter_SayHello_FullMethodName,
				// 默认会注册同名的event key
				//task.WithEvents("say/hello:invoke"),
				task.WithDefaultFilters(
					// 静态filter, 可通过
					task.DefaultFilter{
						//Scope:      fmt.Sprintf("test-feature:%s", instanceName),
						Scope:      "test-feature",
						Expression: `input.routing.message == payload.routing.target_msg`,
						Payload: model.Input{
							// 如果Expression匹配
							Routing: map[string]any{
								"target_msg": "hello-filter",
							},
							// 就把value带到worker, 通过 task.Info(ctx).FilterPayload 读取
							Value: map[string]any{
								"channel":   "email",
								"recipient": "alerts@example.com",
							},
						},
					},
				),
				// 配置重试次数
				task.WithRetries(2),
				// 重试指数退避, 以2为底，最多10s
				task.WithRetryBackoff(2, 4*time.Second),
				// 配合：
				// 1. 绕过重试逻辑: hatchet.NewNonRetryableError(errors.New("intentional failure"))
				// 2. 自定义重试等待时间: hatchet.NewRetryAfterError(2*time.Second, errors.New("upstream rate limited"))
				// 执行超时配置
				task.WithExecutionTimeout(time.Second*4),
				// 调度超时配置
				task.WithScheduleTimeout(time.Second*300),
				// 并发策略配置(分组公平调度)
				task.WithConcurrency(task.Concurrency{
					Expression:    "input.routing.group",
					MaxRuns:       1,
					LimitStrategy: model.GroupRoundRobin,
					Name:          "test-concurrency",
				}),
				// 同组活跃期间只接受一次触发，运行终结即释放键；60 秒为兜底有效期。
				task.WithIdempotency(model.Idempotency{
					Expression: "input.routing.group",
					Method:     model.IdempotencyMethodStatus,
					TTL:        time.Minute,
				}),
				// 单次运行消耗槽位
				task.WithSlotCost(1),
			),
			worker.WithDurableTask(
				pb.Greeter_WaitHello_FullMethodName,
				task.WithExecutionTimeout(2*time.Minute), // 覆盖子任务执行与结果收集预算。
			),
		),
	)

	// 注册服务
	pb.RegisterGreeterServer(s, NewGreeter(instanceName))

	// 上下文取消时排空已有业务，GracefulStop 完成后 Serve 解除阻塞。
	stopSignal := context.AfterFunc(ctx, s.GracefulStop)
	defer stopSignal()

	err := s.Serve()
	if err != nil {
		log.Error(ctx, "server stopped", "error", err)
		return
	}
}

// main 注册服务并阻塞运行；SDK 不自行接管应用的进程信号
func main() {
	// 应用显式接收进程信号，SDK 通过 Stop 关闭
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	instanceNum, err := strconv.Atoi(os.Getenv("INSTANCE_NUM"))
	if err != nil {
		panic(err)
	}

	// group 等待所有实例的 Serve 和关闭流程完成，主进程不能提前退出。
	var group sync.WaitGroup
	for i := range instanceNum {
		group.Go(func() {
			runSingle(ctx, fmt.Sprintf("feature-%d", i))
		})
	}
	group.Wait()
}
