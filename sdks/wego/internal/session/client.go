package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/wire"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// Client 实现 grpc.ClientStream；两方向独立流控，终态不代表输出已经消费完成。
type Client struct {
	// backend 内部后端接口；业务层不能取出其具体实现。
	backend ports.Backend
	// config 当前实例使用的配置快照。
	config spec.Runtime
	// method 当前 RPC 方法或绑定信息；完整方法名用于查询投影、handler 与任务名称。
	method string
	// description gRPC 服务或流描述，决定 handler 与请求类型的绑定。
	description *grpc.StreamDesc
	// id 当前资源的唯一身份，用于查找对应运行或会话。
	id string
	// owner 拥有当前资源或会话的对象；流会话后续控制任务必须回到同一 owner。
	owner string
	// runID 会话业务任务的运行身份，用于订阅输出和取消。
	runID string

	// 生命周期和状态通知；所有窗口及终态字段由 mu 保护。
	ctx context.Context
	// cancel 取消当前对象所属的执行上下文。
	cancel context.CancelFunc
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁。
	mu sync.Mutex
	// changeSignal 在同一把 mu 下合并状态变化；仅实际等待时创建广播通道。
	changeSignal
	// ready READY 握手 nonce 通道，避免旧握手响应被新连接接收。
	ready chan string

	// gRPC 元数据与 CallOption 写回状态。
	headersReady chan struct{}
	// headers 响应头缓存；调用方读取时返回副本。
	headers metadata.MD
	// headersSeen 是否已经接收响应头，避免重复 HEADER 再次关闭通知。
	headersSeen bool
	// headerOptionsApplied 是否已将响应头写入 grpc.Header 选项指定的目标。
	headerOptionsApplied bool
	// trailers 响应尾部 metadata，随最终状态交付。
	trailers metadata.MD

	// 发送窗口：累计 ACK 后释放 pending 中的字节。
	sent uint64
	// acked 发送方向已经收到的累计确认序号；ACK=3 表示 1、2、3 均已消费。
	acked uint64
	// sendBytes 发送窗口中尚未确认的总字节数。
	sendBytes int
	// pending 序号到未确认字节成本的映射；ACK=3 后释放序号 1～3 的成本。
	pending map[uint64]int
	// sendClosed 是否已半关闭输入方向；之后 SendMsg 不再接受消息。
	sendClosed bool

	// 接收窗口：received 是已见最大序号，consumed 是已连续交付序号。
	received uint64
	// consumed 按序交付给业务的最后序号；收到 2 而缺少 1 时不能推进。
	consumed uint64
	// inputBytes 接收缓冲中保存的消息字节总数。
	inputBytes int
	// messages 输出序号到 protobuf 字节的缓存，支持窗口内乱序到达。
	messages map[uint64][]byte

	// 任务终态与订阅终态独立，不能用其中一个代替全部输出交付完成。
	final *wire.StreamResult
	// finalAt 首次观察到终态的时间，用于检测终态之后仍未填补的输出缺口。
	finalAt time.Time
	// responseRead client stream 的最终 unary 响应是否已交付。
	responseRead bool
	// failure 首个协议或传输故障；清理产生的取消不能覆盖它。
	failure error
	// subscriptionClosed 输出订阅是否已经结束，用于辨别正常完成与断流。
	subscriptionClosed bool
	// opts 当前入口使用的原生 gRPC 选项。
	opts []grpc.CallOption
	// cleanupDone 会话清理完成通知，用于保证关闭后不残留订阅 goroutine。
	cleanupDone chan struct{}
}

// NewClient 创建独立流客户端，完成握手与订阅；未额外登记实例调用计数，结束钩子为空操作。
func NewClient(
	ctx context.Context,
	backend ports.Backend,
	config spec.Runtime,
	description *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (*Client, error) {
	return NewClientManaged(ctx, backend, config, description, method, func(error) {}, opts...)
}

// NewClientManaged 执行 START → RUN → PING/READY → OPEN 握手。
// READY 通过输出订阅返回，证明订阅已建立；closed 在取消控制清理完成后才释放调用计数。
func NewClientManaged(
	ctx context.Context,
	backend ports.Backend,
	config spec.Runtime,
	description *grpc.StreamDesc,
	method string,
	closed func(error),
	opts ...grpc.CallOption,
) (out *Client, resultErr error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return nil, err
	}

	id := hex.EncodeToString(data)
	derived, cancel := context.WithCancel(ctx)
	c := &Client{
		backend:      backend,
		config:       config,
		method:       method,
		description:  description,
		id:           id,
		ctx:          derived,
		cancel:       cancel,
		ready:        make(chan string, 8),
		headersReady: make(chan struct{}),
		cleanupDone:  make(chan struct{}),
		pending:      make(map[uint64]int, initialWindowCapacity(config.Stream.Window)),
		messages:     make(map[uint64][]byte, initialWindowCapacity(config.Stream.Window)),
		opts:         opts,
	}
	// 请求 metadata 保留多值与 -bin 原始字节，不能按 JSON 文本解释二进制值。
	md, _ := metadata.FromOutgoingContext(ctx)
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	envelope := wire.Envelope{Version: wire.Version, Metadata: md.Copy(), Trace: carrier}
	if deadline, ok := ctx.Deadline(); ok {
		envelope.Deadline = deadline.UnixNano()
	}
	handshake, stop := context.WithTimeout(derived, config.Stream.HandshakeTimeout)
	defer stop()
	started := time.Now()
	defer func() {
		c.logLifecycle("流握手完成", started, resultErr)
	}()

	ref, err := backend.Run(handshake, StartName(method), Control{
		StreamID: id,
		Method:   method,
		Kind:     "START",
		Input:    envelope,
	}, model.RunOptions{})
	if err != nil {
		cancel()
		return nil, err
	}

	result, err := ref.Wait(handshake)
	if err != nil {
		cancel()
		return nil, err
	}

	// ack 握手控制任务的确认，读取 owner 后附加必需亲和性标签。
	var ack Acknowledgment
	if err = single(result.Outputs, &ack); err != nil {
		cancel()
		return nil, err
	}

	c.owner = ack.Owner
	c.logLifecycle("流 owner 已分配", started, nil)
	// initialized 在启动路径退出时发布后台操作集合，清理不能与 Add 并发。
	initialized := make(chan struct{})
	// background 确认结果等待和输出订阅都已经释放传输资源。
	var background sync.WaitGroup
	go func() {
		defer close(c.cleanupDone)

		<-derived.Done()
		cleanupStarted := time.Now()
		c.mu.Lock()
		// finished 终态已到且最后输出序号全部消费的快照；只收到终态但有缺口不能判为成功。
		finished := c.final != nil && c.final.LastSeq == c.consumed
		c.mu.Unlock()
		if !finished {
			c.cancelControl()
		}
		<-initialized
		background.Wait()
		c.logLifecycle("流传输清理完成", cleanupStarted, c.completionError())
		closed(c.completionError())
	}()
	defer func() {
		if resultErr != nil {
			c.cancel()
			<-c.cleanupDone
		}
	}()

	// 先发布后台集合，再等待清理屏障，失败启动也不会互相等待。
	defer close(initialized)

	ref, err = backend.Run(handshake, binding.Name(method)+"-session", Control{StreamID: id, Method: method, Kind: "RUN"}, c.options())
	if err != nil {
		c.abort(err)
		return nil, err
	}

	c.runID = ref.ID
	c.logLifecycle("流 RUN 已提交", started, nil)
	// 两个后台操作必须在关闭流程开始等待之前完整登记。
	background.Add(2)
	go func() {
		defer background.Done()
		err := backend.Stream(derived, ref.ID, c.receiveFrame)
		c.mu.Lock()
		c.subscriptionClosed = true
		if err != nil && derived.Err() == nil && c.failure == nil {
			c.failure = err
			if status.Code(err) == codes.Unknown {
				c.failure = status.Error(codes.Unavailable, err.Error())
			}
		}
		c.notify()
		c.mu.Unlock()
		if err != nil && derived.Err() == nil {
			c.cancel()
		}
	}()
	go func() {
		defer background.Done()
		result, err := ref.Wait(derived)
		if err != nil {
			c.abort(waitError(derived, err))
			return
		}

		// final 会话最终响应，包含 LastSeq、状态与响应 metadata；不能绕过输出缺口直接成功。
		var final wire.StreamResult
		if err = single(result.Outputs, &final); err != nil {
			c.abort(err)
			return
		}

		c.mu.Lock()
		c.final = &final
		c.finalAt = time.Now()
		c.trailers = wire.FromMetadata(final.Trailers)
		// 仅首次收到响应头时保存副本并关闭通知；重复 HEADER 不得二次 close。
		if !c.headersSeen {
			c.headers = wire.FromMetadata(final.Headers)
			c.headersSeen = true
			close(c.headersReady)
		}
		c.notify()
		c.mu.Unlock()
	}()
	if err = c.openWhenReady(handshake); err != nil {
		return nil, c.failCall(err)
	}
	return c, nil
}

// single 将控制任务唯一输出解码到目标；缺失或多输出不能作为握手成功。
func single(values map[string]any, target any) error {
	if len(values) != 1 {
		return status.Error(codes.Internal, "wego: standalone result missing")
	}

	for _, value := range values {
		return Decode(value, target)
	}
	return nil
}

// 编译期接口或签名检查，确保适配对象可以被标准 gRPC 或 wego 入口使用。
var _ grpc.ClientStream = (*Client)(nil)
