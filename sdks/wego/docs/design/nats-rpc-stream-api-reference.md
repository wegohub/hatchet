# RPC 流 API 与示例

先读 [主方案](nats-rpc-stream-design.md)。本文按需查阅：历史接口、快照语义与完整用例。原章节编号保留，便于定位已有评审引用。

<a id="s8"></a>

## 8 已发布历史 API

<a id="s8-1"></a>

### 8.1 对外接口

以下是协议 7 的目标接口，尚非已实现 API：

```go
// task 包：读取当前 RPC 两个方向各自的历史。
func ClientStream(ctx context.Context) (model.StreamHistory, error)
func ServerStream(ctx context.Context) (model.StreamHistory, error)

// model 包
type StreamHistory interface {
	Snapshot() HistorySnapshot
	AllMessage() ([]StreamMessage, error)
	Iterator() iter.Seq2[*StreamMessage, error]
	FirstMessage() (*StreamMessage, error)
	LastMessage() (*StreamMessage, error)
	Bloom(key string, opts ...BloomOption) (*StreamBloom, error)
	Contains(key string, value any) (bool, error)
	Close() error
}
```

两个入口返回相同的 model.StreamHistory，所有历史操作都通过返回对象执行：obj.AllMessage()、obj.Iterator()、obj.FirstMessage()、obj.LastMessage()、obj.Bloom(key) 和 obj.Contains(key, value)。方法使用创建句柄时绑定的 Context。

| 入口 | 对应 subject | 在 Worker 侧读取 | 在 Client 侧读取 |
| --- | --- | --- | --- |
| task.ClientStream(ctx) | rpc_xxx_client | 请求历史 | 自己已经发布的请求 |
| task.ServerStream(ctx) | rpc_xxx_server | 自己已经发布的响应 | 响应历史 |

ClientStream/ServerStream 命名固定表示消息方向，不随调用者角色反转。ctx 必须携带当前调用引用，可使用 Worker 的任务 Context，或 Client SDK 返回的 stream.Context()；普通 Context 和原生网络 gRPC Context 返回 ErrStreamContext。

StreamMessage 暴露服务端返回的 Sequence、由调用描述取得的 TypeName、独立的 ProtoJSON Payload，以及 Decode(proto.Message) error。Sequence 是持久位置，不是业务消息计数。只返回 DATA，不把 HEADERS/EOF 当业务消息。First/Last 无 DATA 时返回 ErrNoMessages，AllMessage 返回空 slice，Iterator 直接结束。

这两个对象是对应 subject 的只读历史句柄。它们不执行业务 Send/Recv，不发布消息，不推进或重置方向 consumer 的消费位置。Close 幂等，取消当前句柄的在途读取、释放精确索引及其他缓存，并拒绝后续操作；它不关闭 RPC 或方向 consumer。绑定创建 Context 是此资源句柄的生命周期约定；需要独立 deadline 时用从流 Context 派生的 Context 创建另一个句柄。

<a id="s8-2"></a>

### 8.2 有限快照与懒加载

创建 ClientStream/ServerStream 句柄时，验证当前调用身份和生命周期，读取所选 subject 的持久尾部，取得固定上界 H0。HistorySnapshot 包含 CallID、Direction、UpperSequence、ExpiresAt、Sealed 和 Status；Status 仅已封口时非 nil，包含 EOF 的最终状态。UpperSequence 即 H0。空的新方向对应 H0=0；Snapshot() 直接返回这些元数据，不扫描或预先统计消息数量。

创建句柄只读取并校验尾记录以提取快照元数据，不扫描历史 DATA。首次读取才调用 Extra 的只读 Read：指定精确 subject、起始序号、H0 和单页条数/字节限制；SDK 不创建或删除扫描 consumer。该传输接口是 [Extra 能力计划](hatchet-extra-implementation-plan.md)的前置要求；缺失时初始化报能力错误，不静默回到逐消息查询。

每页在 SDK 有界缓冲中收齐并结束 RPC，再 yield 给业务；下一页从上一页最后一个实际序号加 1 开始，仍以 H0 为上界。服务端用独立 AckNone 扫描 consumer，RPC 结束时删除，异常时空闲回收。服务端扫描 consumer 仍占 JetStream consumer/meta 预算，只是资源责任在 Extra；Read 的独立并发限额不能代替 NATS 总 consumer 限额。业务在两页间停顿不保留服务端扫描资源，超过句柄 Context、扫描期限或恢复截止时间仍报错。H0=0 时不创建读取 RPC。

扫描按服务端持久位置截断到 H0，验证实际 subject、记录类型和有效期限。发布者随后提交的新消息不进入这个快照。普通空批次不能当历史扫描结束；只有读到选定尾记录才完成。遇到 TTL 过期、尾记录不可达、codec 或类型错误时返回错误，不通过连续业务序号判断结束。

Iterator 每次调用创建一次独立、有限的扫描；同一句柄同时只允许一个需要扫描的操作，冲突返回 ErrHistoryBusy；实例并发限额则限制多个句柄的总资源，两者作用不同。for range 中 break 停止后续页请求并释放本页缓冲；Context 取消和 Close 取消在途页。Read 因页上限正常结束不代表整个快照已读完；未到 H0 的空页要核对记录是否仍可达，不能当成功完成。

正常结束不额外 yield io.EOF；失败时 yield(nil, err) 一次后结束。调用者主动 break 只表示提前停止，不证明后面的历史完整。AllMessage 在失败时返回 nil、error，不把半份数据当全部历史；Bloom 也只有完整扫描成功才返回过滤器。

FirstMessage 读取最早 DATA；LastMessage 找 H0 范围内最后一条 DATA，尾部是 EOF 时仍返回最后一条业务消息。首版允许顺序扫描，LastMessage 最坏为 O(n)，不依赖当前 Extra 尚未提供的业务键查询或倒序分页。AllMessage 使用 O(n) 内存，Iterator 与 LastMessage 使用有界批次内存。

同一句柄的所有操作使用相同 H0。再次调用 ClientStream/ServerStream 创建新的独立句柄，重新捕获当时已提交的尾部；已有句柄的边界不变。两个方向的快照各自取得，不构成跨 subject 的原子快照。

历史读取不刷新 writer 的 expected，不是 Flush；Send 返回 nil 但尚未提交的消息不会进入历史。更新快照只需创建新句柄，不需要重新附着调用或创建生产者，更不能通过读到新尾部绕过发布竞争失败。

<a id="s8-3"></a>

### 8.3 Bloom 与精确判断

Bloom(key) 扫描该快照内所有 DATA，用指定 protobuf 字段构建过滤器。key 使用 protobuf 字段名路径，例如 id、order.id；首版支持存在的单值 string、bool、整数、enum、bytes，拒绝 repeated/map、浮点数和缺失字段。查询值按相同 descriptor 转为带类型标记的规范字节，避免数字、字符串及 ProtoJSON 大整数混淆。

默认误判率 0.0001。首版对同一 H0 做两次有界扫描：第一遍统计 DATA 数 n，检查位图预算后分配；第二遍提取 key 并填充。m = ceil(-n × ln(p) / ln(2)^2) 位，k ≈ (m/n) × ln(2) 个哈希。空历史返回有效的空过滤器。这样无需在每条消息或 EOF 中维护累计数量；代价是两遍读取，均计入扫描预算。

```go
func (b *StreamBloom) MayContain(value any) (bool, error)
func (b *StreamBloom) Snapshot() HistorySnapshot
```

- 返回 false：该 key/value 不在已完整扫描的快照内。
- 返回 true：可能存在，需要 Contains 或业务精确索引确认。
- 不覆盖 H0 之后的发布，也不覆盖尚在本地 Send 队列的消息。
- Bloom 不可替代条件发布、NATS 消息去重或业务事务。[Bloom 的误判语义](https://redis.io/docs/latest/develop/data-types/probabilistic/bloom-filter/)

Contains(key, value) 第一次查询某个字段时完整扫描同一 H0，建立规范化键的精确集合；成功后在句柄内复用。使用原始规范字节精确比较，不能把有碰撞可能的短哈希当精确键。建表失败或超出 History.IndexMaxBytes 返回错误，不回退为每次查询全量扫描；所有字段缓存共享这项预算，Close 释放。n 条历史、q 次同字段查询的读取成本为 O(n)，本地查询平均 O(q)，而非 O(nq)。false 只在完整索引建立后返回。

Bloom 命中后直接跳过发送会因误判漏消息。大量候选已存在时，直接复用 Contains 的精确索引即可，Bloom 没有额外收益；候选大多不存在时可用 Bloom 预筛，首次正命中才创建精确索引。业务若定义同 key 不同内容为冲突，还需比较载荷；Contains 只比较 key。需要超过缓存预算的大规模精确判断时，业务使用自己的持久索引。

扫描有数量、字节和时间上限。Iterator 可以处理超过 AllMessage 内存上限的历史，但仍受总扫描上限；Bloom 采用流式读取，不调用 AllMessage 把所有载荷装进内存。

<a id="s10"></a>

## 10 使用示例

以下新增配置与历史类型是目标 API；pb 的三个 RPC 名称对应仓库 examples/proto/demo.proto，生成桩的 Send/Recv 形态保持不变。

<a id="s10-1"></a>

### 10.1 配置

```go
messageLimit := 512 << 10
options := []runtime.Option{
	runtime.WithStream(runtime.StreamConfig{
		Address:   "localhost:7080",
		TLSConfig: tlsConfig,
		Send: runtime.StreamSendConfig{
			BatchWait: 2 * time.Millisecond, // 此应用选择额外聚合，提高小消息吞吐。
		},
	}),
	runtime.WithStreamOptions("/wego.example.v1.Greeter/ChatHellos", runtime.StreamOptions{
		MaxMessageBytes: &messageLimit, // 此方法收紧消息上限，其余继承实例配置。
	}),
}
// Client 与 Worker 使用同一套配置结构。
```

Address/TLSConfig 是流服务连接配置；业务不用知道服务内部使用哪种消息组件。发布批次和接收批次分别配置为 Send.BatchMessages 与 Recv.BatchMessages，不把原生发布、拉取或 consumer 字段作为 SDK 配置名。

<a id="s10-2"></a>

### 10.2 Client stream 与 Server stream

```go
func upload(ctx context.Context, api pb.GreeterClient, requests []*pb.Request) (*pb.Reply, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := api.UploadHellos(ctx)
	if err != nil {
		return nil, err
	}
	for _, request := range requests {
		if err := stream.Send(request); err != nil {
			return nil, err
		}
	}
	// 排空 client subject 并封口，再等待唯一响应和最终状态。
	return stream.CloseAndRecv()
}

func watch(ctx context.Context, api pb.GreeterClient, use func(*pb.Reply) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := api.WatchHellos(ctx, &pb.Request{Count: 3})
	if err != nil {
		return err
	}
	for {
		reply, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := use(reply); err != nil {
			return err
		}
	}
}
```

示例派生的 Context 保证提前返回时释放本次调用资源；终态确认后的 cancel 不再请求 CancelRun。Send 成功后消息可能仍在本地；只有 CloseSend/CloseAndRecv 的发送收尾成功才确认 client EOF 已提交。

<a id="s10-3"></a>

### 10.3 Bidi 同时发送与接收

```go
func chat(ctx context.Context, api pb.GreeterClient, requests []*pb.Request, use func(*pb.Reply) error) error {
	// 建流失败时也能取消派生 Context；errgroup 负责运行中的两侧联动。
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, streamCtx := errgroup.WithContext(callCtx)
	stream, err := api.ChatHellos(streamCtx)
	if err != nil {
		return err
	}

	group.Go(func() error {
		for _, request := range requests {
			if err := stream.Send(request); err != nil {
				return err
			}
		}
		return stream.CloseSend()
	})

	group.Go(func() error {
		for {
			reply, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := use(reply); err != nil {
				return err
			}
		}
	})
	return group.Wait()
}
```

示例使用 golang.org/x/sync/errgroup，首个错误取消另一侧并保留错误原因。发送与接收并行，防止双方都只 Send、接收 chan 填满后形成双向背压等待。不要用“先发送所有内容，再开始 Recv”作为任意大小 bidi 流的默认模板。

<a id="s10-4"></a>

### 10.4 两个方向的历史与迭代器

```go
func restore(ctx context.Context, apply func(*pb.Reply) error) error {
	history, err := task.ServerStream(ctx)
	if err != nil {
		return err
	}
	defer history.Close()

	for message, err := range history.Iterator() {
		if err != nil {
			return err
		}
		var reply pb.Reply
		if err := message.Decode(&reply); err != nil {
			return err
		}
		// 根据已经持久发布的业务输出重建业务进度。
		// 这里不能推导此前所有输入都已被业务处理。
		if err := apply(&reply); err != nil {
			return err
		}
	}
	return nil
}

func historyEdges(ctx context.Context) error {
	history, err := task.ClientStream(ctx)
	if err != nil {
		return err
	}
	defer history.Close()

	first, err := history.FirstMessage()
	if errors.Is(err, model.ErrNoMessages) {
		return nil
	}
	if err != nil {
		return err
	}
	// 尾部含 EOF 时 LastMessage 允许 O(n) 扫描，结果是业务消息。
	last, err := history.LastMessage()
	if err != nil {
		return err
	}
	_ = first.Sequence
	_ = last.Sequence
	return nil
}
```

restore 读取 server 方向的响应历史，historyEdges 读取 client 方向的请求历史；两者都使用相同的 StreamHistory 方法。Client 侧同样可以传入其 stream.Context() 查询这两个方向。

小历史可使用 messages, err := history.AllMessage()；它和同一句柄的 Iterator 读取相同 H0。迭代器采用 Go 的 iter.Seq2，提前 break 会通知迭代器停止。[Go range over function](https://go.dev/blog/range-functions)

<a id="s10-5"></a>

### 10.5 恢复补发与可复用精确索引

```go
func sendMissing(ctx context.Context, stream grpc.ServerStreamingServer[pb.Reply], replies []*pb.Reply) error {
	history, err := task.ServerStream(ctx)
	if err != nil {
		return err
	}
	defer history.Close()

	// 此业务每个子任务只输出一条结果，child_run_id 在重试时保持稳定。
	accepted := make(map[string]struct{})

	for _, reply := range replies {
		key := reply.GetChildRunId()
		if key == "" {
			return status.Error(codes.InvalidArgument, "child_run_id is required")
		}
		if _, ok := accepted[key]; ok {
			continue
		}

		// 首次查询建立索引，后续查询复用；不会为每个 key 重扫历史。
		exists, err := history.Contains("child_run_id", key)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if err := stream.Send(reply); err != nil {
			return err
		}
		accepted[key] = struct{}{}
	}
	return nil // 框架随后排空并提交 server EOF。
}
```

accepted 仅防止本次进程重复接纳同一键，不表示这些消息已经持久化。崩溃后重新读取提交历史；不能将 accepted 当业务检查点。示例还要求同一 child_run_id 的结果内容稳定；该集合并不证明副作用完成。示例 candidates 有界，长流应使用有预算的集合或业务持久索引。

当候选键绝大多数不存在时，可先调用 history.Bloom("child_run_id")，用 MayContain(value) 的 false 跳过精确查询；true 仍查询同一 history.Contains。两者固定在同一 H0，不能用过滤器判断刚刚 Send 的消息已持久化。
