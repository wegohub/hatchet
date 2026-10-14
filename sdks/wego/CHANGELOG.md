# Changelog

## 0.4.27

- 本机 Hatchet Compose 集成 hatchet-extra：源码多阶段镜像构建、TLS 与公钥初始化、持久 ACK 密钥、JetStream 数据卷及 gRPC 健康检查；消息协议保持 6，SDK 调用 API 不变。

## 0.4.26

- WaitForEvent / WaitForEventInto 改用 EventWaitOption：WithEventScope 和 WithConsiderEventsSince 接受值参数，内部保留省略与显式空值的区别，重复配置最后一次生效。回溯缺少 scope、非法时间和 nil 选项在登记等待前失败；选项可并发复用。同步示例、测试和文档，并复核枚举、并发、限流、触发优先级、Batch 和事件 scope 的零值语义。消息协议保持 6。

## 0.4.25

- 将任务上下文的借用连接入口更名为 `task.Conn(ctx)`，迁移所有调用示例、测试和使用文档，不保留旧入口。连接所有权、子调用和 durable 行为不变，Close / Shutdown 继续返回 ErrBorrowedResource；消息协议保持 6。

## 0.4.24

- feature `WaitHello` 使用生成的 protobuf 客户端经 wego Conn 并发创建子任务，以 `WithRunOptions` 传递稳定 child key 和独立 routing；按输入顺序收集全部响应，保留 RunMany 注释示例。消息协议保持 6。

## 0.4.23

- feature `WaitHello` 改为 durable 子任务编排：使用可重放时间生成固定随机种子，创建 3–10 个带稳定 key 的子任务并收集全部结果；响应增加 `child_results`，子响应包含真实 ParentRunID。新增 `Test_ChildSpawning` 验证数量、输出顺序、唯一身份和父子关系；消息协议保持 6。

## 0.4.22

- 正式载荷组件迁入 `middleware/payload`，统一更新 SDK、示例、测试和文档导入路径；旧 `examples/codec/payload` 兼容层直接指向新包。父包仅提供接口及注册入口，CodecID、构造方式、持久格式和消息协议 6 保持不变；使用方需将 `wego/payload` 导入改为 `wego/middleware/payload`。

## 0.4.21

- 补全手动 feature `Test_RunMany`：按十项、三个独立分组批量提交，验证 STATUS 幂等的三个成功运行与七次冲突、未知输入位置和实际业务结果；失败时取消本轮已确认运行。消息协议保持 6。

## 0.4.20

- 幂等配置统一为 `task.WithIdempotency(model.Idempotency{Expression, Method, TTL})`，移除布尔位置参数与 `WithIdempotencyTTL`。Method 缺省使用 TTL，STATUS 按运行终态释放键并以 TTL 兜底；策略整体覆盖，在注册前拒绝空表达式、非法枚举和不足 1 毫秒的有效期。同步示例、测试与配置文档，保留流任务的内部提交幂等策略，消息协议保持 6。

## 0.4.19

- 新增 Hatchet 分组加权公平调度的设计评估、实施计划及待人工审阅的英文 issue 草稿。此版本仅更新提案文档和版本标识，尚未实现新调度策略；消息协议保持 6。

## 0.4.18

- 策略中的动态 CEL 字段和 Batch 分组改为字符串，空值表示未指定；Batch 聚合窗口和分组并发上限改为值类型，0 保留官方默认，并在转换前校验范围。`RateLimit.Units` 保留指针以区分默认消耗与显式零消耗。同步示例和配置文档，协议保持 6。

## 0.4.17

- `model.Concurrency.MaxRuns` 改为 `int32`：0 表示未指定，正数为静态并发上限，负数在注册前拒绝；后端仅为正数设置官方可选字段，默认仍由服务端使用 1。示例直接填写数值，动态上限表达式与协议 6 不变。

## 0.4.16

- 官方 Worker 的 RPC 执行日志通过实例注册表显示完整方法名，覆盖失败、完成、动作列表和结构化 action 字段；内部摘要路由、业务错误内容及协议 6 保持不变。

## 0.4.15

- `task.WithRefreshTimeout` 默认每隔一个续期间隔只延长相同的时长；新增 `WithRefreshIncrement` 和 `WithRefreshMaxDuration` 可选项，以及适用于三种 gRPC 流 handler 的 `WithRefreshStreamTimeout`。任务入口按实际增量续期，网络入口直接运行回调；到期、取消和续期失败共用安全收尾逻辑。消息协议保持 6。

## 0.4.14

- wego 任务失败改用控制台可读的 JSON 对象上报 gRPC 错误码、消息和可识别的 details；全部 detail 保留原始字节，客户端无需注册相同的 protobuf 类型。失败错误不经过业务 payload codec，客户端仍能还原 status、响应 metadata、流结束清单及不可重试标记；已有二进制错误仍可读取。原生调用的失败格式不变，任务输入协议保持 6。

## 0.4.13

- 注册静态 Cron／Filter 输入时，若旧封包已损坏或其 S3 对象缺失，使用已校验的当前封包注册新版本；其他解码配置或传输故障继续明确失败。Worker 事件读回错误增加阶段与 topic 诊断。
- 重试退避上限在注册前校验官方要求的 1 秒至 24 小时整秒范围；手动 feature Worker 改用 `10*time.Second`，本机 MinIO 测试桶对齐为 `wego`。协议保持 6。

## 0.4.12

- gzip、zstd、AES-GCM 和 S3 载荷组件迁入正式公开包 `payload`；示例、测试及当前文档统一使用新路径。旧 `examples/codec/payload` 提供类型别名和构造函数转发，现有导入仍可编译。CodecID、阈值、持久字节格式和协议 6 均不变。

## 0.4.11

- Worker 初始化时通过本次发布的完整编码字节定位 JOIN 游标，只解码自己的屏障；加入前已过期的广播对象不会阻止启动。屏障本身仍验证 codec 可读，加入后的通知及断线重连继续严格解码，协议保持 6。

## 0.4.10

- 提交和事件使用官方 traceparent/tracestate 调度字段；普通与可靠流执行接续引擎父 trace，输入封包仅在缺少有效执行上下文时恢复 trace，避免 Console 把业务与引擎 spans 分成两条链路。协议保持 6。

## 0.4.9

- feature Worker 和 Conn 统一通过 NewGzip 与组件阈值选项构造 gzip，初始化失败释放已有 S3 连接。协议保持 6。

## 0.4.8

- 发送阈值归属组件：NewGzip、NewZstd、NewS3 使用 payload.WithThreshold 配置，默认 256 KiB，0 强制执行；移除 Runtime 阈值配置，发送链仅选择组件和顺序。协议保持 6。

## 0.4.7

- 压缩与 S3 默认按当前步骤输入的 256 KiB 阈值执行，支持 WithEncodingThreshold 覆盖；普通消息和完整流帧只记录实际执行的 CodecID，解码不依赖发送阈值。协议保持 6。

## 0.4.6

- CodecID 去掉 example 前缀，新增有界 zstd codec；feature Worker 和 Conn 显式选择 zstd、AES-GCM、MinIO/S3 发送链，gzip 保留解码注册。旧 ID 消息需排空后切换，协议保持 6。

## 0.4.5

- 本机 MinIO 默认登录改为 admin / 12345678，同步 feature S3 凭证；Compose 仍支持环境变量覆盖。协议保持 6。

## 0.4.4

- 在手动 feature Worker 与 Conn 的启动文件直接展示 gzip、AES-GCM、MinIO/S3 初始化和 Middleware 配置，移除仅供配置转发的 support 包。协议保持 6。

## 0.4.3

- 手动 feature Worker 与 Conn 统一配置 gzip、AES-GCM、MinIO/S3 codec；读取私有环境配置，共用稳定对象前缀，退出时释放实例 HTTP 连接。协议保持 6。

## 0.4.2

- Events.Push 的可选 scope 改为字符串值：省略参数不设置范围，显式空字符串保留为空范围；同步迁移示例、测试及文档。协议保持 6。

## 0.4.1

- 混合 Batch 的整体失败按成员响应契约分别上报；原生结果超限也不暴露 wego 错误载体。Filter 解码先登记取消能力，关闭同时保留排空与资源清理错误。

- 收紧协议 6 控制字段：拒绝重复字段、已定义字段的 null 和大小写别名；未知可选扩展及业务 value 仍按原约定处理。非法或超限发送头在 codec I/O 前失败。
- 执行 deadline 覆盖普通任务及 RPC 输入解码；历史结果和流恢复继续使用观察者预算。unary 响应必须符合注册的 protobuf 输出类型。
- CLAIM 首次读回核对 RunID；服务端终态封闭后禁止迟到发送与 metadata 修改，客户端关闭后拒绝迟到解码及背压回调。协议保持 6。

## 0.4.0

- 协议升至 6：以 `_wego` 声明协议、`value` 承载业务、`routing` 承载调度；删除每条输入中的 method、scope、format、类型及普通调用摘要。目标由持久注册绑定确定。
- 接受原生业务 JSON 和显式 Input；输入 codec 与响应契约分别选择。SDK 请求严格读取 wego 响应；Console 原生调用返回业务 JSON，原生 server/bidi 使用官方 PutStream 并仅保存输出数量。
- 无 codec 消息可读；不安全 JSON 数字自动使用 base64 字节承载。公开 `model.Input` 支持 ProtoJSON 序列化，流内容摘要统一对象顺序和等价数字表示。
- Payload 移除 scope 参数；AES-GCM 使用随机 nonce、nil AAD，示例格式 ID 升至 `example.aes256-gcm.v2`。接收端按消息 CodecID 解码，最多 16 步，协议头最多 64 KiB。
- 所有流提交键位于 `_wego.idempotency_key`，按注册方法隔离。恢复核验运行实际使用的 workflow 版本；原生运行不会被 SDK 当作可靠流附着。
- 同步普通任务、durable、DAG、Batch、事件、Filter、触发器及结果链路；新增 Console 三入口、原生三种流、混合 Batch 和数值保真验证。协议 5 不提供读取兼容，需排空后协调升级。

## 0.3.3

- 启用的四种 RPC 默认注册完整方法名事件，与自定义事件按 namespace 后的实际 key 稳定去重；禁用方法及纯网络入口不注册。流事件使用 ProtoJSON 请求数组、一对多方法幂等隔离和现有可靠／实时输出链路。
- Payload 组件按 CodecID 建立实例只读注册表，接收端按消息记录逆序解码；新增 runtime.WithEncodingCodecs，独立选择发送链，支持显式空链和重复执行步骤。未知 ID 在任何 codec I/O 前失败，历史消息不依赖当前注册顺序。
- 示例 CodecID 使用常量；静态 Cron／Filter 摘要采用实际消息编码链，保留随机密文复用与有界解码。协议保持 5，启用不同发送链前升级两端；仍只接受 wego 封包。

## 0.3.2

- CreateCronTrigger.Priority、CreateScheduledRunTrigger.Priority 改为 int32；0 表示未指定，1–3 为合法调度优先级，其他值在目标查询、codec 上传及提交前失败。
- Cron 创建在 backend 中显式传递官方 REST priority 字段，避免配置被丢弃；补充真实 HTTP 映射、默认值和本机触发验收，更新示例与可选字段说明。源码 API 变更，协议保持 5。

## 0.3.1

- DesiredWorkerLabel.Comparator、Concurrency.LimitStrategy 和 RateLimit.Duration 改为值类型，可直接配置枚举常量；Comparator 零值为 LabelEqual，其余两项空值保持未指定语义。
- 同步迁移示例、测试与配置文档，验证默认值、非法枚举、快照隔离及官方协议映射；这属于源码 API 变更，通信协议保持 5。

## 0.3.0

- 协议升级为 5：普通 JSON 与 protobuf 任务统一封包、routing 和有界 codec，业务字节不进入引擎浮点 map。
- 使用 RunOption 配置所有提交，并通过 WithRunOptions 适配生成客户端；触发器使用 model.Input。
- Result 和 ResumeStream 取消只结束当前观察，RunRef.Cancel 显式终止运行；异步提交不固化调用 deadline。
- 原生输入、输出、Batch、父输出、事件与实时输出采用统一 codec；客户端与 Worker 需要协调升级。

## 0.2.10

- 规范调用配置命名：官方运行选项保持 WithRunKey / WithRunSticky / WithRunPriority / WithRunMetadata / WithDesiredWorkerLabels；自有 RunOptions 字段 Metadata、Labels 对齐为 AdditionalMetadata、DesiredWorkerLabels，并明确 Key 是稳定子调用键。
- wego 扩展改名：WithIdempotencyKey → WithStreamIdempotencyKey，WithRouting → WithRPCRouting，WithRoutingDefaults → WithRoutingDefaults；流幂等键仅用于流提交，RPC routing 是 envelope 中供 CEL 读取的数据。
- Client 消费凭证的函数及模型 StreamCheckpoint → StreamResumeToken；Worker 保留 task.Checkpoint 恢复发布历史。流提交与消费接口集中到 client/stream.go，迁移示例、测试与当前使用文档。
- 以上为源码 API 更名，旧名称不保留别名；消费凭证 JSON 字段与协议 4 保持一致。

## 0.2.9

- worker.WithDisableMethod 改为可变参数，支持一次禁用多个 RPC 或展开方法切片；单方法调用继续可用。空参数无效果，重复方法幂等，输入切片在构造 Option 时复制，各实例禁用集合保持隔离。
- 补充批量筛选、未知方法、切片快照、空参数及真实引擎验证，更新配置示例与版本清单；协议保持 4，仅修改 wego SDK。

## 0.2.8

- 新增 worker.WithDisableMethod：按完整 RPC 方法名禁止本实例的任务注册和消费，覆盖 unary 与三种流；网络 gRPC 和 GetServiceInfo 保留完整服务。禁用优先于普通 / durable 任务策略，重复配置幂等，不删除引擎中已有 workflow。
- 启动前校验未知方法；禁用方法不进行任务 protobuf 绑定或策略验证。全部方法禁用时明确报错，纯网络模式使用 runtime.WithDisableWorker。补充配置隔离、方法筛选、网络调用及真实引擎注册与执行回归，协议保持 4。

## 0.2.7

- 重设计 log：Runtime.WithLogger 原子配置 wego 共享输出，不修改 slog.Default；普通 context、网络 gRPC、任务及 R 系列使用同一出口。新增 With、Handler、FromContext、Enabled、Log 与 LogAttrs，支持不可变上下文属性、分组去重、业务 source 及受保护的执行/trace 身份。
- R 系列先按级别打印，再同步上报原始 message、结构化 metadata、时间和 retry；缺少任务能力、关闭上报及两个出口失败均明确返回错误。新增 WithLogReportTimeout，默认 5 秒，上报使用实例连接并登记自有 I/O。
- 更新 feature Worker、结构化日志示例、配置文档及验收映射，增加 Handler/上报协议/并发替换及显式关闭期间的双入口真实日志回归。通信协议仍为 4，仅修改 wego SDK。

## 0.2.6

- 普通结构体统一使用指针接收者，覆盖实例配置、流状态、管理适配器、日志 / trace 适配器、codec 示例与测试 fixture；构造和接口传参同步使用指针。
- Clone / Snapshot 显式复制后返回独立值，保留嵌套容器隔离；预算校验先保存可寻址的配置。公开 StreamOptions / WorkerEventOptions 的 Validate 通过变量或指针调用，不支持临时结构体值直接调用。
- 协议 Envelope 的值查询 / JSON 编码及封闭接口的无状态标记保留值接收者，保证按值传输的接口与二进制 metadata 编码语义。map 类型与生成代码遵循各自契约，通信协议仍为 4。

## 0.2.5

- 完善 worker.md 的“注册与连接”：展示全部公开构造 option、流与通知预算字段，用中文注释说明用途、默认值、覆盖规则和作用边界。
- 分组展示 Runtime、任务策略、中间件、客户端调用、原生 Worker，以及 TLS、纯网络与嵌入引擎的替代配置；运行行为与通信协议 4 不变。

## 0.2.4

- 修复 client stream 最终响应在状态锁内执行远程 codec 的问题，解码应用独立预算，关闭与取消不被对象下载阻塞，迟到响应不能覆盖终态。
- 优化完整流帧封装，无变换编码一次分配，codec 输入与发布结果仍为独立快照；不缓存跨执行 CLAIM，不池化逃逸字节。
- 按职责拆分提交/结果等待与输出观察/消费文件，降低日志解释器和提交路径复杂度；仅内部父身份读取避免冗余 metadata 复制。
- 增加帧编解码实例指标、CLAIM/预算/快照/执行索引回收回归、分配基准、CPU/heap profile、真实引擎并发负载与覆盖率/复杂度 CI 门禁。手动 feature 调用拥有独立连接及 deadline，CI 单元入口显式排除其外部 Worker 依赖，必需场景由 e2e 验收。
- 补齐 embedded 独立示例与验收模块的 AWS S3 间接依赖和校验和，避免主模块/工作区掩盖子进程缺失依赖。
- 通信协议保持 4，公开 API 不变，Hatchet 源码和根依赖不修改。

## 0.2.3

- Worker 注册自动追加 `worker_name` 标签，取最终展示名称，包含默认生成名、原生 Worker 指定名及 `WithInstanceName` 覆盖。
- 每个实例独立复制标签，保留其他业务标签与整数类型；同名标签以实际 Worker 名称为准，注册与任务执行上下文使用同一值。
- 增加共享配置隔离回归及真实引擎标签查询断言；通信协议仍为 4。

## 0.2.2

- 新增自托管 Durable Streams 开启脚本，隐藏输入租户 token，先通过 Hatchet API 验证身份，再事务更新目标租户授权。
- 数据库连接复用私有部署配置，支持本机及容器 psql；重复执行不改变其他授权或已开启记录的更新时间。凭证不进入命令行与诊断。
- 增加认证拒绝、租户不匹配、跳转、非法配置、超时及凭证隐私回归；通信协议仍为 4。

## 0.2.1

- Docker Compose 增加 MinIO，固定官方发布源码构建，使用独立数据卷和本机端口；可复现模板纳入版本控制，私有凭证继续忽略。
- 新增 codec 独立示例，使用 AWS SDK for Go v2 的 S3 客户端连接 MinIO，组合有界 gzip、AES-256-GCM 和 SHA256 对象引用。
- 新增真实 S3 验收：四种 RPC、CEL routing、Worker 历史恢复、独立客户端断点消费、完整 JOIN/EVENT 帧及缺失/越界/超限错误。对象保留期必须覆盖持久流与任务历史，不随关闭删除。

## 0.2.0

- RPC 与流日志使用协议 4；业务输入切换 ProtoJSON，routing 支持方法默认值及按调用覆盖。namespace 默认空，Worker 展示名称可覆盖，逻辑身份按实例生成，副本共享稳定 workflow 与版本。
- 三种流各使用一个 StandaloneTask。client/bidi 输入在 CloseSend 时一次提交；可靠输出使用官方 Durable Streams，实时输出显式使用 PutStream。移除 START/SESSION、控制 Worker 和控制 workflow。
- 增加消费 checkpoint、ResumeStream、Worker 有限历史 checkpoint、CLAIM 竞争与代次隔离；幂等提交响应丢失时恢复实际 RunID，预取不推进消费进度，成功 EOF 核对引擎结果与完整输出清单。
- 所有协议帧统一执行有界 codec；冻结发布重试的 producer、序号和字节，补齐输入、输出、扫描、恢复及完整后端消息预算。超大结果明确失败，不无限重试上报。
- 增加 Worker 定向与广播事件、JOIN 屏障、有限重连和独立业务队列；内部取消不占 slots。补齐共享运行取消、关闭排空和强制停止边界。
- 增加实例协议指标和执行身份 trace 属性，默认关闭指标且不修改全局 OTel provider。固定未经修改的官方 Hatchet v0.110.5 依赖；本版与协议 3 的历史运行不兼容。
- 官方 v0.110.5 镜像与 PostgreSQL MQ 下，18 项门禁、28 源文件/74 构造片段、三种流及故障恢复、真实 engine race 和独立 embedded 验收通过；版本一致的命令、断言和清理记录见验收报告。

## 0.2.0-dev.2

- 公开 RPC 切换协议 4 和 ProtoJSON；新增实例名称覆盖、按调用 routing 及方法默认值。RPC workflow 保留服务和方法大小写，内部 action 使用稳定独立标识。
- 三种流各自只注册一个 StandaloneTask；输入在 CloseSend 时整批提交，移除 START、SESSION、控制 Worker、控制容量及握手配置。
- 默认可靠输出使用完整帧 codec、持久 CLAIM 及最终输出清单；实时输出必须明确选择，不自动降级。增加按方法的消息、批次、预取和扫描预算。
- 增加 Worker 有限 checkpoint 历史；已存在输出的重试必须恢复至 EOF 后继续发布。消费恢复、Worker 事件及剩余验收仍在实施，不代表完整 0.2.0 已交付。

## 0.2.0-dev.1

- 固定未经修改的官方 Hatchet v0.110.5 发行依赖；后端通过实例连接调用持久流协议，保留 producer、序号、游标及订阅错误。
- 增加协议 4 的整帧有界 codec、CLAIM 竞争/读回、共享日志解释器、JOIN 屏障和最终输出清单校验；不明确发布结果复用已经编码的字节。
- 可靠输出原型使用按代次隔离的 backend 执行路径，先确认 CLAIM 再启动业务和上报 START；落选者不报告终态，旧 CANCEL 不影响新代次。
- 结果通知仅唤醒持久结果查询；补齐订阅重连、终态轮询及旧代次上报隔离，避免把接收 ACK 或旧通知当成最终结果。
- 增加真实引擎 P0 探针及受控保留期故障 fixture，验证加密、去重、响应丢失、接管、重试、幂等身份、业务详情、取消和容量释放。
- 此版本为 P0 开发预览。公开 RPC 仍使用协议 3 和现有会话入口；v0.2.0 单任务流、恢复 API 和 Worker 事件尚未交付，不宣称完整验收通过。

## 0.1.13

- 根据 Claude 评审重写 v0.2.0 设计：补齐 CLAIM 读回、结果上报隔离、恢复与对账预算、事件屏障、配置限制和 P0 故障验收，逐项说明采纳与修正理由。
- 明确输入消息及全部流协议帧统一经过 codec，包含 CLAIM、headers、结束帧、JOIN 和取消；补齐整帧单次编码、恢复解码、分层大小限制、对象保留及重试复用编码字节的要求。
- 核对官方 v0.110.5 Streams 的现有 Go API、producer 重试及订阅错误语义，明确 backend 的适配依据和待验证边界。
- 本次仅更新设计文档及版本元数据，运行逻辑和协议版本 3 不变；未执行或宣称通过 v0.2.0 的 P0 验收。

## 0.1.12

- 控制 Worker 使用业务 Worker 完整名称追加 `-control`，便于在管理界面识别同一 Server 的两个实例；路由仍使用共享 owner 标签。

## 0.1.11

- 清理流会话中的机械性局部注释，保留中文声明、锁顺序、广播、半关闭和协议边界说明；注释门禁不再逐个要求短变量和循环变量说明。
- 复用 protobuf 类型校验与客户端故障出口，命名输入、输出窗口及终态缺口判断；共享状态仍在同一临界区内判断，远程 I/O 仍在锁外执行。
- 流接收在消费缓存前拒绝 nil protobuf 指针，补充非法输入不推进窗口、首个状态不被取消覆盖的回归。
- 本机升级使用官方 v0.110.5 镜像，完成数据库与认证配置备份、数据库迁移，并以 PostgreSQL MQ 验证 SDK；协议版本仍为 3，Hatchet 源码不变。

## 0.1.10

- 双向累计 ACK 按新增确认范围释放窗口，控制结果同样检查确认上界；修复最大序号窗口比较和发送序号回绕。
- 状态广播仅在实际等待时创建通道，无等待者的更新零通知分配；保留广播、取消、故障优先级与消费原子性。
- 控制入口统一锁边界，远程 READY 发布在锁外执行；端点按值分组输入、输出、生命周期与响应状态。
- JSON 字节与 RawMessage 直接解析，缓存复制保留独立所有权与精确切片容量；窗口预分配限制初始容量。
- 补充中文注释、ACK 与广播边界回归、前后基准及供 Claude 复审的逐项处理报告；协议版本仍为 3，Hatchet 源码不变。
- 事件示例在 handler 通知后等待异步查询记录可见，仅在明确 404 时按场景预算重试，服务错误、空成功响应和取消立即失败。

## 0.1.9

- 流握手只保留一个在途 PING，重发按 250ms～1s 退避；READY 可以取消 PING 结果等待，OPEN 前回收后台操作，握手总预算保持有效。
- CANCEL 使用独立短预算，默认 5 秒且不超过实例清理预算；增加无敏感载荷的握手与清理阶段诊断。
- durable ACK 等待明确响应根生命周期退出，根退出后禁止重新登记；补充 listener Stop、task cleanup 与迟到 ACK 隔离回归。
- 结果取消状态轮询可由 runtime.WithResultPollInterval 配置，默认 1 秒；慢查询完成后再计时，不积累补发。
- 关闭流程按调用排空、自有 I/O 确认拆分；超时报告具体操作类别、编号、耗时及数量，在锁外记录日志。
- model 按文件分离执行 DTO、策略和错误；内部实现包改为 internal/client、internal/features、internal/telemetry，公开导入路径保持不变。
- 新增真实 pending memo 丢帧与两次 Worker 重启恢复验收、四会话并发流成本样本；MQ 仍使用 PostgreSQL，Hatchet 上游源码不变。

## 0.1.8

- 新增中文评审背景文档，说明目标、实际 API、模块与资源边界、功能覆盖、历史修复、验收证据及待关注的问题，供独立代码评审使用。
- 本次只更新文档与版本元数据，协议仍为 3；真实引擎验收报告保留实际执行版本 0.1.7，不将历史结果改写为 0.1.8 验收。

## 0.1.7

- durable ACK 登记在 invocation 内串行化，长期等待不占确认锁；相同子节点共享一次监听，观察者独立取消并可重复读取结果。恢复到尚未完成的 memo 时补算并提交 UTC 时间。
- 批量提交累计所有分块的成功身份、幂等冲突和错误；durable 后续块失败仍交付已经确认的句柄和原输入下标。
- 驱逐等待计数绑定到具体 invocation，迟到退出不能清除后续执行的状态；驱逐取消也绑定具体记录，迟到确认不能取消恢复执行。
- 流 RUN 提交纳入握手预算，关闭等待订阅与结果 goroutine 退出后才释放实例登记。
- 原生 JSON 字符串保持类型，显式 null 与缺失输出分别处理；新增 Result.IntoContext，载荷解码 I/O 可取消并纳入连接排空。
- 深复制实例和 Worker 配置的可变容器，失败创建也不能改写 Conn，拒绝 Worker 局部覆盖实例连接与协议；管理端口使用封闭的类型化请求，动态 SDK 参数只存在于 backend。
- trace 协议属性使用真实协议版本；新增并发与故障回归和真实 bidi 调度成本记录，保持 Hatchet 上游源码及根依赖不变。

## 0.1.6

- 后端改为适配未经修改的官方执行器和协议，移除上游连接注入、上下文桥接、缓存及生命周期补丁；依赖与 CI 模板收敛至 SDK 内。
- 独立模块固定官方 Hatchet v0.109.0；发行依赖门禁关闭工作区并拒绝本地替换，服务端另在 v0.107.0 实例验证。
- 修复 Cron 输入启动取消、RunMany 部分成功身份保留、结果订阅取消及辅助状态轮询阻塞。
- 批量幂等冲突保留成功和冲突 RunID；流取消保留 gRPC 状态，致命解码失败不再消费后续 DATA。
- 协议升级至 3，显式保存 client stream 响应存在性，区分零字节响应与未发送响应，并拒绝重复最终响应。
- 复用根 durable 监听器和稳定子身份；补齐逐成员 batch 结果和广播转换，管理删除支持 204。
- durable Now 在同一次 invocation 内保留已确认的时间，不为重复调用增加 memo 节点；恢复执行仍查询引擎记录。
- 远端 durable 子任务缺少本地定义时，按已确认 RunID 读取持久结果以恢复任务名映射，不重新提交子任务。


## 0.1.5 — Unreleased

- 修复注册、普通/批量/子调用提交的 deadline 与 cancel 贯通；共享订阅独立于单次提交预算，停止能取消初始化中的真实 RPC。
- Worker 资源以实例键索引，同名实例独立注销；流 START 按完整方法名注册，只选择支持该方法的 owner。
- 协议升级为 2：JSON metadata 使用 bytes/base64，流 metadata 使用 protobuf bytes；任意二进制、多值 headers/trailers 和错误结果均无损传输。客户端与 Worker 必须同时升级，协议 1 不兼容。
- 显式转换整数亲和标签并检查 int32 范围；CronInput 支持独立覆盖、继承与显式 nil 清除；拦截器传给 invoker 的 CallOption 与 reply 生效。
- 修复致命流解码错误的生命周期释放；事件、Cron 与 Schedule 载荷 I/O 纳入连接排空并保留取消预算，远期任务不携带发布 deadline。
- 管理客户端惰性初始化并共享工作流缓存；指标查询应用 namespace，删除使缓存失效并阻止旧在途查询回填。
- 用编译期校验的明确 SDK 方法替代反射查找；把连接装配和观测资源工厂移至 internal，公开包仅保留业务 API 与配置。
- 验收采用稳定片段 ID 和必需断言 ID；缺失、Skip、失败或场景错配均拒绝通过。补充定向回归、多 Worker/异构服务真实测试和中文注释校正。

## 0.1.4

- 为公开及私有声明、结构体字段、局部变量和关键逻辑块补充中文说明，包含示例、单元测试、质量门禁和真实引擎测试；说明协议数据、并发约束、失败路径及实际输入输出示例。
- 为 proto 消息、字段和 RPC 补充中文说明；固定生成流程补充运行时缓存、描述符和生成适配器的中文注释，生成结果保持可复现。
- 仅改变注释及版本元数据，协议仍为 1；完整真实引擎验收报告保留其实际运行版本 0.1.3。

## 0.1.3

- Server 共享服务注册，支持 Worker 与原生网络 gRPC 双入口；网络请求直接执行 handler，原生配置通过 server.WithGRPC 透传。
- runtime.WithDisableWorker 控制 Server 的 Worker 入口，纯网络模式不要求任务后端或 token；Conn 调用不受开关影响，原生 Worker 创建拒绝配置冲突。
- Server 统一使用 Serve、Stop、GracefulStop 与 GetServiceInfo，移除 ServeContext、WaitReady、Shutdown；强制停止可以中断排空。
- TraceConfig.DisableHatchet 更名为 DisableWorkerExporter；补充网络 TLS、三种流、原生 middleware、取消、资源关闭及真实双入口验收。

## 0.1.2

- `wego.NewServer(opts ...ServerOption) *Server` 和 `server.New` 使用与 gRPC 一致的单返回值构造形式。构造不建立连接；初始化错误通过 Serve / ServeContext / WaitReady 返回，并支持启动前关闭。

## 0.1.1

- 按职责拆分连接、后端、管理客户端和流会话文件，展开密集语句、配置及示例，统一 import 和逻辑分段。
- 为包边界、durable 上下文、流协议与背压、关闭顺序和观测资源补充中文注释；公开 API 与协议版本保持不变。

## 0.1.0

- Introduce wego-owned connection, task, worker, scheduling, management and error contracts; Hatchet implementation remains inside internal/backend.
- Support standard protobuf unary and three gRPC stream modes, explicit scheduling projections, metadata/status details and error response headers/trailers, flow control and session ownership.
- Add durable replay, memo/time/event waits, child calls, eviction, batch, execution policies and native JSON webhook/output-stream APIs.
- Add instance telemetry, middleware payload transformations, controlled draining with per-execution cancellation and optional embedded engine support.
- Add a 28-source acceptance manifest, independent examples, real PostgreSQL MQ tests, protocol/race/API gates and CI.
