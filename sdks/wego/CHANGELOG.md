# Changelog

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
