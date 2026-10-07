# Changelog

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
