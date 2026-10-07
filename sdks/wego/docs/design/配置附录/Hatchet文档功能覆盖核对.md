# Hatchet 文档功能覆盖核对

> 核对日期：2026-09-16。仅评估设计覆盖，不修改 `worker.md`、主方案或实现代码。

## 结论

**没有全部覆盖。普通 standalone 的声明配置比较完整；Worker 运行时、持久执行语义和 Workflow 编排仍有明显缺口，调用与管理面尚未收敛。全部候选 API 都没有实现验收证据。**

“参数有类型”“主方案提到功能”“附录发现原生字段”“引擎本身支持”均不等于 wego 已支持。

## 核对口径

- 本地 `zh/v1` 共 59 篇 Markdown；重点逐篇全文核对 02–09 共 41 篇，与 Worker、Task、Workflow 直接相关。
- 01/10/11/13 用作开发、部署、平台限制背景；12 的迁移指南只核对相关章节，不把这两篇算作全文精读。
- 以最新 `worker.md` 为当前设计；主方案中不冲突的内容计作目标或早期候选，不能反向恢复已撤销的配置文件、proto kind 或 Runtime 引擎参数。
- 状态：**具体候选**=已有入口/类型和主要约束；**部分**=只有一部分或缺关键语义；**缺口**=无完整用法；**独立面**=调用/管理/部署等需要另行设计。以上均不是实现状态。
- 文档含多语言 API、beta/Cloud 限制及页签转换问题；不能仅凭“Go”标题认定 Go SDK 有相应接口。少量控制台 GIF/图片未视觉检视，未作为功能证据；Mermaid 按文本图语义核对。
- 少量冲突用本仓 `src/hatchet` 的 `315d43a` 核实。本次未启动服务、执行功能测试或验证 Temporal 适配。

## Worker 与可观测性

| 功能 | 当前设计状态 | 缺口 / 边界 | 文档依据 |
|---|---|---|---|
| 标准注册、实例命名、横向扩容 | 具体候选 | RegisterXServer / WithWorker / Serve；实际注册、心跳和重连未验收。 | [02. workers](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/02. core-concepts（核心概念）/02. workers.md:3>) |
| 普通 / durable slots | 具体候选 | 两个本地容量池已区分；不是集群并发策略，也不自动改变任务类型。 | [02. workers](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/02. core-concepts（核心概念）/02. workers.md:148>) |
| Worker 静态 labels | 具体候选 | WithLabels 只发布标签，不等于任务已能按标签选 Worker。 | [04. worker-affinity](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/04. worker-affinity.md:5>) |
| 任务亲和选择 | 缺口 | desired labels 的 required/comparator/weight、无匹配排队、与 sticky 组合均缺具体用法；主方案仅列字段族。 | [04. worker-affinity](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/04. worker-affinity.md:83>) |
| 运行中更新 Worker labels | 缺口 | 无 upsertLabels 的实例运行时入口、更新效果及竞态约定。 | [04. worker-affinity](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/04. worker-affinity.md:184>) |
| Sticky SOFT / HARD | 部分 | 任务扩展已有枚举；HARD 无原实例时等待、子任务同实例注册和路由规则未完整表达。 | [03. sticky-assignment](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/03. sticky-assignment.md:3>) |
| 普通任务 slot cost | 具体候选 | 已有 SlotCost；成本必须由单 Worker 满足，不跨实例拼容量；当前设计不允许 durable 设置。 | [05. slot-cost](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/05. slot-cost.md:59>) |
| 手动释放 slot | 缺口 | 无运行时入口；文档警告释放后 Worker 崩溃可能不再重分配，不能无条件纳入通用安全 API。 | [06. manual-slot-release](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/06. manual-slot-release.md:7>) |
| 两种关闭排空 | 部分 | 已有限时 / 等完成模式；Pause 下发屏障、迟到分配、结果确认和 durable 恢复仍待验证。 | [01. index](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/01. get-started（入门）/01. index.md:47>) |
| slog 与任务日志上报 | 具体候选 | Info / RInfo、队列、级别、重试、flush 有定义；日志上报与 OTel trace 是独立通道。 | [01. logging](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/08. observability（可观测性）/01. logging.md:1>) |
| Trace provider / exporters | 具体候选 | 默认 Hatchet、多 exporter、实例所有权已定义；producer/consumer/durable span 命名、关联属性和跨 child/event 传播尚未完整定义。 | [02. opentelemetry](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/08. observability（可观测性）/02. opentelemetry.md:27>) |
| Worker 自身 Metrics | 部分 | OTel 初始化、默认关闭、HTTP /metrics 已定义；指标名、label、bucket、运行时采集点未定。 | [03. worker-healthchecks](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/08. observability（可观测性）/03. worker-healthchecks.md:3>) |
| 健康 / 就绪 / 存活检查 | 缺口 | 没有 /health、/readyz、/livez 的设计；断连与排空时的健康状态未定义。文档现成接口主要面向 Python/TypeScript，Go 需自行实现。 | [03. worker-healthchecks](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/08. observability（可观测性）/03. worker-healthchecks.md:83>) |
| Run / Event 附加元数据 | 部分 | Runtime WithEventMetadata 仅发布事件的默认元数据；单次 run/event、父子继承和查询规则仍待设计。 | [05. additional-metadata](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/08. observability（可观测性）/05. additional-metadata.md:3>) |
| 自动扩缩容 | 独立面 | Task Stats、队列积压、KEDA/Prometheus 策略属部署/控制面；本地 /metrics 不等价于引擎队列指标。 | [02. autoscaling-workers](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/02. autoscaling-workers.md:3>) |
| 租户 Prometheus API | 独立面 | 文档标注 Cloud Enterprise；不属于目前 Worker 自身 /metrics。 | [04. prometheus-metrics](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/08. observability（可观测性）/04. prometheus-metrics.md:3>) |
| Docker / 本地开发 | 独立面 | 标准 Go 程序可容器化；正式模板、开发命令、Embedded 测试尚非已交付能力。 | [01. docker](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/01. docker.md:173>) |

## 普通 Task 与流控

| 功能 | 当前设计状态 | 缺口 / 边界 | 文档依据 |
|---|---|---|---|
| RPC → standalone | 具体候选 | 普通默认、WithDurableTask 显式声明、完整 RPC 身份、task defaults 已定义。 | [01. tasks](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/02. core-concepts（核心概念）/01. tasks.md:3>) |
| 重试次数 / 指数退避 | 具体候选 | 额外重试次数、factor/max、显式 retries=0、默认继承已有规则；不包含业务副作用恰好一次保证。 | [01. retry-policies](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/04. reliability（可靠性）/01. retry-policies.md:8>) |
| 不可重试错误 | 缺口 | 无业务侧 NonRetryable 包装、gRPC status/error 分类与原生错误映射契约；WithRetries(0)不能代替按某次错误决定不重试。 | [01. retry-policies](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/04. reliability（可靠性）/01. retry-policies.md:199>) |
| 传输重试 | 部分 | Runtime 只有 GRPC/HTTP 开关；次数/jitter/Retry-After 是具体传输策略，不要当作 Task retry 字段。 | [01. retry-policies](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/04. reliability（可靠性）/01. retry-policies.md:353>) |
| 排队 / 执行超时 | 具体候选 | 分别定义了 ScheduleTimeout / ExecutionTimeout；durable wait 是否计入 deadline 需固定版本进一步核实。 | [02. timeouts](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/04. reliability（可靠性）/02. timeouts.md:5>) |
| 运行时续期 | 部分 | 闭包 WithRefreshTimeout 有形状、累加和清理原则；失败策略、取消竞态和真实续期仍待定。 | [02. timeouts](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/04. reliability（可靠性）/02. timeouts.md:118>) |
| 任务取消 | 部分 | context.Done 和超时取消原则已有；客户端显式取消、取消原因/终态、子调用传播未闭合。 | [03. cancellation](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/04. reliability（可靠性）/03. cancellation.md:138>) |
| 五种并发策略 | 具体候选 | task/workflow 两作用域、key/max/策略链已列；不是本地 slots。 | [01. concurrency](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/05. flow-control（流控）/01. concurrency.md:22>) |
| 共享 / 动态并发 | 部分 | Name/TenantScoped/MaxRunsExpr 已有字段；engine >=0.106 的版本门槛、DAG 路径、跨 workflow 有序链和动态降限需验证。 | [01. concurrency](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/05. flow-control（流控）/01. concurrency.md:565>) |
| 静态 / 动态限流消费 | 具体候选 | Key/KeyExpr/Units/UnitsExpr/LimitExpr/Window 已定义；静态额度预建是前提，管理面还未设计。 | [02. rate-limits](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/05. flow-control（流控）/02. rate-limits.md:24>) |
| 默认优先级 | 具体候选 | 仅 1/2/3；单次 run、cron、schedule 覆盖还属于调用面；不能承诺跨 workflow 全局排序。 | [03. priority](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/05. flow-control（流控）/03. priority.md:3>) |
| TTL / STATUS 幂等 | 部分 | 复合类型已有，文档标 beta；还需明确冲突响应、事件冲突静默丢弃及每个已接受 run 的 TTL 窗口。 | [04. idempotency](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/05. flow-control（流控）/04. idempotency.md:3>) |
| 最终失败回调 | 具体候选 | OnFailure 回调和独立重试/超时有设计；取消/跳过是否触发、原生失败任务配置桥接待验证。 | [01. from-celery-to-hatchet](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/12. migration-guides（迁移指南）/01. from-celery-to-hatchet.md:726>) |
| Workflow 成功钩子 | 缺口 / 语言差异 | 迁移文档提供 Python on_success_task；当前 wego 无契约，本仓 Go SDK 未找到同名公开入口，不按跨语言通用能力承诺。 | [01. from-celery-to-hatchet](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/12. migration-guides（迁移指南）/01. from-celery-to-hatchet.md:732>) |
| CEL 表达式 | 部分 | 目前只有按用途校验原则；缺用途→变量/函数/返回值/失败方式矩阵，不存在一个所有地方通用的 CEL 环境。 | [06. cel-expressions](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/05. flow-control（流控）/06. cel-expressions.md:23>) |
| Batch task | 缺口 | 文档 beta：按数量/时间/载荷阈值聚合、分组、memberID→输入/输出、broadcast；没有专门声明及 handler 契约。 | [05. batch-tasks](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/05. flow-control（流控）/05. batch-tasks.md:3>) |

## Durable 与 Workflow 编排

| 功能 | 当前设计状态 | 缺口 / 边界 | 文档依据 |
|---|---|---|---|
| Durable 任务声明 | 具体候选 | WithDurableTask + Eviction 类型齐备不代表恢复运行时已经完成。 | [01. durable-tasks](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/01. durable-tasks.md:3>) |
| 确定性 / 检查点 / 重放 | 缺口 | 需定义检查点身份、恢复重入、代码分支稳定、外部副作用放普通 child task 的约束；不能把 context.Context 包装视为恢复实现。 | [01. durable-tasks](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/01. durable-tasks.md:8>) |
| 持久 Sleep / SleepUntil | 部分 | 只有 task.Sleep 示例；到绝对时间、恢复保持原 deadline、资源释放的完整契约未定。 | [03. durable-sleep](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/03. durable-sleep.md:3>) |
| 持久事件等待 | 部分 | WaitEvent 仅待讨论名；payload 返回、匹配、scope/lookback、事件保留期、超时/取消均需运行时用法。 | [04. durable-event-waits](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/04. durable-event-waits.md:117>) |
| Memo / 副作用记录 | 缺口 | worker.md 提到 Memo 待讨论，无具体 API；不能把文档中的 child 检查点当作任意代码都能 Memo。 | [01. durable-tasks](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/01. durable-tasks.md:15>) |
| 派生 child task / workflow | 缺口 | 普通或 durable 父调用、等待/不等待、稳定 child 身份、批量派生、部分失败和父子取消语义未完整设计。 | [02. child-spawning](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/02. child-spawning.md:3>) |
| Durable eviction | 具体候选 | TTL/capacity/priority、TTL=0、本地取消与恢复已有规则；策略与重放、安全退出的配合尚未实现。 | [05. task-eviction](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/05. task-eviction.md:11>) |
| Workflow 注册与入口 | 部分 | 只有 WithWorkflow + WithEntry 用法；不是完整图定义或运行时。 | [06. directed-acyclic-graphs](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/06. directed-acyclic-graphs.md:3>) |
| DAG 拓扑 / 父输出 / 并行 / 汇聚 | 部分 | 主方案写了 protobuf 推图与有限 fan-in/out 愿景；最新示例没有具体图、节点绑定、父输出访问、分组/排序的完整定义。 | [06. directed-acyclic-graphs](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/06. directed-acyclic-graphs.md:95>) |
| DAG 条件 / Skip / Cancel | 缺口 | standalone 的 WithWaitFor/WithSkipIf 不覆盖 ParentCondition、CancelIf、WasSkipped、空输出及下游传播。 | [06. directed-acyclic-graphs](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/06. directed-acyclic-graphs.md:176>) |
| 条件 OR groups / AND 组合 | 部分 | 已有 And/Or 条件树；原生组内 OR、组间 AND 的编译等价性、结果身份和取消分支还未定义。 | [06. directed-acyclic-graphs](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/06. directed-acyclic-graphs.md:473>) |
| 循环 / 动态图 / 子流程 | 部分 | 主方案列为目标；当前缺可审阅的业务示例、恢复身份和边界。 | [04. durable-execution](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/02. core-concepts（核心概念）/04. durable-execution.md:18>) |
| 整条流程 deadline / 版本兼容 | 部分 | task timeout 不是 workflow timeout；WithVersion 不是重放兼容保证。破坏性 durable 变更需新定义并保留旧执行。 | [02. from-temporal-to-hatchet](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/12. migration-guides（迁移指南）/02. from-temporal-to-hatchet.md:1412>) |

## 触发、调用及管理面

| 功能 | 当前设计状态 | 缺口 / 边界 | 文档依据 |
|---|---|---|---|
| 同步 Run / 异步 RunRef / Result | 部分 | 主方案已有同步桩、StartRun、Result 示例；当前未收敛完整返回/错误/取消契约，不能算已实现。 | [03. running-your-task](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/02. core-concepts（核心概念）/03. running-your-task.md:7>) |
| RunMany / bulk child | 部分 | 主方案列每项 input/options/item_key；尚无完整公共类型与结果映射，不等于 Batch handler。 | [03. bulk-run](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/03. bulk-run.md:3>) |
| 静态事件订阅 | 部分 | WithEvents 已有；wildcard、多 workflow 命中、注册前事件行为需明确。 | [04. events](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/04. events.md:75>) |
| 默认事件过滤器 | 部分 | DefaultFilter 有类型；同 scope、多 filter 命中多 run、注册更新覆盖和 ID 生命周期尚未定。 | [04. events](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/04. events.md:125>) |
| 事件 Push / scope / priority / metadata | 独立面 | WithEvents 仅声明订阅；Signal/Push 的消息结构、批量发送和错误处理没有完整接口。 | [04. events](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/04. events.md:3>) |
| 静态 cron + input | 具体候选 | WithCron/CronInput 已有；共享 input、UTC/调度时刻与执行时刻、更新覆盖规则需在适配验收中保留。 | [02. cron-runs](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/02. cron-runs.md:17>) |
| 动态 cron 管理 | 独立面 | Create/List/Delete、名称唯一性、删除不取消在途 run 未形成当前客户端契约。 | [02. cron-runs](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/02. cron-runs.md:220>) |
| 一次性 schedule 管理 | 独立面 | UTC、未来入队、更新/批量修改删除、已触发不可改、不补跑等未形成客户端契约。 | [01. scheduled-runs](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/01. scheduled-runs.md:14>) |
| Webhook 接入 | 独立面 | key/scope CEL、headers、静态 payload、Basic/APIKey/HMAC 等资源及验证规则缺具体管理 API。 | [05. webhooks](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/05. webhooks.md:25>) |
| 跨服务 stub 调用 | 部分 | gRPC 生成桩符合目标；跨服务持久调用、契约版本漂移及运行身份尚未实现。 | [06. inter-service-triggering](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/06. inter-service-triggering.md:3>) |
| 暂停 / 恢复 Workflow | 独立面 | 不是暂停 Worker：在途继续、新 run排队、cron/schedule QUEUE/DROP、queue TTL 未设计。 | [04. pausing-workflows](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/04. reliability（可靠性）/04. pausing-workflows.md:3>) |
| 批量 Cancel / Replay | 独立面 | 按 IDs 或 status/time/workflow/metadata 选择、重放创建新执行；当前只有职责占位。 | [05. bulk-retries-and-cancellations](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/04. reliability（可靠性）/05. bulk-retries-and-cancellations.md:5>) |
| 查询 / 日志 / 状态观察 | 部分 | 主方案有 Describe/Watch 名称；过滤、分页、metadata 的 run OR / event AND 语义未具体化。 | [05. additional-metadata](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/08. observability（可观测性）/05. additional-metadata.md:124>) |
| 实时进度 / 结果流 | 缺口 | Hatchet PutStream + SubscribeToStream 是实时流；订阅前消息会丢失。当前 RPC stream 是有限扇入扇出，不能算覆盖。 | [02. streaming](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/09. operations（运维）/02. streaming.md:199>) |

## 中间件、环境与开发边界

| 功能 | 当前设计状态 | 缺口 / 边界 | 文档依据 |
|---|---|---|---|
| handler 中间件 | 部分 | WithMiddleware 与洋葱执行有原则；完整公共接口、错误/取消、实例依赖生命周期和 protobuf 输入输出类型约束未定。 | [01. middleware](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/09. operations（运维）/01. middleware.md:3>) |
| 压缩 / 加密 / S3 卸载 | 部分 | 已经明确载荷阶段及 Client↔Worker 双向边界；构造用法和历史载荷兼容仍未完整示例化。仅 Worker before 无法加密已进入引擎的输入。 | [01. middleware](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/09. operations（运维）/01. middleware.md:490>) |
| 租户 / Namespace 隔离 | 部分 | token→tenant 已定；Namespace 由最新决定不公开，不能声称与 tenant 一样从 token 自动推导。 | [03. environments](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/09. operations（运维）/03. environments.md:3>) |
| Embedded / 本地测试 / 热重载 | 独立面 | 主方案有开发体验目标；没有交付 CLI、假时钟/故障注入或 Embedded 装配。 | [05. embedded](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/01. get-started（入门）/05. embedded.md:3>) |
| Cloud 计算 / SSO / 组织权限 / 审计 | 独立面 | 部署和平台管理能力，不应自动加入 SDK WorkerOption；有 Cloud/计划级别限制。 | [04. audit-logs](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/11. evaluating-hatchet（评估）/04. audit-logs.md:5>) |

## 必须避免的等同关系

1. **Worker labels ≠ 任务路由策略**：标签发布、required/comparator/weight、动态更新是不同能力。
2. **WithDurableTask ≠ 持久运行时完成**：检查点、重放确定性、child 调用、等待和取消需要完整契约。
3. **WithEvents / WithCron ≠ 发送 / 管理 API**：订阅与事件投递、定时声明与资源 CRUD 分开。
4. **RunMany ≠ Batch ≠ RPC stream**：批量提交、跨 run 聚合处理、流程 fan-in/out 分开；实时进度流是第四件事。
5. **Worker Pause ≠ Workflow Pause ≠ Cancel**：实例排空、定义暂停和运行取消对象不同。
6. **本地 /metrics ≠ 引擎 Metrics**：Worker自身运行信号与集群积压/利用率的采集面不同。
7. **版本标签 ≠ 重放兼容**：durable handler 改变执行路径不能靠版本字符串自动恢复旧执行。

## 文档和当前设计中需要校准的语义

- 文档 `middleware.md` 写 Go 支持即将推出，但当前 `src/hatchet/sdks/go/client.go` 已公开 `Worker.Use`；不能据此判定 Go 没有 handler 中间件，更不能倒推载荷传输中间件已经完成。
- 文档的“exactly-once”描述围绕 durable 检查点；普通 task 及检查点间业务副作用仍不能承诺恰好一次。
- 核心 durable sleep 页使用“不消耗资源”的概括，但 eviction 页明确驱逐策略。应拆开调度 slot、本地等待与 handler 内存占用，不承诺一次 Sleep 立刻释放全部本地资源。
- durable execution timeout 是否覆盖等待全程需专项核实。当前 `WaitHello` sleep 1 分钟，继承的 execution timeout 也是 1 分钟，存在计时边界风险；迁移指南 Ruby 示例提示 timeout 需要覆盖 sleep，但这不能直接替代 Go 固定版本验证。本次不擅自改示例参数。
- `ReleaseSlot` 的故障恢复后果较强，列为可选高级能力；并非为了覆盖文档就必须默认提供。
- 共享/动态并发文档要求 engine >=0.106；用户实验项目 v0.105.16 与本仓源码版本不同，不能混用能力结论。

## 下一轮优先落到前三节的内容

本轮只盘点，不直接发明并写入新的公共 API。建议按以下顺序逐项对齐：

1. **一、Worker 模式**：健康/就绪状态；任务亲和路由；中间件具体装配；排空完整验收边界。动态标签与手动释放 slot 单独决定是否纳入。
2. **三、业务实现**：不可重试错误、运行标识/元数据、取消与续期、durable 等待/child 调用/重放规则、实时进度流。
3. **二、Workflow 模式**：用一个完整 DAG 示例确定节点绑定、父输出、分支、汇聚、跳过、取消、失败路径和节点策略。
4. **独立客户端专题**：同步/异步/RunMany、事件、Schedule、Pause/Resume、Cancel/Replay 与资源管理；不把这些变成 Worker 启动配置。
5. **专项可选能力**：Batch、Embedded、autoscaling 集成、平台管理；先定范围，再定接口。

## 41 篇重点文档阅读索引

- [01. tasks](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/02. core-concepts（核心概念）/01. tasks.md:1>)
- [02. workers](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/02. core-concepts（核心概念）/02. workers.md:1>)
- [03. running-your-task](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/02. core-concepts（核心概念）/03. running-your-task.md:1>)
- [04. durable-execution](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/02. core-concepts（核心概念）/04. durable-execution.md:1>)
- [01. scheduled-runs](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/01. scheduled-runs.md:1>)
- [02. cron-runs](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/02. cron-runs.md:1>)
- [03. bulk-run](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/03. bulk-run.md:1>)
- [04. events](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/04. events.md:1>)
- [05. webhooks](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/05. webhooks.md:1>)
- [06. inter-service-triggering](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/03. triggers（触发）/06. inter-service-triggering.md:1>)
- [01. retry-policies](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/04. reliability（可靠性）/01. retry-policies.md:1>)
- [02. timeouts](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/04. reliability（可靠性）/02. timeouts.md:1>)
- [03. cancellation](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/04. reliability（可靠性）/03. cancellation.md:1>)
- [04. pausing-workflows](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/04. reliability（可靠性）/04. pausing-workflows.md:1>)
- [05. bulk-retries-and-cancellations](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/04. reliability（可靠性）/05. bulk-retries-and-cancellations.md:1>)
- [01. concurrency](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/05. flow-control（流控）/01. concurrency.md:1>)
- [02. rate-limits](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/05. flow-control（流控）/02. rate-limits.md:1>)
- [03. priority](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/05. flow-control（流控）/03. priority.md:1>)
- [04. idempotency](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/05. flow-control（流控）/04. idempotency.md:1>)
- [05. batch-tasks](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/05. flow-control（流控）/05. batch-tasks.md:1>)
- [06. cel-expressions](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/05. flow-control（流控）/06. cel-expressions.md:1>)
- [01. durable-tasks](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/01. durable-tasks.md:1>)
- [02. child-spawning](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/02. child-spawning.md:1>)
- [03. durable-sleep](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/03. durable-sleep.md:1>)
- [04. durable-event-waits](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/04. durable-event-waits.md:1>)
- [05. task-eviction](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/05. task-eviction.md:1>)
- [06. directed-acyclic-graphs](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/06. durable-execution（持久化执行）/06. directed-acyclic-graphs.md:1>)
- [01. docker](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/01. docker.md:1>)
- [02. autoscaling-workers](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/02. autoscaling-workers.md:1>)
- [03. sticky-assignment](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/03. sticky-assignment.md:1>)
- [04. worker-affinity](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/04. worker-affinity.md:1>)
- [05. slot-cost](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/05. slot-cost.md:1>)
- [06. manual-slot-release](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/07. workers（工作进程）/06. manual-slot-release.md:1>)
- [01. logging](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/08. observability（可观测性）/01. logging.md:1>)
- [02. opentelemetry](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/08. observability（可观测性）/02. opentelemetry.md:1>)
- [03. worker-healthchecks](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/08. observability（可观测性）/03. worker-healthchecks.md:1>)
- [04. prometheus-metrics](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/08. observability（可观测性）/04. prometheus-metrics.md:1>)
- [05. additional-metadata](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/08. observability（可观测性）/05. additional-metadata.md:1>)
- [01. middleware](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/09. operations（运维）/01. middleware.md:1>)
- [02. streaming](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/09. operations（运维）/02. streaming.md:1>)
- [03. environments](</Users/fatcat/workspace/go/src/github.com/probe/hatchet-docs/zh/v1/09. operations（运维）/03. environments.md:1>)
