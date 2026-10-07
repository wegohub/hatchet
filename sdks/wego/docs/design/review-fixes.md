> 本文件记录 0.1.5 的历史修复。当前 SDK 为 0.1.7，Hatchet 上游源码及根依赖未修改；本轮 F01～F11 修复对应关系见 [module.md](module.md) 的 0.1.7 条目，实际运行结果见 [本轮验收报告](acceptance-report.md)。

# wego 0.1.5 评审问题修复记录

本轮对应 2026-10-07 评审的 R01～R15。SDK `0.1.5`，协议 `2`，本机 Hatchet `v0.107.0`，PostgreSQL MQ。R01～R15 已修复并通过定向回归；2026-10-07 本机完整验收通过 13 个质量门禁、28 个源文件 / 74 个 standalone 片段、30 个示例场景 / 252 条断言。逐片段证据已验证场景、断言 ID 与内容，未复用历史版本的通过状态。

| 问题 | 修复与约束 | 回归证据入口 |
| --- | --- | --- |
| R01 提交丢失预算 | 普通、批量、子调用贯通真实 RPC context；子提交与共享订阅初始化排队可取消。提交完成后共享结果监听器由连接生命周期拥有。 | backend 的 TriggerDeadline / UnarySubmissionCancellation / BulkSubmissionCancellation；worker ContextSubmission；client ListenerInitializationQueue；streaming PublishedStreamOutlivesSubmissionBudget |
| R02 停止卡在初始化 | 注册、版本探测、建立监听使用启动预算，网络 I/O 不持引擎或 Worker 状态锁；初始化取消与发布后的监听生命周期分离，保留原始 DeadlineExceeded，StartBlocking 预算覆盖初始化，排空控制通道继续工作。 | ServerStopDuringRegistration；WorkerStartupDeadlineHonorsOriginalCause；StartBlockingBudgetCoversInitialization；grpc-streams / shutdown 真实引擎回归 |
| R03 同名实例混淆 | Worker 使用唯一实例键；注册 ID、监听器完成通知、注销预算各自拥有，同名显示名称不影响身份。 | SameNameListenersCanBothClose；真实引擎 SameNameWorkerInstances，两个不同 WorkerID，分别关闭后继续执行 |
| R04 START 选错 owner | 每个 fullMethod 独立 START 任务，只注册本实例支持的方法；后续控制任务依旧 Required owner。 | 真实引擎 HeterogeneousStreamOwners，Upload / Watch 两个注册集合交替调用；9 组流故障 |
| R05 binary metadata | JSON metadata 编码为 bytes/base64，protobuf Values 改为 repeated bytes；请求、headers/trailers、错误通道均保留多值和 ff00fe 等任意字节。 | BinaryMetadata；wire / session metadata 回归；生成复现 |
| R06 整数亲和标签 | 显式保留 string/int/int32/int64，数值检查 int32 范围；浮点、nil 和非法类型在提交前失败。Required、Weight、Comparator 独立复制。 | NumericAffinity 实际 gRPC 请求；AffinityIntegerBounds |
| R07 缓存资源泄漏 | feature 客户端惰性初始化，Metrics 共享主 Workflows 缓存；独立构造的 Metrics 明确拥有并关闭其缓存。 | ClosedConnectionsLeaveNoCacheGoroutines：重复初始化 Metrics 后关闭连接；上游回归 |
| R08 CronInput 覆盖 | 显式设置标记独立于 Cron；只改输入、只改表达式、nil 清空和未设置继承各自有效。 | CronInputOverride 四种合并场景 |
| R09 invoker 参数被忽略 | 使用拦截器真正传入的 CallOption 和 reply，成功及错误分支均填写响应 metadata。 | InterceptorCallOptionsReachInvoker；InterceptorReplacementOptionsAndReply |
| R10 致命解码未释放 | DATA、最终响应 protobuf / middleware、非法 status 与输出缺口统一保存首个故障并取消会话；观测和后续读取保留错误。 | MalformedFinalReleasesClientLifetime；MalformedBusinessPayloadReleasesLifetime；MiddlewareDecodeFailureReleasesLifetime；MalformedFinalResponseRetainsFailure |
| R11 触发编码失去预算 | 发布/编码使用原预算并在编码前登记在途调用；只省略远期任务 envelope 的 deadline，不剥离 payload I/O 取消。 | EventPayloadHonorsCancellation；TriggerEncodingParticipatesInClose |
| R12 Metrics 缺少 namespace | 在明确的 workflow-name 适配入口统一补 namespace，Metrics 与 Get/Delete/Cron 遵守相同规则。 | WorkflowMetricsUsesNamespace 捕获实际 HTTP 查询名称；Cron/Schedule 真实引擎场景 |
| R13 删除后缓存旧对象 | 成功删除调用统一 SDK Delete/Invalidate；缓存代数阻止删除前的查询回填，Metrics 共享失效；同名重建获得新身份。 | WorkflowDeleteInvalidatesLookup：Get → Delete → 不存在 → 同名新 ID；上游 Workflows 回归 |
| R14 片段覆盖证据不充分 | 源片段使用稳定 ID，每个片段列出必需断言 ID、内容与场景。所有记录通过统一报告入口生成身份；缺失、Skip、失败、错误归组均拒绝完成。 | acceptance.py 的五组负例；生成与 manifest 门禁；本轮逐片段报告 |
| R15 错误和重复注释 | 校正执行确认、载荷契约、child key 与 Print 错误说明；删除冗余分支模板，区分墙上时钟与 durable Now、反射解码值与资源所有权。保留中文声明及协议/并发说明。 | 全 SDK 中文声明门禁；人工核对上述语义；生成复现 |

## 模块与示例修订

- 保留 session/rpc/wire/engine 的职责边界，分别管理状态机、gRPC 语义、协议编码和资源所有权。没有为了缩减目录数而合并有独立不变量的模块。
- `client` / `features` 为公开业务门面；内部实现和工厂集中到 `clientcore` / `featurecore`。`telemetry` 仅提供配置，资源工厂由 `telemetrycore` 管理。公开装配出口 FromEngine、features.New、telemetry.Resources/New 已移除。
- 管理适配用明确的 SDK 方法和参数类型在编译期绑定，移除 MethodByName。亲和标签不经过 JSON roundtrip。REST 动态查询/资源仍使用 wego Query/Resource，不能据此宣称整个 Hatchet 管理 API 已强类型覆盖。
- simple 示例把 WaitHello 注册为 durable 并实际调用。其余场景入口对应的 handler/config 位于 examples/scenarios，27 个独立入口新增 README 直接链接业务实现及公共配置，验收 manifest 给出文件到场景的映射。
- module.md 修正 wire 执行 Payload.Encode/Decode、Server 负责控制任务注册的职责说明，并列出 Worker 与原生网络 gRPC 的兼容范围及性能约束。

## 版本与运行证据

协议 2 不与协议 1 混用；客户端与 Worker 必须同时升级。协议状态、实例装配 API 与功能修复均记录在 CHANGELOG.md。

定向测试覆盖受控 gRPC/HTTP 黑洞、真实调度请求和资源生命周期；使用 fixture 证明真正的 I/O 取消，不通过外层 goroutine 提前返回制造成功。真实多 Worker、多服务、流故障、durable 重启与 embedded 由完整运行器统一执行。

本轮曾暴露并保留的失败：共享结果订阅错误地继承提交预算；启动 context 在排空时提前关闭监听器；trace 断言手工追加记录漏生成 ID。对应修复均补入当前代码，失败日志不改写为成功。最终结果以 [acceptance-report.md](acceptance-report.md) 与 [acceptance-report.json](acceptance-report.json) 为准。

高频流的任务放大和每运行 REST 兜底轮询仍需要专门性能基准；本版没有用未经测量的吞吐声明代替正确性验收。托管 CI 的执行状态与本机门禁分别记录。

## 最终验收与清理

- SDK 单元测试、vet、race、API 隔离、中文声明检查、生成复现与协议 fuzz 均通过；相关上游测试及上游完整 race 通过，两个 embedded 独立模块编译通过。
- 本机完整引擎验收通过三种流及 9 组故障、durable 重启、batch 预算关闭、middleware、Jaeger trace、纯网络与双入口。真实引擎 race 覆盖 batch、流、排空、双入口、同名 Worker 和异构方法集合；embedded 独立进程实际迁移、调用、关闭并删除测试库。
- 独立运行 simple、grpc-streams、dual-entry 成功；simple 实际调用 durable WaitHello。独立命令及退出码见 `.test-results/review-fixes-independent.json`。新增同名 / 异构拓扑的最终 race 命令、18 条运行及 Worker 身份记录见 `.test-results/review-fixes-topology.json`，成功日志保留在 `final-review-topology-race.log`。
- 复查完整验收的 48 个隔离 namespace，剩余工作流定义为 0；工作流与触发清理断言均通过。失败迭代的 40 个工作流资源已清理，失败日志与清理记录保留在 `.test-results`。运行历史与引擎 Worker 记录保留。
- 完整报告生成后，再用最终源码复查 format、unit、vet、race，并将最终门禁记录关联到报告；没有改写失败日志。协议 2 必须配套升级，托管 CI 尚未执行。

复现入口（令牌由本机脚本读取，不写入报告）：

```bash
python3 sdks/wego/scripts/acceptance.py
python3 sdks/wego/scripts/local-test.py go test -race -json -count=1 -tags=e2e ./sdks/wego/tests/e2e/... -run '^TestReview' -timeout 3m
python3 sdks/wego/scripts/local-test.py go run ./sdks/wego/examples/simple
python3 sdks/wego/scripts/local-test.py go run ./sdks/wego/examples/grpc-streams
python3 sdks/wego/scripts/local-test.py go run ./sdks/wego/examples/dual-entry
```
