# wego 0.1.6 二次评审修复记录

RR01～RR08 全部修复。实现、依赖、测试和 CI 模板仅位于 `sdks/wego`；已撤回 21 处上游修改和 9 个外部新增文件，源码边界检查覆盖 SDK 之外的整个仓库。

| 问题 | 当前行为 | 回归入口 |
|---|---|---|
| RR01 Cron 输入阻塞启动取消 | 编码使用启动 context；未来 envelope 不携带启动 deadline | `TestReReviewStopCancelsCronEncoding` |
| RR02 后续分块失败丢失 RunID | 保留已确认句柄、输入索引和 `PartialSubmissionError.SuccessfulRunIDs` | `TestReReviewBulkCancellationRetainsAcceptedRuns` |
| RR03 订阅发送无法取消 | 每次等待独占可取消订阅，退出前确认发送者已结束 | `TestResultCancellationWhileSubscriptionSendBlocked` |
| RR04 状态轮询阻塞成功结果 | 独立轮询，每次最多 500ms，结果到达后取消并回收 | `TestReReviewRESTPollDoesNotBlockCompletedResult` |
| RR05 批量冲突丢失身份 | SDK 包装错误与原始协议 details 均转为自有错误，保留成功和冲突 RunID | `TestReReviewNormalizeSDKBulkCollision`、`TestReReviewBulkCollisionThroughGRPC` |
| RR06 流取消变成 Unavailable | 保留 Canceled、DeadlineExceeded 和已有 status | `TestReReviewCancellationRetainsGRPCCode`、`TestStreamWaitErrorPreservesDeadlineAndStatus` |
| RR07 致命错误后继续消费 DATA | 致命错误优先于缓冲数据，消费序号不再推进 | `TestReReviewDecodeFailureIsTerminal` |
| RR08 空响应与无响应混淆 | 协议 3 使用 `has_response`；空字节有效，无响应和重复最终响应失败 | `TestEmptyClientStreamResponseRoundTrip`、`TestReReviewClientStreamMissingResponse`、`TestReReviewEmptyFinalResponseHasPresence` |

撤回上游补丁后还补齐了 batch 的逐成员及广播转换、单任务 durable 输出层级、同一 invocation 的 Now 记忆、跨 Worker 子结果读取和 JSON 标量输出。32 次并发 Now、引擎 memo 恢复、标量/null/对象回调有单元回归；跨 Worker 子调用由真实引擎及 race 验证。

发行依赖固定为未经修改的官方 **Hatchet v0.109.0**，没有指向本地 Hatchet 源码的 `replace`。官方 Worker 的依赖接口仍使用私有 `pkg/client.Client`；业务 API 不暴露该类型，不使用旧工作流定义系统。执行连接与带预算的协议连接分别拥有并统一关闭，无需连接注入或缓存关闭补丁。

完整验收通过 **15 项门禁、28 源文件 / 74 片段、30 场景 / 252 条断言**，以及三种流、9 组故障、durable 重启、关闭、真实引擎 race 和 embedded。普通服务为 **v0.107.0 / PostgreSQL MQ**；embedded 在独立数据库中使用官方 **v0.109.0**。关闭工作区后，官方发行依赖也实际跑通基础 RPC、durable、batch、三种流和跨 Worker 子调用。

完整命令、断言、运行身份和清理证据见 [acceptance-report.json](acceptance-report.json)，摘要见 [acceptance-report.md](acceptance-report.md)。测试定义和触发器已删除，embedded 测试库已删除；运行历史和引擎 Worker 记录保留。托管 CI 未执行，模板位于 `tests/ci.yml`。

客户端与 Worker 必须同时升级到协议 3。性能吞吐尚未专项压测，本次结论是功能与正确性验收。
