# RPC 流实施与验收参考

先读 [主方案](nats-rpc-stream-design.md)。本文按需查阅：各阶段交付细节、能力证据和验收清单。原章节编号保留，便于定位已有评审引用。

<a id="s11"></a>

## 11 实现结构与计划

目标模块按职责收敛：Extra transport 处理代理 API；direction writer/reader 处理消息提交与接收；history 只读有限快照；RPC adapter 映射生成桩、handler 和运行状态。两个方向复用同一组收发实现，不复制 client/server 两套恢复算法。

当前实现的核对入口：[Extra API](../../../../internal/services/extra/proto/v1/extra.proto)、[发布器](../../../../internal/services/extra/publisher.go)、[Fetch](../../../../internal/services/extra/consumers.go)、[RPC Client](../../internal/rpc/stream_client.go)、[RPC Worker](../../internal/rpc/stream_worker.go)、[旧历史扫描](../../internal/stream/history.go)及 [task.Checkpoint](../../task/checkpoint.go)。

### P0 固定能力与故障探针

核验仓库使用的 nats-server 2.15.1、nats.go 1.54.0、grpc-go 1.84.0 的实际部署能力：原子批次开关、同 subject 条件检查、BatchAck 丢失后读回、consumer ACK floor、AckNone 临时读取与期限清理。

增加 ProtoJSON 与二进制 protobuf 的体积、编解码和 codec 组合基准，覆盖 bytes、大整数、多字段与接近大小上限的消息；当前编码合同保持 [§4](nats-rpc-stream-protocol-reference.md#s4)，若实测支持切换则先统一更新帧、大小定义和历史 Payload 合同，再实现。

固定协议 7 的三类消息编码、StreamRef、历史接口和本节配置，验证租户 subject 映射及反向映射。用稳定 Nats-Msg-Id 与 GetMessage(E+1, subject) 核对未知结果；原生批次 ID 不透明，不能要求计算物理前缀或查询暂存批次的放弃状态。

2026-10-15：Extra 的专项测试已验证批内 Stream Sequence 连续、Msg-Id 原样读回、最终确认不可用后的单次首条查询，以及旧会话仍暂存时的新会话重试。另验证批内命中已存 Msg-Id 会拒绝整批，不会留下此前记录的已提交前缀。见 [核对测试](../../../../internal/services/extra/batch_reconcile_test.go)。这些是 P2 实现的能力前提，SDK 核对循环仍待实现。

### P1 Extra 必要补充

2026-10-15：原子发布、批量 ACK 等基础接口已有实现、测试与本地 TLS 部署记录；Read、GetLimits 等后续增补需分别核验，见 [Extra 验收记录](../../../../internal/services/extra/TEST_REPORT.md)。wego 的方向协议、RPC 适配与历史 API 仍按 P2–P4 实施。

| 能力 | 当前情况 | 实施项 |
| --- | --- | --- |
| 原子 PublishBatch | 已实现 client-streaming RPC；PublishAsync 保留单条流水线语义 | 完整验证后提交同 subject 的原子批次，返回 batch_id、batch_size 和最终确认 |
| 批量 ACK | 已实现有界 AckBatch | 按 index 返回逐条提交结果；confirm=false 为正常路径 |
| 双预算 Fetch | 已支持双预算，默认条数上限为 100 | SDK 在 P2 继续管理解码预算和取消收尾 |
| 有限历史 Read | 工作树已增加接口，接入须验收实际服务 | 精确 subject、序号闭区间、条数/字节分页、独立读名额与服务端清理；仍计入 NATS consumer 预算 |
| 限额与错误分类 | 工作树已有 GetLimits 与 NatsError.reason | SDK 按实际限额预算，按 reason 分类；逐项以 Extra 验收为准 |
| 基础资源初始化 | Stream/Consumer/Bucket 均已支持 create_only | 已存在返回 AlreadyExists，SDK 读回校验 |
| 可发行客户端桩 | 已提供 pkg/extra/v1，依赖图不含服务端 internal | 随根模块发行；wego 引用包含此包的版本，鉴权映射保留在服务端 |

PublishBatch 不是把 PublishAsync 改成串行循环：必须保证 N 条 NATS 记录全有或全无，且同 subject 的前置条件在提交时检查。服务端限制批次条数、累计字节和并发内存；客户端断流时不伪造批次成功。

### P2 底层方向协议

实现 producer FIFO、组批、串行原子提交、条件竞争、结果未知核对、最终 EOF 和关闭并发。实现按容量 Pull、ACK 前移、有限 chan、EOF 状态检查、失败唤醒与资源收尾。

公开配置扩展既有 WithStream/StreamConfig 与方法级 StreamOptions，按 Send、Recv、History 组织。原生发布/拉取、ACK 参数和资源配置只在适配层映射，不增加组件名称进入业务 API。

完成单方向记录配额、EOF 预留、scope 留存调用准入与部署预算校验；Send 接纳时同时计入已提交、已接纳未提交及控制记录的位置，不能仅检查已确认数量。接收支持逐条到达即 ACK、字节预算下的补充低水位、独立结束计时和过期唤醒。

保存足够的调用生命周期信息防止过期后误初始化；方向 consumer 缺失即失败，不通过删除重建“修复”进度。暂不接入业务 API，先完成独立协议测试。

### P3 RPC 适配与运行集成

替换 internal/rpc 的输入数组提交、旧输出日志解释、Checkpoint 门禁和 completion 合取。三个形态都传输两个方向，校验单请求/单响应基数。

将同一调用的两个 subject 引用及生命周期信息注入 Worker 任务 Context 和 Client 流 Context，使两个入口均能独立选择 client/server 方向并创建历史快照。接入运行观察器处理最终失败、取消、无 EOF 和 EOF 已提交后的上报恢复。将与 CLAIM 绑定的执行上报逻辑拆开，验证旧执行竞争失败不覆盖新执行状态。

保留标准生成桩的调用方式，补齐 CloseAndRecv、SendAndClose、Header/Trailer、并发 Send/CloseSend、半关闭及错误映射。输入业务恢复不因 transport 重试而获得额外保证。

### P4 历史读取与 Bloom

实现 task.ClientStream / task.ServerStream，统一返回 model.StreamHistory，替换旧 Next(proto.Message) 合同；在句柄上实现 Snapshot、First/Last、AllMessage、Iterator、Contains、Bloom 和 Close。适配 Extra.Read 有限分页、固定 H0 和精确索引缓存；每项操作包含缓存命中时都校验句柄未关闭且未过期。

实现类型规范化、内存限额、迭代 break/取消清理、错误终止及同对象并发保护。删除 task.Checkpoint 的“读完才能发布”约束，迁移示例。

### P5 清理与发布

1. 发布支持所需能力的 Extra 及可发行客户端桩，部署初始化 RPC 共享资源、容量准入和过期清理任务。
2. Client、Worker、方法绑定和部署配置按目标协议协调发布；本文不设计双协议并行运行或历史数据转换。
3. 移除 [§2](nats-rpc-stream-protocol-reference.md#s2) 指定的 Reliable/Realtime、旧游标、CLAIM/ATTEMPT_END 及其专属配置，接入 [§9.4](nats-rpc-stream-config-reference.md#s9-4) 的观测与诊断。
4. 更新 module.md、worker.md、README、示例、CHANGELOG、version.go 与质量 manifest。目标版本只在实现和验收完成后提升，不在文档阶段提前宣布支持。
5. 仍被非 RPC 功能使用的 backend/通知/codec 保留其独立职责，不以重构流为由删除其他功能。

发布门禁包含 GOWORK=off 的独立模块构建；不能依靠本地 go.work 解析尚未发布的根模块源码。所有非 go test 的 CI Hatchet 启动入口设置 SERVER_SECURITY_CHECK_ENABLED=false。

<a id="s12"></a>

## 12 验收与边界

| 场景 | 必须验证的结果 |
| --- | --- |
| 三种 RPC 正常调用 | 请求/响应条数、双 subject、半关闭、最终状态正确 |
| 并发 Send 与 CloseSend | 入队顺序确定，EOF 唯一且最后，关闭后不接纳 |
| 两写者相同 expected | 一个批次获胜，落败者停止，不刷新 expected 重新加入 |
| BatchAck 丢失 | 核对已提交的自身批次，不误判竞争、不重复提交下一批 |
| consumer 新旧并存 | 共享进度；重复可出现；没有收到 EOF 的实例也能退出 |
| chan 满与空位补齐 | 不新增超预算 Pull；在途预留无超卖；取消不会向关闭 chan 写入 |
| 稀疏输入与小内存预算 | 单条到达不等 Fetch 超时；低水位使用有效批次大小，不会卡在无法达到的阈值 |
| 批次大小、配额与错误 reason | 计算完整计费字节；EOF 有预留；容量暂忙与永久超限采用不同动作 |
| ACK 生效后崩溃 | chan 未读数据允许丢失，恢复不声称自动补回 |
| 单请求或单响应已 ACK 后丢失 | 返回 ErrRequestUnavailable/ErrResponseUnavailable，不伪造零值业务消息 |
| EOF 先于其他实例完成 | 检查共享 NumPending/NumAckPending；本实例缓冲读空后才结束 |
| handler 失败/重试/取消 | 中间失败不写最终 EOF；终态无 EOF 时唤醒等待者 |
| EOF 成功但运行上报失败 | 不重复 handler 输出，恢复终态上报 |
| ClientStream/ServerStream 方向选择 | 分别绑定 rpc_xxx_client / rpc_xxx_server，两个端点均不反转含义 |
| 历史句柄未调用消息读取方法 | 只读取并校验尾记录，不创建 consumer、不扫描历史 DATA |
| AllMessage/Iterator/First/Last | 同一句柄使用相同 H0、正确过滤控制记录、空历史、错误不伪装完整结果 |
| 扫描时继续生产或再次创建句柄 | 已有 H0 固定；新句柄读取新尾部，不修改 writer 的 expected |
| Iterator break/取消/超时 | 释放请求及临时 consumer，生命周期可收敛 |
| Read 分页与慢 Iterator | 一页结束不是快照结束；业务停顿不持有服务端 consumer；总预算仍执行 |
| 同字段重复 Contains | 首次 O(n) 建表，后续不发历史查询；超出精确索引预算返回错误 |
| Bloom 误判 | true 必须精确复核；已插入 key 无假阴性；缺失字段和不可完成的快照扫描报错 |
| 历史 TTL、资源删除、未知 codec | 返回过期/缺失/解码错误，不构建“完整” Bloom |
| scope 与方法伪造 | 不访问其他租户或 namespace，不信任调用引用自报身份 |
| 观察器与取消 | 最后一次失败且无 EOF 时唤醒；已确认终态后的 cancel 不取消 Run |
| 资源生命周期 | EOF 加 consumer 缺失报错；恢复截止时间到达必定退出；GC 失败可观测 |

性能矩阵覆盖三种 RPC、1 KiB/64 KiB/接近实际帧上限、1/100/1000 并发；高并发点须先满足 [§3.1.1](nats-rpc-stream-protocol-reference.md#s3-1-1)，不能在开发默认容量下声称通过。固定硬件、网络、codec、复制数和数据分布，记录 P50/P99、吞吐、常驻 goroutine、SDK/Extra 内存、consumer 与长期 RPC 数、历史读取量。发布前按部署 SLO 填入延迟与吞吐目标，当前没有实测依据，不编造通用性能数字。

可直接判定的性能门禁：低流量第一条不等待 Fetch 满批/超时；默认 Send 无额外组批等待；同字段 q 次 Contains 只进行一次完整建表；单页结束后扫描 consumer 可回收；空闲检查按退避上限收敛；正常收尾后 goroutine/资源存量回到基线。故障测试覆盖消费者暂停、未知 BatchAck、单条/批次大小边界、额外 scope 负载和过期清理失败。

明确不提供：本地缓冲持久化、业务处理 ACK、严格单实例执行、跨实例全局 Recv 顺序、历史无限保留、Bloom 精确去重，以及 NATS 与 Hatchet/业务数据库的跨系统事务。
