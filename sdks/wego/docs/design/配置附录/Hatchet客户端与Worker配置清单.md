# Hatchet 客户端与 Worker 配置清单

> 基线：`src/hatchet` commit `315d43a72fd771b049b304b865a81dbab98c466c`。仅覆盖连接、Worker 运行、观测、嵌入式与开发 CLI；Workflow/Run 定义不在本文展开。`公开`指 `sdks/go` 可直接使用；`v0` 是仍可编译但源码标记 Deprecated 的旧入口。

## wego 公共入口更新

配置归属以[主方案第四章第 8 节](../wego SDK设计方案.md)为准：`server.WithWorker(addr, ...worker.Option)` 同时配置内部调度 client 和 worker；标准 `RegisterXServer` 负责注册定义，不向用户提供 Hatchet `WithWorkflows`。本附录下方保留原始源码盘点，用于适配实现。

`worker.Option` 必须是 wego 自有类型，不是 `hatchet.ClientOpt`、`hatchet.WorkerOption` 或 `v0Client.ClientOpt` 的别名。当前 `sdks/go.ClientOpt` 虽仍桥接 Deprecated 的 v0 类型，不能因此推荐公共 API 直接接受旧类型。公共调用现为 WithToken、WithSlots、WithDurableSlots；名称由注册的 ServiceName 组合与 hostname 自动生成，多个 Service 共享一个 Worker 与 slots 池。文件可独立配置 worker/gRPC 的 enabled。载荷编解码统一归中间件，不提供独立 Codec 模块。实例连接和 Worker 参数归 `worker`，原生租户、namespace、REST/Cloud 等归 `worker.engines.hatchet`；任务/流程策略仍保留其独立作用域。原生构造缺口、文件覆盖和日志/OTel 生命周期见主方案。

## 1. 配置面与 wego 映射

|配置面|Hatchet 实际入口与生效链|wego 归属|结论|
|---|---|---|---|
|Go Client|`hatchet.NewClient` 将公开 Option 原样桥接给 `pkg/client.New`；后者先环境/`client.yaml`取默认，再按 Option 覆盖，建 gRPC、REST、Cloud REST 客户端。[sdks/go/client.go:48-128](../../../src/hatchet/sdks/go/client.go) [pkg/client/client.go:109-153](../../../src/hatchet/pkg/client/client.go)|公共|可成为 `wego.NewClient` 代码 Options；文件/环境只应作为部署输入，不应泄漏 Hatchet 名称。|
|Worker|`Client.NewWorker` 只把 slots、durable slots、labels、logger、panic handler 透传至 `pkg/worker`；注册、启动、心跳由其执行。[sdks/go/client.go:193-300](../../../src/hatchet/sdks/go/client.go)|公共+适配器专属|公共暴露执行池容量、路由标签、日志；Hatchet 的 `slot_config` 是适配器映射。|
|OTel|SDK worker middleware；默认读同一 client 配置并向 Hatchet OTLP collector 发 span。[sdks/go/opentelemetry/instrumentor.go:24-115](../../../src/hatchet/sdks/go/opentelemetry/instrumentor.go)|公共（开关/资源）+代码注入|Exporter、TracerProvider、BSP Options 不能 YAML 反序列化。|
|嵌入式|`WithEmbedded` 写入旧 ClientOpts，`NewClient` 先启动 blank-import 注册的后端，再创建连接。[sdks/go/embedded.go:11-120](../../../src/hatchet/sdks/go/embedded.go)|开发/部署|不是通用生产连接配置；后端函数不可配置化。|
|CLI/profile|`~/.hatchet/config.yaml` 管 CLI；profiles 默认 `~/.hatchet/profiles.yaml`；当前目录 `hatchet.yaml` 管开发 worker reload。[pkg/config/cli/config.go:10-67](../../../src/hatchet/pkg/config/cli/config.go) [cmd/hatchet-cli/cli/internal/config/worker/config.go:10-78](../../../src/hatchet/cmd/hatchet-cli/cli/internal/config/worker/config.go)|CLI/部署|不应混入 SDK 服务端运行配置。|

## 2. 公开 Go ClientOption（连接）

|API|字段/默认或验证|实际生效|wego 映射|
|---|---|---|---|
|`WithToken(string)`|未给时读 `HATCHET_CLIENT_TOKEN`；空 token 报错。|写 `ClientOpts.token`，用于 gRPC metadata、REST Bearer、OTLP exporter。[sdks/go/client.go:53-57](../../../src/hatchet/sdks/go/client.go) [pkg/client/loader/loader.go:51-75](../../../src/hatchet/pkg/client/loader/loader.go)|公共凭据；值应由凭据提供器/secret 注入。|
|`WithHostPort(host,port)`|覆盖 token/环境地址；底层拼为 `host:port`。|gRPC `grpc.NewClient`；无地址报错。[sdks/go/client.go:59-63](../../../src/hatchet/sdks/go/client.go) [pkg/client/client.go:185-189](../../../src/hatchet/pkg/client/client.go)|适配器连接端点。|
|`WithNamespace(string)`|Option 仅追加 `_`；空则得到 `_`。|影响 workflow/event/cron 名；仅 loader 配置路径会 lower-case。[sdks/go/client.go:65-68](../../../src/hatchet/sdks/go/client.go) [pkg/client/client.go:199-204](../../../src/hatchet/pkg/client/client.go)|适配器专属，不能当业务 identity。|
|`WithTenantId(string)`|覆盖 token 内 tenant；未给取 JWT claim。|传给 REST 子客户端/请求上下文。[sdks/go/client.go:70-73](../../../src/hatchet/sdks/go/client.go) [pkg/client/loader/loader.go:99-102](../../../src/hatchet/pkg/client/loader/loader.go)|适配器专属多租户路由。|
|`WithTLSConfig(*tls.Config)`|`nil`=明文；覆盖环境构造的 TLS。|直接变为 gRPC transport credentials，OTel 不自动使用此显式对象。[sdks/go/client.go:75-79](../../../src/hatchet/sdks/go/client.go) [pkg/client/client.go:294-315](../../../src/hatchet/pkg/client/client.go)|代码注入不可序列化；文件应表达 TLS 参数而非 `tls.Config`。|
|`WithGRPCHeaders(map[string]string)`|无默认。|与 `authorization` 合并为每个 RPC outgoing metadata；同名可覆盖 authorization。[sdks/go/client.go:81-84](../../../src/hatchet/sdks/go/client.go) [pkg/client/context.go:15-29](../../../src/hatchet/pkg/client/context.go)|公共 metadata，但需限制保密/保留键。|
|`WithSharedMeta(map[string]string)`|无默认。|附到 client 推送的每个 event，不是全 RPC header。[sdks/go/client.go:86-89](../../../src/hatchet/sdks/go/client.go)|公共事件 metadata。|
|`WithClientLogger(*zerolog.Logger)`|无默认时由 client config 建 stderr logger。|Client logger；worker 没有显式 logger 时派生 `service=worker` logger。[sdks/go/client.go:91-99](../../../src/hatchet/sdks/go/client.go) [sdks/go/worker.go:63-81](../../../src/hatchet/sdks/go/worker.go)|代码注入不可序列化；文件可只放 level/format。|
|`WithClientLogLevel(string)`|非法 level 静默保留默认 logger level。|新建 default client logger；与 `WithClientLogger` 后者执行顺序决定最终值。[pkg/client/client.go:157-170](../../../src/hatchet/pkg/client/client.go)|公共 level；需在 wego 做显式校验，避免静默。|
|`WithEmbedded(...EmbeddedOption)`|见第 5 节；`HATCHET_CLIENT_EMBEDDED_DATABASE_URL` 也可触发。|先启动内嵌引擎；缺 blank import 后端报错。[sdks/go/client.go:101-128](../../../src/hatchet/sdks/go/client.go)|开发/部署。|

连接固定行为：gRPC keepalive 为 `10s/60s/PermitWithoutStream=true`；gzip 默认开；gRPC retry 默认开（最多 5 次，`ResourceExhausted/DeadlineExceeded/Internal/Unavailable`，每次 30s，full-jitter 5s→80s），REST retry 默认开。[pkg/client/client.go:305-329](../../../src/hatchet/pkg/client/client.go) [pkg/client/retry/grpc.go:15-53](../../../src/hatchet/pkg/client/retry/grpc.go) stream 重连是另一套固定策略：`Unavailable/Internal/DeadlineExceeded/ResourceExhausted` 可重连，listen loop 可持续重连；基数 1s、上限 30s、2 倍 full-jitter，同步订阅/发送最多 5 次；没有暴露为 ClientOption/YAML。[pkg/client/retry/stream.go:11-75](../../../src/hatchet/pkg/client/retry/stream.go) [pkg/client/reconnecting_stream.go:1-82](../../../src/hatchet/pkg/client/reconnecting_stream.go) 这些属于适配器专属默认；如 wego 公开，须定义自己的稳定语义。

云端 managed compute 不是 ClientOpt，而是 workflow/action 的 `compute.Compute`：`pool?`、`numReplicas(0..1000)`、`regions[]`、`cpus(1..64)`、`computeKind`（shared 或 performance）、`memoryMb(256..65536)`、`gpuKind?`、`gpus?(1..8)`；`ComputeHash` 为 JSON 的 SHA-256。[pkg/client/compute/config.go:12-44](../../../src/hatchet/pkg/client/compute/config.go) 它面向 Hatchet Cloud，归适配器专属部署/计算资源字段，不应并入通用 worker slots。

### 公开连接 Option 的 AST 对账

对 `sdks/go/client.go` 与 `sdks/go/embedded.go` 的 `^func With[A-Z]` 索引，当前公开连接入口为 **10 个**：前表 9 个 `WithToken` 至 `WithClientLogLevel`，加 `WithEmbedded`；没有 `Without*` ClientOption。`DisableHatchetCollector` 是 OTel `InstrumentorOption`，不计入连接。源树在此 commit 中**不存在** `pkg/client.WithServerURL`、`WithNoGrpcRetry`、`WithNoRetry`、`WithCloudRegisterID` 或 `WithAutoscalingTarget` 函数；这些是 `pkg/v1.Config`/`ClientConfigFile` 字段，不能伪称可作为当前 ClientOpt 传入。

`pkg/client.ClientOpt` 的全部公开函数（10 个）是：`WithLogLevel`、`WithLogger`、`WithTenantId`、`WithHostPort`、`WithToken`、`WithNamespace`、`WithSharedMeta`、`InitWorkflows`、`WithGRPCHeaders`、`WithTLSConfig`。[pkg/client/client.go:157-246](../../../src/hatchet/pkg/client/client.go) 前 8 个对应或支撑当前 SDK；`InitWorkflows` 未由新版导出；所有仍可直接传给 `hatchet.NewClient`，因为 `hatchet.ClientOpt` 是该类型别名。[sdks/go/client.go:48-99](../../../src/hatchet/sdks/go/client.go) 其中 `WithSharedMeta`/`WithGRPCHeaders` 是累加合并，其他相同字段按最后执行的 Option 覆盖。

|旧 `pkg/client.ClientOpt`|字段/验证|当前 SDK 可直接传入与结果|
|---|---|---|
|`WithLogLevel`|非法 zerolog level 不报错，使用 newly created default client logger。|可传；新版别名未另包装，等价 `WithClientLogLevel`。|
|`WithLogger`|直接替换 logger 指针，无 nil 防护。|可传；新版 `WithClientLogger` 调此函数。|
|`WithTenantId`|直接覆盖 string。|可传；新版包装。|
|`WithHostPort`|`fmt.Sprintf("%s:%d")`，不在 Option 时验证。|可传；新版包装。|
|`WithToken`|直接覆盖 string。|可传；新版包装。|
|`WithNamespace`|直接追加 `_`，不 lower-case；与 loader 路径的 lower-case 行为不同。|可传；新版包装。|
|`WithSharedMeta`|逐 key merge。|可传；新版包装。|
|`InitWorkflows`|设 `initWorkflows=true`，`newFromOpts` 调 files loader 和 `initWorkflows`。|可传；新版未导出，不是推荐入口。|
|`WithGRPCHeaders`|逐 key merge。|可传；新版包装。|
|`WithTLSConfig`|替换 `*tls.Config`，nil=明文。|可传；新版包装。|

## 3. `client.yaml` / 环境 / TLS

|YAML字段（环境）|默认、校验和下游效果|wego 分类|
|---|---|---|
|`tenantId` (`HATCHET_CLIENT_TENANT_ID`)、`token` (`...TOKEN`)|token 必须存在；tenant 空/UUID nil 用 JWT tenant。|适配器专属/secret。|
|`hostPort` (`...HOST_PORT`)、`serverURL` (`...SERVER_URL`)|任一缺失时可从 JWT 取；仍缺则报错。前者给 gRPC，后者给 REST/Cloud REST。|适配器连接端点。|
|`namespace` (`...NAMESPACE`)|非空小写并加 `_`。|适配器专属。|
|`log.level`,`log.format` (`...LOG_LEVEL`,`...LOG_FORMAT`)|client loader 实际默认 `debug/json`；shared struct tag 的 `warn/console` 不改变该 client binder 结果。|部署参数。|
|`noGrpcRetry`,`noRetry` (`...NO_GRPC_RETRY`,`...NO_RETRY`)|前者只关 gRPC retry；后者同时关 gRPC/REST retry。|公共重试策略候选。|
|`disableGzipCompression` (`...DISABLE_GZIP_COMPRESSION`)|true 不添加 gzip call option。|适配器传输。|
|`cloudRegisterID` (`HATCHET_CLOUD_REGISTER_ID`)、`runnableActions` (`HATCHET_CLOUD_ACTIONS`)|后者逐项 trim 并加 namespace；值透传 Cloud/worker client。|Hatchet Cloud 专属。|
|`autoscalingTarget` (`...AUTOSCALING_TARGET`)|转成预置 worker label `hatchet-autoscaling-target`，由 dispatcher 使用。|Hatchet 专属 label 注入。|
|`tls.base.*`,`tls.tlsServerName`（见下）|loader 建立 `*tls.Config`；`none` 产生 nil。|部署 TLS 参数。|

`tls.base` 字段：`tlsStrategy` 枚举 `tls|mtls|none`（注释默认 `tls`），`tlsCert/tlsCertFile/tlsKey/tlsKeyFile/tlsRootCA/tlsRootCAFile`，`tlsMinVersion` 枚举 `1.2|1.3`（注释默认 1.3）；client 特有 `tlsServerName` 未给时从 host:port 推导。[pkg/config/shared/shared.go:5-18](../../../src/hatchet/pkg/config/shared/shared.go) [pkg/config/client/client.go:68-91](../../../src/hatchet/pkg/config/client/client.go) 实际证书组合/字段互斥验证在 `loaderutils.LoadClientTLSConfig`，本文未复述，避免以注释代替验证事实。

### `client.yaml` 的字段级环境、默认与校验

`LoadConfigFromViper` 先 BindEnv、Merge YAML、执行结构体 defaults、再 Unmarshal；client binder 另显式 `v.SetDefault("log.level","debug")`、`v.SetDefault("log.format","json")`，故 **client 实际无输入 logger 默认是 debug/json**，不是 shared struct tag 的 warn/console。[pkg/config/loader/loaderutils/viper.go:11-35](../../../src/hatchet/pkg/config/loader/loaderutils/viper.go) [pkg/config/client/client.go:50-61](../../../src/hatchet/pkg/config/client/client.go)

|字段|环境变量|实际默认/loader 校验与作用|
|---|---|---|
|`tenantId`|`HATCHET_CLIENT_TENANT_ID`|空或 UUID nil 时取 JWT tenant；无 UUID 格式验证。|
|`token`|`HATCHET_CLIENT_TOKEN`|必填；空立即报错；随后必须能解析 JWT。|
|`hostPort`|`HATCHET_CLIENT_HOST_PORT`|空时取 JWT `GrpcBroadcastAddress`，仍空报错；用于 gRPC，后续 TLS server name 可从它解析。|
|`serverURL`|`HATCHET_CLIENT_SERVER_URL`|空时取 JWT `ServerURL`，仍空报错；用于 REST/Cloud REST。|
|`namespace`|`HATCHET_CLIENT_NAMESPACE`|空无前缀；非空 lower-case 并补 `_`。|
|`log.level`|`HATCHET_CLIENT_LOG_LEVEL`|默认 `debug`；交给 `logger.NewStdErr`，本 loader 未见枚举验证。|
|`log.format`|`HATCHET_CLIENT_LOG_FORMAT`|默认 `json`；交给 `logger.NewStdErr`，本 loader 未见枚举验证。|
|`noGrpcRetry`|`HATCHET_CLIENT_NO_GRPC_RETRY`|bool 零值 false；true 关闭 gRPC interceptor retry，但 `noRetry=true` 也会关闭它。|
|`noRetry`|`HATCHET_CLIENT_NO_RETRY`|bool 零值 false；true 同时关闭 gRPC 和 REST retry。|
|`cloudRegisterID`|`HATCHET_CLOUD_REGISTER_ID`|`*string`，未给 nil；保留在 client config。|
|`runnableActions`|`HATCHET_CLOUD_ACTIONS`|未给 nil；给出后每项 trim 并补 namespace，不校验 action 格式。|
|`autoscalingTarget`|`HATCHET_CLIENT_AUTOSCALING_TARGET`|空不加；非空生成 preset label `hatchet-autoscaling-target`。|
|`disableGzipCompression`|`HATCHET_CLIENT_DISABLE_GZIP_COMPRESSION`|bool 默认 false；true 移除 gRPC gzip call option。|

|TLS 字段|环境变量|默认、优先级、校验和生效|
|---|---|---|
|`tls.base.tlsStrategy`|`HATCHET_CLIENT_TLS_STRATEGY`|struct default `tls`；只接受 tls、mtls、none，`none` 返回 nil TLS（明文）。|
|`tls.base.tlsCert`|`HATCHET_CLIENT_TLS_CERT`|空；仅当 `tlsKey` 同时非空才从 PEM 建 certificate；若同时给 file，内联优先。|
|`tls.base.tlsKey`|`HATCHET_CLIENT_TLS_KEY`|空；仅与 `tlsCert` 配对；不成对时静默不装 certificate。|
|`tls.base.tlsCertFile`|`HATCHET_CLIENT_TLS_CERT_FILE`|空；仅当 `tlsKeyFile` 同时非空才 `LoadX509KeyPair`，读取/解析错误返回。|
|`tls.base.tlsKeyFile`|`HATCHET_CLIENT_TLS_KEY_FILE`|空；仅与 `tlsCertFile` 配对；不成对时静默不装 certificate。|
|`tls.base.tlsRootCA`|`HATCHET_CLIENT_TLS_ROOT_CA`|空；非空优先于 rootCAFile，PEM 无法 append 返回错误。|
|`tls.base.tlsRootCAFile`|`HATCHET_CLIENT_TLS_ROOT_CA_FILE`|空；仅 rootCA 为空时读取，读失败/无有效 PEM 返回错误。|
|`tls.base.tlsMinVersion`|`HATCHET_CLIENT_TLS_MIN_VERSION`|空/`1.3`/`tls1.3`/`tls_1.3`/`tls13`=TLS1.3；`1.2` 同类别名=TLS1.2；其他报错。|
|`tls.tlsServerName`|`HATCHET_CLIENT_TLS_SERVER_NAME`|空时从 hostPort URL hostname 推导；`tls` 使用推导值，`mtls` 覆盖回该原字段值（空也可），无额外必填校验。|

TLS 真实构造与上述边界见 [pkg/config/loader/loaderutils/tls.go:14-124](../../../src/hatchet/pkg/config/loader/loaderutils/tls.go)。`WithTLSConfig` 在 v0 `New` 读取环境后执行，故可覆盖该构造结果；但 OTel Instrumentor 自己再读 loader，不会继承这个对象。[pkg/client/client.go:257-290](../../../src/hatchet/pkg/client/client.go) [sdks/go/opentelemetry/instrumentor.go:76-95](../../../src/hatchet/sdks/go/opentelemetry/instrumentor.go)

## 4. 新版 Worker 与底层 workerOpts

|新版公开 API|默认/验证|下层生效|wego 映射|
|---|---|---|---|
|`Client.NewWorker(name,... )`|先探测 engine：`GetVersion` 未实现/空/低于 `v0.78.23` 走旧双 worker；其他连接错误假定新引擎。|新引擎发送 `slot_config`；旧引擎使用 legacy `slots`。[sdks/go/client.go:193-249](../../../src/hatchet/sdks/go/client.go) [sdks/go/deprecated_worker.go:20-63](../../../src/hatchet/sdks/go/deprecated_worker.go)|适配器能力探测。|
|`WithWorkflows(...WorkflowBase)`|默认空。|Dump 后逐个 `RegisterWorkflowV1`，并注册 actions。|代码注册，不可 YAML。|
|`WithSlots(int)`|未显式时只在所需 default slot type 出现时置 100；无工作流也最终 default=100。|映射 `slot_config[default]`。|公共执行池容量。|
|`WithDurableSlots(int)`|需要 durable 时默认 1000。|映射 `slot_config[durable]`；也作为 eviction manager 容量。|公共持久等待容量。|
|`WithLabels(map[string]any)`|无新版值校验；底层仅允许 string/int，nil value 会 panic（`reflect.TypeOf(nil)`）。|注册请求 Labels；与 preset autoscaling labels 在 dispatcher 合并。|公共路由标签；wego 应补 nil/type 校验。|
|`WithLogger(*zerolog.Logger)`|优先于 client logger；否则派生 worker logger。|传底层 worker 与 durable eviction manager。|代码注入。|
|`WithPanicHandler(func(Context,any))`|无默认。|调用 `SetPanicHandler`；函数不能配置化。|代码注入。|

底层 `pkg/worker.WorkerOpt` 仍可编译但全部标记 Deprecated（`WithLegacySlots` 除外且仅兼容用途）：`WithInternalData`、`WithName`、`WithClient`、`WithIntegration`、`WithErrorAlerter`、`WithMaxRuns`、`WithSlots`、`WithDurableSlots`、`WithSlotConfig`、`WithLegacySlots`、`WithLabels`、`WithLogger`、`WithLogLevel`。[pkg/worker/worker.go:149-303](../../../src/hatchet/pkg/worker/worker.go) 其验证：`slotConfig` 不可与 slots/durableSlots 并用；二者皆无时默认 `{default:100}`；labels 仅 string/int；`WithLogger` 在已有 logger 后忽略并告警。新版 `NewWorker` 实际只透传 name/client/slot config/logger/labels，panic handler 事后设置；integration、alerter、internal actions、worker log level 均不从新版公开 API 透传。

### 容量、labels 与 durable eviction 的遗漏项

|项|事实与证据|wego 归属|
|---|---|---|
|多维 slots|Hatchet 当前只识别 `default`、`durable` 两类；任务 `SlotRequests` 决定需要的 pool。无足够 default 空位不能跑；durable task 忽略 task `WithSlotCost`。[sdks/go/client.go:441-492](../../../src/hatchet/sdks/go/client.go) [sdks/go/workflow.go:445-459](../../../src/hatchet/sdks/go/workflow.go)|wego 用可移植 execution pool/capacity，Hatchet 映射为这两键。|
|label comparator/weight|`DesiredWorkerLabel` 由 Run options 下传；比较器枚举为 `EQUAL/NOT_EQUAL/GREATER_THAN/GREATER_THAN_OR_EQUAL/LESS_THAN/LESS_THAN_OR_EQUAL`，可带 Weight。[sdks/go/workflow.go:52-68](../../../src/hatchet/sdks/go/workflow.go) [pkg/client/types/file.go:117-139](../../../src/hatchet/pkg/client/types/file.go)|路由偏好为适配器专属；不要与 worker labels 的声明混同。|
|durable eviction|Task 的 `EvictionPolicy{TTL,AllowCapacityEviction,Priority}`；默认政策为 15m/true/0，但仅显式 `WithEvictionPolicy` 才注册；选择顺序 TTL，再容量，低 priority 先、同 priority 等待更久先。manager 固定检查 1s、保留 0 slot、容量等待至少 10s。[sdks/go/eviction_policy.go:5-31](../../../src/hatchet/sdks/go/eviction_policy.go) [sdks/go/internal/eviction/manager.go:11-22](../../../src/hatchet/sdks/go/internal/eviction/manager.go)|公共持久等待释放语义可抽象；检查频率/协议为适配器专属。|
|版本降级|durable eviction 仅 engine `>=v0.80.0`；不支持时 SDK warning 并丢弃 policy/manager，不失败。[sdks/go/client.go:505-549](../../../src/hatchet/sdks/go/client.go) [sdks/go/engine_version.go:11-32](../../../src/hatchet/sdks/go/engine_version.go)|wego 不可默默降级，应声明 capability/failure policy。|

## 5. 嵌入式、OTel、CLI 开发配置

|面|字段/API、默认和验证|生效与 wego 归属|
|---|---|---|
|Embedded|`WithEmbeddedDatabaseURL`、`WithEmbeddedGRPCPort`、`WithEmbeddedAPIPort`、`WithoutEmbeddedAPI`、`WithoutEmbeddedMigrations`、`WithEmbeddedRabbitMQ`、`WithEmbeddedLogLevel`；API/migrations 注释默认 true；数据库 URL 若未 Option 则读取 `HATCHET_CLIENT_EMBEDDED_DATABASE_URL`。|交给 blank-import backend；端口/DB/RabbitMQ/log level 是开发/部署参数，backend 是代码注入。[sdks/go/embedded.go:11-120](../../../src/hatchet/sdks/go/embedded.go)|
|OTel Instrumentor|`WithTracerProvider(*sdktrace.TracerProvider)`、`DisableHatchetCollector()`、`WithBatchSpanProcessorOptions(...)`、废弃但仍可用的 `EnableHatchetCollector()`；collector 默认 true，未传 provider 时 service.name=`hatchet-worker`。|collector 从 client env/file 重载 endpoint/token/TLS；`Unimplemented` 后静默停 5 分钟再尝试。provider/exporter/BSP 是代码注入，collector 开关可作部署参数。[sdks/go/opentelemetry/instrumentor.go:31-115](../../../src/hatchet/sdks/go/opentelemetry/instrumentor.go) [sdks/go/opentelemetry/exporter.go:15-72](../../../src/hatchet/sdks/go/opentelemetry/exporter.go)|
|共享 server OTel 配置|`collectorURL,serviceName(default server),traceIdRatio(default 1),insecure(default false),collectorAuth,metricsEnabled(default false)`。|这是 engine 自身 outbound OTel，非 SDK worker exporter；归部署。[pkg/config/shared/shared.go:39-46](../../../src/hatchet/pkg/config/shared/shared.go)|
|CLI 全局|`profileFileName` 默认 `profiles.yaml`；logger level=`warn`, format=`text`, prefix；telemetry enabled=`true`, endpoint=`https://security.hatchet.run`, anonymousId。环境 `HATCHET_CLI_*` 仅绑定 file/logger/telemetry enabled+endpoint。|CLI 专属；`anonymousId` 未绑定环境。[pkg/config/cli/config.go:10-67](../../../src/hatchet/pkg/config/cli/config.go)|
|CLI Profile|`tenantId,name,token,expiresAt,apiServerURL,grpcHostPort,tlsStrategy(default tls)`；Profile store 写入时要求 name/token/API/gRPC，TLS 仅允许 tls/mtls/none，空补 tls。|部署/本地登录状态，不能视作 worker SDK config。[pkg/config/cli/config.go:29-40](../../../src/hatchet/pkg/config/cli/config.go) [pkg/config/cli/profilestore/store.go:220-270](../../../src/hatchet/pkg/config/cli/profilestore/store.go)|
|`hatchet.yaml` worker dev|`triggers[]:{command,name,description}`；`dev:{preCmds[],runCmd,files[],reload}`。只从当前工作目录读取，不存在返回 nil；无字段默认或语义验证。|开发 CLI 专属；不透传 SDK Worker。[cmd/hatchet-cli/cli/internal/config/worker/config.go:10-78](../../../src/hatchet/cmd/hatchet-cli/cli/internal/config/worker/config.go)|

### 嵌入式字段级清单

|API/字段|默认与验证|实际生效|
|---|---|---|
|`WithEmbedded()`|空 `EmbeddedConfig`；如果无 blank-import 注册的 backend，`NewClient` 返回 error。|在创建 v0 client 前启动引擎；Close 调 backend shutdown。|
|`databaseURL` / `WithEmbeddedDatabaseURL`|空=使用 bundled Postgres；若未 `WithEmbedded`，但 `HATCHET_CLIENT_EMBEDDED_DATABASE_URL` 非空，也构造 EmbeddedConfig。|交给 backend，现文件不校验 URL。|
|`grpcPort` / `WithEmbeddedGRPCPort`|`*int` nil=backend 默认；不校验端口范围。|交给 backend。|
|`apiPort` / `WithEmbeddedAPIPort`|`*int` nil=backend 默认；不校验端口范围。|交给 backend。|
|`startAPI` / `WithoutEmbeddedAPI`|nil=注释默认 true；调用设 `false`。|交给 backend。|
|`runMigrations` / `WithoutEmbeddedMigrations`|nil=注释默认 true；调用设 `false`。|交给 backend。|
|`rabbitMQURL` / `WithEmbeddedRabbitMQ`|nil=backend 默认；不校验 URL。|交给 backend。|
|`logLevel` / `WithEmbeddedLogLevel`|nil=backend 默认；不校验 level。|交给 backend。|

证据：[sdks/go/embedded.go:11-120](../../../src/hatchet/sdks/go/embedded.go)。这些 Options 只填 `EmbeddedConfig`，具体默认和端口/URL validation 不在本 commit 的 SDK 层，不能补猜。

### OTel 字段级清单

|API/字段|默认与验证|实际生效|
|---|---|---|
|`WithTracerProvider(*sdktrace.TracerProvider)`|nil 时新建 SDK provider，resource `service.name=hatchet-worker`。|向该 provider 注册 exporter processor，并设为 global provider。|
|`DisableHatchetCollector()`|默认 collector=true；调用设 false。|不加载 client config、不创建 Hatchet exporter。|
|`EnableHatchetCollector()`|默认已 true；Deprecated，调用设 true。|显式恢复 Hatchet exporter。|
|`WithBatchSpanProcessorOptions(... )`|默认空；无校验。|原样传 `sdktrace.NewBatchSpanProcessor`。|
|collector endpoint|不是 Instrumentor Option；每次 `NewInstrumentor` 从 client loader 取 `GRPCBroadcastAddress`。|OTLP gRPC endpoint。|
|collector token|同上，从 client loader token 取。|OTLP header `authorization: Bearer <token>`。|
|collector TLS|同上，从 client loader TLS 取。|nil 时 `WithInsecure`，否则 TLS credentials。|
|unsupported 退避|`codes.Unimplemented` 时吞掉错误并禁用 5 分钟；其他 export error 原样返回。|仅 exporter 内部状态，不可配置。|

证据：[sdks/go/opentelemetry/instrumentor.go:31-115](../../../src/hatchet/sdks/go/opentelemetry/instrumentor.go) [sdks/go/opentelemetry/exporter.go:15-72](../../../src/hatchet/sdks/go/opentelemetry/exporter.go)。

### Compute、CLI Profile 与 `hatchet.yaml` 字段级清单

|面/字段|默认与校验|分类|
|---|---|---|
|Compute.`pool`|nil；`omitempty`，无更多验证。|Cloud 专属。|
|Compute.`numReplicas`|0；`0..1000`。|Cloud 专属。|
|Compute.`regions`|nil；`omitempty`。|Cloud 专属。|
|Compute.`cpus`|0；必须 `1..64`。|Cloud 专属。|
|Compute.`computeKind`|空；必填 `shared/performance`。|Cloud 专属。|
|Compute.`memoryMb`|0；必须 `256..65536`。|Cloud 专属。|
|Compute.`gpuKind`|nil；`omitempty`，枚举具体值由 REST generated type 定义。|Cloud 专属。|
|Compute.`gpus`|nil；非 nil 必须 `1..8`。|Cloud 专属。|
|Profile.`tenantId`|无默认；写入 profile 时必填。|CLI 登录状态。|
|Profile.`name`|无默认；写入 profile 时必填。|CLI 登录状态。|
|Profile.`token`|无默认；写入 profile 时必填。|CLI secret。|
|Profile.`expiresAt`|零 time 可存；本 struct 无校验。|CLI 登录状态。|
|Profile.`apiServerURL`|无默认；写入 profile 时必填。|CLI 连接。|
|Profile.`grpcHostPort`|无默认；写入 profile 时必填。|CLI 连接。|
|Profile.`tlsStrategy`|空时 profile store 写回 `tls`；仅 tls、mtls、none。|CLI 连接。|
|`hatchet.yaml`.triggers[].`command`|无默认/本 loader 无必填检查。|CLI 开发。|
|`hatchet.yaml`.triggers[].`name`|空，注释称默认 command，但 loader 不执行该填充。|CLI 开发。|
|`hatchet.yaml`.triggers[].`description`|空；无验证。|CLI 开发。|
|`hatchet.yaml`.dev.`preCmds`|nil；无验证。|CLI 开发。|
|`hatchet.yaml`.dev.`runCmd`|空；无验证。|CLI 开发。|
|`hatchet.yaml`.dev.`files`|nil；无验证。|CLI 开发。|
|`hatchet.yaml`.dev.`reload`|false；无验证。|CLI 开发。|

证据：[pkg/client/compute/config.go:12-44](../../../src/hatchet/pkg/client/compute/config.go) [pkg/config/cli/config.go:29-40](../../../src/hatchet/pkg/config/cli/config.go) [pkg/config/cli/profilestore/store.go:220-270](../../../src/hatchet/pkg/config/cli/profilestore/store.go) [cmd/hatchet-cli/cli/internal/config/worker/config.go:10-78](../../../src/hatchet/cmd/hatchet-cli/cli/internal/config/worker/config.go)。

### `pkg/client` 与 `pkg/worker` Option 函数索引边界

主审 AST 的 27 个 `pkg/client` option-function 候选不含 generated REST/Cloud。其连接/观测/worker 项已覆盖 ClientOpt 的 9 个 `With*` 加 `InitWorkflows`、`pkg/client/v1` 的 5 个连接 Option、DurableTaskListener 的 2 个 Option；generated REST/Cloud 的 `WithHTTPClient`、`WithRequestEditorFn`、`WithBaseURL` 各 3 个单独计数，不并入 27。Run/Schedule/Event Option 见任务与编排附录。`pkg/worker` 的 17 个候选中，WorkerOpt 13 项已在第 4 节列名，`RegisterActionOpt` 的 `WithActionName/WithCompute` 和 condition 的 `WithEventScope/WithConsiderEventsSince` 为 workflow/task 定义面，不在本 Worker 实例配置范围。[pkg/worker/service.go:90-106](../../../src/hatchet/pkg/worker/service.go) [pkg/worker/condition/condition.go:98-114](../../../src/hatchet/pkg/worker/condition/condition.go)

|DurableTaskListener Option|默认、验证|实际用法|
|---|---|---|
|`WithReconnectInterval(time.Duration)`|默认 `2s`；仅赋值，无正数校验。|listener 的 stream 结束且非 terminal 后调用 `retry.Sleep(ctx, interval)` 再重连；当前新版 `Client.NewWorker` 创建 listener 时未传该 Option，故实际走 2s。|
|`WithEvictionAckTimeout(time.Duration)`|默认 `30s`；仅赋值，无正数校验。|发送 eviction request 后以此创建 timer；超时清理 pending acknowledgement 并返回 timeout error；当前新版 `Client.NewWorker` 未传该 Option，故实际走 30s。|

证据：[pkg/client/durable_task_listener.go:19-24](../../../src/hatchet/pkg/client/durable_task_listener.go) [pkg/client/durable_task_listener.go:126-170](../../../src/hatchet/pkg/client/durable_task_listener.go) [pkg/client/durable_task_listener.go:401-421](../../../src/hatchet/pkg/client/durable_task_listener.go) [pkg/client/durable_task_listener.go:549-577](../../../src/hatchet/pkg/client/durable_task_listener.go)。

### `pkg/v1.Config` 的委托边界

`pkg/v1/config.go` 整个包头已标明 legacy v0 workflow definition system；其中 `Config{Token,TenantId,HostPort,ServerURL,Namespace,TLSConfig}` 可经 `pkg/v1.NewHatchetClient` 创建旧客户端，但**当前** `sdks/go.NewClient` 没有调用它。当前公开 `hatchet.ClientOpt` 的真实委托是 `pkg/client.ClientOpt`，再进入 `pkg/client.New`，所以 `pkg/v1.Config` 的连接字段不会因传入当前 ClientOpt 而触达；只有名称相同的新版 `WithToken/WithHostPort/WithNamespace/WithTenantId/WithTLSConfig` 分别直接写同一条 v0 client 路径。[pkg/v1/config.go:1-45](../../../src/hatchet/pkg/v1/config.go) [sdks/go/client.go:48-99](../../../src/hatchet/sdks/go/client.go) [pkg/client/client.go:257-290](../../../src/hatchet/pkg/client/client.go) 因此不可把整个 `pkg/client` 等同为“v1 不可用”：它虽为 Deprecated 旧层，却是当前新版 SDK 的实际连接委托；`pkg/v1.Config` 则不在此调用链。

## 6. 对 wego 配置设计的落点

1. **公共配置**：连接端点、凭据引用、TLS 参数、可移植执行池容量、worker 路由 labels、日志级别/格式、事件 metadata、retry/压缩开关、OTel 开关与资源名。
2. **适配器专属**：Hatchet namespace/tenant/Cloud 字段、default 与 durable slot_config、autoscaling label、label comparator/weight、Hatchet OTLP collector、legacy engine/version 与 eviction 协议。必须放 `engines.hatchet`，不能提升为跨引擎公共字段。
3. **只能代码注入**：`*tls.Config`、`*zerolog.Logger`、handler/workflow、panic handler、middleware、OTel TracerProvider/BatchSpanProcessor/Exporter、EmbeddedBackend、error alerter/integration。YAML 只能引用已注册名字或工厂键。
4. **CLI/部署**：profiles、`hatchet.yaml dev`、embedded DB/RabbitMQ/port、进程的 env/TLS files；与 `wego.NewServer` 的运行时配置分开。Hatchet 源码没有单一 YAML 覆盖所有上述 Go Options 的合并模型，wego 不能宣称其继承该行为。

## 覆盖统计

- 新版 ClientOption：10 项（9 个 `sdks/go/client.go` 连接 Option，加 `WithEmbedded`）；新版 WorkerOption：6 项；Embedded 子 Option：7 项；OTel InstrumentorOption：4 项（含已废弃启用项）。
- `ClientConfigFile`：13 个顶层字段、1 个 TLS 子结构；共享 TLS 8 字段、shared OTel 6 字段；CLI 全局 3 组、Profile 7 字段、`hatchet.yaml` worker 2 组；另含 managed Compute 8 字段与固定 stream reconnect 策略。
- 旧 `pkg/worker.WorkerOpt`：13 个函数逐项列名并标注新版透传边界；未将 Workflow/Run API 重复计入覆盖数，仅保留 labels/slot/eviction 所必需的交叉证据。
