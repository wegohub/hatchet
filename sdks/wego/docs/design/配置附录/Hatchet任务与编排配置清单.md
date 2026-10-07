# Hatchet 任务与编排配置清单

> **取证基线**：`src/hatchet` commit `315d43a72fd771b049b304b865a81dbab98c466c`（本地版本标识 `v0.106.11-4-g315d43a72`）。本附录只描述该提交的 Go SDK 及其 v1 声明 wire；不把旧 YAML/v0 API 当作新 SDK 能力。`路径:行` 都相对 `src/hatchet`。

## 判定口径

|分类|含义|wego 建议|
|---|---|---|
|公共语义|跨引擎可表达的声明或调用语义|进入 `wego` 公共配置/运行选项；启动前校验引擎支持，无法保持语义即拒绝|
|适配器专属|Hatchet wire 或本地 worker 行为，Temporal 未必等价|置于 `engines.hatchet` 扩展，不得伪装成中性键|
|不作配置|运行输入、查询参数、回调/函数、管理 API 或当前不生效字段|保留为方法参数、资源管理 API，或不暴露|

表中 `task.*` / `workflow.*` 是逻辑字段族，具体作用域遵循主方案第四章第 7 节；`engines.hatchet.admin.*` 表示管理 API 的参数分组，不表示自动写入 NewServer YAML。

`WithX` 后面的“发送”指源码能追至 `CreateWorkflowVersionRequest`、`CreateTaskOpts` 或触发请求；不是只依据注释。全局声明最终由 `Dump` 形成 v1 请求（`sdks/go/internal/declaration.go:715-813`）。

## 1. Workflow / Standalone 声明

|Hatchet 公开项 / 字段|wire 映射、默认与约束|拟议 wego 键|分类|证据|
|---|---|---|---|---|
|`WithWorkflowCron(expressions...)`|替换 cron 列表；有 cron 而未给 input 时 SDK 填 JSON `"{}"`。序列化为 `cron_triggers`,`cron_input`|`workflow.triggers.cron[]`,`workflow.triggers.cron_input`|公共语义|`sdks/go/workflow.go:262-285,369-385`; `sdks/go/internal/declaration.go:745-753`|
|`WithWorkflowCronInput(input any)`|非 nil 先 JSON marshal（失败 panic），nil 写 `{}`；只影响 cron run 的 `cron_input`，没有 cron 时仍可写进声明|`workflow.triggers.cron_input`|公共语义|`sdks/go/workflow.go:270-285,374-385`; `sdks/go/internal/declaration.go:745-753`|
|`WithWorkflowEvents(events...)`|替换事件键列表，发 `event_triggers`；无 SDK 事件格式校验|`workflow.triggers.events[]`|公共语义|`sdks/go/workflow.go:288-293`; `sdks/go/internal/declaration.go:745-753`|
|`WithWorkflowVersion(string)`|非空才填 request `version`；空等于未设|`workflow.version`|公共元数据（映射原生版本标签，不承诺 replay 兼容）|`sdks/go/workflow.go:295-300`; `sdks/go/internal/declaration.go:755-757`|
|`WithWorkflowDescription(string)`|非空才填 request `description`|`workflow.description`|公共语义（可选元数据）|`sdks/go/workflow.go:302-307`; `sdks/go/internal/declaration.go:759-761`|
|`WithWorkflowConcurrency(...Concurrency)`|逐项发 `concurrency_arr`。SDK 若显式 `MaxRuns<=0` panic；其余结构约束见第 4 节|`workflow.concurrency[]`|公共语义|`sdks/go/workflow.go:309-314,346-367`; `sdks/go/internal/declaration.go:763-790`|
|`WithWorkflowTaskDefaults(*TaskDefaults)`|只在任务内部同字段指针为 nil 且默认值非零时填 wire；公开 TaskOption 的零值会转为 nil，即显式零也可能继承默认；batch retries 恒为 0，不能被默认值覆盖|`workflow.task_defaults`|公共语义|`sdks/go/workflow.go:316-321`; `pkg/client/create/tasks.go:36-52`; `sdks/go/internal/task/task.go:274-296`|
|`TaskDefaults.ExecutionTimeout`|仅任务未给 timeout 且该值非零时填 `timeout`；零=省略|`workflow.task_defaults.attempt_timeout`|公共语义|`pkg/client/create/tasks.go:36-42`; `sdks/go/internal/task/task.go:280-282`|
|`TaskDefaults.ScheduleTimeout`|仅任务未给且该值非零时填 `schedule_timeout`；零=省略|`workflow.task_defaults.schedule_timeout`|公共语义|`pkg/client/create/tasks.go:41-42`; `sdks/go/internal/task/task.go:284-287`|
|`TaskDefaults.Retries`|仅非 batch、任务未给 retries 且该值非零时填；零=省略|`workflow.task_defaults.retry.max_attempts`（原生次数 + 1）|公共语义|`pkg/client/create/tasks.go:44-45`; `sdks/go/internal/task/task.go:274-278`|
|`TaskDefaults.RetryBackoffFactor`|仅任务未给且该值非零时填 `backoff_factor`|`workflow.task_defaults.retry.backoff.factor`|公共语义|`pkg/client/create/tasks.go:47-48`; `sdks/go/internal/task/task.go:289-291`|
|`TaskDefaults.RetryMaxBackoffSeconds`|仅任务未给且该值非零时填 `backoff_max_seconds`|`workflow.task_defaults.retry.backoff.max_interval`|公共语义|`pkg/client/create/tasks.go:50-51`; `sdks/go/internal/task/task.go:293-295`|
|`WithWorkflowDefaultPriority(RunPriority)`|发 `default_priority`。枚举 Low=1、Medium=2、High=3；未设交给服务端|`workflow.default_priority`|公共语义|`sdks/go/features/crons.go:13-23`; `sdks/go/workflow.go:323-328,387-390`; `api-contracts/v1/workflows.proto:109-111`|
|`WithWorkflowStickyStrategy(Soft/Hard)`|发 `sticky`；SOFT=0,HARD=1，未设为 server 行为|`engines.hatchet.workflow.sticky_strategy`|适配器专属（worker 亲和）|`sdks/go/workflow.go:330-335`; `sdks/go/internal/declaration.go:797-800`; `api-contracts/v1/workflows.proto:71-74`|
|`WithWorkflowIdempotency`：`Expression string`|CEL 幂等键表达式，直接发 `expression`，无 SDK 非空/CEL 校验|`workflow.idempotency.key_expr`|公共语义|`sdks/go/workflow.go:236-246,337-344`; `sdks/go/internal/declaration.go:802-812`|
|`WithWorkflowIdempotency`：`TTL time.Duration`|直接换算毫秒发 `ttl_ms`，无正值校验；STATUS 下为最长保留上限|`workflow.idempotency.ttl`|公共语义|`sdks/go/workflow.go:240-245`; `sdks/go/internal/declaration.go:808-812`; `api-contracts/v1/workflows.proto:123-127`|
|`WithWorkflowIdempotency`：`Method`|`TTL`/`STATUS`；零值或未知值均被 SDK 映射为 TTL|`workflow.idempotency.method`|公共语义|`sdks/go/workflow.go:219-229,244-245`; `sdks/go/internal/declaration.go:802-812`|
|v1 `input_json_schema bytes`|wire 确有该 optional 字段，但 `workflowConfig`、所有 `WithWorkflow*` 与 `Dump` 都不写它；当前新 Go SDK 无公开透传入口|不暴露（以后需专门 schema API）|不作配置|`api-contracts/v1/workflows.proto:96-116`; `sdks/go/workflow.go:248-260`; `sdks/go/internal/declaration.go:745-813`|
|`WithDefaultFilters(...DefaultFilter)`|替换默认事件过滤器列表；通过 workflow/standalone 声明发送，字段见下三行|`workflow.event_filters[]`|公共语义|`sdks/go/workflow.go:491-496`; `sdks/go/internal/declaration.go:730-743`|
|`DefaultFilter.Expression string`|直接发 `expression`；wire required，SDK 不校验空|`workflow.event_filters[].expression`|公共语义|`pkg/client/types/file.go:159-163`; `sdks/go/internal/declaration.go:730-742`; `api-contracts/v1/workflows.proto:139-143`|
|`DefaultFilter.Scope string`|直接发 `scope`；wire required，SDK 不校验空|`workflow.event_filters[].scope`|公共语义|`pkg/client/types/file.go:159-163`; `sdks/go/internal/declaration.go:730-742`; `api-contracts/v1/workflows.proto:139-143`|
|`DefaultFilter.Payload map`|JSON marshal 后发 optional payload；nil 可发 JSON `null`；marshal 失败的单项被 **静默丢弃**|`workflow.event_filters[].payload`|公共语义|`pkg/client/types/file.go:159-163`; `sdks/go/internal/declaration.go:730-743`; `api-contracts/v1/workflows.proto:139-143`|
|`Workflow.OnFailure(fn)`|注册额外 `on_failure_task`；函数不是配置值，任务默认配置仍经 `CreateTaskOpts`。签名运行时检查|`workflow.on_failure`（声明节点引用）|公共语义，函数本身不作配置|`sdks/go/workflow.go:865-885,923-926`; `sdks/go/internal/declaration.go:793-795`; `api-contracts/v1/workflows.proto:108`|
|`NewStandaloneTask/BatchTask/DurableTask`|并非独立 wire 类型：创建同名 Workflow + 单任务。其 `TaskOption` 的 Cron/Events 被提升为 workflow trigger；错误类型 panic|`standalone` 仅为 API 构造器，不设独立配置根|不作配置（便捷构造入口）|`sdks/go/client.go:694-713,715-853`|

## 2. 普通、durable 与 batch Task 声明

|公开项 / 字段|实际 wire、默认与约束|拟议 wego 键|分类|证据|
|---|---|---|---|---|
|`NewTask(name,fn)`|name 非空、fn 非 nil/反射签名必须 `(Context,input)(output,error)`；发 `readable_id`,`action`、父依赖等|`task.name`,`task.handler`|名称公共；handler 不作配置|`sdks/go/workflow.go:562-616,651-682`; `api-contracts/v1/workflows.proto:180-197`|
|`NewDurableTask`|同 Task 加 `is_durable=true`，默认 durable slot `{DURABLE:1}`；handler 首参必须 `DurableContext`|`engines.hatchet.task.durable=true`|适配器专属（需 wego durable runtime 抽象）|`sdks/go/workflow.go:846-857`; `sdks/go/internal/task/task.go:362-372`|
|`WithRetries(int)`|直接转 `int32` 发 `retries`，没有正负校验；wire 是**失败后的重试次数**，故总尝试次数=`retries+1`。零值在声明转换时成为 nil，可继承 Workflow TaskDefaults；两者都未设时 wire 为 0（一次尝试）|`task.retry.max_attempts`（适配时 `retries = max_attempts - 1`）|公共语义|`sdks/go/workflow.go:430-435,651-664`; `sdks/go/internal/task/task.go:227-230`; `sdks/go/internal/declaration.go:282-303`; `api-contracts/v1/workflows.proto:187`|
|`WithRetryBackoff(factor,maxSeconds)`|均转 int32/float；没有值域检查，非零时发 `backoff_factor`,`backoff_max_seconds`|`task.retry.backoff.{factor,max_interval}`|公共语义|`sdks/go/workflow.go:437-443,651-664`; `sdks/go/internal/task/task.go:232-238`|
|`WithExecutionTimeout(duration)`|非零时间转换为截断秒字符串 `timeout`；0 不发（不能表达显式零）|`task.attempt_timeout`|公共语义|`sdks/go/workflow.go:468-473`; `sdks/go/internal/task/task.go:218-225,354-359`|
|`WithScheduleTimeout(duration)`|同样截断秒，发 optional `schedule_timeout`；0 不发|`task.schedule_timeout`|公共语义|`sdks/go/workflow.go:461-466`; `sdks/go/internal/task/task.go:222-225,354-359`|
|`WithSlotCost(int)`|1..MaxInt32，否则 panic；普通任务发 `slot_requests[DEFAULT]`（未设为 1）。durable task 忽略此设置且强制 DURABLE=1|`engines.hatchet.task.slot_cost`|适配器专属|`sdks/go/workflow.go:445-459`; `sdks/go/internal/task/task.go:301-313,362-369`|
|v1 `CreateTaskOpts.worker_labels` / 任意 `slot_requests`|wire 各自存在，但新 `TaskOption` 没有 worker-label setter，slot requests 也只由 `WithSlotCost` 派生 DEFAULT 或 durable 派生 DURABLE；不是完整透传|不暴露为通用 map；上述受限键除外|不作配置|`api-contracts/v1/workflows.proto:180-197`; `sdks/go/workflow.go:411-550`; `sdks/go/internal/task/task.go:301-313,362-369`|
|`WithConcurrency(...*Concurrency)`|多个项目按次序发 `CreateTaskOpts.concurrency`；nil 元素会在 dump 解引用，属 SDK 未防护输入|`task.concurrency[]`|公共语义|`sdks/go/workflow.go:498-503,584`; `sdks/go/internal/task/task.go:188-215`|
|`WithRateLimits(...*RateLimit)`|逐项发 task rate limit，字段及枚举见第 5 节；没有非 nil/数值校验|`task.rate_limits[]`|公共语义|`sdks/go/workflow.go:512-517`; `sdks/go/internal/task/task.go:143-185`|
|`WithParents(...*Task)`|转换为 parent readable ids；空为根任务。DAG 依赖是公共编排语义但不是“运行参数”|`task.depends_on[]`|公共语义|`sdks/go/workflow.go:519-529,651-664`; `sdks/go/internal/task/task.go:314-315`; `api-contracts/v1/workflows.proto:186`|
|`WithWaitFor(Condition)` / `WithSkipIf(Condition)`|分别以 action `QUEUE`/`SKIP` 序列化 TaskConditions；条件构件见第 6 节|`task.wait_for` / `task.skip_if`|公共语义（表达式/事件语义须 runtime 支持）|`sdks/go/workflow.go:531-543`; `sdks/go/internal/task/task.go:317-349`|
|`WithCron(...)`,`WithEvents(...)`|仅 standalone 便捷入口：构造器将它们提升为 workflow trigger。对普通 workflow task 没有发送路径|同第 1 节 trigger|公共语义，仅 standalone 可用|`sdks/go/workflow.go:475-489`; `sdks/go/client.go:698-713`|
|`WithDescription(string)`|**当前无效**：写 `taskConfig.description`，但建立 `WorkflowTask` 时未复制；v1 `CreateTaskOpts` 也无 description 字段|不暴露，或仅本地元数据|不作配置|`sdks/go/workflow.go:411-428,545-550,651-664`; `api-contracts/v1/workflows.proto:180-197`|
|`WithEvictionPolicy(*EvictionPolicy)`：`TTL`,`AllowCapacityEviction`,`Priority`|仅 durable task；不进 CreateTaskOpts，而是随 `NamedFunction` 注册到本地 worker eviction manager。默认常量 TTL=15m、Allow=true、Priority=0，但**并未自动套用**（未调用该常量即 nil）|`engines.hatchet.durable.eviction`|适配器专属、本地 worker 配置|`sdks/go/eviction_policy.go:5-31`; `sdks/go/workflow.go:666-677`; `sdks/go/internal/declaration.go:857-866`|
|`NewBatchTask(name,fn,BatchConfig,opts...)`|Preview/Beta；不能 durable；按**到达的并发 run**聚合，满足 size 或 interval 才 flush，绝非 DAG 的“所有 parents 完成后扇入”。`Retries` wire 永远覆盖为 0，不能承诺 batch retry 生效|`task.batch`|公共语义但标为 experimental|`sdks/go/workflow.go:685-733,826-843`; `sdks/go/internal/task/task.go:240-272`|
|`BatchConfig.MaxSize`|必填且 >0，发 `batch_max_size`|`task.batch.max_size`|公共语义/experimental|`sdks/go/batch.go:10`; `pkg/client/types/batch.go:5-30`; `sdks/go/workflow.go:711-721`; `api-contracts/v1/workflows.proto:172-178`|
|`BatchConfig.MaxInterval *Duration`|nil 表示仅 size 刷新；非 nil >0，毫秒 int32 发 `batch_max_interval_ms`（很长 duration 溢出未防护）|`task.batch.max_wait`|公共语义/experimental|`pkg/client/types/batch.go:14-16`; `sdks/go/internal/task/task.go:245-251`|
|`BatchConfig.GroupKey *string`|非 nil 发 CEL `batch_group_key`|`task.batch.group_key`|公共语义/experimental|`pkg/client/types/batch.go:18-21`; `sdks/go/internal/task/task.go:253-255`|
|`BatchConfig.GroupMaxRuns *int32`|非 nil 必须 >0，发 `batch_group_max_runs`|`task.batch.group_max_runs`|公共语义/experimental|`pkg/client/types/batch.go:23-24`; `sdks/go/workflow.go:719-721`; `sdks/go/internal/task/task.go:257-259`|
|`BatchConfig.BroadcastOutput bool`|true 才发 optional `broadcast_output=true`；false 省略且是 wire 默认|`task.batch.broadcast_output`|公共语义/experimental|`pkg/client/types/batch.go:26-29`; `sdks/go/internal/task/task.go:261-264`; `api-contracts/v1/workflows.proto:172-178`|

## 3. Run / RunMany / Event（都是运行时输入，非声明配置）

|公开项 / 字段|真实发送与限制|wego 归类|证据|
|---|---|---|---|
|`Run`/`RunNoWait` 的 `input any`|运行载荷；`Run` 等结果、`RunNoWait` 返回 ref。不是配置|运行输入|`sdks/go/workflow.go:931-983`; `sdks/go/client.go:1022-1072`|
|`WithRunMetadata(map[string]string)`|写 `additional_metadata`，SDK 还注入 OpenTelemetry `traceparent`|`run.metadata`，公共语义|`sdks/go/workflow.go:124-139,998-1006`; `api-contracts/v1/shared/trigger.proto:65-67`|
|`WithRunPriority(Low/Medium/High)`|发 priority；上述 1/2/3。未设由 workflow default/server 决定|`run.priority`，公共语义|`sdks/go/workflow.go:141-146,993-1010`|
|`WithDesiredWorkerLabels(map[string]*DesiredWorkerLabel)`|替换本次 run 的目标标签条件；传给触发请求，字段见下四行|`engines.hatchet.run.worker_labels`，适配器专属|`sdks/go/workflow.go:162-167,1012-1014`|
|`DesiredWorkerLabel.Value any`|wire 仅可表达 string 或 int32；新层不校验，转换在 legacy bridge|`engines.hatchet.run.worker_labels[].value`，适配器专属|`sdks/go/workflow.go:162-167,1012-1014`; `pkg/client/types/file.go:132-137`; `api-contracts/v1/shared/trigger.proto:16-20,75-76`|
|`DesiredWorkerLabel.Required bool`|wire optional，省略默认 false|`engines.hatchet.run.worker_labels[].required`，适配器专属|`pkg/client/types/file.go:132-137`; `api-contracts/v1/shared/trigger.proto:21-27`|
|`DesiredWorkerLabel.Comparator *WorkerLabelComparator`|枚举 EQUAL/NOT_EQUAL/GREATER_THAN/GREATER_THAN_OR_EQUAL/LESS_THAN/LESS_THAN_OR_EQUAL；省略默认 EQUAL|`engines.hatchet.run.worker_labels[].comparator`，适配器专属|`sdks/go/workflow.go:54-69`; `pkg/client/types/file.go:117-137`; `api-contracts/v1/shared/trigger.proto:7-14,29-33`|
|`DesiredWorkerLabel.Weight int32`|wire optional，省略默认 100|`engines.hatchet.run.worker_labels[].weight`，适配器专属|`pkg/client/types/file.go:132-137`; `api-contracts/v1/shared/trigger.proto:35-40`|
|`WithRunKey(string)`,`WithRunSticky(bool)`|**仅当 ctx 是 Hatchet task Context 的 child workflow** 时传给 `SpawnWorkflow`（Key/Sticky）；外部 Admin Run 只收 metadata/priority/labels，故二者静默不发送|`engines.hatchet.workflow.child.{key,sticky}`，适配器专属|`sdks/go/workflow.go:148-160,1002-1041`; `sdks/go/client.go:1131-1140`|
|`RunManyOpt{Input any,Opts []RunOptFunc}`、`RunMany`|每项运行输入和运行选项；返回顺序与输入一致。不是持久声明配置；child durable path/legacy bulk path由 ctx 决定|运行输入|`sdks/go/client.go:1153-1195`; `sdks/go/workflow.go:1062-1081`|
|`EventClient.Push/BulkPush`; `WithEventMetadata`,`WithEventPriority`,`WithFilterScope`|向事件发送附加元数据、触发 run priority、filter scope；它们是一次事件消息属性，不是 workflow 配置。EventClient/Option 是 v0 bridge alias|运行输入 / 适配器桥接|`sdks/go/events.go:4-33`|

## 4. Concurrency 完整结构及枚举

`Concurrency` 同时用于 workflow 和 task；数组**声明顺序有意义**。`Expression string` 为必填语义但 SDK 未检查空；`MaxRuns *int32` 若省略，v1 默认 1。Tenant scoped 时 name 在 wire 注释中为必需，SDK 不预验证；同一共享策略链顺序不一致会被注册端拒绝。

|字段/枚举|wire 与默认|wego 键|分类|证据|
|---|---|---|---|---|
|`Expression string`|`expression`|`*.concurrency[].key_expr`|公共语义|`pkg/client/types/file.go:23-39`; `api-contracts/v1/workflows.proto:155-169`|
|`MaxRuns *int32`|`max_runs`；显式 <=0 SDK panic；缺省=1|`*.concurrency[].max_runs`|公共语义|`sdks/go/workflow.go:346-353`; `api-contracts/v1/workflows.proto:163-165`|
|`LimitStrategy *WorkflowConcurrencyLimitStrategy`|`limit_strategy`；缺省 `CANCEL_IN_PROGRESS`|`*.concurrency[].on_limit`|公共语义|`sdks/go/internal/task/task.go:208-213`; `api-contracts/v1/workflows.proto:145-165`|
|`CANCEL_IN_PROGRESS`,`CANCEL_NEWEST`,`GROUP_ROUND_ROBIN`,`CANCEL_QUEUED_EXCEPT_NEWEST`,`CANCEL_QUEUED_EXCEPT_OLDEST`|当前 v1 枚举，未标 deprecated|相应中性策略枚举（引擎无等价则拒绝）|公共语义|`pkg/client/types/file.go:63-73`; `api-contracts/v1/workflows.proto:145-152`|
|`DROP_NEWEST`,`QUEUE_NEWEST`|仍可序列化，但 v1 proto 标 `deprecated`|`engines.hatchet.*.concurrency.on_limit` 或兼容层 deprecated|适配器兼容值|`pkg/client/types/file.go:69-70`; `api-contracts/v1/workflows.proto:147-148`|
|`Name string`|非空才发送指针；tenant scoped 时必需，作为跨 workflow 共享策略名|`engines.hatchet.*.concurrency.shared.name`|适配器专属|`pkg/client/types/file.go:23-38`; `sdks/go/internal/task/task.go:194-206`|
|`IsTenantScoped bool`|true 才发送；false 省略；true 表示同租户跨 workflow 共用策略|`engines.hatchet.*.concurrency.shared.tenant_scoped`|适配器专属|`pkg/client/types/file.go:23-38`; `api-contracts/v1/workflows.proto:158-168`|
|`MaxRunsExpression *string`|发 CEL；覆盖 `MaxRuns`。非整数/负数失败，0 保持组直到新任务提高|`*.concurrency[].max_runs_expr`|公共语义（动态限制）|`pkg/client/types/file.go:35-38`; `api-contracts/v1/workflows.proto:168`|
|workflow-level 运行路径|单任务且无 on-failure 时，server 将 workflow concurrency 依序移到唯一 DEFAULT task；多 task 且 DAG operator 已启用时，tenant-scoped 条目附到 durable orchestrator，其他条目保留 workflow 层；旧 DAG 路径拒绝 workflow-level tenant-scoped 和 `MaxRunsExpression`|`engines.hatchet.workflow.concurrency.execution_model`；公共配置需声明支持矩阵|适配器专属执行限制|`pkg/repository/workflow.go:823-905`|

## 5. Rate limit、cron/scheduled trigger 与 feature 管理面

|类型 / 字段|发送、默认、约束|wego 键|分类|证据|
|---|---|---|---|---|
|`RateLimit.Key string`|直接发 required wire `key`；SDK 不验证空|`task.rate_limits[].key`|公共语义|`pkg/client/types/file.go:165-172`; `sdks/go/internal/task/task.go:149-155`; `api-contracts/v1/workflows.proto:200-206`|
|`RateLimit.KeyExpr *string`|非 nil 透传 optional `key_expr`|`task.rate_limits[].key_expr`|公共语义|`pkg/client/types/file.go:165-172`; `sdks/go/internal/task/task.go:150-155`|
|`RateLimit.Units *int`|非 nil 转 int32 发 `units`；没有溢出/正数检查|`task.rate_limits[].units`|公共语义|`pkg/client/types/file.go:165-172`; `sdks/go/internal/task/task.go:157-160`|
|`RateLimit.UnitsExpr *string`|非 nil 透传 `units_expr`|`task.rate_limits[].units_expr`|公共语义|`pkg/client/types/file.go:165-172`; `sdks/go/internal/task/task.go:150-155`|
|`RateLimit.LimitValueExpr *string`|非 nil 透传 `limit_values_expr`|`task.rate_limits[].limit_expr`|公共语义|`pkg/client/types/file.go:165-172`; `sdks/go/internal/task/task.go:150-155`|
|`RateLimit.Duration *RateLimitDuration`|非 nil 转换为 `duration`；未知 Go enum 强制 Minute|`task.rate_limits[].window`|公共语义|`pkg/client/types/file.go:165-172`; `sdks/go/internal/task/task.go:162-182`|
|Duration 枚举 `Second,Minute,Hour,Day,Week,Month,Year`|wire 0..6，未给则 server 语义；未知 Go enum 被 SDK 映射 Minute|上述 `window`|公共语义|`api-contracts/v1/workflows.proto:76-84`; `sdks/go/internal/task/task.go:162-182`|
|`RateLimits().Upsert(CreateRatelimitOpts{Key,Limit,Duration})`|租户级 rate-limit 资源写入，走 v0 `PutRateLimit`；不是任务声明|`engines.hatchet.admin.rate_limit`|适配器专属管理 API|`sdks/go/features/ratelimits.go:15-56`|
|`Crons().Create`：`Name`|REST `cronName`，无空名本地校验|`engines.hatchet.admin.cron_trigger.name`|适配器专属管理 API|`sdks/go/features/crons.go:25-41,70-94`; `pkg/client/rest/gen.go:574-581`|
|`Crons().Create`：`Expression`|REST `cronExpression`；唯一 SDK 验证是 robfig cron（可选秒 + 分时日月周）|`engines.hatchet.admin.cron_trigger.expression`|适配器专属管理 API|`sdks/go/features/crons.go:62-75,88-94`|
|`Crons().Create`：`Input`,`AdditionalMetadata`,`Priority`|分别发 REST `input`,`additionalMetadata`,`priority`；前两 nil 被改为空 map，priority nil 省略|`engines.hatchet.admin.cron_trigger.{input,metadata,priority}`|适配器专属管理 API|`sdks/go/features/crons.go:77-101`; `pkg/client/rest/gen.go:574-581`|
|`Schedules().Create`：`TriggerAt`|REST 必填 `triggerAt`，无“必须未来”的 SDK 校验|`workflow.scheduled_run.at`（公共命令，非静态配置）|不作声明配置|`sdks/go/features/schedules.go:19-31,56-88`; `pkg/client/rest/gen.go:982-988`|
|`Schedules().Create`：`Input`,`AdditionalMetadata`,`Priority`|REST `input`,`additionalMetadata`,`priority`；此 SDK 直接传入，nil map 是否被 server 接受未在本层判定|`workflow.scheduled_run.{input,metadata,priority}`|运行输入|`sdks/go/features/schedules.go:70-88`; `pkg/client/rest/gen.go:982-988`|
|`Filters().Create`：`Expression`,`Payload`,`Scope`,`WorkflowId`|生成 REST request 的四字段直接传入；是已部署 workflow 的 filter 资源，不是 WorkflowOption|`engines.hatchet.admin.filter.{expression,payload,scope,workflow_id}`|适配器专属管理 API|`sdks/go/features/filters.go:66-78`; `pkg/client/rest/gen.go:1592-1605`|
|`Filters().Update`：`Expression *`,`Payload *`,`Scope *`|仅以上三字段可更新，全部 optional，直接传 REST|`engines.hatchet.admin.filter.{expression,payload,scope}`|适配器专属管理 API|`sdks/go/features/filters.go:84-96`; `pkg/client/rest/gen.go:2165-2175`|
|`Webhooks().Create(CreateWebhookOpts)`|字段 `Name,SourceName,EventKeyExpression,ScopeExpression,StaticPayload,ReturnEventAsResponsePayload,Auth`；Auth 必填。Basic(username,password)、APIKey(header,key)、HMAC(secret,header,algorithm,encoding)、Svix(secret，固定 header/SHA256/BASE64) 都转换为 Hatchet REST union|`engines.hatchet.admin.webhook`（含 secret，不进入 workflow config）|适配器专属管理 API|`sdks/go/features/webhooks.go:12-124,188-213`|
|`Webhooks().Update(UpdateWebhookOpts)`|仅四个可选字段：event key expr、scope expr、static payload、返回 payload 开关；认证不可在此结构更新|同上|适配器专属管理 API|`sdks/go/features/webhooks.go:126-131,215-236`|
|`Runs().Cancel/Replay`：`ExternalIds *[]UUID`,`Filter *V1TaskFilter`|两种控制命令共享选择器；filter 字段是 `AdditionalMetadata *[]string`,`Since time.Time`,`Statuses *[]V1TaskStatus`,`Until *time.Time`,`WorkflowIds *[]UUID`。不是部署配置|控制命令参数，不作配置|不作配置|`sdks/go/features/runs.go:106-152`; `pkg/client/rest/gen.go:1579-1584,1909-1914,1966-1973`|
|List 查询参数（filter / webhook / rate-limit / cron / schedule）|分别是：Filter `offset,limit,workflowIds,scopes`；Webhook `offset,limit,sourceNames,webhookNames`；RateLimit `offset,limit,search,orderByField,orderByDirection`；Cron `offset,limit,workflowId,workflowName,cronName,additionalMetadata,orderByField,orderByDirection`；Schedule `offset,limit,orderByField,orderByDirection,workflowId,parentWorkflowRunId,parentStepRunId,additionalMetadata,statuses`|查询参数，不作配置|不作配置|`pkg/client/rest/gen.go:2845-2858,2959-2972,3145-3161,3205-3230,3304-3332`|
|Runs/Workflows/Workers/Metrics/Logs/Tenant/CEL feature clients|Get/List/Status/Cancel/Replay/Restore/Branch/Subscribe 等是查询、控制命令或调试输入；`rest.*ListParams` 均为查询参数，不是声明配置|不作配置（独立观测/运维 API）|不作配置|`sdks/go/features/runs.go:42-201`; `sdks/go/features/workflows.go:40-101`; `sdks/go/features/workers.go:32-101`; `sdks/go/features/metrics.go:35-76`; `sdks/go/features/logs.go:25`; `sdks/go/features/cel.go:33`; `sdks/go/features/tenant.go:32`|

### Webhook 资源逐字段补表

下列字段属于管理接口。`CreateWebhookOpts` 除 `Auth == nil` 会在 SDK 立即报错外，其余字段主要直接交给 REST；表中“无 SDK 默认”不代表服务端不验证。敏感值由凭据引用解析，不能写入脱敏后的 EffectiveConfig。证据：`sdks/go/features/webhooks.go:12-131,188-236`，枚举：`pkg/client/rest/gen.go:325-347`。

|字段|类型 / 可选性|映射与约束|
|---|---|---|
|Create.`Name`|string|Webhook 名，SDK 无默认。|
|Create.`SourceName`|V1WebhookSourceName|GENERIC/GITHUB/LINEAR/SLACK/STRIPE/SVIX；SvixAuth 强制 SVIX。|
|Create.`EventKeyExpression`|string|生成事件键的表达式；SDK 无默认。|
|Create.`ScopeExpression`|*string|可选 scope 表达式；nil 省略。|
|Create.`StaticPayload`|*map[string]interface{}|可选静态载荷；nil 省略。|
|Create.`ReturnEventAsResponsePayload`|*bool|可选返回事件载荷开关；nil 与显式 false 保持区别。|
|Create.`Auth`|WebhookAuth|必填；通过 BasicAuth/APIKeyAuth/HMACAuth/SvixAuth 转换认证 union。|
|BasicAuth.`Username`|string|发送 `auth.username`；SDK 无默认。|
|BasicAuth.`Password`|string|发送 `auth.password`；凭据。|
|APIKeyAuth.`HeaderName`|string|发送 `auth.headerName`；SDK 无默认。|
|APIKeyAuth.`APIKey`|string|发送 `auth.apiKey`；凭据。|
|HMACAuth.`SigningSecret`|string|发送 `auth.signingSecret`；签名凭据。|
|HMACAuth.`SignatureHeaderName`|string|发送 `auth.signatureHeaderName`；SDK 无默认。|
|HMACAuth.`Algorithm`|V1WebhookHMACAlgorithm|原生枚举 MD5/SHA1/SHA256/SHA512；如实记录原生值，不作为 wego 推荐默认。|
|HMACAuth.`Encoding`|V1WebhookHMACEncoding|BASE64/BASE64URL/HEX；SDK 无默认。|
|SvixAuth.`SigningSecret`|string|唯一可设字段；固定 header=svix-signature、algorithm=SHA256、encoding=BASE64。|
|Update.`EventKeyExpression`|*string|可选，仅发送此字段的更新值。|
|Update.`ScopeExpression`|*string|可选；SDK 原样透传，不在本层定义清空规则。|
|Update.`StaticPayload`|*map[string]interface{}|可选；SDK 原样透传。|
|Update.`ReturnEventAsResponsePayload`|*bool|可选，可显式 false。|

Update 的目标 `webhookName` 是方法参数；更新结构不含名称、来源或认证字段。认证方式自身由类型选择，不提供任意 `auth_type` 字符串覆盖。

## 6. Task 条件（配置值）与 v1 wire 边界

|构件|字段/效果|wego 键与分类|证据|
|---|---|---|---|
|`SleepCondition(duration)`|发 `SleepMatchCondition.SleepFor`，毫秒字符串；SDK 不校验正值|`task.wait_for.sleep` / `skip_if.sleep`，公共语义|`sdks/go/hatchet.go:127-130`; `pkg/worker/condition/condition.go:52-84`|
|`UserEventCondition(eventKey,expression,opts...)`|eventKey、CEL expression 发 user event 条件；`WithEventScope(scope)`填 scope；`WithConsiderEventsSince(time)`填 lookback timestamp，注释要求同时 scope，但 SDK **不执行该要求**|`task.wait_for.event.{key,filter,scope,lookback_since}`，公共语义|`sdks/go/hatchet.go:132-147`; `pkg/worker/condition/condition.go:86-150`|
|`ParentCondition(task,expression)`|关联 parent readable id、CEL expression|`task.wait_for.parent_condition`，公共语义|`sdks/go/hatchet.go:149-152`; `pkg/worker/condition/condition.go:152-188`|
|`AndCondition(...)`,`OrCondition(...)`|默认条件交集；Or 给所有子条件同一随机 UUID `or_group_id`；这是 Hatchet wire 表达细节|公共布尔组合；UUID 不暴露|`sdks/go/hatchet.go:154-162`; `pkg/worker/condition/condition.go:190-233`|
|`CancelIf`|internal TaskShared/serializer 支持 Action_CANCEL，但新公开 `TaskOption` 没有 `WithCancelIf`，故不能由本 SDK 新表面配置|不暴露（除非 wego 明确另建能力）|`sdks/go/internal/task/task.go:337-349`; `sdks/go/workflow.go:498-550`|

## 7. 不应误纳入新 wego 中性配置的遗留/占位面

1. `pkg/client/types.Workflow`、`WorkflowTriggers`、`WorkflowJob`、YAML parse/emit 都明确 **Deprecated v0**；其 `WorkflowStep.With` 又明确“无效果”。不能从这些结构补全新 SDK 配置清单（`pkg/client/types/file.go:41-61,85-157,174-203`）。
2. v1 `CreateWorkflowVersionRequest.concurrency` 字段自身标 deprecated；新 SDK 使用 `concurrency_arr`（`api-contracts/v1/workflows.proto:105-112`; `sdks/go/internal/declaration.go:763-790`）。
3. `CreateTaskOpts.worker_labels` 与 internal `CancelIf` 虽有 wire/内部字段，但 `NewTask` 的公开 options 没有设置入口，不能误报为当前 Go SDK 可配置（`api-contracts/v1/workflows.proto:188-197`; `sdks/go/workflow.go:498-550`）。
4. `Batch` 在构造器注释中标 **Preview/Beta**；`DROP_NEWEST`、`QUEUE_NEWEST` 在 v1 enum 标 deprecated（`sdks/go/workflow.go:685-702`; `api-contracts/v1/workflows.proto:145-152`）。本附录的任务声明入口未发现 cloud-only 标记；Cloud Compute 单列在客户端附录，不能据此推断全部功能的云端可用性。

## 对 wego 配置层的直接约束

* 公共层要区分**部署声明**（workflow/task）、**单次运行选项**（input、metadata、priority 等）和**管理资源/查询参数**；把三者压成一份 Config 会使 Temporal 替换面泄漏。
* 例如 `WithRetries(0)` 在 `sdks/go/internal/declaration.go:282-303` 被转换为 nil，仍会被非零的 Workflow TaskDefaults.Retries 填充；wego 的显式 `retry.max_attempts=1` 不能直接使用这一组合下发，适配器应先展开继承后的每任务配置，消除原生 workflow 默认的干扰。
* 默认必须显式建模“未设置”：Hatchet 多处以零值=省略（TaskDefaults、timeout、false broadcast），而 wire/default 有独立语义；尤其并发默认 1、策略默认 CANCEL_IN_PROGRESS、任务 retries 默认 0。
* 公共验证不应复制 Hatchet 的缺口：wego 应拒绝空表达式、非法 rate-limit、无 scope 的 event lookback、nil concurrency/rate-limit 项；同时把 Hatchet 的 `int32` 截断/溢出边界封进 adapter。
* `WithDescription` 不应为兼容表面承诺“会下发”；若保留只能定义成 SDK 本地/文档元数据。Durable eviction、sticky、slot cost、tenant-scoped concurrency、worker labels 应放 adapter 扩展。
