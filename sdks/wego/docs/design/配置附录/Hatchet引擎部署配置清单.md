# Hatchet 引擎部署配置清单

> 源码基线：`src/hatchet` commit `315d43a72fd771b049b304b865a81dbab98c466c`（v0.106.11-4）。覆盖服务端部署、引擎/API、数据库、队列、可观测性与静态文件托管；未读取真实环境文件或密钥。

## 加载、默认与覆盖规则

- `ConfigLoader` 读取 `server.yaml` 与 `database.yaml`；`LoadConfigFromViper` 注册 BindEnv、合并 YAML、填充 struct tag 默认值、最后 Unmarshal（`pkg/config/loader/loader.go:61-83`；`pkg/config/loader/loaderutils/viper.go:10-32`）。本表“默认”只写该 struct tag；无 tag 时写“未设置/需 loader 或调用方决定”，不把 Go 零值伪称默认。
- Viper 1.21 的读取顺序是 override、已变更 flag、环境变量、配置文件；同一 key 多次 BindEnv 时将变量依传入顺序追加并取第一个已设置者。因此 `SERVER_TASKQUEUE_*`（legacy）优先于后绑定的 `SERVER_MSGQUEUE_*`，两者均未设才读 YAML（`viper.go:1113-1127, 1149-1249`）。
- `DATABASE_URL` 不是 YAML 字段。引擎 loader 优先读取它，未设置才由 `database.yaml` 的连接字段组装；迁移库 `RunMigrations` 则先取嵌入调用方 `WithDatabaseURL`，再取 `DATABASE_URL`，两者皆空返回 MissingEnvError，不组装字段（`pkg/config/loader/loader.go:151-164`；`cmd/hatchet-migrate/migrate/run.go:104-112`）。
- “来源对象”仅用于区分同名 Go 配置树，**不是 YAML 根节点**：`server.*` 的 YAML 路径来自 `server.yaml`，`database.*` 来自 `database.yaml`；表中“实际 YAML 路径”已经剥除前缀。直接 env/flag 没有 YAML 路径。
- `pkg/v1/config.go` 是已弃用 legacy v0 workflow system 的 Go 客户端配置；Token、TenantId、HostPort、TLS 等保留在业务 SDK adapter，不是部署公共 Options。以下部署项也不应进入 wego 业务 SDK。

## 逐字段索引

| 来源对象 | 实际 YAML 路径 | 类型 | 声明默认 / 显式后备 | 环境变量 / flag | 中文含义 | 归属 | 源码 |
|---|---|---|---|---|---|---|---|
| `server.auth.restrictedEmailDomains` | `auth.restrictedEmailDomains` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_AUTH_RESTRICTED_EMAIL_DOMAINS` | 允许登录的邮箱域限制 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:475` |
| `server.auth.basicAuthEnabled` | `auth.basicAuthEnabled` | `bool` | true | `SERVER_AUTH_BASIC_AUTH_ENABLED` | 是否启用邮箱密码登录 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:479` |
| `server.auth.setEmailVerified` | `auth.setEmailVerified` | `bool` | false | `SERVER_AUTH_SET_EMAIL_VERIFIED` | 是否自动验证邮箱 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:482` |
| `server.auth.cookie.name` | `auth.cookie.name` | `string` | hatchet | `SERVER_AUTH_COOKIE_NAME` | Cookie 名称 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:545` |
| `server.auth.cookie.domain` | `auth.cookie.domain` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_AUTH_COOKIE_DOMAIN` | Cookie 域 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:546` |
| `server.auth.cookie.secrets` | `auth.cookie.secrets` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_AUTH_COOKIE_SECRETS` | Cookie 密钥串 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:547` |
| `server.auth.cookie.insecure` | `auth.cookie.insecure` | `bool` | false | `SERVER_AUTH_COOKIE_INSECURE` | 是否使用不安全连接 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:548` |
| `server.auth.google.enabled` | `auth.google.enabled` | `bool` | false | `SERVER_AUTH_GOOGLE_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:529` |
| `server.auth.google.clientID` | `auth.google.clientID` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_AUTH_GOOGLE_CLIENT_ID` | OAuth/集成客户端 ID | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:531` |
| `server.auth.google.clientSecret` | `auth.google.clientSecret` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_AUTH_GOOGLE_CLIENT_SECRET` | OAuth/集成客户端密钥 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:532` |
| `server.auth.google.scopes` | `auth.google.scopes` | `[]string` | ["openid", "profile", "email"] | `SERVER_AUTH_GOOGLE_SCOPES` | OAuth/集成授权 scope 列表 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:533` |
| `server.auth.github.enabled` | `auth.github.enabled` | `bool` | false | `SERVER_AUTH_GITHUB_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:537` |
| `server.auth.github.clientID` | `auth.github.clientID` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_AUTH_GITHUB_CLIENT_ID` | OAuth/集成客户端 ID | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:539` |
| `server.auth.github.clientSecret` | `auth.github.clientSecret` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_AUTH_GITHUB_CLIENT_SECRET` | OAuth/集成客户端密钥 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:540` |
| `server.auth.github.scopes` | `auth.github.scopes` | `[]string` | ["read:user", "user:email"] | `SERVER_AUTH_GITHUB_SCOPES` | OAuth/集成授权 scope 列表 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:541` |
| `server.auth.controlPlaneExchangeToken.jwtPublicKeyset` | `auth.controlPlaneExchangeToken.jwtPublicKeyset` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_AUTH_CONTROL_PLANE_EXCHANGE_TOKEN_JWT_PUBLIC_KEYSET` | 控制面交换 token 的 JWT 公钥集 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:503` |
| `server.auth.controlPlaneExchangeToken.jwtPublicKeysetFile` | `auth.controlPlaneExchangeToken.jwtPublicKeysetFile` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_AUTH_CONTROL_PLANE_EXCHANGE_TOKEN_JWT_PUBLIC_KEYSET_FILE` | 控制面交换 token JWT 公钥集文件 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:504` |
| `server.auth.controlPlaneExchangeToken.issuer` | `auth.controlPlaneExchangeToken.issuer` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_AUTH_CONTROL_PLANE_EXCHANGE_TOKEN_ISSUER` | 交换 token 预期签发者 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:507` |
| `server.auth.controlPlaneExchangeToken.audience` | `auth.controlPlaneExchangeToken.audience` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_AUTH_CONTROL_PLANE_EXCHANGE_TOKEN_AUDIENCE` | 交换 token 预期受众 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:510` |
| `server.auth.controlPlaneExchangeToken.enabled` | `auth.controlPlaneExchangeToken.enabled` | `bool` | false | `SERVER_AUTH_CONTROL_PLANE_EXCHANGE_TOKEN_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:513` |
| `server.alerting.sentry.enabled` | `alerting.sentry.enabled` | `bool` | 未设置/需 loader 或调用方决定 | `SERVER_ALERTING_SENTRY_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:393` |
| `server.alerting.sentry.dsn` | `alerting.sentry.dsn` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ALERTING_SENTRY_DSN` | Sentry DSN | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:396` |
| `server.alerting.sentry.environment` | `alerting.sentry.environment` | `string` | development | `SERVER_ALERTING_SENTRY_ENVIRONMENT` | 告警/追踪运行环境名 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:399` |
| `server.alerting.sentry.sampleRate` | `alerting.sentry.sampleRate` | `float64` | 1.0 | `SERVER_ALERTING_SENTRY_SAMPLE_RATE` | 告警事件采样比例 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:402` |
| `server.analytics.posthog.enabled` | `analytics.posthog.enabled` | `bool` | 未设置/需 loader 或调用方决定 | `SERVER_ANALYTICS_POSTHOG_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:414` |
| `server.analytics.posthog.apiKey` | `analytics.posthog.apiKey` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ANALYTICS_POSTHOG_API_KEY` | PostHog API Key | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:417` |
| `server.analytics.posthog.endpoint` | `analytics.posthog.endpoint` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ANALYTICS_POSTHOG_ENDPOINT` | 外部服务端点 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:420` |
| `server.analytics.posthog.feApiKey` | `analytics.posthog.feApiKey` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ANALYTICS_POSTHOG_FE_API_KEY` | 前端 PostHog API Key | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:423` |
| `server.analytics.posthog.feApiHost` | `analytics.posthog.feApiHost` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ANALYTICS_POSTHOG_FE_API_HOST` | 前端 PostHog API Host | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:426` |
| `server.analytics.aggregateEnabled` | `analytics.aggregateEnabled` | `bool` | false | `SERVER_ANALYTICS_AGGREGATE_ENABLED` | 是否聚合分析事件 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:407` |
| `server.analytics.aggregateFlushInterval` | `analytics.aggregateFlushInterval` | `string` | 60m | `SERVER_ANALYTICS_AGGREGATE_FLUSH_INTERVAL` | 分析聚合刷写间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:408` |
| `server.analytics.aggregateMaxKeys` | `analytics.aggregateMaxKeys` | `int` | 500 | `SERVER_ANALYTICS_AGGREGATE_MAX_KEYS` | 分析聚合键数量上限 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:409` |
| `server.pylon.enabled` | `pylon.enabled` | `bool` | 未设置/需 loader 或调用方决定 | `SERVER_PYLON_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:719` |
| `server.pylon.appID` | `pylon.appID` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_PYLON_APP_ID` | Pylon 应用 ID | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:720` |
| `server.pylon.secret` | `pylon.secret` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_PYLON_SECRET` | Pylon 密钥 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:721` |
| `server.encryption.masterKeyset` | `encryption.masterKeyset` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ENCRYPTION_MASTER_KEYSET` | 主加密密钥集内容 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:433` |
| `server.encryption.masterKeysetFile` | `encryption.masterKeysetFile` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ENCRYPTION_MASTER_KEYSET_FILE` | 主加密密钥集文件 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:436` |
| `server.encryption.jwt.publicJWTKeyset` | `encryption.jwt.publicJWTKeyset` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ENCRYPTION_JWT_PUBLIC_KEYSET` | JWT 公钥集内容 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:447` |
| `server.encryption.jwt.publicJWTKeysetFile` | `encryption.jwt.publicJWTKeysetFile` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ENCRYPTION_JWT_PUBLIC_KEYSET_FILE` | JWT 公钥集文件 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:450` |
| `server.encryption.jwt.privateJWTKeyset` | `encryption.jwt.privateJWTKeyset` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ENCRYPTION_JWT_PRIVATE_KEYSET` | JWT 私钥集内容 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:454` |
| `server.encryption.jwt.privateJWTKeysetFile` | `encryption.jwt.privateJWTKeysetFile` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ENCRYPTION_JWT_PRIVATE_KEYSET_FILE` | JWT 私钥集文件 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:457` |
| `server.encryption.cloudKms.enabled` | `encryption.cloudKms.enabled` | `bool` | false | `SERVER_ENCRYPTION_CLOUDKMS_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:462` |
| `server.encryption.cloudKms.keyURI` | `encryption.cloudKms.keyURI` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ENCRYPTION_CLOUDKMS_KEY_URI` | 云 KMS 密钥 URI | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:466` |
| `server.encryption.cloudKms.credentialsJSON` | `encryption.cloudKms.credentialsJSON` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ENCRYPTION_CLOUDKMS_CREDENTIALS_JSON` | 云 KMS 服务账号 JSON | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:469` |
| `server.runtime.port` | `runtime.port` | `int` | 8080 | `SERVER_PORT` | 监听端口 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:172` |
| `server.runtime.url` | `runtime.url` | `string` | http://localhost:8080 | `SERVER_URL` | 服务 URL | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:175` |
| `server.runtime.frontendUrl` | `runtime.frontendUrl` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_FRONTEND_URL` | 前端 URL | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:179` |
| `server.runtime.healthcheck` | `runtime.healthcheck` | `bool` | true | `SERVER_HEALTHCHECK` | 是否启用健康检查端点 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:182` |
| `server.runtime.healthcheckPort` | `runtime.healthcheckPort` | `int` | 8733 | `SERVER_HEALTHCHECK_PORT` | 健康检查监听端口 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:185` |
| `server.runtime.grpcPort` | `runtime.grpcPort` | `int` | 7070 | `SERVER_GRPC_PORT` | gRPC 监听端口 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:188` |
| `server.runtime.grpcBindAddress` | `runtime.grpcBindAddress` | `string` | 127.0.0.1 | `SERVER_GRPC_BIND_ADDRESS` | gRPC 绑定地址 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:191` |
| `server.runtime.grpcBroadcastAddress` | `runtime.grpcBroadcastAddress` | `string` | 127.0.0.1:7070 | `SERVER_GRPC_BROADCAST_ADDRESS` | gRPC 对外广播地址 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:194` |
| `server.runtime.grpcInsecure` | `runtime.grpcInsecure` | `bool` | false | `SERVER_GRPC_INSECURE` | gRPC 是否禁用 TLS | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:197` |
| `server.runtime.grpcMaxMsgSize` | `runtime.grpcMaxMsgSize` | `int` | 4194304 | `SERVER_GRPC_MAX_MSG_SIZE` | gRPC 最大接收消息字节数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:200` |
| `server.runtime.grpcWorkerMaxLockAcquisitionTime` | `runtime.grpcWorkerMaxLockAcquisitionTime` | `time.Duration` | 250ms | `SERVER_GRPC_WORKER_MAX_LOCK_ACQUISITION_TIME` | gRPC 向 worker 获取锁的最长等待时间 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:204` |
| `server.runtime.grpcStaticStreamWindowSize` | `runtime.grpcStaticStreamWindowSize` | `int32` | 10485760 | `SERVER_GRPC_STATIC_STREAM_WINDOW_SIZE` | gRPC 静态流窗口字节数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:208` |
| `server.runtime.grpcRateLimit` | `runtime.grpcRateLimit` | `float64` | 1000 | `SERVER_GRPC_RATE_LIMIT` | 每 API token/每引擎的 gRPC 每秒限流值 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:211` |
| `server.runtime.grpcShutdownTimeout` | `runtime.grpcShutdownTimeout` | `time.Duration` | 10s | `SERVER_GRPC_SHUTDOWN_TIMEOUT` | gRPC 优雅停机排空超时 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:214` |
| `server.runtime.enforceLimits` | `runtime.enforceLimits` | `bool` | false | `SERVER_ENFORCE_LIMITS` | 是否执行租户限额 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:217` |
| `server.runtime.limits.defaultTenantRetentionPeriod` | `runtime.limits.defaultTenantRetentionPeriod` | `string` | 720h | `SERVER_LIMITS_DEFAULT_TENANT_RETENTION_PERIOD` | 默认租户数据保留期 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:6` |
| `server.runtime.limits.corePartitionRetention` | `runtime.limits.corePartitionRetention` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_LIMITS_CORE_PARTITION_RETENTION` | 核心表分区保留期覆盖值 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:7` |
| `server.runtime.limits.olapPartitionRetention` | `runtime.limits.olapPartitionRetention` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_LIMITS_OLAP_PARTITION_RETENTION` | OLAP 分区保留期覆盖值 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:8` |
| `server.runtime.limits.defaultTaskRunLimit` | `runtime.limits.defaultTaskRunLimit` | `int32` | 2000 | `SERVER_LIMITS_DEFAULT_TASK_RUN_LIMIT` | 默认任务运行数量上限 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:10` |
| `server.runtime.limits.defaultTaskRunAlarmLimit` | `runtime.limits.defaultTaskRunAlarmLimit` | `int32` | 1600 | `SERVER_LIMITS_DEFAULT_TASK_RUN_ALARM_LIMIT` | 默认任务运行告警阈值 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:11` |
| `server.runtime.limits.defaultTaskRunWindow` | `runtime.limits.defaultTaskRunWindow` | `time.Duration` | 24h | `SERVER_LIMITS_DEFAULT_TASK_RUN_WINDOW` | 默认任务运行限额窗口 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:12` |
| `server.runtime.limits.defaultWorkerLimit` | `runtime.limits.defaultWorkerLimit` | `int32` | 3 | `SERVER_LIMITS_DEFAULT_WORKER_LIMIT` | 默认 worker 数量上限 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:14` |
| `server.runtime.limits.defaultWorkerAlarmLimit` | `runtime.limits.defaultWorkerAlarmLimit` | `int32` | 2 | `SERVER_LIMITS_DEFAULT_WORKER_ALARM_LIMIT` | 默认 worker 数量告警阈值 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:15` |
| `server.runtime.limits.defaultWorkerSlotLimit` | `runtime.limits.defaultWorkerSlotLimit` | `int32` | 2000 | `SERVER_LIMITS_DEFAULT_WORKER_SLOT_LIMIT` | 默认 worker 槽位上限 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:17` |
| `server.runtime.limits.defaultWorkerSlotAlarmLimit` | `runtime.limits.defaultWorkerSlotAlarmLimit` | `int32` | 1600 | `SERVER_LIMITS_DEFAULT_WORKER_SLOT_ALARM_LIMIT` | 默认 worker 槽位告警阈值 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:18` |
| `server.runtime.limits.defaultEventLimit` | `runtime.limits.defaultEventLimit` | `int32` | 1000 | `SERVER_LIMITS_DEFAULT_EVENT_LIMIT` | 默认事件数量上限 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:20` |
| `server.runtime.limits.defaultEventAlarmLimit` | `runtime.limits.defaultEventAlarmLimit` | `int32` | 800 | `SERVER_LIMITS_DEFAULT_EVENT_ALARM_LIMIT` | 默认事件数量告警阈值 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:21` |
| `server.runtime.limits.defaultEventWindow` | `runtime.limits.defaultEventWindow` | `time.Duration` | 24h | `SERVER_LIMITS_DEFAULT_EVENT_WINDOW` | 默认事件限额窗口 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:22` |
| `server.runtime.limits.defaultIncomingWebhookLimit` | `runtime.limits.defaultIncomingWebhookLimit` | `int32` | 5 | `SERVER_LIMITS_DEFAULT_INCOMING_WEBHOOK_LIMIT` | 默认入站 webhook 数量上限 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:24` |
| `server.runtime.limits.defaultIncomingWebhookAlarmLimit` | `runtime.limits.defaultIncomingWebhookAlarmLimit` | `int32` | 4 | `无显式绑定` | 默认入站 webhook 告警阈值 | 部署层：Hatchet Engine/API | `pkg/config/limits/limits.go:25` |
| `server.runtime.singleQueueLimit` | `runtime.singleQueueLimit` | `int` | 100 | `SERVER_SINGLE_QUEUE_LIMIT` | 单次队列读取数量上限 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:223` |
| `server.runtime.optimisticSchedulingEnabled` | `runtime.optimisticSchedulingEnabled` | `bool` | true | `SERVER_OPTIMISTIC_SCHEDULING_ENABLED` | 是否启用乐观调度 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:226` |
| `server.runtime.optimisticSchedulingSlots` | `runtime.optimisticSchedulingSlots` | `int` | 5 | `SERVER_OPTIMISTIC_SCHEDULING_SLOTS` | 乐观调度槽位数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:229` |
| `server.runtime.grpcTriggerWritesEnabled` | `runtime.grpcTriggerWritesEnabled` | `bool` | true | `SERVER_GRPC_TRIGGER_WRITES_ENABLED` | 是否允许 gRPC 直接写入触发请求 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:232` |
| `server.runtime.grpcTriggerWriteSlots` | `runtime.grpcTriggerWriteSlots` | `int` | 5 | `SERVER_GRPC_TRIGGER_WRITE_SLOTS` | gRPC 直接写槽位数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:235` |
| `server.runtime.allowSignup` | `runtime.allowSignup` | `bool` | true | `SERVER_ALLOW_SIGNUP` | 是否允许注册 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:238` |
| `server.runtime.allowInvites` | `runtime.allowInvites` | `bool` | true | `SERVER_ALLOW_INVITES` | 是否允许创建邀请 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:241` |
| `server.runtime.maxPendingInvites` | `runtime.maxPendingInvites` | `int` | 100 | `SERVER_MAX_PENDING_INVITES` | 单邀请人待处理邀请上限 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:244` |
| `server.runtime.allowCreateTenant` | `runtime.allowCreateTenant` | `bool` | true | `SERVER_ALLOW_CREATE_TENANT` | 是否允许创建租户 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:247` |
| `server.runtime.allowChangePassword` | `runtime.allowChangePassword` | `bool` | true | `SERVER_ALLOW_CHANGE_PASSWORD` | 是否允许修改密码 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:250` |
| `server.runtime.embedded` | `runtime.embedded` | `bool` | false | `无显式绑定` | 是否为嵌入式运行 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:253` |
| `server.runtime.apiRateLimit` | `runtime.apiRateLimit` | `int` | 10 | `SERVER_API_RATE_LIMIT` | 按客户端 IP 的 API 每秒限流值 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:256` |
| `server.runtime.apiRateLimitWindow` | `runtime.apiRateLimitWindow` | `time.Duration` | 300s | `SERVER_API_RATE_LIMIT_WINDOW` | API 限流窗口 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:257` |
| `server.runtime.apiTrustedProxies` | `runtime.apiTrustedProxies` | `[]string` | 未设置/需 loader 或调用方决定 | `SERVER_API_TRUSTED_PROXIES` | 可信转发代理 CIDR 列表 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:260` |
| `server.runtime.apiTrustPrivateProxies` | `runtime.apiTrustPrivateProxies` | `bool` | true | `SERVER_API_TRUST_PRIVATE_PROXIES` | 是否信任来自私网/回环代理的转发头 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:263` |
| `server.runtime.webhookRateLimit` | `runtime.webhookRateLimit` | `float64` | 50 | `SERVER_INCOMING_WEBHOOK_RATE_LIMIT` | 每 webhook 每秒限流值 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:266` |
| `server.runtime.webhookRateLimitBurst` | `runtime.webhookRateLimitBurst` | `int` | 100 | `SERVER_INCOMING_WEBHOOK_RATE_LIMIT_BURST` | webhook 限流突发容量 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:269` |
| `server.runtime.disableTenantPubs` | `runtime.disableTenantPubs` | `bool` | 未设置/需 loader 或调用方决定 | `SERVER_DISABLE_TENANT_PUBS` | 是否禁用租户发布订阅 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:272` |
| `server.runtime.maxInternalRetryCount` | `runtime.maxInternalRetryCount` | `int32` | 10 | `SERVER_MAX_INTERNAL_RETRY_COUNT` | 步骤内部最大重试次数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:275` |
| `server.runtime.monitoring.enabled` | `runtime.monitoring.enabled` | `bool` | true | `SERVER_MONITORING_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:643` |
| `server.runtime.monitoring.permittedTenants` | `runtime.monitoring.permittedTenants` | `[]string` | 未设置/需 loader 或调用方决定 | `SERVER_MONITORING_PERMITTED_TENANTS` | 可使用监控的租户 ID 列表 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:646` |
| `server.runtime.monitoring.probeTimeout` | `runtime.monitoring.probeTimeout` | `time.Duration` | 30s | `SERVER_MONITORING_PROBE_TIMEOUT` | 监控探针超时 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:649` |
| `server.runtime.monitoring.tlsRootCAFile` | `runtime.monitoring.tlsRootCAFile` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_MONITORING_TLS_ROOT_CA_FILE` | TLS 根 CA 文件路径 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:652` |
| `server.runtime.preventTenantVersionUpgrade` | `runtime.preventTenantVersionUpgrade` | `bool` | false | `SERVER_PREVENT_TENANT_VERSION_UPGRADE` | 是否阻止租户版本升级 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:280` |
| `server.runtime.replayEnabled` | `runtime.replayEnabled` | `bool` | true | `SERVER_REPLAY_ENABLED` | 是否启用任务回放 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:283` |
| `server.runtime.allowedOrigins` | `runtime.allowedOrigins` | `[]string` | 未设置/需 loader 或调用方决定 | `无显式绑定` | 允许 CORS 的来源模式列表 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:289` |
| `server.runtime.allowedOriginsString` | `runtime.allowedOriginsString` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_ALLOWED_ORIGINS` | 用于生成允许来源列表的空格分隔原始值 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:294` |
| `server.runtime.operatorInfraBlockedCIDRs` | `runtime.operatorInfraBlockedCIDRs` | `[]string` | 未设置/需 loader 或调用方决定 | `无显式绑定` | operator 出站访问额外禁止 CIDR 列表 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:302` |
| `server.runtime.operatorInfraBlockedCIDRsString` | `runtime.operatorInfraBlockedCIDRsString` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_OPERATOR_INFRA_BLOCKED_CIDRS` | 用于生成禁止 CIDR 列表的空格分隔原始值 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:307` |
| `server.runtime.dagOperatorDefaultSlots` | `runtime.dagOperatorDefaultSlots` | `int` | 10000 | `SERVER_DAG_OPERATOR_DEFAULT_SLOTS` | DAG operator 并发槽位数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:311` |
| `server.runtime.schedulerConcurrencyRateLimit` | `runtime.schedulerConcurrencyRateLimit` | `int` | 20 | `SCHEDULER_CONCURRENCY_RATE_LIMIT` | 调度并发策略每秒执行上限 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:314` |
| `server.runtime.schedulerConcurrencyPollingMinInterval` | `runtime.schedulerConcurrencyPollingMinInterval` | `time.Duration` | 500ms | `SCHEDULER_CONCURRENCY_POLLING_MIN_INTERVAL` | 调度并发轮询最小间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:317` |
| `server.runtime.schedulerConcurrencyPollingMaxInterval` | `runtime.schedulerConcurrencyPollingMaxInterval` | `time.Duration` | 5s | `SCHEDULER_CONCURRENCY_POLLING_MAX_INTERVAL` | 调度并发轮询最大间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:320` |
| `server.runtime.schedulerCheckActiveMinInterval` | `runtime.schedulerCheckActiveMinInterval` | `time.Duration` | 30s | `SCHEDULER_CHECK_ACTIVE_MIN_INTERVAL` | 调度活动检查最小间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:323` |
| `server.runtime.schedulerCheckActiveMaxInterval` | `runtime.schedulerCheckActiveMaxInterval` | `time.Duration` | 60s | `SCHEDULER_CHECK_ACTIVE_MAX_INTERVAL` | 调度活动检查最大间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:326` |
| `server.runtime.schedulerAdvisoryLockTimeout` | `runtime.schedulerAdvisoryLockTimeout` | `time.Duration` | 5s | `SCHEDULER_ADVISORY_LOCK_TIMEOUT` | 调度 advisory lock 超时 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:328` |
| `server.runtime.concurrencyInMemoryIndexEnabled` | `runtime.concurrencyInMemoryIndexEnabled` | `bool` | true | `SERVER_CONCURRENCY_IN_MEMORY_INDEX_ENABLED` | 是否启用并发策略内存索引和 outbox | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:331` |
| `server.runtime.logIngestionEnabled` | `runtime.logIngestionEnabled` | `bool` | true | `SERVER_LOG_INGESTION_ENABLED` | 是否接收任务日志 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:334` |
| `server.runtime.o11yUsageFlushInterval` | `runtime.o11yUsageFlushInterval` | `time.Duration` | 30s | `SERVER_O11Y_USAGE_FLUSH_INTERVAL` | 观测用量聚合刷写间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:338` |
| `server.runtime.taskOperationLimits.timeoutLimit` | `runtime.taskOperationLimits.timeoutLimit` | `int` | 1000 | `无显式绑定` | 单次超时处理任务数上限 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:124` |
| `server.runtime.taskOperationLimits.reassignLimit` | `runtime.taskOperationLimits.reassignLimit` | `int` | 1000 | `无显式绑定` | 单次重分配任务数上限 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:127` |
| `server.runtime.taskOperationLimits.retryQueueLimit` | `runtime.taskOperationLimits.retryQueueLimit` | `int` | 1000 | `无显式绑定` | 单次重试队列处理数上限 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:130` |
| `server.runtime.taskOperationLimits.durableSleepLimit` | `runtime.taskOperationLimits.durableSleepLimit` | `int` | 1000 | `无显式绑定` | 单次 durable sleep 处理数上限 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:133` |
| `server.runtime.workflowRunBufferSize` | `runtime.workflowRunBufferSize` | `int` | 1000 | `SERVER_WORKFLOW_RUN_BUFFER_SIZE` | dispatcher 中 workflow run 事件批处理缓冲容量 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:344` |
| `server.runtime.durableEventBufferFlushInterval` | `runtime.durableEventBufferFlushInterval` | `time.Duration` | 5ms | `SERVER_DURABLE_EVENT_BUFFER_FLUSH_INTERVAL` | durable 任务事件写库缓冲的刷写间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:348` |
| `server.runtime.durableEventBufferMaxSize` | `runtime.durableEventBufferMaxSize` | `int` | 100 | `SERVER_DURABLE_EVENT_BUFFER_MAX_SIZE` | 单次 durable 事件刷写的最大批量 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:352` |
| `server.runtime.durableEventBufferMaxConcurrentFlushes` | `runtime.durableEventBufferMaxConcurrentFlushes` | `int` | 16 | `SERVER_DURABLE_EVENT_BUFFER_MAX_CONCURRENT_FLUSHES` | durable 事件并发刷写事务数上限 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:356` |
| `server.runtime.streamEventBufferTimeout` | `runtime.streamEventBufferTimeout` | `time.Duration` | 5s | `SERVER_STREAM_EVENT_BUFFER_TIMEOUT` | dispatcher 等待乱序流事件的缓冲超时 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:360` |
| `server.msgQueue.enabled` | `msgQueue.enabled` | `bool` | true | `无显式绑定` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:552` |
| `server.msgQueue.kind` | `msgQueue.kind` | `string` | rabbitmq | `SERVER_TASKQUEUE_KIND<br>SERVER_MSGQUEUE_KIND` | 实现类型 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:554` |
| `server.msgQueue.postgres.qos` | `msgQueue.postgres.qos` | `int` | 100 | `无显式绑定` | 消费者预取数量 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:619` |
| `server.msgQueue.rabbitmq.url` | `msgQueue.rabbitmq.url` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_TASKQUEUE_RABBITMQ_URL<br>SERVER_MSGQUEUE_RABBITMQ_URL` | 服务 URL | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:623` |
| `server.msgQueue.rabbitmq.qos` | `msgQueue.rabbitmq.qos` | `int` | 100 | `SERVER_MSGQUEUE_RABBITMQ_QOS` | 消费者预取数量 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:624` |
| `server.msgQueue.rabbitmq.maxPubChans` | `msgQueue.rabbitmq.maxPubChans` | `int32` | 20 | `SERVER_MSGQUEUE_RABBITMQ_MAX_PUB_CHANS` | 发布通道最大数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:625` |
| `server.msgQueue.rabbitmq.maxSubChans` | `msgQueue.rabbitmq.maxSubChans` | `int32` | 100 | `SERVER_MSGQUEUE_RABBITMQ_MAX_SUB_CHANS` | 订阅通道最大数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:626` |
| `server.msgQueue.rabbitmq.compressionEnabled` | `msgQueue.rabbitmq.compressionEnabled` | `bool` | false | `SERVER_MSGQUEUE_RABBITMQ_COMPRESSION_ENABLED` | 是否启用消息压缩 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:627` |
| `server.msgQueue.rabbitmq.compressionThreshold` | `msgQueue.rabbitmq.compressionThreshold` | `int` | 5120 | `SERVER_MSGQUEUE_RABBITMQ_COMPRESSION_THRESHOLD` | 启用压缩的消息字节阈值 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:628` |
| `server.msgQueue.rabbitmq.enableMessageRejection` | `msgQueue.rabbitmq.enableMessageRejection` | `bool` | false | `SERVER_MSGQUEUE_RABBITMQ_ENABLE_MESSAGE_REJECTION` | 是否启用消息拒绝处理 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:629` |
| `server.msgQueue.rabbitmq.maxDeathCount` | `msgQueue.rabbitmq.maxDeathCount` | `int` | 1000 | `SERVER_MSGQUEUE_RABBITMQ_MAX_DEATH_COUNT` | 消息最大死亡计数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:630` |
| `server.msgQueue.pubSub.kind` | `msgQueue.pubSub.kind` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_MSGQUEUE_PUBSUB_KIND` | 实现类型 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:568` |
| `server.msgQueue.pubSub.rabbitmq.url` | `msgQueue.pubSub.rabbitmq.url` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_MSGQUEUE_PUBSUB_RABBITMQ_URL` | 服务 URL | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:581` |
| `server.msgQueue.pubSub.rabbitmq.maxPubChans` | `msgQueue.pubSub.rabbitmq.maxPubChans` | `int32` | 10 | `SERVER_MSGQUEUE_PUBSUB_RABBITMQ_MAX_PUB_CHANS` | 发布通道最大数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:583` |
| `server.msgQueue.pubSub.rabbitmq.maxSubChans` | `msgQueue.pubSub.rabbitmq.maxSubChans` | `int32` | 20 | `SERVER_MSGQUEUE_PUBSUB_RABBITMQ_MAX_SUB_CHANS` | 订阅通道最大数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:584` |
| `server.msgQueue.pubSub.postgres.maxConns` | `msgQueue.pubSub.postgres.maxConns` | `int32` | 5 | `SERVER_MSGQUEUE_PUBSUB_POSTGRES_MAX_CONNS` | 最大连接数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:590` |
| `server.msgQueue.pubSub.postgres.minConns` | `msgQueue.pubSub.postgres.minConns` | `int32` | 1 | `SERVER_MSGQUEUE_PUBSUB_POSTGRES_MIN_CONNS` | 最小连接数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:591` |
| `server.msgQueue.pubSub.nats.url` | `msgQueue.pubSub.nats.url` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_MSGQUEUE_PUBSUB_NATS_URL` | 服务 URL | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:599` |
| `server.msgQueue.pubSub.nats.username` | `msgQueue.pubSub.nats.username` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_MSGQUEUE_PUBSUB_NATS_USERNAME` | 用户名 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:601` |
| `server.msgQueue.pubSub.nats.password` | `msgQueue.pubSub.nats.password` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_MSGQUEUE_PUBSUB_NATS_PASSWORD` | 密码 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:602` |
| `server.msgQueue.pubSub.nats.tlsEnabled` | `msgQueue.pubSub.nats.tlsEnabled` | `bool` | 未设置/需 loader 或调用方决定 | `SERVER_MSGQUEUE_PUBSUB_NATS_TLS_ENABLED` | 是否启用 TLS | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:607` |
| `server.msgQueue.pubSub.nats.tlsRootCAFile` | `msgQueue.pubSub.nats.tlsRootCAFile` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_MSGQUEUE_PUBSUB_NATS_TLS_ROOT_CA_FILE` | TLS 根 CA 文件路径 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:611` |
| `server.msgQueue.pubSub.nats.subjectPrefix` | `msgQueue.pubSub.nats.subjectPrefix` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_MSGQUEUE_PUBSUB_NATS_SUBJECT_PREFIX` | 发布订阅主题前缀 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:615` |
| `server.services` | `services` | `[]string` | ["all"] | `无显式绑定` | 启用的服务列表 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:49` |
| `server.servicesString` | `servicesString` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_SERVICES` | 由环境变量输入的服务列表原始值 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:52` |
| `server.pausedControllers` | `pausedControllers` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_PAUSED_CONTROLLERS` | 暂停的 controller 列表原始值 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:54` |
| `server.enableDataRetention` | `enableDataRetention` | `bool` | true | `SERVER_ENABLE_DATA_RETENTION` | 是否执行数据保留清理 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:56` |
| `server.enableWorkerRetention` | `enableWorkerRetention` | `bool` | false | `SERVER_ENABLE_WORKER_RETENTION` | 是否执行 worker 保留清理 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:58` |
| `server.tls.tlsStrategy` | `tls.tlsStrategy` | `string` | tls | `SERVER_TLS_STRATEGY` | TLS 策略（tls、mtls 或 none） | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:7` |
| `server.tls.tlsCert` | `tls.tlsCert` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_TLS_CERT` | TLS 证书内容 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:9` |
| `server.tls.tlsCertFile` | `tls.tlsCertFile` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_TLS_CERT_FILE` | TLS 证书文件路径 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:10` |
| `server.tls.tlsKey` | `tls.tlsKey` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_TLS_KEY` | TLS 私钥内容 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:11` |
| `server.tls.tlsKeyFile` | `tls.tlsKeyFile` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_TLS_KEY_FILE` | TLS 私钥文件路径 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:12` |
| `server.tls.tlsRootCA` | `tls.tlsRootCA` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_TLS_ROOT_CA` | TLS 根 CA 内容 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:13` |
| `server.tls.tlsRootCAFile` | `tls.tlsRootCAFile` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_TLS_ROOT_CA_FILE` | TLS 根 CA 文件路径 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:14` |
| `server.tls.tlsMinVersion` | `tls.tlsMinVersion` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_TLS_MIN_VERSION` | 允许的最低 TLS 版本 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:17` |
| `server.internalClient.inheritBase` | `internalClient.inheritBase` | `bool` | true | `SERVER_INTERNAL_CLIENT_BASE_INHERIT_BASE` | 内部客户端是否继承服务端 TLS 设置 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:368` |
| `server.internalClient.internalGRPCBroadcastAddress` | `internalClient.internalGRPCBroadcastAddress` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_INTERNAL_CLIENT_INTERNAL_GRPC_BROADCAST_ADDRESS` | 内部 API 连接 gRPC 的广播地址 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:372` |
| `server.internalClient.tlsServerName` | `internalClient.tlsServerName` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_INTERNAL_CLIENT_TLS_SERVER_NAME` | 内部 TLS 校验的服务名 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:376` |
| `server.internalClient.base.tlsStrategy` | `internalClient.base.tlsStrategy` | `string` | tls | `SERVER_INTERNAL_CLIENT_BASE_STRATEGY` | TLS 策略（tls、mtls 或 none） | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:7` |
| `server.internalClient.base.tlsCert` | `internalClient.base.tlsCert` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_INTERNAL_CLIENT_TLS_BASE_CERT` | TLS 证书内容 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:9` |
| `server.internalClient.base.tlsCertFile` | `internalClient.base.tlsCertFile` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_INTERNAL_CLIENT_TLS_BASE_CERT_FILE` | TLS 证书文件路径 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:10` |
| `server.internalClient.base.tlsKey` | `internalClient.base.tlsKey` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_INTERNAL_CLIENT_TLS_BASE_KEY` | TLS 私钥内容 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:11` |
| `server.internalClient.base.tlsKeyFile` | `internalClient.base.tlsKeyFile` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_INTERNAL_CLIENT_TLS_BASE_KEY_FILE` | TLS 私钥文件路径 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:12` |
| `server.internalClient.base.tlsRootCA` | `internalClient.base.tlsRootCA` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_INTERNAL_CLIENT_TLS_BASE_ROOT_CA` | TLS 根 CA 内容 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:13` |
| `server.internalClient.base.tlsRootCAFile` | `internalClient.base.tlsRootCAFile` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_INTERNAL_CLIENT_TLS_BASE_ROOT_CA_FILE` | TLS 根 CA 文件路径 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:14` |
| `server.internalClient.base.tlsMinVersion` | `internalClient.base.tlsMinVersion` | `string` | 未设置/需 loader 或调用方决定 | `无显式绑定` | 允许的最低 TLS 版本 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:17` |
| `server.logger.level` | `logger.level` | `string` | warn | `SERVER_LOGGER_LEVEL` | 日志最低级别 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:21` |
| `server.logger.format` | `logger.format` | `string` | console | `SERVER_LOGGER_FORMAT` | 日志输出格式 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:24` |
| `server.additionalLoggers.queue.level` | `additionalLoggers.queue.level` | `string` | warn | `SERVER_ADDITIONAL_LOGGERS_QUEUE_LEVEL` | 日志最低级别 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:21` |
| `server.additionalLoggers.queue.format` | `additionalLoggers.queue.format` | `string` | console | `SERVER_ADDITIONAL_LOGGERS_QUEUE_FORMAT` | 日志输出格式 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:24` |
| `server.additionalLoggers.pgxStats.level` | `additionalLoggers.pgxStats.level` | `string` | warn | `SERVER_ADDITIONAL_LOGGERS_PGXSTATS_LEVEL` | 日志最低级别 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:21` |
| `server.additionalLoggers.pgxStats.format` | `additionalLoggers.pgxStats.format` | `string` | console | `SERVER_ADDITIONAL_LOGGERS_PGXSTATS_FORMAT` | 日志输出格式 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:24` |
| `server.otel.collectorURL` | `otel.collectorURL` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_OTEL_COLLECTOR_URL` | OpenTelemetry Collector URL | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:40` |
| `server.otel.serviceName` | `otel.serviceName` | `string` | server | `SERVER_OTEL_SERVICE_NAME` | OpenTelemetry 服务名 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:41` |
| `server.otel.traceIdRatio` | `otel.traceIdRatio` | `string` | 1 | `SERVER_OTEL_TRACE_ID_RATIO` | 追踪采样比例 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:42` |
| `server.otel.insecure` | `otel.insecure` | `bool` | false | `SERVER_OTEL_INSECURE` | 是否使用不安全连接 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:43` |
| `server.otel.collectorAuth` | `otel.collectorAuth` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_OTEL_COLLECTOR_AUTH` | Collector 认证信息 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:44` |
| `server.otel.metricsEnabled` | `otel.metricsEnabled` | `bool` | false | `SERVER_OTEL_METRICS_ENABLED` | 是否导出 OpenTelemetry 指标 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:45` |
| `server.prometheus.prometheusServerURL` | `prometheus.prometheusServerURL` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_PROMETHEUS_SERVER_URL` | Prometheus 服务 URL | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:50` |
| `server.prometheus.prometheusServerUsername` | `prometheus.prometheusServerUsername` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_PROMETHEUS_SERVER_USERNAME` | Prometheus Basic Auth 用户名 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:53` |
| `server.prometheus.prometheusServerPassword` | `prometheus.prometheusServerPassword` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_PROMETHEUS_SERVER_PASSWORD` | Prometheus Basic Auth 密码 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:56` |
| `server.prometheus.address` | `prometheus.address` | `string` | :9090 | `SERVER_PROMETHEUS_ADDRESS` | 指标端点监听地址 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:59` |
| `server.prometheus.enabled` | `prometheus.enabled` | `bool` | false | `SERVER_PROMETHEUS_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:62` |
| `server.prometheus.path` | `prometheus.path` | `string` | /metrics | `SERVER_PROMETHEUS_PATH` | 指标端点路径 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:65` |
| `server.prometheus.tenantScoped` | `prometheus.tenantScoped` | `bool` | false | `SERVER_PROMETHEUS_SERVER_TENANT_SCOPED` | 是否按租户 entitlement 限制 Prometheus 指标 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:70` |
| `server.observability.enabled` | `observability.enabled` | `bool` | false | `SERVER_OBSERVABILITY_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:78` |
| `server.observability.maxBatchSize` | `observability.maxBatchSize` | `int` | 1000 | `SERVER_OBSERVABILITY_MAX_BATCH_SIZE` | 单次观测 span 批量上限 | 部署层：Hatchet Engine/API | `pkg/config/shared/shared.go:82` |
| `server.securityCheck.enabled` | `securityCheck.enabled` | `bool` | true | `SERVER_SECURITY_CHECK_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:382` |
| `server.securityCheck.endpoint` | `securityCheck.endpoint` | `string` | https://security.hatchet.run | `SERVER_SECURITY_CHECK_ENDPOINT` | 外部服务端点 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:383` |
| `server.tenantAlerting.slack.enabled` | `tenantAlerting.slack.enabled` | `bool` | 未设置/需 loader 或调用方决定 | `SERVER_TENANT_ALERTING_SLACK_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:521` |
| `server.tenantAlerting.slack.clientID` | `tenantAlerting.slack.clientID` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_TENANT_ALERTING_SLACK_CLIENT_ID` | OAuth/集成客户端 ID | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:523` |
| `server.tenantAlerting.slack.clientSecret` | `tenantAlerting.slack.clientSecret` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_TENANT_ALERTING_SLACK_CLIENT_SECRET` | OAuth/集成客户端密钥 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:524` |
| `server.tenantAlerting.slack.scopes` | `tenantAlerting.slack.scopes` | `[]string` | ["incoming-webhook"] | `SERVER_TENANT_ALERTING_SLACK_SCOPES` | OAuth/集成授权 scope 列表 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:525` |
| `server.email.kind` | `email.kind` | `string` | postmark | `SERVER_EMAIL_KIND` | 实现类型 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:634` |
| `server.email.postmark.enabled` | `email.postmark.enabled` | `bool` | 未设置/需 loader 或调用方决定 | `SERVER_EMAIL_POSTMARK_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:656` |
| `server.email.postmark.serverKey` | `email.postmark.serverKey` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_EMAIL_POSTMARK_SERVER_KEY` | 邮件服务密钥 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:658` |
| `server.email.postmark.fromEmail` | `email.postmark.fromEmail` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_EMAIL_POSTMARK_FROM_EMAIL` | 发件邮箱 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:659` |
| `server.email.postmark.fromName` | `email.postmark.fromName` | `string` | Hatchet Support | `SERVER_EMAIL_POSTMARK_FROM_NAME` | 发件人显示名 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:660` |
| `server.email.postmark.supportEmail` | `email.postmark.supportEmail` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_EMAIL_POSTMARK_SUPPORT_EMAIL` | 支持邮箱 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:661` |
| `server.email.smtp.basicAuth.username` | `email.smtp.basicAuth.username` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_EMAIL_SMTP_AUTH_USERNAME` | 用户名 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:675` |
| `server.email.smtp.basicAuth.password` | `email.smtp.basicAuth.password` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_EMAIL_SMTP_AUTH_PASSWORD` | 密码 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:676` |
| `server.email.smtp.serverKey` | `email.smtp.serverKey` | `string` | 未设置/需 loader 或调用方决定 | `无显式绑定` | 邮件服务密钥 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:666` |
| `server.email.smtp.serverAddr` | `email.smtp.serverAddr` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_EMAIL_SMTP_SERVER_ADDR` | SMTP 服务地址 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:667` |
| `server.email.smtp.fromEmail` | `email.smtp.fromEmail` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_EMAIL_SMTP_FROM_EMAIL` | 发件邮箱 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:668` |
| `server.email.smtp.fromName` | `email.smtp.fromName` | `string` | Hatchet Support | `SERVER_EMAIL_SMTP_FROM_NAME` | 发件人显示名 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:669` |
| `server.email.smtp.supportEmail` | `email.smtp.supportEmail` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_EMAIL_SMTP_SUPPORT_EMAIL` | 支持邮箱 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:670` |
| `server.email.smtp.enabled` | `email.smtp.enabled` | `bool` | 未设置/需 loader 或调用方决定 | `SERVER_EMAIL_SMTP_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:671` |
| `server.monitoring.enabled` | `monitoring.enabled` | `bool` | true | `无显式绑定` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:643` |
| `server.monitoring.permittedTenants` | `monitoring.permittedTenants` | `[]string` | 未设置/需 loader 或调用方决定 | `无显式绑定` | 可使用监控的租户 ID 列表 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:646` |
| `server.monitoring.probeTimeout` | `monitoring.probeTimeout` | `time.Duration` | 30s | `无显式绑定` | 监控探针超时 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:649` |
| `server.monitoring.tlsRootCAFile` | `monitoring.tlsRootCAFile` | `string` | 未设置/需 loader 或调用方决定 | `无显式绑定` | TLS 根 CA 文件路径 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:652` |
| `server.sampling.enabled` | `sampling.enabled` | `bool` | false | `SERVER_SAMPLING_ENABLED` | 是否启用 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:105` |
| `server.sampling.samplingRate` | `sampling.samplingRate` | `float64` | 1.0 | `SERVER_SAMPLING_RATE` | 采样比例 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:108` |
| `server.olap.jitter` | `olap.jitter` | `int` | 0 | `SERVER_OPERATIONS_JITTER` | 操作轮询抖动 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:113` |
| `server.olap.pollInterval` | `olap.pollInterval` | `int` | 2 | `SERVER_OPERATIONS_POLL_INTERVAL` | 操作轮询间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:116` |
| `server.olap.olapMqQos` | `olap.olapMqQos` | `int` | 100 | `SERVER_OLAP_MQ_QOS` | OLAP 消费者预取数量 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:119` |
| `server.payloadStore.externalCutoverProcessInterval` | `payloadStore.externalCutoverProcessInterval` | `time.Duration` | 15s | `SERVER_PAYLOAD_STORE_EXTERNAL_CUTOVER_PROCESS_INTERVAL` | 外部 payload 迁移处理间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:810` |
| `server.payloadStore.externalCutoverBatchSize` | `payloadStore.externalCutoverBatchSize` | `int32` | 1000 | `SERVER_PAYLOAD_STORE_EXTERNAL_CUTOVER_BATCH_SIZE` | 外部 payload 迁移批大小 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:811` |
| `server.payloadStore.externalCutoverNumConcurrentOffloads` | `payloadStore.externalCutoverNumConcurrentOffloads` | `int32` | 10 | `SERVER_PAYLOAD_STORE_EXTERNAL_CUTOVER_NUM_CONCURRENT_OFFLOADS` | 外部 payload 并发迁出数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:812` |
| `server.payloadStore.inlineStoreTTLDays` | `payloadStore.inlineStoreTTLDays` | `int32` | 2 | `SERVER_PAYLOAD_STORE_INLINE_STORE_TTL_DAYS` | 内联 payload 存储保留天数 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:813` |
| `server.payloadStore.enableWindowSizeOptimization` | `payloadStore.enableWindowSizeOptimization` | `bool` | true | `SERVER_PAYLOAD_STORE_ENABLE_WINDOW_SIZE_OPTIMIZATION` | 是否启用 payload 窗口优化 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:814` |
| `server.cronOperations.taskAnalyzeCronInterval` | `cronOperations.taskAnalyzeCronInterval` | `time.Duration` | 3h | `SERVER_CRON_OPERATIONS_TASK_ANALYZE_CRON_INTERVAL` | 任务表分析间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:139` |
| `server.cronOperations.olapAnalyzeCronInterval` | `cronOperations.olapAnalyzeCronInterval` | `time.Duration` | 3h | `SERVER_CRON_OPERATIONS_OLAP_ANALYZE_CRON_INTERVAL` | OLAP 表分析间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:142` |
| `server.cronOperations.dbHealthMetricsInterval` | `cronOperations.dbHealthMetricsInterval` | `time.Duration` | 60s | `无显式绑定` | 数据库健康指标采集间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:145` |
| `server.cronOperations.olapMetricsInterval` | `cronOperations.olapMetricsInterval` | `time.Duration` | 5m | `无显式绑定` | OLAP 指标采集间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:148` |
| `server.cronOperations.workerMetricsInterval` | `cronOperations.workerMetricsInterval` | `time.Duration` | 60s | `无显式绑定` | worker 指标采集间隔 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:151` |
| `server.cronOperations.yesterdayRunCountHour` | `cronOperations.yesterdayRunCountHour` | `uint` | 0 | `无显式绑定` | 昨日运行数采集小时 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:154` |
| `server.cronOperations.yesterdayRunCountMinute` | `cronOperations.yesterdayRunCountMinute` | `uint` | 5 | `无显式绑定` | 昨日运行数采集分钟 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:157` |
| `server.statusUpdates.dagBatchSizeLimit` | `statusUpdates.dagBatchSizeLimit` | `int` | 1000 | `SERVER_OLAP_STATUS_UPDATE_DAG_BATCH_SIZE_LIMIT` | DAG 状态批更新大小 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:163` |
| `server.statusUpdates.taskBatchSizeLimit` | `statusUpdates.taskBatchSizeLimit` | `int` | 1000 | `SERVER_OLAP_STATUS_UPDATE_TASK_BATCH_SIZE_LIMIT` | 任务状态批更新大小 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:166` |
| `server.versionOverride` | `versionOverride` | `string` | 未设置/需 loader 或调用方决定 | `SERVER_VERSION_OVERRIDE` | 服务端版本覆盖值 | 部署层：Hatchet Engine/API | `pkg/config/server/server.go:92` |
| `database.host` | `host` | `string` | 127.0.0.1 | `DATABASE_POSTGRES_HOST` | PostgreSQL 主机地址 | 部署层：数据库 adapter | `pkg/config/database/config.go:15` |
| `database.port` | `port` | `int` | 5431 | `DATABASE_POSTGRES_PORT` | 监听端口 | 部署层：数据库 adapter | `pkg/config/database/config.go:16` |
| `database.username` | `username` | `string` | hatchet | `DATABASE_POSTGRES_USERNAME` | 用户名 | 部署层：数据库 adapter | `pkg/config/database/config.go:17` |
| `database.password` | `password` | `string` | hatchet | `DATABASE_POSTGRES_PASSWORD` | 密码 | 部署层：数据库 adapter | `pkg/config/database/config.go:18` |
| `database.dbName` | `dbName` | `string` | hatchet | `DATABASE_POSTGRES_DB_NAME` | 数据库名称 | 部署层：数据库 adapter | `pkg/config/database/config.go:19` |
| `database.sslMode` | `sslMode` | `string` | disable | `DATABASE_POSTGRES_SSL_MODE` | PostgreSQL SSL 模式 | 部署层：数据库 adapter | `pkg/config/database/config.go:20` |
| `database.readReplicaEnabled` | `readReplicaEnabled` | `bool` | false | `READ_REPLICA_ENABLED` | 是否启用只读副本 | 部署层：数据库 adapter | `pkg/config/database/config.go:22` |
| `database.readReplicaDatabaseUrl` | `readReplicaDatabaseUrl` | `string` | 未设置/需 loader 或调用方决定 | `READ_REPLICA_DATABASE_URL` | 只读副本连接 URL | 部署层：数据库 adapter | `pkg/config/database/config.go:23` |
| `database.readReplicaMaxConns` | `readReplicaMaxConns` | `int` | 50 | `READ_REPLICA_MAX_CONNS` | 只读副本最大连接数 | 部署层：数据库 adapter | `pkg/config/database/config.go:24` |
| `database.readReplicaMinConns` | `readReplicaMinConns` | `int` | 10 | `READ_REPLICA_MIN_CONNS` | 只读副本最小连接数 | 部署层：数据库 adapter | `pkg/config/database/config.go:25` |
| `database.maxConns` | `maxConns` | `int` | 50 | `DATABASE_MAX_CONNS` | 最大连接数 | 部署层：数据库 adapter | `pkg/config/database/config.go:27` |
| `database.minConns` | `minConns` | `int` | 1 | `DATABASE_MIN_CONNS` | 最小连接数 | 部署层：数据库 adapter | `pkg/config/database/config.go:28` |
| `database.pgbouncerUrl` | `pgbouncerUrl` | `string` | 未设置/需 loader 或调用方决定 | `DATABASE_PGBOUNCER_URL` | PgBouncer 连接 URL | 部署层：数据库 adapter | `pkg/config/database/config.go:32` |
| `database.ddlPoolMaxConns` | `ddlPoolMaxConns` | `int` | 5 | `DATABASE_DDL_POOL_MAX_CONNS` | DDL 直连池最大连接数 | 部署层：数据库 adapter | `pkg/config/database/config.go:34` |
| `database.ddlPoolMinConns` | `ddlPoolMinConns` | `int` | 1 | `DATABASE_DDL_POOL_MIN_CONNS` | DDL 直连池最小连接数 | 部署层：数据库 adapter | `pkg/config/database/config.go:35` |
| `database.maxConnLifetime` | `maxConnLifetime` | `time.Duration` | 15m | `DATABASE_MAX_CONN_LIFETIME` | 连接最大存活时间 | 部署层：数据库 adapter | `pkg/config/database/config.go:37` |
| `database.maxConnIdleTime` | `maxConnIdleTime` | `time.Duration` | 1m | `DATABASE_MAX_CONN_IDLE_TIME` | 连接最大空闲时间 | 部署层：数据库 adapter | `pkg/config/database/config.go:38` |
| `database.applicationNamePrefix` | `applicationNamePrefix` | `string` | 未设置/需 loader 或调用方决定 | `K8S_POD_NAMESPACE` | PostgreSQL application_name 前缀 | 部署层：数据库 adapter | `pkg/config/database/config.go:43` |
| `database.seed.adminEmail` | `seed.adminEmail` | `string` | admin@example.com | `ADMIN_EMAIL` | 初始化管理员邮箱 | 部署层：数据库 adapter | `pkg/config/database/config.go:60` |
| `database.seed.adminPassword` | `seed.adminPassword` | `string` | Admin123!! | `ADMIN_PASSWORD` | 初始化管理员密码 | 部署层：数据库 adapter | `pkg/config/database/config.go:61` |
| `database.seed.adminName` | `seed.adminName` | `string` | Admin | `ADMIN_NAME` | 初始化管理员显示名 | 部署层：数据库 adapter | `pkg/config/database/config.go:62` |
| `database.seed.defaultTenantName` | `seed.defaultTenantName` | `string` | Default | `DEFAULT_TENANT_NAME` | 初始化默认租户名称 | 部署层：数据库 adapter | `pkg/config/database/config.go:64` |
| `database.seed.defaultTenantSlug` | `seed.defaultTenantSlug` | `string` | default | `DEFAULT_TENANT_SLUG` | 初始化默认租户 slug | 部署层：数据库 adapter | `pkg/config/database/config.go:65` |
| `database.seed.defaultTenantId` | `seed.defaultTenantId` | `string` | 707d0855-80ab-4e1f-a156-f1c4546cbf52 | `DEFAULT_TENANT_ID` | 初始化默认租户 ID | 部署层：数据库 adapter | `pkg/config/database/config.go:66` |
| `database.logger.level` | `logger.level` | `string` | warn | `DATABASE_LOGGER_LEVEL` | 日志最低级别 | 部署层：数据库 adapter | `pkg/config/shared/shared.go:21` |
| `database.logger.format` | `logger.format` | `string` | console | `DATABASE_LOGGER_FORMAT` | 日志输出格式 | 部署层：数据库 adapter | `pkg/config/shared/shared.go:24` |
| `database.logQueries` | `logQueries` | `bool` | false | `DATABASE_LOG_QUERIES` | 是否记录 SQL 查询日志 | 部署层：数据库 adapter | `pkg/config/database/config.go:49` |
| `database.cacheDuration` | `cacheDuration` | `time.Duration` | 5s | `CACHE_DURATION` | 仓库缓存时长 | 部署层：数据库 adapter | `pkg/config/database/config.go:51` |
| `database.enforceUtcTimezone` | `enforceUtcTimezone` | `bool` | true | `DATABASE_ENFORCE_UTC_TIMEZONE` | 启动时是否强制数据库时区为 UTC | 部署层：数据库 adapter | `pkg/config/database/config.go:56` |
| `directEnv.SERVER_DEFAULT_BUFFER_FLUSH_INTERVAL` | `—` | `time.Duration` | 100ms | `SERVER_DEFAULT_BUFFER_FLUSH_INTERVAL` | 消息队列核心缓冲批量刷写间隔 | 部署层：消息队列 adapter | `internal/msgqueue/mq_buffer_core.go:20` |
| `directEnv.SERVER_DEFAULT_BUFFER_IDLE_TIMEOUT` | `—` | `time.Duration` | 1s | `SERVER_DEFAULT_BUFFER_IDLE_TIMEOUT` | 消息队列核心缓冲空闲超时 | 部署层：消息队列 adapter | `internal/msgqueue/mq_buffer_core.go:26` |
| `directEnv.SERVER_DEFAULT_BUFFER_SIZE` | `—` | `int` | 1000 | `SERVER_DEFAULT_BUFFER_SIZE` | 消息队列核心缓冲容量 | 部署层：消息队列 adapter | `internal/msgqueue/mq_buffer_core.go:31` |
| `directEnv.SERVER_DEFAULT_BUFFER_CONCURRENCY` | `—` | `int` | 1 | `SERVER_DEFAULT_BUFFER_CONCURRENCY` | 消息队列核心缓冲并发消费者数 | 部署层：消息队列 adapter | `internal/msgqueue/mq_buffer_core.go:37` |
| `directEnv.K8S_POD_NAME` | `—` | `string` | 未设置时不附加 | `K8S_POD_NAME` | 遥测资源属性 pod.name | 部署层：Kubernetes adapter | `pkg/telemetry/telemetry.go:270` |
| `directEnv.K8S_POD_NAMESPACE` | `—` | `string` | 未设置时不附加；亦绑定 database.applicationNamePrefix | `K8S_POD_NAMESPACE` | 遥测 pod.namespace / PG application_name 前缀 | 部署层：Kubernetes adapter | `pkg/telemetry/telemetry.go:274` |
| `directEnv.K8S_CLOUD_REGION` | `—` | `string` | 未设置时尝试 AWS_REGION/AWS_DEFAULT_REGION | `K8S_CLOUD_REGION` | 遥测 cloud.region | 部署层：Kubernetes adapter | `pkg/telemetry/telemetry.go:278` |
| `directEnv.AWS_REGION` | `—` | `string` | 同上 | `AWS_REGION` | 遥测 cloud.region 后备值 | 部署层：云 adapter | `pkg/telemetry/telemetry.go:278` |
| `directEnv.AWS_DEFAULT_REGION` | `—` | `string` | 同上 | `AWS_DEFAULT_REGION` | 遥测 cloud.region 最后后备值 | 部署层：云 adapter | `pkg/telemetry/telemetry.go:278` |
| `directEnv.HOSTNAME` | `—` | `string` | 未设置时 hostname 字段为空 | `HOSTNAME` | 租户行为日志的宿主机名 | 部署层：运行环境 | `pkg/repository/tenant.go:923` |
| `directEnv.LITE_STATIC_ASSET_DIR` | `—` | `string` | 未设置/需 loader | `LITE_STATIC_ASSET_DIR` | hatchet-lite 静态资源目录 | 部署层：hatchet-lite 专属 | `cmd/hatchet-lite/main.go:73` |
| `directEnv.LITE_FRONTEND_BASE_PATH` | `—` | `string` | 未设置/需 loader | `LITE_FRONTEND_BASE_PATH` | hatchet-lite 前端 base path | 部署层：hatchet-lite 专属 | `cmd/hatchet-lite/main.go:74` |
| `directEnv.LITE_FRONTEND_PORT` | `—` | `string` | 未设置/需 loader | `LITE_FRONTEND_PORT` | hatchet-lite 前端端口 | 部署层：hatchet-lite 专属 | `cmd/hatchet-lite/main.go:75` |
| `directEnv.LITE_RUNTIME_PORT` | `—` | `string` | 未设置/需 loader | `LITE_RUNTIME_PORT` | hatchet-lite runtime 端口 | 部署层：hatchet-lite 专属 | `cmd/hatchet-lite/main.go:76` |
| `directEnv.DATABASE_URL` | `—` | `string` | 若未设则由 database.* 组装并写回进程环境 | `DATABASE_URL` | 数据库直连 URL：ConfigLoader 先读取；未设置才由 database.yaml 的 host/port/username/password/dbName/sslMode 组装。迁移库 RunMigrations 则先取嵌入调用方 WithDatabaseURL，再取此变量；两者皆空返回 MissingEnvError，不会自行组装。 | 部署层：数据库 adapter | `pkg/config/loader/loader.go:151; cmd/hatchet-migrate/migrate/run.go:104` |
| `directFlag.hatchet-staticfileserver.port` | `—` | `string` | 80 | `无（flag）` | 静态文件 HTTP 监听端口。 | 部署层：静态文件托管 adapter | `cmd/hatchet-staticfileserver/main.go:16` |
| `directFlag.hatchet-staticfileserver.static-asset-dir` | `—` | `string` | . | `无（flag）` | 静态资源根目录。 | 部署层：静态文件托管 adapter | `cmd/hatchet-staticfileserver/main.go:17` |
| `directFlag.hatchet-staticfileserver.base-path` | `—` | `string` | 环境 BASE_PATH；未设置时 / | `BASE_PATH（仅为该 flag 默认值）` | 应用被反向代理到子路径时使用的 URL 基路径；flag 显式值覆盖其默认值。 | 部署层：静态文件托管 adapter | `cmd/hatchet-staticfileserver/main.go:18` |

## 非 YAML 的代码注入

| 项 | 处理方式 | 结论 | 源码 |
|---|---|---|---|
| `shared.LoggerConfigFile.Writer` | 嵌入调用方通过 `loader.WithLogWriter` 或 `ServerConfigFileOverride` 传入 `io.Writer` | `mapstructure:"-"`、`json:"-"`，不属于部署字段；已从逐字段索引剔除。 | `pkg/config/shared/shared.go:25-33`、`pkg/config/loader/loader.go:103-128` |
| Repository、Logger、MessageQueue、PubSub、Validator、Alerter、Partition、Analytics、PrometheusGate 等 `With...` | `createControllerLayer` 构建后传入服务 constructor | runtime 依赖注入，不能伪报为 YAML/env 或 SDK Options。 | `pkg/config/loader/loader.go:560-1399` |
| RabbitMQ dead-letter backoff、OLAP controller jitter/timeout/requeue、dispatcher slot fallback | constructor 内硬编码默认 | 没有发现独立 YAML/env/flag 装载，保留内部实现默认。 | `internal/msgqueue/rabbitmq/rabbitmq.go`、`internal/services/controllers/olap/controller.go`、`internal/services/dispatcher/server.go` |

## 生产入口、CLI 与模板

| 入口 | 可调项 | 默认/优先级 | 归属 | 源码 |
|---|---|---|---|---|
| hatchet-engine | `--config-directory`、`--version`、`--debug` | Cobra 定义值；配置目录决定 server.yaml/database.yaml 位置 | 服务端启动/诊断 | `cmd/hatchet-engine/main.go:52-70` |
| hatchet-api | `--config-directory`、`--version` | OTel 仍使用表内 `server.otel.*` 的环境绑定 | 服务端启动 | `cmd/hatchet-api/main.go:42-55`、`cmd/hatchet-api/api/run.go:14-18` |
| hatchet-migrate | `--version`、`--down`、`--up-to-penultimate`、`--up-to` | 没有 `--config-directory` 或 `--database-url`；数据库来源按上文 RunMigrations 规则 | 迁移 adapter 专属 | `cmd/hatchet-migrate/main.go:65-100` |
| hatchet-lite | `--config-directory`、`--version`、LITE_* | LITE_* 已在表内；队列未显式配置时选择内置路径 | 本地/嵌入式 adapter 专属 | `cmd/hatchet-lite/main.go:50-105` |
| hatchet-staticfileserver | `--port`、`--static-asset-dir`、`--base-path`，`BASE_PATH` | 分别默认 `80`、`.`、`BASE_PATH` 或 `/`；已列入表 | 静态文件托管 adapter 专属 | `cmd/hatchet-staticfileserver/main.go:16-18` |

模板核查：`docker-compose.yml`、`docker-compose.release.yml` 和 compression-test compose 仅出现本表已有命名变量（含 DATABASE_URL、SERVER_PORT、SERVER_URL、SERVER_GRPC_*、SERVER_MSGQUEUE_KIND、SERVER_SECURITY_CHECK_ENABLED、SERVER_OBSERVABILITY_ENABLED、日志/Cookie/内部 gRPC 变量）；未读取 `.env` 或变量值。

## 排除与未核实边界

- 排除其他语言 SDK、`pkg/config/client`、CLI profile/`hatchet.yaml`、前端/UI、测试、hack、loadtest、CI/dev 脚本。
- `pkg/security/security.go` 的 `GITHUB_ACTIONS`、`CI`、`KUBERNETES_SERVICE_HOST` 是环境探测分支，不是管理员配置字段。
- 部署模板值、真实环境值、密钥内容均未读取；本表只陈述源码默认、绑定和调用语义。

字段计数：281 个叶子部署字段/入口（server 234，database 29，直接生产 env 15，直接 flag 3）。本计数对应 JSON 索引；“生产入口、CLI 与模板”表中其余启动 flag 单列说明，不重复加入该索引。
