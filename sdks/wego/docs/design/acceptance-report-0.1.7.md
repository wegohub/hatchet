wego SDK 0.1.7 本机验收记录

完成时间：2026-10-07T11:29:11.102801+00:00。Hatchet v0.107.0，PostgreSQL MQ，明文 gRPC。

SDK 发行依赖：未经修改的官方 Hatchet v0.109.0；发行依赖门禁在 GOWORK=off 下执行。仓库源码边界单独检查。

28 个源文件、74 个 standalone 构造片段全部通过；三种流、9 组故障、durable 重启、middleware、排空和 embedded 通过。

| 官方源文件 | 片段数 | 场景 | 结果 |
|---|---:|---|---|
| `sdks/go/examples/batch_assign/main.go` | 3 | batch | PASSED |
| `sdks/go/examples/child-workflows/main.go` | 2 | child-workflows | PASSED |
| `sdks/go/examples/concurrency/main.go` | 9 | concurrency | PASSED |
| `sdks/go/examples/cron/main.go` | 4 | cron | PASSED |
| `sdks/go/examples/durable/event/main.go` | 1 | durable-event | PASSED |
| `sdks/go/examples/durable/eviction/main.go` | 4 | eviction | PASSED |
| `sdks/go/examples/durable/eviction/trigger/main.go` | 1 | eviction | PASSED |
| `sdks/go/examples/durable/sleep/main.go` | 1 | durable-sleep | PASSED |
| `sdks/go/examples/embedded/main.go` | 1 | embedded | PASSED |
| `sdks/go/examples/events/main.go` | 1 | events | PASSED |
| `sdks/go/examples/idempotency/worker.go` | 2 | idempotency | PASSED |
| `sdks/go/examples/logs_test/main.go` | 1 | logs | PASSED |
| `sdks/go/examples/migration-guides/mergent.go` | 1 | mergent | PASSED |
| `sdks/go/examples/migration-guides/temporal.go` | 16 | temporal, concurrency, rate-limiting, logs, cron | PASSED |
| `sdks/go/examples/on-event/main.go` | 3 | on-event | PASSED |
| `sdks/go/examples/opentelemetry_instrumentation/main.go` | 2 | observability | PASSED |
| `sdks/go/examples/panic-handler/main.go` | 1 | panic-handler | PASSED |
| `sdks/go/examples/rate-limiting/main.go` | 2 | rate-limiting | PASSED |
| `sdks/go/examples/retries/main.go` | 4 | retries | PASSED |
| `sdks/go/examples/runtime-affinity/main.go` | 1 | runtime-affinity | PASSED |
| `sdks/go/examples/sdk-migration/main.go` | 1 | sdk-migration | PASSED |
| `sdks/go/examples/sdk-migration/v1.go` | 1 | sdk-migration-v1 | PASSED |
| `sdks/go/examples/simple/main.go` | 1 | simple, child-workflows | PASSED |
| `sdks/go/examples/slot-cost/main.go` | 2 | slot-cost | PASSED |
| `sdks/go/examples/sticky-workers/main.go` | 2 | sticky-workers | PASSED |
| `sdks/go/examples/streaming/shared/task.go` | 1 | streaming | PASSED |
| `sdks/go/examples/stubs/stub-workflow.go` | 1 | stubs | PASSED |
| `sdks/go/examples/webhooks/main.go` | 5 | webhooks | PASSED |

质量门禁命令：

- `python3 sdks/wego/scripts/check-upstream.py`：PASSED。
- `python3 sdks/wego/scripts/check-released.py`：PASSED。
- `gofmt -l sdks/wego`：PASSED。
- `go test ./sdks/wego/...`：PASSED。
- `go vet ./sdks/wego/...`：PASSED。
- `go test -race ./sdks/wego/...`：PASSED。
- `go test ./sdks/go/... ./pkg/client/... ./pkg/worker/...`：PASSED。
- `go test ./...`：PASSED。
- `go test -tags=wego_embedded ./...`：PASSED。
- `go test ./sdks/wego/tests/quality -run ^TestGenerationAndAcceptanceManifestAreReproducible$`：PASSED。
- `go test ./sdks/wego/internal/wire -run ^$ -fuzz FuzzDecodeFrame -fuzztime=10s`：PASSED。
- `docker compose -f sdks/wego/tests/compose.yml config --quiet`：PASSED。
- `sdks/wego/scripts/local-test.py go test -json -count=1 -tags=e2e ./sdks/wego/tests/e2e/... -timeout 30m`：PASSED。
- `sdks/wego/scripts/local-test.py go test -race -json -count=1 -tags=e2e ./sdks/wego/tests/e2e/... -run TestExamples/(batch|grpc-streams|shutdown|dual-entry)$|TestBatchShutdownBudget$|TestReview|TestIndependentReview -timeout 10m`：PASSED。
- `sdks/wego/scripts/local-test.py go test -json -count=1 -tags=e2e,wego_embedded ./sdks/wego/tests/e2e/... -run ^TestEmbedded$ -timeout 10m`：PASSED。

逐片段映射、断言、RunID、WorkerID、traceID 和执行命令见 [acceptance-report.json](acceptance-report.json)。

测试使用唯一 namespace，已删除工作流和触发资源；运行历史及引擎 Worker 记录保留。Embedded 测试数据库已删除。报告不包含凭证。

CI 模板与 Compose 配置已在本机检查；仓库 CI 未注册，托管 CI 未执行。

最终源码补充验证：完整引擎套件启动后补入未完成 memo 的恢复修复；后续真实引擎 race / embedded 使用更新源码，最终源码另行通过 unit、vet、race、发行依赖、源码边界和真实 durable 等待 / 驱逐 / 重启复验。`TestNowCompletesPendingMemo` 验证已存在但无载荷的 memo 补算、完成通知和单次 invocation 缓存。实际命令、日志及源码摘要见 JSON 报告的 `final_source_checks`。

此前两次完整复验出现流握手超时；定向连续复跑通过，失败日志与测试预算调整说明保留在 [module.md](module.md) 的 0.1.7 条目。当前完整门禁已重新通过；SDK 默认握手预算未改变。
