wego SDK 0.2.0 本机验收记录

完成时间：2026-10-09T04:01:44.757006+00:00。Hatchet v0.110.5，PostgreSQL MQ，明文 gRPC。

SDK 发行依赖：未经修改的官方 Hatchet v0.110.5；发行依赖门禁在 GOWORK=off 下执行。仓库源码边界单独检查。

官方镜像：`ghcr.io/hatchet-dev/hatchet/hatchet-engine:v0.110.5`，digest `sha256:185c4311d23fad2faecd5fdd771369a369f33cbfa0e9f4083a952c4fe1c15808`。

28 个源文件、74 个 standalone 构造片段全部通过；三种流、10 组输出故障、流恢复和进程崩溃重启、Worker 事件、durable 重启、middleware、排空和 embedded 通过。

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

- `/opt/homebrew/opt/python@3.14/bin/python3.14 sdks/wego/scripts/check-upstream.py`：PASSED。
- `/opt/homebrew/opt/python@3.14/bin/python3.14 sdks/wego/scripts/check-released.py`：PASSED。
- `gofmt -l sdks/wego`：PASSED。
- `GOWORK=off go -C sdks/wego test ./...`：PASSED。
- `GOWORK=off go -C sdks/wego vet ./...`：PASSED。
- `GOWORK=off go -C sdks/wego test -race ./...`：PASSED。
- `go test ./sdks/go/... ./pkg/client/... ./pkg/worker/...`：PASSED。
- `go test github.com/hatchet-dev/hatchet-embedded/...`：PASSED。
- `go test -tags=wego_embedded ./...`：PASSED。
- `go test ./tests/quality -run '^TestGenerationAndAcceptanceManifestAreReproducible$'`：PASSED。
- `go test ./internal/wire -run '^$' -fuzz '^FuzzFrameCodecDecode$' -fuzztime=10s`：PASSED。
- `GOWORK=off go -C sdks/wego test ./internal/stream -run '^$' -fuzz '^FuzzLogInterpreter$' -fuzztime=10s`：PASSED。
- `/opt/homebrew/opt/python@3.14/bin/python3.14 -m unittest test_acceptance`：PASSED。
- `docker compose -f sdks/wego/tests/compose.yml config --quiet`：PASSED。
- `python3 sdks/wego/scripts/local-test.py env GOWORK=off GOFLAGS=-mod=readonly WEGO_P0_FAULT_FIXTURES=1 go -C sdks/wego test -race -count=1 -tags=e2e ./internal/backend -run 'P0|TestRPCBindingsP1|TestWorkerEventReconnect|TestOversizedResultReport' -timeout 10m`：PASSED。
- `/opt/homebrew/opt/python@3.14/bin/python3.14 /Users/huaanhuang/workspace/src/github.com/wegohub/hatchet/sdks/wego/scripts/local-test.py env GOWORK=off GOFLAGS=-mod=readonly go -C /Users/huaanhuang/workspace/src/github.com/wegohub/hatchet/sdks/wego test -json -count=1 -tags=e2e ./tests/e2e/... -timeout 30m`：PASSED。
- `/opt/homebrew/opt/python@3.14/bin/python3.14 /Users/huaanhuang/workspace/src/github.com/wegohub/hatchet/sdks/wego/scripts/local-test.py env GOWORK=off GOFLAGS=-mod=readonly go -C /Users/huaanhuang/workspace/src/github.com/wegohub/hatchet/sdks/wego test -race -json -count=1 -tags=e2e ./tests/e2e/... -run 'TestExamples/(batch|grpc-streams|middleware|shutdown|dual-entry)$|TestBatchShutdownBudget$|TestReview|TestIndependentReview|TestStream|TestReliableStreamRecovery|TestRealtimeStreams|TestResultCancellation|TestWorkerEvents' -timeout 15m`：PASSED。
- `/opt/homebrew/opt/python@3.14/bin/python3.14 /Users/huaanhuang/workspace/src/github.com/wegohub/hatchet/sdks/wego/scripts/local-test.py env GOWORK=off GOFLAGS=-mod=readonly go -C /Users/huaanhuang/workspace/src/github.com/wegohub/hatchet/sdks/wego test -json -count=1 -tags=e2e,wego_embedded ./tests/e2e/... -run '^TestEmbedded$' -timeout 10m`：PASSED。

逐片段映射、断言、RunID、WorkerID、traceID 和执行命令见 [acceptance-report.json](acceptance-report.json)。

业务示例使用唯一 namespace，已删除工作流和触发资源；协议探针明确保留的定义、持久 topic、运行历史及引擎 Worker 记录保留。Embedded 测试数据库已删除。报告不包含凭证。

CI 模板与 Compose 配置已在本机检查；仓库 CI 未注册，托管 CI 未执行。

最终源码补验：共享日志解释器统一拒绝缺失响应头的 DATA/结束帧；随后重跑 unit、vet、race、状态机 fuzz、P0，并完成最终真实 engine race 与 embedded。完整命令和日志见 JSON 的 `final_source_checks`。

实现/测试/协议/门禁脚本指纹：`8b8b87e4c4add696dbf86cad037457fec9eefa0bd286fdced36fa73e7a6f3c15`（294 个文件）。
