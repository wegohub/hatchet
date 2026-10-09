# MinIO 部署与 codec 专项验收

日期：2026-10-09。SDK **0.2.1**，协议 **4**。本轮部署和专项验收 **PASSED**，完整机器记录见 [JSON 报告](minio-codec-report.json)。

本机新增 MinIO，使用 AWS SDK for Go v2 的 S3 客户端连接；代码和部署模板仅修改 `sdks/wego/**`，Hatchet 源码与根依赖未修改。

## 部署结果

| 项目 | 实际结果 |
|---|---|
| Hatchet | 官方 v0.110.5 镜像，PostgreSQL MQ，健康 |
| MinIO | 官方 RELEASE.2025-10-15T17-29-55Z 源码构建，健康 |
| 源提交 | `9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a` |
| S3 / 控制台 | http://127.0.0.1:9000 / http://127.0.0.1:9001 |
| S3 客户端 | AWS SDK for Go v2，service/s3 v1.114.2；无 MinIO Go 客户端依赖 |
| 数据 / 凭证 | 独立 minio_data 卷；私有 .env 权限 0600，构建上下文排除凭证 |

MinIO 本地构建镜像 ID：`sha256:c875bbc66ec5439458885dc385d3a5bf116607166aeaed575f028017ca3423d3`。官方社区最新发布要求 [按源码构建容器](https://github.com/minio/minio/releases/tag/RELEASE.2025-10-15T17-29-55Z)，模板固定发布和基础镜像 digest，不使用第三方 MinIO 镜像。

## 实际验证

- gzip → AES-256-GCM → S3 上传，反向有界还原；真实 unary 和 client/server/bidi 调用全部成功。
- 三个同组 unary 并发提交，CEL 仍能读取 routing，handler 并发峰值为 1。
- client/bidi 整批输入及输出顺序、空输入/空输出、慢消费、提前返回、业务错误与终态均通过。
- server stream 首次输出 0、1 后失败；第二次执行读取对象历史，从 2 继续。四条业务输出连续；二进制 headers 和 trailers 保留。
- 独立 Conn、S3 客户端及 codec 从 JSON checkpoint 恢复同一 RunID；没有重新提交或第三次执行 handler。
- CLAIM、JOIN、HEADERS、DATA、结束和 EVENT 经过完整 codec；广播事件真实到达业务回调。
- 真实 S3 缺失对象、跨 prefix 引用和超预算下载明确失败；单元测试另覆盖 SigV4、实际响应超限、SHA256 篡改、解压膨胀、跨方法解密与取消。

race 真实验收保存 **22 条成功断言**。本轮 prefix：`codec-211e0b91-e889-427d-b582-b7312c2d44aa/`，桶 `wego-codec`，保留 **86 个对象**。初始共享 codec 成功上传 86 次、还原 107 次；不包含独立恢复客户端或 S3 SDK 内部重试计数。

| 场景 | 本轮 RunID |
|---|---|
| unary | `9b3f2b20-88ff-46b9-b9e3-7e2401bfd421` |
| unary | `b7f52ac6-cbf7-4555-86ae-a8ec3500df0e` |
| unary | `e5e11126-bf9f-46e8-b33a-a3c8b00c721f` |
| client stream | `d73a7107-f8f1-4955-88ae-9a4346c83068` |
| bidi | `fc923842-c98f-4d63-9a73-9413210f8631` |
| server stream / 恢复 | `eb644155-643e-4c16-a0d6-11a7cb4dcf46` |
| grpc-streams | `5b01da89-648f-4b62-a29e-25007be617dc` |
| grpc-streams | `6c4fae89-610a-4500-b48a-6eb0717a2971` |
| grpc-streams | `c9f0b646-8ec8-4225-9a47-84913b1e428d` |
| bidi | `7934ef2c-f952-4503-9160-c3b8158f4fa4` |
| grpc-streams | `18601648-ca75-43c8-9627-6ac296d52bcd` |

## 质量门禁

SDK 全量单元测试、vet、race 通过；中文注释、公开 API 隔离、生成代码与 manifest 复现、报告契约通过。官方 SDK/client/worker 在当前 S3 依赖图下回归通过。源码边界、发行依赖、两个 Compose 模板、格式和空白检查通过。

从仓库根目录复现：

```sh
python3 sdks/wego/scripts/setup-minio.py
docker compose -f sdks/wego/deployment/compose/docker-compose.yml up -d --build --wait minio
python3 sdks/wego/scripts/local-test.py env GOWORK=off go -C sdks/wego run ./examples/codec
env GOWORK=off go -C sdks/wego test -race -tags=e2e ./tests/e2e -run '^TestMinIOCodec$' -count=1 -v -timeout 5m
GOWORK=off go -C sdks/wego test ./...
GOWORK=off go -C sdks/wego vet ./...
GOWORK=off go -C sdks/wego test -race ./...
```

全部详细命令、日志位置、镜像身份及源文件身份见 JSON 报告；使用说明见 [codec 示例](../../examples/codec/README.md) 和 [部署模板](../../deployment/compose/README.md)。

## 清理与范围

本轮 15 个工作流定义均已通过官方软删除清理，数据库查询确认未删除的定义为 0。Worker、原连接及恢复连接已关闭，独立探针对象删除。任务/topic 历史及其引用的 S3 对象保留；独立示例和较早测试使用各自独立 prefix。删除对象或历史密钥将使对应记录无法恢复，因此未配置短期对象自动过期。

本轮为 0.2.1 的部署和专项验收，没有重新执行全部 18 项门禁、28 个官方源文件及 embedded 完整矩阵；0.2.0 完整结果保留为 [历史报告](acceptance-report.md)，未改标为 0.2.1。CI 模板加入 MinIO、凭证及持久流初始化，已本地校验配置，未在远程 CI 执行。
