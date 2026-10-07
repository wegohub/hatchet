# retries 示例

独立入口：[main.go](main.go)。业务 handler、任务策略及结果断言见 [Retries](../scenarios/retries.go)；共享连接、注册及清理配置见 [harness.go](../scenarios/harness.go)。

从仓库根目录运行：

```bash
sdks/wego/scripts/local-test.py go run ./sdks/wego/examples/retries
```

示例实际调用本机引擎，使用独立 namespace，验证业务输出后清理工作流及触发资源；运行历史保留。凭证由本机环境或显式配置文件读取，不写入报告。
