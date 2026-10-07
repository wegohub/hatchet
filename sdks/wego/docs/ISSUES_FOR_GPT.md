# Wego SDK 问题列表 - 供 GPT 修复

本文档列出代码审查和验收测试中发现的所有问题，供自动化修复使用。

---

## P1 - 需要优先修复

### V-R1: 流握手 PING 循环优化

**位置**: `internal/session/client.go:283-310`

**当前实现**:
```go
for {
    if _, err := c.control(handshake, &wire.Frame{Kind: "PING", Nonce: nonce}); err != nil {
        if status.Code(err) != codes.Unavailable {
            c.abort(err)
            return nil, c.terminalError(err)
        }
    }
    select {
    case value := <-c.ready:
        if value == nonce {
            _, err = c.control(handshake, &wire.Frame{Kind: "OPEN"})
            return c, nil
        }
    case <-handshake.Done():
        return nil, c.terminalError(handshake.Err())
    case <-derived.Done():
        return nil, c.terminalError(derived.Err())
    case <-time.After(100 * time.Millisecond):  // ⚠️ 问题：无条件休眠
    }
}
```

**问题描述**:
1. 使用 `time.After(100ms)` 导致每次循环都无条件等待 100ms
2. 即使 READY 已经到达，也会等待 100ms 才检查
3. 没有最大重试次数限制
4. 历史测试中观察到 2 次握手超时（延迟订阅场景）

**修复建议**:
```go
ticker := time.NewTicker(100 * time.Millisecond)
defer ticker.Stop()

maxPings := 100 // 10秒 / 100ms
pings := 0

for {
    select {
    case value := <-c.ready:
        if value == nonce {
            _, err = c.control(handshake, &wire.Frame{Kind: "OPEN"})
            return c, nil
        }
    case <-ticker.C:
        pings++
        if pings > maxPings {
            c.abort(fmt.Errorf("wego: PING timeout after %d attempts", pings))
            return nil, c.terminalError(fmt.Errorf("wego: handshake timeout"))
        }
        if _, err := c.control(handshake, &wire.Frame{Kind: "PING", Nonce: nonce}); err != nil {
            if status.Code(err) != codes.Unavailable {
                c.abort(err)
                return nil, c.terminalError(err)
            }
        }
    case <-handshake.Done():
        c.abort(handshake.Err())
        return nil, c.terminalError(handshake.Err())
    case <-derived.Done():
        return nil, c.terminalError(derived.Err())
    }
}
```

**验证方法**:
1. 运行现有的 `tests/e2e/stream_faults_test.go`
2. 检查握手时间是否改善
3. 确认超时场景正确失败

**预期影响**:
- 握手响应更快（无无谓等待）
- 超时更可预测
- 日志更明确（显示重试次数）

---

## P2 - 建议下个版本修复

### V-R2: 迟到 ACK 等待的超时保护

**位置**: `internal/backend/durable.go:47-51`

**当前实现**:
```go
case <-ctx.Done():
    // 调用方及时返回；此登记仍拥有 ACK 槽，直到引擎确认或 listener 清理。
    go func() {
        <-channel  // ⚠️ 问题：可能永久阻塞
        e.ackGate.Unlock()
    }()
    return nil, ctx.Err()
```

**问题描述**:
如果 listener 在 SendRequest 后但在后台 goroutine 完成前被 Stop，`<-channel` 可能永久阻塞，导致 goroutine 泄漏。

**修复建议**:
```go
case <-ctx.Done():
    go func() {
        timeout := e.backend.config.Shutdown.Timeout
        if timeout <= 0 {
            timeout = 30 * time.Second
        }
        select {
        case <-channel:
            // 正常收到 ACK
        case <-time.After(timeout):
            // 超时但仍释放锁，记录警告
            if e.backend.config.Logger != nil {
                e.backend.config.Logger.Warn("durable ACK wait timeout",
                    "task_id", e.original.StepRunId(),
                    "timeout", timeout)
            }
        }
        e.ackGate.Unlock()
    }()
    return nil, ctx.Err()
```

**验证方法**:
1. 创建测试模拟 listener Stop
2. 检查 goroutine 计数
3. 确认超时后正确释放锁

**预期影响**:
- 避免极端情况下的 goroutine 泄漏
- 提高系统健壮性

---

### V-R3: 流清理的可观测性改进

**位置**: `internal/session/client.go:204-209`

**当前实现**:
```go
if !finished {
    cleanup, stop := context.WithTimeout(context.Background(), config.Stream.HandshakeTimeout)
    defer stop()
    _, _ = c.control(cleanup, &wire.Frame{Kind: "CANCEL"})  // ⚠️ 忽略错误
}
```

**问题描述**:
1. CANCEL 发送失败没有日志
2. 使用握手超时（10秒）可能过长
3. 清理问题难以诊断

**修复建议**:
```go
if !finished {
    cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
    defer stop()
    
    if _, err := c.control(cleanup, &wire.Frame{Kind: "CANCEL"}); err != nil {
        // 记录但不阻塞后续清理
        if c.config.Logger != nil {
            c.config.Logger.Debug("stream CANCEL failed",
                "stream_id", c.id,
                "error", err,
                "method", c.method)
        }
    }
}
```

**验证方法**:
1. 检查日志输出
2. 确认 5 秒超时足够
3. 验证清理仍然继续

**预期影响**:
- 提高问题可诊断性
- 缩短清理超时

---

### V-R4: 端到端 memo 测试补充

**位置**: 新增测试文件 `tests/e2e/durable_memo_eviction_test.go`

**问题描述**:
当前的 `internal/backend/memo_test.go:TestNowCompletesPendingMemo` 使用 mock listener，不是真实的驱逐后恢复场景。

**建议新增测试**:
```go
func TestNowAfterEvictionRestoresValue(t *testing.T) {
    // 1. 启动 Worker，执行 Now() 并记录时间
    worker1 := startWorker(t)
    runID := triggerDurableTask(t, "now_eviction_test")
    
    // 等待第一次 Now() 完成
    firstTime := waitForNowCompletion(t, runID)
    
    // 2. 触发驱逐
    evictTask(t, runID)
    worker1.Stop()
    
    // 3. 启动新 Worker，恢复执行
    worker2 := startWorker(t)
    
    // 4. 等待第二次 Now() 完成
    secondTime := waitForSecondNow(t, runID)
    
    // 5. 断言两次返回相同时间
    if !firstTime.Equal(secondTime) {
        t.Errorf("Now() returned different times: %v vs %v", firstTime, secondTime)
    }
    
    t.Logf("✓ Now() correctly restored time after eviction: %v", firstTime)
}
```

**验证方法**:
运行新测试并确认通过

**预期影响**:
- 增强对 F12 修复的信心
- 覆盖真实驱逐场景

---

## P3 - 可选改进（代码质量）

### V-R5: ownedIO 泄漏的诊断信息

**位置**: `internal/engine/engine.go:321-326`

**建议**: 记录具体卡住的 I/O 操作 ID

---

### V-R6: model 包拆分

**位置**: `model/execution.go`

**建议**: 分离配置、DTO、错误类型到不同文件或子包

---

### V-R7: 包命名重构

**位置**: `internal/clientcore/`, `internal/featurecore/`, `internal/telemetrycore/`

**建议**: 移除 "core" 后缀，改为 `internal/client`, `internal/features`, `internal/telemetry`

---

### V-R8: 流性能基准测试

**位置**: 新增 `tests/benchmark/stream_performance_test.go`

**建议**: 补充多会话并发的性能基准

---

### V-R9: 结果等待轮询优化

**位置**: `internal/backend/run.go:302`

**建议**: 增加轮询间隔到 1-2 秒，或使其可配置

---

## 修复优先级

1. **立即修复**: V-R1 (P1)
2. **下个版本**: V-R2, V-R3, V-R4 (P2)
3. **按需修复**: V-R5, V-R6, V-R7, V-R8, V-R9 (P3)

---

## 验证清单

修复后需要验证：

- [ ] 所有现有测试通过 (`go test ./...`)
- [ ] Race 测试通过 (`go test -race ./...`)
- [ ] E2E 测试通过 (`tests/e2e/`)
- [ ] 新增测试覆盖修复场景
- [ ] 代码审查通过
- [ ] 文档已更新

---

**文档生成时间**: 2026-10-07  
**SDK 版本**: 0.1.8  
**评审者**: Claude (Anthropic)
