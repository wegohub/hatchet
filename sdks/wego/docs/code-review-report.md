# Wego SDK 代码评审报告

**评审版本**: 0.1.8  
**评审日期**: 2025-01-09  
**评审范围**: `/Users/huaanhuang/workspace/src/github.com/wegohub/hatchet/sdks/wego` 完整源码  
**评审基准**: review-context.md (截至 2026-10-07 的实现与验收)

---

## 执行摘要

本次评审系统地审查了 wego SDK 的全部源码（约 9000 行核心实现 + 测试），重点关注并发安全、流协议、durable 逻辑、资源管理和 API 边界隔离。总体而言，**代码质量较高，架构设计合理**，已经过多轮评审和修复。发现的问题主要集中在边界情况处理、潜在的资源泄漏风险和可维护性改进方面。

**核心优势**:
- ✅ 严格的 API 边界隔离（质量测试自动检查 Hatchet 类型泄漏）
- ✅ 清晰的并发控制模型（submissionGate、sharedCallback 等）
- ✅ 完善的测试覆盖（包括 race、故障注入和真实引擎验收）
- ✅ 详细的中文注释解释关键不变量和边界条件

**需要关注的领域**:
- ⚠️ 某些资源清理路径的复杂性可能导致泄漏
- ⚠️ 流握手超时问题在某些场景下仍未完全确定根因
- ⚠️ 部分并发控制的正确性依赖细节实现而非类型系统保证

---

## 1. 确定性与并发安全 [高优先级]

### 1.1 Durable ACK 隔离机制 ✅

**位置**: `internal/backend/durable.go:17-53`

**评估**: 设计合理，实现正确

```go
func (e *execution) eventAck(ctx context.Context, request *v1.DurableTaskRequest) (*v1.DurableTaskResponse, error) {
    if err := e.ackGate.Lock(ctx); err != nil {
        return nil, err
    }
    // listener 的 Stop 和根执行清理都会结束登记的等待
    listener := e.durableListener()
    if listener == nil {
        e.ackGate.Unlock()
        return nil, model.ErrDurableContext
    }
    channel := listener.AddPendingEventAck(...)
    if err := listener.SendRequest(ctx, request); err != nil {
        e.ackGate.Unlock()
        return nil, Normalize(err)
    }
    select {
    case ack := <-channel:
        e.ackGate.Unlock()
        return ack.Resp, Normalize(ack.Err)
    case <-ctx.Done():
        // 迟到 ACK 隔离：不立即释放锁，后台等待确认
        go func() {
            <-channel
            e.ackGate.Unlock()
        }()
        return nil, ctx.Err()
    }
}
```

**优点**:
1. `submissionGate` 确保 Sleep/memo/子提交串行登记
2. 取消后仍等待迟到 ACK，避免下次请求收到旧确认
3. 只有发送成功才进入等待，失败直接释放锁

**潜在问题** [严重程度: 低]:

**问题 1.1.1**: goroutine 泄漏风险

如果 listener 在 `SendRequest` 后但在后台 goroutine 完成前被 Stop，`<-channel` 可能永久阻塞。

**建议**:
```go
case <-ctx.Done():
    go func() {
        // 添加超时保护
        select {
        case <-channel:
        case <-time.After(e.backend.config.Shutdown.Timeout):
            // 记录警告但仍释放锁
        }
        e.ackGate.Unlock()
    }()
```

**触发条件**: Worker 在请求取消后、ACK 到达前执行 Stop
**影响**: goroutine 泄漏，长期运行后可能累积
**优先级**: P2 (建议修复)

---

### 1.2 共享子结果等待 ✅

**位置**: `internal/backend/durable.go:99-154`

**评估**: 设计优秀，已正确处理 F11 问题

```go
func (s *sharedCallback) wait(ctx context.Context, e *execution, key callbackKey) ([]byte, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    s.mu.Lock()
    run := s.current
    if run == nil {
        // 创建唯一监听
        listener := e.durableListener()
        budget, cancel := context.WithCancel(lifetime)
        run = &callbackWait{done: make(chan struct{})}
        s.current = run
        go func() {
            data, err := listener.WaitForCallback(budget, taskID, invocation, key.branch, key.node)
            cancel()
            s.mu.Lock()
            run.payload, run.err = data, Normalize(err)
            close(run.done)
            s.mu.Unlock()
        }()
    }
    run.observers++
    s.mu.Unlock()
    
    // 等待完成或取消
    var data []byte
    var err error
    select {
    case <-run.done:
        data, err = append([]byte(nil), run.payload...), run.err
    case <-ctx.Done():
        err = ctx.Err()
    }
    
    s.mu.Lock()
    run.observers--
    s.mu.Unlock()
    return data, err
}
```

**优点**:
1. 多观察者共享底层监听，避免重复注册
2. 每个观察者获得独立字节副本（`append([]byte(nil), run.payload...)`）
3. 观察者取消不影响其他观察者
4. 无观察者时仍保留完成结果

**已验证**: `tests/e2e/shared_result_test.go` 测试多观察者场景

---

### 1.3 Invocation 身份隔离 ✅

**位置**: `internal/backend/native_worker.go:178-230`

**评估**: 正确实现，避免了 F04 问题

```go
func (n *nativeWorker) invokeDurable(ctx oldworker.HatchetContext, p spec.Task, fn func(...) (any, error)) (any, error) {
    // ...
    invocationContext, cancelInvocation := context.WithCancel(ctx.GetContext())
    ctx.SetContext(invocationContext)
    
    // 固定持有这一代预算
    run := &durableRun{
        invocation: max(1, ctx.DurableTaskInvocationCount()), 
        policy: p, 
        cancel: cancelInvocation,
    }
    n.runs[ctx.StepRunId()] = run
    // ...
    
    defer func() {
        n.mu.Lock()
        if n.runs[ctx.StepRunId()] == run {  // 指针比较！
            delete(n.runs, ctx.StepRunId())
        }
        n.mu.Unlock()
    }()
}
```

**优点**:
1. 使用指针比较 (`n.runs[id] == run`) 确保只删除当前 invocation
2. 驱逐回调检查 invocation 编号匹配：`run != nil && run.invocation == invocation`
3. 旧 invocation 退出不会影响恢复执行

**已验证**: F04 修复测试通过

---

### 1.4 未完成 Memo 处理 ✅

**位置**: `internal/backend/execution.go:182-231`

**评估**: 已正确补算，F12 已修复

```go
func (e *execution) Now(ctx context.Context) (time.Time, error) {
    // ...
    response, err := e.eventAck(ctx, &v1.DurableTaskRequest{Message: &v1.DurableTaskRequest_Memo{...}})
    ack := response.GetMemoAck()
    data := ack.MemoResultPayload
    
    // 已建立但未完成的 memo 仍需计算
    if !ack.MemoAlreadyExisted || len(data) == 0 {
        data, err = json.Marshal(time.Now().UTC())
        if err == nil {
            err = listener.SendMemoCompleted(ctx, ack.Ref, key, data)
        }
        if err != nil {
            return time.Time{}, Normalize(err)
        }
    }
    // ...
}
```

**优点**: 
1. 区分"已建立但未完成"和"已完成"
2. 补算后调用 `SendMemoCompleted` 通知引擎

**测试覆盖**: `internal/backend/memo_test.go:TestNowCompletesPendingMemo`

**注意**: 测试使用受控协议对端，不是端到端驱逐恢复场景。建议补充真实驱逐后恢复的集成测试。

---

## 2. 流协议实现 [高优先级]

### 2.1 握手序列与订阅建立 ✅

**位置**: `internal/session/client.go:110-311`

**评估**: 协议设计合理，但有复杂性

握手序列: `START → RUN → PING/READY → OPEN`

```go
func NewClientManaged(...) (*Client, error) {
    // 1. START 分配 owner
    ref, err := backend.Run(handshake, StartName(method), Control{...}, model.RunOptions{})
    result, err := ref.Wait(handshake)
    var ack Acknowledgment
    single(result.Outputs, &ack)
    c.owner = ack.Owner
    
    // 2. RUN 提交业务任务
    ref, err = backend.Run(handshake, binding.Name(method)+"-session", Control{...}, c.options())
    c.runID = ref.ID
    
    // 3. 后台订阅和结果等待
    background.Add(2)
    go func() { backend.Stream(derived, ref.ID, c.receiveFrame) }()
    go func() { ref.Wait(derived) }()
    
    // 4. PING/READY 握手
    for {
        c.control(handshake, &wire.Frame{Kind: "PING", Nonce: nonce})
        select {
        case value := <-c.ready:
            if value == nonce {
                _, err = c.control(handshake, &wire.Frame{Kind: "OPEN"})
                return c, nil
            }
        case <-handshake.Done():
            return nil, c.terminalError(handshake.Err())
        case <-time.After(100 * time.Millisecond):
        }
    }
}
```

**潜在问题** [严重程度: 中]:

**问题 2.1.1**: 握手超时的两次观察未完全解释

根据 review-context.md 第 10.1 节：
- 延迟订阅故障场景耗尽 8 秒预算，后续通过 3 次
- middleware 最后一条 bidi 流耗尽 30 秒预算，后续通过 3 次

**根因分析**:
1. PING 循环使用 `time.After(100ms)` 无条件休眠，即使 READY 已到达
2. 如果订阅建立延迟（如 800ms 注入），累计 PING 次数可能达到 80+ 次
3. 每次 PING 的 START 控制任务提交到 MQ，在高负载/竞争下可能排队

**建议**:
```go
// 使用 ticker 而非 time.After，避免无谓轮询
ticker := time.NewTicker(100 * time.Millisecond)
defer ticker.Stop()

// 添加最大重试次数
maxPings := 100 // 10 秒 / 100ms
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
            return nil, fmt.Errorf("wego: PING timeout after %d attempts", pings)
        }
        if _, err := c.control(handshake, &wire.Frame{Kind: "PING", Nonce: nonce}); err != nil {
            if status.Code(err) != codes.Unavailable {
                c.abort(err)
                return nil, c.terminalError(err)
            }
        }
    case <-handshake.Done():
        return nil, c.terminalError(handshake.Err())
    }
}
```

**优先级**: P1 (建议修复以提高稳定性)

---

### 2.2 窗口与背压控制 ✅

**位置**: `internal/session/control.go:15-173`

**评估**: 实现正确，协议清晰

```go
func (s *Endpoint) control(ctx context.Context, frame *wire.Frame) error {
    s.mu.Lock()
    // ...
    
    case "DATA":
        if frame.Seq <= s.consumed {
            s.mu.Unlock()
            return nil  // 已消费的旧序号，幂等处理
        }
        
        if data, ok := s.incoming[frame.Seq]; ok {
            equal := bytes.Equal(data, frame.Payload)
            s.mu.Unlock()
            if !equal {
                return status.Error(codes.DataLoss, "wego: duplicate DATA differs")
            }
            return nil
        }
        
        // 窗口检查：序号、条数、字节三重限制
        if frame.Seq > s.consumed+uint64(s.config.Stream.Window) ||
            len(s.incoming) >= s.config.Stream.Window ||
            s.inputBytes+len(frame.Payload) > s.config.Stream.BufferBytes {
            s.mu.Unlock()
            return status.Error(codes.ResourceExhausted, "wego: input window full")
        }
        
        s.incoming[frame.Seq] = append([]byte(nil), frame.Payload...)
        s.inputBytes += len(frame.Payload)
        s.notify()
        s.mu.Unlock()
        return nil
}
```

**优点**:
1. 同序号重复消息检查内容相等性
2. 窗口满时明确返回 `ResourceExhausted`
3. 字节和条数双重限制

**已验证**: `tests/e2e/stream_faults_test.go` 包含窗口满场景

---

### 2.3 半关闭与终态处理 ✅

**位置**: `internal/session/control.go:108-136`

```go
case "END":
    if frame.Direction != "input" || !s.open {
        s.mu.Unlock()
        return status.Error(codes.InvalidArgument, "wego: invalid input END")
    }
    
    // 检查 END 不排除已缓存的消息
    for seq := range s.incoming {
        if seq > frame.Seq {
            s.mu.Unlock()
            return status.Error(codes.DataLoss, "wego: END excludes buffered input")
        }
    }
    
    // 检查冲突的 END
    if s.ended && s.lastInput != frame.Seq {
        s.mu.Unlock()
        return status.Error(codes.DataLoss, "wego: conflicting END")
    }
    
    // END 不能早于已消费序号
    if frame.Seq < s.consumed {
        s.mu.Unlock()
        return status.Error(codes.DataLoss, "wego: END precedes consumed input")
    }
    
    s.ended = true
    s.lastInput = frame.Seq
    s.notify()
    s.mu.Unlock()
    return nil
```

**优点**:
1. END 只半关闭输入，输出可继续
2. 幂等的 END（相同序号）被接受
3. 明确拒绝冲突的 END 和排除已缓存消息的 END

---

### 2.4 清理与资源释放 ⚠️

**位置**: `internal/session/client.go:192-223`

**评估**: 清理逻辑复杂，有改进空间

```go
go func() {
    defer close(c.cleanupDone)
    
    <-derived.Done()
    c.mu.Lock()
    finished := c.final != nil && c.final.LastSeq == c.consumed
    c.mu.Unlock()
    
    if !finished {
        cleanup, stop := context.WithTimeout(context.Background(), config.Stream.HandshakeTimeout)
        defer stop()
        _, _ = c.control(cleanup, &wire.Frame{Kind: "CANCEL"})
    }
    
    <-initialized  // 等待后台 goroutine 登记完成
    background.Wait()
    closed(c.completionError())
}()
```

**潜在问题** [严重程度: 中]:

**问题 2.4.1**: 清理顺序的依赖链较长

清理依赖: `derived.Done() → CANCEL → initialized → background.Wait() → closed()`

如果某个环节卡住（如 CANCEL 发送阻塞），整个清理链会延迟。

**建议**:
1. 为 CANCEL 发送添加独立的短超时（如 5 秒）
2. 记录清理阶段的耗时，便于诊断

```go
if !finished {
    cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
    defer stop()
    if _, err := c.control(cleanup, &wire.Frame{Kind: "CANCEL"}); err != nil {
        // 记录但不阻塞后续清理
        if c.config.Logger != nil {
            c.config.Logger.Debug("CANCEL failed", "error", err)
        }
    }
}
```

**优先级**: P2 (改进可观测性)

---

## 3. 资源管理与生命周期 [高优先级]

### 3.1 Engine 关闭序列 ✅

**位置**: `internal/engine/engine.go:234-352`

**评估**: 设计合理，实现正确

```go
func (e *Engine) Shutdown(ctx context.Context) error {
    e.mu.Lock()
    if e.closing {
        done := e.done
        e.mu.Unlock()
        select {
        case <-done:
            return e.closeErr
        case <-ctx.Done():
            return ctx.Err()
        }
    }
    e.closing = true
    workers := append([]ports.Worker(nil), e.workers...)
    e.mu.Unlock()
    
    var errs []error
    
    // 1. 开始排空：拒绝新会话
    if e.BeginDrain != nil {
        e.BeginDrain()
    }
    
    // 2. 业务 Worker 排空
    for _, w := range workers {
        errs = append(errs, w.Close(ctx))
    }
    
    // 3. 强制停止剩余会话
    if e.StopSessions != nil {
        e.StopSessions()
    }
    
    // 4. 等待在途调用归零
    for {
        e.mu.Lock()
        if len(e.calls) == 0 {
            e.mu.Unlock()
            break
        }
        changed := e.changed
        e.mu.Unlock()
        
        select {
        case <-changed:
        case <-ctx.Done():
            // 取消剩余调用
            e.mu.Lock()
            for _, cancel := range e.calls {
                cancel()
            }
            e.mu.Unlock()
            errs = append(errs, ctx.Err())
            goto finish
        }
    }
    
finish:
    // 5. 清理预算：等待 ownedIO 归零
    cleanupCtx := ctx
    if ctx.Err() != nil {
        cleanupCtx, cancelCleanup = context.WithTimeout(context.Background(), timeout)
    }
    
    for {
        e.mu.Lock()
        if len(e.ownedIO) == 0 {
            e.releasing = true
            e.mu.Unlock()
            break
        }
        remaining := e.changed
        e.mu.Unlock()
        
        select {
        case <-remaining:
        case <-cleanupCtx.Done():
            e.mu.Lock()
            e.releasing = true
            e.mu.Unlock()
            errs = append(errs, fmt.Errorf("wego: unfinished operations during cleanup: %w", cleanupCtx.Err()))
            goto release
        }
    }
    
release:
    // 6. flush telemetry 并关闭后端
    errs = append(errs, e.telemetry.Close(cleanupCtx), e.Backend.Close())
    e.mu.Lock()
    e.closeErr = errors.Join(errs...)
    e.closed = true
    close(e.done)
    e.mu.Unlock()
    return e.closeErr
}
```

**优点**:
1. 清晰的六阶段关闭序列
2. 调用方预算耗尽后使用独立清理预算
3. `ownedIO` 区分业务调用和 SDK I/O，强制停止后仍等待 SDK 资源退出

**潜在问题** [严重程度: 低]:

**问题 3.1.1**: ownedIO 泄漏的检测不够明确

如果 `ownedIO` 在清理预算内未归零，只记录一个通用错误。无法知道具体是哪个操作卡住。

**建议**:
```go
case <-cleanupCtx.Done():
    e.mu.Lock()
    e.releasing = true
    if len(e.ownedIO) > 0 && e.config.Logger != nil {
        e.config.Logger.Warn("unfinished I/O operations", 
            "count", len(e.ownedIO),
            "ids", mapKeys(e.ownedIO))
    }
    e.mu.Unlock()
    errs = append(errs, fmt.Errorf("wego: %d unfinished operations during cleanup: %w", 
        len(e.ownedIO), cleanupCtx.Err()))
    goto release
```

**优先级**: P3 (改进可诊断性)

---

### 3.2 Server 停止与并发 ✅

**位置**: `server/lifecycle.go:8-72`

**评估**: 实现正确，支持强制中断

```go
func (s *Server) Stop() {
    s.forceOnce.Do(func() {
        close(s.force)
        s.cancelDrain()
    })
    s.stop()
}

func (s *Server) GracefulStop() {
    s.stop()
}

func (s *Server) stop() {
    s.mu.Lock()
    s.stopping = true
    started := s.started
    s.mu.Unlock()
    s.cancelStart()
    
    s.stopOnce.Do(func() {
        go func() {
            defer close(s.stopDone)
            defer s.cancelDrain()
            if started {
                <-s.initialized
            }
            s.closeEngines()
        }()
    })
    <-s.stopDone
}
```

**优点**:
1. `Stop` 可以中断 `GracefulStop` 的等待
2. 并发 `Stop` 等待同一次关闭结果
3. 启动前停止安全处理

---

### 3.3 Worker Runtime 配置隔离 ✅

**位置**: `internal/clientcore/worker_config_test.go` 和 `internal/spec/clone.go`

**评估**: 已正确修复 F09 问题

```go
// internal/spec/runtime.go
func (r Runtime) Clone() Runtime {
    return Runtime{
        // ... 标量字段直接复制 ...
        Labels:      maps.Clone(r.Labels),
        Middleware:  append([]middleware.Option(nil), r.Middleware...),
        Projections: cloneProjections(r.Projections),
        TLS:         cloneTLS(r.TLS),
        Embedded:    cloneEmbedded(r.Embedded),
    }
}

func cloneProjections(p map[string]map[string]string) map[string]map[string]string {
    if p == nil {
        return nil
    }
    out := make(map[string]map[string]string, len(p))
    for k, v := range p {
        out[k] = maps.Clone(v)
    }
    return out
}
```

**优点**:
1. 深复制嵌套 map（Projections）
2. TLS 配置使用 `tls.Config.Clone()`
3. 有专门的测试验证配置隔离

**已验证**: `internal/clientcore/worker_config_test.go:TestWorkerConfigMutationDoesNotAffectConn`

---

## 4. 批量提交与部分成功 [中优先级]

### 4.1 普通批量冲突合并 ✅

**位置**: `internal/backend/run.go:180-218`

**评估**: 已正确修复 F03 问题

```go
if len(errs) > 0 {
    cause := errs[0]
    if len(errs) > 1 {
        cause = errors.Join(errs...)
    }
    
    var collision *model.BulkIdempotencyCollisionError
    merged := &model.BulkIdempotencyCollisionError{Err: cause}
    
    // 累计各块冲突
    for _, chunkErr := range errs {
        if errors.As(chunkErr, &collision) {
            merged.SuccessfulRunIDs = append(merged.SuccessfulRunIDs, collision.SuccessfulRunIDs...)
            for _, item := range collision.Collisions {
                merged.Collisions = append(merged.Collisions, &model.IdempotencyCollisionError{ExistingRunID: item.ExistingRunID})
            }
        }
    }
    
    if len(merged.Collisions) > 0 || len(merged.SuccessfulRunIDs) > 0 {
        // 包含已成功的 refs
        for _, ref := range refs {
            merged.SuccessfulRunIDs = append(merged.SuccessfulRunIDs, ref.ID)
        }
        return refs, merged
    }
    
    if len(refs) > 0 {
        ids := make([]string, len(refs))
        for i, ref := range refs {
            ids[i] = ref.ID
        }
        return refs, &model.PartialSubmissionError{SuccessfulRunIDs: ids, Err: cause}
    }
    return refs, cause
}
```

**优点**:
1. 跨块合并冲突详情
2. 保留所有成功的 RunID（包括非冲突块）
3. 区分 `BulkIdempotencyCollisionError` 和 `PartialSubmissionError`

**已验证**: 1002 条跨块 fixture 测试

---

### 4.2 Durable 子提交 ✅

**位置**: `internal/backend/durable_submit.go:17-77`

**评估**: 已正确修复 F02 问题

```go
func (e *execution) submitChildren(ctx context.Context, name string, requests []*v1.TriggerWorkflowRequest) ([]ports.Run, error) {
    refs := make([]ports.Run, 0, len(requests))
    
    fail := func(err error) ([]ports.Run, error) {
        if len(refs) == 0 {
            return refs, err
        }
        ids := make([]string, len(refs))
        for i, ref := range refs {
            ids[i] = ref.ID
        }
        return refs, &model.PartialSubmissionError{SuccessfulRunIDs: ids, Err: err}
    }
    
    // 分块提交
    for start := 0; start < len(requests); {
        end, size := start+1, proto.Size(requests[start])
        for end < len(requests) && end-start < 100 && size+proto.Size(requests[end])+8 <= 3<<20 {
            size += proto.Size(requests[end]) + 8
            end++
        }
        
        response, err := e.eventAck(ctx, &v1.DurableTaskRequest{...})
        if err != nil {
            return fail(err)
        }
        
        ack := response.GetTriggerRunsAck()
        if ack == nil {
            return fail(fmt.Errorf("wego: child submission acknowledgment missing"))
        }
        
        // 保留每块的 RunID 和 branch/node
        for i, entry := range ack.RunEntries {
            if i >= end-start || entry == nil || entry.WorkflowRunExternalId == "" {
                return fail(fmt.Errorf("wego: invalid durable child identity"))
            }
            ref := e.childRef(v0.TriggerRunAckEntry{
                WorkflowRunID: entry.WorkflowRunExternalId, 
                BranchID: entry.BranchId, 
                NodeID: entry.NodeId,
            }, name)
            ref.InputIndex = start + i
            refs = append(refs, ref)
        }
        
        if len(ack.RunEntries) != end-start {
            return fail(fmt.Errorf("wego: durable child result count differs"))
        }
        
        start = end
    }
    return refs, nil
}
```

**优点**:
1. 每块确认后保留 RunID、InputIndex、BranchID、NodeID
2. 后续块失败不清空已接受的身份
3. 使用 `eventAck` 串行化提交

**已验证**: F02 修复测试通过

---

## 5. API 边界与类型隔离 [高优先级]

### 5.1 自动化边界检查 ✅✅

**位置**: `tests/quality/api_test.go`

**评估**: 优秀的设计，自动化保证隔离

测试策略:
1. **Import 检查**: 扫描所有 `.go` 文件，确保 Hatchet 导入只在 `internal/backend/` 中
2. **类型图检查**: 加载 Go export 数据，递归检查公开类型的字段、方法、泛型约束

```go
func TestPublicTypeGraphContainsNoHatchetTypes(t *testing.T) {
    // 加载所有公开包的 export 数据
    loader := importer.ForCompiler(token.NewFileSet(), "gc", ...)
    for _, path := range targets {
        p, err := loader.Import(path)
        for _, name := range p.Scope().Names() {
            if ast.IsExported(name) {
                checkType(t, p.Scope().Lookup(name).Type(), ...)
            }
        }
    }
}

func checkType(t *testing.T, v types.Type, seen map[types.Type]bool, origin string) {
    // 递归检查：别名、指针、切片、map、chan、struct 字段、函数签名、接口方法、泛型约束
    if n, ok := v.(*types.Named); ok {
        if p := n.Obj().Pkg(); p != nil && 
           strings.HasPrefix(p.Path(), rootPath) && 
           !strings.HasPrefix(p.Path(), sdkPath) {
            t.Errorf("%s exposes %s", origin, n)
            return
        }
        // 检查方法集
        for i := 0; i < n.NumMethods(); i++ {
            if n.Method(i).Exported() {
                checkType(t, n.Method(i).Type(), seen, origin)
            }
        }
        // 检查泛型约束
        if params := n.TypeParams(); params != nil {
            for i := 0; i < params.Len(); i++ {
                checkType(t, params.At(i).Constraint(), seen, origin)
            }
        }
    }
    // ... 其他类型的递归检查
}
```

**优点**:
1. 编译时保证，不依赖人工 review
2. 覆盖字段、方法、泛型约束等所有泄漏路径
3. CI 中自动运行

**验证**: 本次评审确认没有 Hatchet 类型泄漏

---

### 5.2 公开 API 设计 ✅

**位置**: `wego.go`, `client/api.go`, `server/server.go`, `task/context.go`, `model/execution.go`

**评估**: 设计简洁，类型安全

公开 API 层次:
```
wego.NewConn() / wego.NewServer()
  ↓
client.Conn (implements grpc.ClientConnInterface)
server.Server (implements grpc.ServiceRegistrar)
  ↓
task.Info() / task.Client() / task.Sleep() / task.WaitForEvent()
  ↓
model.TaskInfo / model.RunOptions / model.DesiredWorkerLabel / ...
```

**优点**:
1. 顶层只暴露两个构造函数
2. `Conn` 实现标准 `grpc.ClientConnInterface`，可传给生成的 `pb.NewXXXClient()`
3. `Server` 实现标准 `grpc.ServiceRegistrar`，注册接口与原生 gRPC 一致
4. `task` 包函数通过 context 获取能力，网络入口调用时明确返回 `ErrTaskContext`

**改进建议** [严重程度: 低]:

**问题 5.2.1**: `model` 包混合了配置和错误类型

`model/execution.go` 包含：
- 配置类型：`Concurrency`, `RateLimit`, `BatchConfig`, `EvictionPolicy`
- DTO 类型：`TaskInfo`, `EventWait`, `RunOptions`
- 错误类型：`NonRetryableError`, `IdempotencyCollisionError`, `PartialSubmissionError`

**建议**: 考虑拆分为 `config`, `dto`, `errors` 三个包，或至少在文件级别分离。

**优先级**: P3 (可维护性改进)

---

## 6. 测试覆盖与可信度 [中优先级]

### 6.1 测试分层 ✅

**评估**: 测试覆盖全面，分层清晰

| 测试层级 | 位置 | 覆盖内容 |
|---------|------|----------|
| 单元测试 | `internal/*/`*_test.go` | 协议解析、配置隔离、memo 处理 |
| 质量测试 | `tests/quality/` | API 隔离、注释完整性、生成代码同步 |
| E2E 测试 | `tests/e2e/` | 真实引擎、流故障、durable 重启 |
| Race 测试 | 选择性场景 | 并发安全、资源竞争 |
| 验收测试 | `examples/scenarios/` | 官方 SDK 示例片段覆盖 |

**优点**:
1. E2E 测试使用真实 Hatchet 引擎（PostgreSQL MQ）
2. 故障注入测试（延迟订阅、窗口满、提前终止）
3. 受控协议对端测试（memo、durable ACK）

---

### 6.2 关键测试案例审查 ✅

**位置**: `tests/e2e/shared_result_test.go`, `tests/e2e/durable_restart_test.go`

**shared_result_test.go**: 测试多观察者等待同一子结果
```go
func TestSharedDurableChildResult(t *testing.T) {
    // 提交一个子任务
    ref := submitChild(ctx)
    
    // 并发等待 10 次
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            result, err := ref.Wait(ctx)
            // 每个观察者都能收到结果
            assert.NoError(t, err)
            assert.Equal(t, expected, result)
        }()
    }
    wg.Wait()
}
```

**durable_restart_test.go**: 测试 Worker 重启后恢复
```go
func TestDurableTaskRestartsAfterWorkerExit(t *testing.T) {
    // 启动 Worker 1，执行到 Sleep
    worker1 := startWorker(t)
    triggerTask(t)
    waitForSleep(t, runID)
    
    // 停止 Worker 1
    worker1.Stop()
    
    // 启动 Worker 2，任务恢复
    worker2 := startWorker(t)
    result := waitForCompletion(t, runID)
    
    // 断言任务完成，invocationCount > 1
    assert.Equal(t, expected, result)
}
```

**优点**: 测试真实场景，不是单纯的单元测试

---

### 6.3 测试改进建议 ⚠️

**问题 6.3.1**: memo 测试不是端到端驱逐恢复

`internal/backend/memo_test.go:TestNowCompletesPendingMemo` 使用受控协议对端：
```go
func TestNowCompletesPendingMemo(t *testing.T) {
    // 使用 mock listener
    listener := &mockListener{
        ackCh: make(chan ack),
    }
    
    // 第一次返回未完成 memo
    listener.ackCh <- ack{
        Resp: &v1.DurableTaskResponse{
            Message: &v1.DurableTaskResponse_MemoAck{
                MemoAck: &v1.DurableTaskMemoAck{
                    Ref: &v1.DurableEventLogEntryRef{...},
                    MemoAlreadyExisted: true,
                    MemoResultPayload: nil, // 未完成！
                },
            },
        },
    }
    
    // 调用 Now()
    result, err := exec.Now(ctx)
    
    // 断言补算并发送 CompleteMemo
    assert.NoError(t, err)
    assert.NotZero(t, result)
}
```

**建议**: 补充真实驱逐后恢复的集成测试
```go
func TestNowAfterEvictionRestoresValue(t *testing.T) {
    // 1. 启动 Worker，执行 Now() 并记录时间
    worker1 := startWorker(t)
    runID := triggerDurableTask(t)
    firstTime := extractTimeFromLogs(t, runID)
    
    // 2. 触发驱逐（等待 TTL 或手动驱逐）
    evictTask(t, runID)
    worker1.Stop()
    
    // 3. 启动新 Worker，恢复执行
    worker2 := startWorker(t)
    secondTime := waitForSecondNow(t, runID)
    
    // 4. 断言两次 Now() 返回相同时间
    assert.Equal(t, firstTime, secondTime)
}
```

**优先级**: P2 (增强测试覆盖)

---

## 7. 代码质量与可维护性 [低优先级]

### 7.1 注释质量 ✅

**评估**: 注释非常详细，中文注释清晰

示例：`internal/backend/durable.go:14-16`
```go
// eventAck 串行登记并确认当前 invocation 的协议操作。
// listener 按任务和 invocation 匹配 ACK，不能同时登记 Sleep、memo 或子提交。
// 请求已发送后若调用方取消，继续接收迟到 ACK 后才归还登记权，防止它被下次请求误收。
```

**优点**:
1. 解释**为什么**（不变量和约束），而非只描述**是什么**
2. 关键并发点有明确注释（如"迟到 ACK 隔离"）
3. 复杂算法有步骤说明（如流握手序列）

**改进建议**: 部分长函数（如 `Engine.Shutdown`）可以提取辅助函数以提高可读性。

---

### 7.2 错误处理 ✅

**评估**: 错误处理一致，使用 `backend.Normalize()` 统一转换

```go
// internal/backend/errors.go
func Normalize(err error) error {
    if err == nil {
        return nil
    }
    // 转换为 gRPC status
    if _, ok := status.FromError(err); ok {
        return err
    }
    // 特殊错误类型转换
    // ...
    return status.Error(codes.Unknown, err.Error())
}
```

**优点**:
1. 统一错误转换，避免泄漏内部错误类型
2. 保留 `errors.Join` 的错误链
3. `model.NonRetryableError` 实现 `GRPCStatus()` 接口

---

### 7.3 包拆分 ⚠️

**评估**: 包拆分较细，但有合理性

当前结构:
```
wego/
├── client/                # 公开门面
├── server/                # 公开门面
├── runtime/               # 公开配置
├── worker/                # 公开配置
├── task/                  # 公开能力
├── model/                 # 公开 DTO
├── middleware/            # 公开扩展点
├── telemetry/             # 公开观测配置
├── log/                   # 公开日志配置
├── features/              # 管理门面
├── option/                # Option[T] 泛型
├── internal/
│   ├── clientcore/        # Conn 实现
│   ├── featurecore/       # 管理实现
│   ├── backend/           # Hatchet 适配层（唯一可导入 Hatchet）
│   ├── engine/            # 资源管理
│   ├── binding/           # gRPC → 任务绑定
│   ├── callctx/           # context 能力注入
│   ├── ports/             # 内部接口
│   ├── rpc/               # unary/stream 适配
│   ├── session/           # 流会话协议
│   ├── spec/              # 配置规约
│   ├── telemetrycore/     # 观测实现
│   └── wire/              # envelope & frames
```

**评估**:
- ✅ 公开/内部分离清晰
- ✅ `internal/backend` 隔离 Hatchet 依赖
- ⚠️ `clientcore` / `featurecore` / `telemetrycore` 命名模式值得商榷

**问题 7.3.1**: "core" 后缀的语义不明确

`clientcore` 是 `client` 的实现，`featurecore` 是 `features` 的实现，但为什么不直接在 `internal/client` 和 `internal/features`？

**建议**: 考虑重命名
- `internal/clientcore` → `internal/client`
- `internal/featurecore` → `internal/features`
- `internal/telemetrycore` → `internal/telemetry`

这样公开包 `client` 和内部实现 `internal/client` 的关系更清晰。

**优先级**: P3 (可选的重构)

---

## 8. 安全性考量 [中优先级]

### 8.1 Token 处理 ✅

**位置**: `internal/backend/backend.go:86-92`

```go
if config.Token == "" {
    config.Token = os.Getenv("HATCHET_CLIENT_TOKEN")
}
if config.Token == "" {
    return nil, fmt.Errorf("wego: token is required")
}
```

**优点**:
1. 禁止匿名后端（必须提供 token）
2. 支持环境变量回退

**改进建议**: 记录时脱敏 token
```go
if config.Logger != nil {
    // 只记录 token 的前 8 位
    masked := "***"
    if len(config.Token) > 8 {
        masked = config.Token[:8] + "..."
    }
    config.Logger.Debug("backend initialized", "token", masked)
}
```

---

### 8.2 TLS 配置 ✅

**位置**: `runtime/options.go:72-83`

```go
func WithTLSConfig(value *tls.Config) Option {
    return func(c *spec.Runtime) {
        if value == nil {
            c.TLS = nil
        } else {
            c.TLS = value.Clone()  // Clone 避免修改调用方配置
        }
        c.TLSSet = true
    }
}
```

**优点**:
1. 使用 `tls.Config.Clone()` 避免共享可变状态
2. `TLSSet` 标记区分"未设置"和"明确选择明文"

---

## 9. 性能考量 [低优先级]

### 9.1 Worker 流调度成本 ⚠️

**位置**: `tests/e2e/stream_cost_test.go`

根据 review-context.md 第 10.2 节，样本数据：
- 握手: ~1962 ms
- 消息往返: 68 / 615 / 2085 ms (min/median/max)
- 任务数: 22（8 条消息产生 16 个 DATA/ACK + 6 个控制任务）

**评估**: 
1. 握手时间较长（2 秒），但这是一次性成本
2. 消息延迟中位数 615ms，对于 PostgreSQL MQ 是可接受的
3. 每条消息产生 2 个任务（DATA + ACK）

**文档正确指出**: "该样本不是无 race 的性能基准，没有验证多会话吞吐、长时间稳定性、跨机器网络或生产规模，不能据此宣称 Worker 流是高频交互的最优传输。"

**建议**: 
1. 补充多会话并发场景的基准测试
2. 记录不同 MQ 实现（PostgreSQL vs RabbitMQ）的性能对比
3. 明确文档化适用场景（如"适合每秒 < 10 次消息的长连接，不适合高频小消息"）

**优先级**: P3 (文档和基准改进)

---

### 9.2 结果等待的轮询 ⚠️

**位置**: `internal/backend/run.go:274-342`

```go
func (b *Backend) await(ctx context.Context, id string, wait func(...) (ports.Result, error)) (ports.Result, error) {
    // 主订阅
    go func() {
        result, err := wait(ctx)
        resultCh <- response{result, err}
    }()
    
    // 辅助轮询（250ms 一次）
    go func() {
        ticker := time.NewTicker(250 * time.Millisecond)
        defer ticker.Stop()
        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                budget, stop := context.WithTimeout(ctx, 500*time.Millisecond)
                var state model.RunStatus
                err := b.Feature(budget, ports.RunsGetStatus{ID: id}, &state)
                stop()
                if err == nil && state == model.Cancelled {
                    statusCh <- status.Error(codes.Canceled, "wego: run was cancelled")
                    return
                }
            }
        }
    }()
    
    select {
    case r := <-resultCh:
        return r.result, r.err
    case err := <-statusCh:
        return ports.Result{}, err
    case <-ctx.Done():
        return ports.Result{}, ctx.Err()
    }
}
```

**问题**: 250ms 轮询可能产生不必要的 HTTP 请求

**评估**: 
- 优点: 能够提前检测到 Cancelled 状态
- 缺点: 每次等待都产生 1 个请求/250ms，高并发时请求数放大

**建议**: 
1. 增加轮询间隔到 1-2 秒
2. 或者让轮询可配置
3. 或者只在特定场景启用轮询（如长时间运行的任务）

**优先级**: P3 (性能优化)

---

## 10. 文档与可诊断性 [低优先级]

### 10.1 日志记录 ✅

**评估**: 关键路径有日志

示例: `server/lifecycle.go:66-68`
```go
if err := s.engine.Shutdown(s.drainCtx); err != nil && s.config.Logger != nil {
    s.config.Logger.Debug("Worker 资源关闭", "error", err)
}
```

**改进建议**:
1. 增加更多结构化日志（如握手阶段、流清理阶段）
2. 使用不同日志级别区分正常事件和异常
3. 关键并发点记录 goroutine 数量和资源状态

---

### 10.2 错误消息 ✅

**评估**: 错误消息清晰，包含上下文

示例:
```go
return nil, fmt.Errorf("wego: %d runs accepted before submission failed: %v", len(refs), err)
```

**优点**: 包含具体数量和原因

---

## 总结与优先级建议

### P0 - 无需立即修复

本次评审未发现 P0 级别的严重缺陷。代码整体质量高，关键路径（并发、资源管理、协议）实现正确。

---

### P1 - 建议优先修复

1. **问题 2.1.1: 握手超时优化**
   - 将 PING 循环改为 ticker 模式
   - 添加最大重试次数
   - 位置: `internal/session/client.go:283-310`
   - 预期影响: 减少握手超时的频率和不确定性

---

### P2 - 建议在下个版本修复

1. **问题 1.1.1: 迟到 ACK 等待的超时保护**
   - 为后台等待 channel 添加超时
   - 位置: `internal/backend/durable.go:47-51`
   - 预期影响: 避免极端情况下的 goroutine 泄漏

2. **问题 2.4.1: 流清理的可观测性**
   - 为 CANCEL 发送添加超时和日志
   - 位置: `internal/session/client.go:208`
   - 预期影响: 提高清理问题的可诊断性

3. **问题 6.3.1: 补充端到端 memo 测试**
   - 添加真实驱逐后恢复的集成测试
   - 位置: 新增 `tests/e2e/durable_memo_test.go`
   - 预期影响: 增强 F12 修复的信心

---

### P3 - 可选的改进

1. **问题 3.1.1: ownedIO 泄漏的诊断信息**
   - 记录具体卡住的 I/O 操作 ID
   - 位置: `internal/engine/engine.go:321-326`

2. **问题 5.2.1: model 包拆分**
   - 分离配置、DTO、错误类型
   - 位置: `model/`

3. **问题 7.3.1: 包命名重构**
   - 移除 "core" 后缀
   - 位置: `internal/clientcore/`, `internal/featurecore/`, `internal/telemetrycore/`

4. **问题 9.1: 流性能基准**
   - 补充多会话并发基准测试
   - 文档化适用场景

5. **问题 9.2: 结果等待轮询优化**
   - 增加轮询间隔或使其可配置
   - 位置: `internal/backend/run.go:302`

---

## 附录A: 检查清单

✅ **确定性与并发**
- [x] Durable ACK 串行化 (eventAck + submissionGate)
- [x] 多观察者共享等待 (sharedCallback)
- [x] Invocation 身份隔离 (指针比较)
- [x] 未完成 memo 补算 (F12)

✅ **流协议**
- [x] 握手序列完整性 (START → RUN → PING/READY → OPEN)
- [x] 窗口与背压 (序号、条数、字节三重检查)
- [x] 半关闭语义 (END 只关闭输入)
- [x] 清理资源等待 (background.Wait)

✅ **资源管理**
- [x] Engine 关闭序列 (六阶段排空)
- [x] Server 停止支持强制中断 (Stop 打断 GracefulStop)
- [x] 配置隔离 (深复制嵌套 map)
- [x] 预算传递正确 (调用方预算 vs 清理预算)

✅ **批量与部分成功**
- [x] 跨块冲突合并 (F03)
- [x] Durable 子提交保留身份 (F02)
- [x] 部分成功错误类型 (PartialSubmissionError)

✅ **API 边界**
- [x] 自动化类型图检查 (递归检查泛型约束)
- [x] Import 隔离 (只有 backend 导入 Hatchet)
- [x] 公开 API 简洁 (wego.NewConn/NewServer)

✅ **测试覆盖**
- [x] 单元测试 (协议、配置隔离)
- [x] 质量测试 (API、注释、生成)
- [x] E2E 测试 (真实引擎、故障注入)
- [x] Race 测试 (并发安全)

---

## 附录B: 代码度量

| 度量项 | 数值 | 备注 |
|--------|------|------|
| 总代码行数 | ~9000 | backend + session + engine |
| 测试文件数 | 50+ | 包含单元/质量/E2E |
| 公开包数 | 9 | client, server, runtime, worker, task, model, middleware, telemetry, log |
| 内部包数 | 12 | clientcore, backend, engine, session, rpc, binding, wire, spec, ports, callctx, featurecore, telemetrycore |
| 验收场景数 | 30 | 对应 28 个官方源文件 74 个片段 |
| 门禁测试数 | 15 | format, unit, vet, race, engine, embedded, ... |

---

## 结论

Wego SDK 是一个设计良好、实现可靠的生产级代码库。经过多轮评审和修复，关键的并发、协议和资源管理问题已得到妥善处理。**建议进入生产试用阶段**，同时继续完善：

1. 握手超时的稳定性（P1）
2. 资源泄漏的保护措施（P2）
3. 端到端测试覆盖（P2）
4. 性能基准和文档（P3）

代码质量和工程实践方面，值得特别赞扬：
- 自动化 API 边界检查
- 详细的中文注释解释不变量
- 真实引擎的 E2E 测试
- 完整的验收覆盖矩阵

评审者签名: Claude (Anthropic)  
评审日期: 2025-01-09
