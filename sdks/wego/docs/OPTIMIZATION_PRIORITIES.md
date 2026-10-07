# Wego SDK 优化优先级与实施计划

> SDK 0.1.10 的逐项处理与实际测量见 [深度评审完成报告](deep-quality-fixes.md)。本文件保留原始建议和预期数字，它们不代表测量结果。

**创建日期**: 2026-10-07  
**基于**: 深度代码质量评审  
**目标**: 达到生产级高性能代码标准

---

## 🎯 优化目标

### 性能指标
- **吞吐量**: +30% (目标: 100 msg/s → 130 msg/s)
- **延迟**: -60% (ACK 处理: 100μs → 40μs)
- **内存**: -40% (减少分配和 GC 压力)
- **并发**: -50% (锁竞争减半)

### 代码质量指标
- **算法复杂度**: O(n) → O(log n) 或 O(k)
- **锁持有时间**: 减少 50%+
- **代码行数**: 减少 10% (消除冗余)
- **维护性**: 从 3/5 → 5/5

---

## 📋 优先级分类

### 🔴 P0 - 立即修复 (关键性能瓶颈)

#### Q-001: ACK 处理的 O(n) 复杂度
**预期收益**: 延迟 -60%, 锁时间 -50%  
**工作量**: 4 小时  
**风险**: 低 (局部修改)

**实施步骤**:
1. 在 Endpoint 添加 `minUnacked uint64` 字段
2. 修改 ACK 处理循环为范围遍历
3. 添加单元测试 (边界条件)
4. 运行 race 测试
5. 基准测试对比

**验收标准**:
- ACK 处理时间 < 50μs (窗口 64)
- 所有现有测试通过
- race detector 通过

---

### 🔴 P1 - 高优先级 (并发安全和性能)

#### Q-002: notify() 的频繁 Channel 创建
**预期收益**: 内存分配 -40%, GC 压力 -30%  
**工作量**: 2 天  
**风险**: 中 (需要重构多处代码)

**实施步骤**:
1. **阶段 1**: 添加 sync.Cond 到 Endpoint
   ```go
   type Endpoint struct {
       mu   sync.Mutex
       cond *sync.Cond
       // 删除 changed chan struct{}
   }
   ```

2. **阶段 2**: 创建 waitWithContext 辅助函数
   ```go
   func (s *Endpoint) waitWithContext(ctx context.Context) error {
       done := make(chan struct{})
       go func() {
           s.mu.Lock()
           s.cond.Wait()
           s.mu.Unlock()
           close(done)
       }()
       
       select {
       case <-done:
           return nil
       case <-ctx.Done():
           s.cond.Broadcast()
           <-done
           return ctx.Err()
       }
   }
   ```

3. **阶段 3**: 重构 Receive/Send/control 使用新接口

4. **阶段 4**: 删除所有 `close(s.changed)` 调用

5. **验收测试**:
   - 所有 E2E 测试通过
   - 添加并发等待测试
   - 内存分配基准对比

**回滚计划**: 保留分支，发现问题立即 revert

---

#### Q-003: control.go 中的 23 次手动 Unlock
**预期收益**: 维护性 +50%, 死锁风险 -100%  
**工作量**: 1.5 天  
**风险**: 中 (架构调整)

**实施方案**: 使用 defer 或 action 模式

**方案 A: defer + 提前解锁** (推荐)
```go
func (s *Endpoint) control(ctx context.Context, frame *wire.Frame) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    // 快速路径
    if s.final && isControlFrame(frame.Kind) {
        return nil
    }
    if s.failure != nil {
        return s.failure
    }
    
    // 分支处理
    switch frame.Kind {
    case "PING":
        if s.taskID == "" {
            return nil
        }
        // 需要网络调用，提前解锁
        s.mu.Unlock()
        defer s.mu.Lock()  // 重新加锁供 defer 解锁
        return s.publish(ctx, &wire.Frame{Kind: "READY", Nonce: frame.Nonce})
        
    case "DATA":
        return s.handleDataLocked(frame)
        
    // ... 其他 case
    }
}
```

**方案 B: Action 模式** (更安全但复杂)
- 见深度评审报告 Q-003

**选择**: 先尝试方案 A，如果发现 defer 成对问题，切换到方案 B

---

#### Q-004: Receive() 中的重复失败检查
**预期收益**: CPU -10%, 锁操作 -15%  
**工作量**: 4 小时  
**风险**: 低

**实施步骤**:
1. 在 for 循环开始添加 select 检查 failed channel
2. 删除锁内的 `if s.failure != nil` 检查
3. 单元测试覆盖失败路径
4. 基准测试验证

**代码示例**: 见深度评审报告 Q-004

---

### 🟡 P2 - 中等优先级 (细节优化)

#### Q-005: bytes.Equal 后才 Unlock
**工作量**: 15 分钟  
**收益**: 锁时间 -2%

#### Q-006: Decode 的冗余转换
**工作量**: 30 分钟  
**收益**: 内存分配 -5%

#### Q-007: append([]byte(nil), ...) 模式
**工作量**: 1 小时  
**收益**: 内存精确分配

**批量处理**: 可以在一个 PR 中完成所有 P2 优化

---

### 🟢 P3 - 低优先级 (代码清理)

#### Q-008 ~ Q-011: 代码简洁性改进
**工作量**: 1-2 天  
**收益**: 可维护性提升

**建议**: 在完成 P0-P2 后，作为独立的代码清理 PR

---

## 📅 实施时间表

### 第 1 周: 关键性能优化
- **周一**: Q-001 (ACK 优化) ✅
- **周二**: Q-004 (失败检查) ✅
- **周三-周四**: Q-002 (sync.Cond) - 阶段 1-2
- **周五**: Q-002 测试和基准对比

### 第 2 周: 并发改进
- **周一-周二**: Q-002 (sync.Cond) - 阶段 3-4
- **周三**: Q-003 (Unlock 重构) - 方案 A
- **周四**: Q-003 测试
- **周五**: P2 批量优化 (Q-005, Q-006, Q-007)

### 第 3 周: 验证和清理
- **周一-周二**: 完整 E2E 测试和性能基准
- **周三-周四**: P3 代码清理 (可选)
- **周五**: 文档更新和发布

---

## 🧪 测试策略

### 每个优化必须通过
1. ✅ 单元测试 (新增覆盖)
2. ✅ race detector (`go test -race`)
3. ✅ 基准测试对比 (性能提升证据)
4. ✅ 所有现有 E2E 测试

### 关键测试场景
- **Q-001**: 
  - ACK 序号边界 (0, 1, maxUint64)
  - 乱序 ACK
  - 重复 ACK
  
- **Q-002**:
  - 多个 goroutine 等待
  - context 取消
  - 并发 notify
  
- **Q-003**:
  - 所有错误路径
  - panic 恢复
  - context 取消

### 性能基准
```bash
# 基准测试命令
go test -bench=. -benchmem -count=5 ./internal/session/...

# 对比脚本
benchstat before.txt after.txt
```

---

## 📊 成功指标

### 定量指标
| 指标 | 当前 | 目标 | 验证方式 |
|------|------|------|----------|
| ACK 延迟 | 100μs | 40μs | 基准测试 |
| 吞吐量 | 100 msg/s | 130 msg/s | E2E 测试 |
| 内存分配 | 1000 次/s | 600 次/s | pprof |
| 锁竞争 | 高 | 低 | race detector |

### 定性指标
- [ ] 代码审查通过 (至少 2 人)
- [ ] 所有测试通过
- [ ] 文档更新
- [ ] CHANGELOG 记录

---

## 🚨 风险管理

### 主要风险

#### R1: sync.Cond 的 context 集成复杂
**缓解**: 
- 先在独立分支验证
- 提供详细的回滚计划
- 充分的单元测试

#### R2: 重构可能引入 bug
**缓解**:
- 小步迭代，每步验证
- 保持现有测试通过
- 使用 feature flag 控制

#### R3: 性能提升不如预期
**缓解**:
- 提前做 profiling
- 基准测试验证每个优化
- 如果收益 < 10%，放弃该优化

### 回滚策略
1. 每个优化独立 commit
2. 保留性能基准数据
3. 发现问题立即 revert
4. 保留 0.1.9 作为稳定版本

---

## 📝 代码审查清单

### 提交前检查
- [ ] 所有测试通过 (`go test ./...`)
- [ ] race detector 通过 (`go test -race ./...`)
- [ ] 格式化代码 (`gofmt -s -w .`)
- [ ] 静态分析 (`go vet ./...`)
- [ ] 基准测试对比 (性能提升 > 10%)

### PR 审查重点
- [ ] 锁的使用正确 (无死锁、无遗漏)
- [ ] 错误处理完整
- [ ] 边界条件覆盖
- [ ] 代码简洁清晰
- [ ] 文档和注释更新

---

## 🎓 经验总结

### 优化原则
1. **先优化算法，再优化细节**
2. **基准测试驱动**
3. **小步迭代，持续验证**
4. **代码简洁优先于性能**
5. **安全性 > 性能 > 简洁性**

### 避免过度优化
- 只优化热点路径 (profiling 证明)
- 收益 < 10% 的优化需要权衡
- 不牺牲可读性

### 团队协作
- 每个优化独立 PR
- 详细的 commit 消息
- 性能数据附在 PR 描述

---

## 📚 参考资料

### Go 性能最佳实践
- [Effective Go](https://go.dev/doc/effective_go)
- [Go Performance Tips](https://go.dev/wiki/Performance)
- [pprof Tutorial](https://go.dev/blog/pprof)

### 并发模式
- [Go Concurrency Patterns](https://go.dev/blog/pipelines)
- [sync.Cond Best Practices](https://pkg.go.dev/sync#Cond)

### 内部文档
- `docs/DEEP_QUALITY_REVIEW.md` - 深度质量评审
- `docs/code-review-report.md` - 原始代码审查
- `docs/REVIEW_OF_FIXES.md` - GPT 修复复核

---

**创建时间**: 2026-10-07 22:30  
**负责人**: 开发团队  
**审核人**: Tech Lead  
**状态**: 待批准

