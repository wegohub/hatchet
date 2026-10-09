package backend

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	clientconfig "github.com/hatchet-dev/hatchet/pkg/config/client"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/spec"
)

// workflowDefinition 编码纯调度数据；例如 standalone hello 只生成 hello:hello 一个 action
// 不构造官方 Workflow 对象，避免定义阶段创建不可关闭的 metadata 缓存
func workflowDefinition(name string, policy spec.Task, namespace string) (*v1.CreateWorkflowVersionRequest, error) {
	name = clientconfig.ApplyNamespace(strings.ToLower(name), &namespace)
	// req 当前协议请求，编码完整后才发送，不能使用未初始化的调度字段
	req := &v1.CreateWorkflowVersionRequest{Name: name, CronTriggers: append([]string(nil), policy.Cron...)}
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
	for _, key := range policy.Events {
		req.EventTriggers = append(req.EventTriggers, clientconfig.ApplyNamespace(key, &namespace))
	}
	if policy.CronInput != nil {
		// data 编码后的业务或协议字节，只有编码成功才可交付
		data, err := json.Marshal(policy.CronInput)
		if err != nil {
			return nil, err
		}
		// value 当前协议字段的独立值，指针不能借用调用方可变容器
		value := string(data)
		req.CronInput = &value
	}
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
	for _, c := range policy.Concurrency {
		// value 当前协议字段的独立值，指针不能借用调用方可变容器
		value := &v1.Concurrency{Expression: c.Expression, MaxRuns: c.MaxRuns, MaxRunsExpression: c.MaxRunsExpression}
		if c.Name != "" {
			value.Name = &c.Name
		}
		if c.IsTenantScoped {
			// yes 取得 true 的结果，确认成功后才进入下一处理阶段
			yes := true
			value.IsTenantScoped = &yes
		}
		if c.LimitStrategy != nil {
			// n, ok 取得 v1.ConcurrencyLimitStrategy_value[string 的结果，确认成功后才进入下一处理阶段
			n, ok := v1.ConcurrencyLimitStrategy_value[string(*c.LimitStrategy)]
			if !ok {
				return nil, fmt.Errorf("wego: invalid concurrency strategy")
			}
			// strategy 取得 v1.ConcurrencyLimitStrategy 的结果，确认成功后才进入下一处理阶段
			strategy := v1.ConcurrencyLimitStrategy(n)
			value.LimitStrategy = &strategy
		}
		req.ConcurrencyArr = append(req.ConcurrencyArr, value)
	}
	if policy.Sticky.Set {
		// value 当前协议字段的独立值，指针不能借用调用方可变容器
		value := v1.StickyStrategy(policy.Sticky.Value)
		req.Sticky = &value
	}
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
	for _, filter := range policy.Filters {
		// data 编码后的业务或协议字节，只有编码成功才可交付
		data, err := json.Marshal(filter.Payload)
		if err != nil {
			return nil, err
		}
		req.DefaultFilters = append(req.DefaultFilters, &v1.DefaultFilter{Expression: filter.Expression, Scope: filter.Scope, Payload: data})
	}
	if policy.IdempotencyExpression != "" {
		// method 取得 v1.IdempotencyMethod_TTL 的结果，确认成功后才进入下一处理阶段
		method := v1.IdempotencyMethod_TTL
		if policy.IdempotencyStatus {
			method = v1.IdempotencyMethod_STATUS
		}
		req.Idempotency = &v1.IdempotencyConfig{Expression: policy.IdempotencyExpression, TtlMs: policy.IdempotencyTTL.Milliseconds(), Method: &method}
	}
	return req, nil
}

// taskDefinition 编码任务策略；任务名保留为结果 key，action 全名统一小写
func taskDefinition(workflow string, d ports.Definition) (*v1.CreateTaskOpts, error) {
	// p 取得 d.Policy 的结果，确认成功后才进入下一处理阶段
	p := d.Policy
	// name 取得 d.Name 的结果，确认成功后才进入下一处理阶段
	name := d.Name
	if d.OnFailure {
		name = "on-failure"
	}
	// task 协议任务定义，action 与 readable_id 各自保持调度和结果语义
	task := &v1.CreateTaskOpts{ReadableId: name, Action: strings.ToLower(workflow + ":" + name), Parents: append([]string(nil), d.Parents...), IsDurable: p.Durable}
	// slot 容量类型，普通任务使用 default，durable 使用 durable
	slot, units := "default", int32(1)
	if p.Durable {
		slot = "durable"
	} else if p.SlotCost.Set {
		units = int32(p.SlotCost.Value)
	}
	task.SlotRequests = map[string]int32{slot: units}
	if p.Retries.Set {
		task.Retries = int32(p.Retries.Value)
	}
	if p.ExecutionTimeout.Set {
		task.Timeout = durationSeconds(p.ExecutionTimeout.Value)
	}
	if p.ScheduleTimeout.Set {
		// value 当前协议字段的独立值，指针不能借用调用方可变容器
		value := durationSeconds(p.ScheduleTimeout.Value)
		task.ScheduleTimeout = &value
	}
	if p.BackoffFactor.Set {
		// value 当前协议字段的独立值，指针不能借用调用方可变容器
		value := p.BackoffFactor.Value
		task.BackoffFactor = &value
	}
	if p.BackoffMax.Set {
		// value 当前协议字段的独立值，指针不能借用调用方可变容器
		value := int32(p.BackoffMax.Value.Seconds())
		task.BackoffMaxSeconds = &value
	}
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
	for _, limit := range p.RateLimits {
		// value 当前协议字段的独立值，指针不能借用调用方可变容器
		value := &v1.CreateTaskRateLimit{Key: limit.Key, KeyExpr: limit.KeyExpr, UnitsExpr: limit.UnitsExpr, LimitValuesExpr: limit.LimitValueExpr}
		if limit.Units != nil {
			// units 本次执行的容量成本，例如容量 4、成本 2 最多同时执行两项
			units := int32(*limit.Units)
			value.Units = &units
		}
		if limit.Duration != nil {
			// n, ok 取得 v1.RateLimitDuration_value[strings.ToUpper 的结果，确认成功后才进入下一处理阶段
			n, ok := v1.RateLimitDuration_value[strings.ToUpper(string(*limit.Duration))]
			if !ok {
				return nil, fmt.Errorf("wego: invalid rate limit duration")
			}
			// duration 取得 v1.RateLimitDuration 的结果，确认成功后才进入下一处理阶段
			duration := v1.RateLimitDuration(n)
			value.Duration = &duration
		}
		task.RateLimits = append(task.RateLimits, value)
	}
	if p.Batch != nil {
		// batch 取得 p.Batch 的结果，确认成功后才进入下一处理阶段
		batch := p.Batch
		task.Retries = 0
		task.Batch = &v1.TaskBatchConfig{BatchMaxSize: batch.MaxSize, BatchGroupKey: batch.GroupKey, BatchGroupMaxRuns: batch.GroupMaxRuns}
		if batch.MaxInterval != nil {
			// ms 取得 int32 的结果，确认成功后才进入下一处理阶段
			ms := int32(batch.MaxInterval.Milliseconds())
			task.Batch.BatchMaxIntervalMs = &ms
		}
		if batch.BroadcastOutput {
			// yes 取得 true 的结果，确认成功后才进入下一处理阶段
			yes := true
			task.Batch.BroadcastOutput = &yes
		}
	}
	return task, nil
}

// durationSeconds 保持引擎支持的秒单位；例如 1500ms 的策略编码为 1s
func durationSeconds(d time.Duration) string { return fmt.Sprintf("%ds", int64(d.Seconds())) }
