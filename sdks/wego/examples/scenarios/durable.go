package scenarios

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/option"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// DurableEvents 验证真实事件等待、scope 隔离与 lookback 每次使用唯一 namespace，成功断言写入报告后清理可删除资源
func DurableEvents(ctx context.Context, report *Report) (err error) {
	// started 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
	started := make(chan string, 8)
	// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份
	service := &Service{
		Wait: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// now, err 接收 task.Now 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
			now, err := task.Now(ctx)
			if err != nil {
				return nil, err
			}

			// opts 当前入口使用的原生 gRPC 选项
			opts := model.EventWait{}
			// expression 当前步骤使用的字符串值 ""，用于路由、请求或断言
			expression := ""
			if in.Message == "filter" {
				expression = "input.user_id == 'target'"
			}
			if in.Message == "lookback" {
				opts.Scope = pointer("user:target")
				opts.ConsiderEventsSince = pointer(now.Add(-time.Minute))
			}
			started <- in.Message
			// event, err 接收 task.WaitForEvent 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
			event, err := task.WaitForEvent(ctx, "durable:update", expression, opts)
			if err != nil {
				return nil, err
			}

			// out 带当前任务身份的 protobuf 响应，业务字段与运行身份一起返回供断言
			out := Reply(ctx, in)
			out.Message = fmt.Sprint(event["message"])
			out.ProcessedAt = now.Format(time.RFC3339Nano)
			return out, nil
		},
	}
	// h, err 接收 worker.WithDurableTask 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	h, err := Start(ctx, "durable-event", report, service, nil, worker.WithDurableTask(pb.UnaryGreeter_WaitHello_FullMethodName))
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// 逐项处理 []string{"basic", "filter", "lookback"}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, variant := range []string{"basic", "filter", "lookback"} {
		if variant == "lookback" {
			// scope 当前步骤使用的字符串值 "user:target"，用于路由、请求或断言
			scope := "user:target"
			if err = h.Conn.Events().Push(ctx, "durable:update", map[string]any{"message": "lookback"}, &scope); err != nil {
				return err
			}
		}
		// ref, e 提交任务并取得真实 RunID，后续通过句柄单独等待结果
		ref, e := h.Conn.RunNoWait(ctx, pb.UnaryGreeter_WaitHello_FullMethodName, routedInput(&pb.Request{Message: variant}))
		if e != nil {
			return e
		}

		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		case <-started:
		case <-ctx.Done():
			return ctx.Err()
		}
		if variant != "lookback" {
			if variant == "filter" {
				if err = h.Conn.Events().Push(ctx, "durable:update", map[string]any{"user_id": "unmatched", "message": "wrong"}); err != nil {
					return err
				}
			}
			if err = pushUntilCompleted(ctx, h, ref, "durable:update", map[string]any{"user_id": "target", "message": variant}, nil); err != nil {
				return err
			}
		}
		// result, e 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据
		result, e := ref.Result(ctx)
		if e != nil {
			return e
		}

		// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
		var out pb.Reply
		if e = result.Into(&out); e != nil {
			return e
		}
		if out.Message != variant || out.ProcessedAt == "" {
			return fmt.Errorf("durable event %s: %v", variant, &out)
		}

		report.Add(h, "durable event "+variant+" and Now", &out)
	}
	return nil
}

// pushUntilCompleted 在 deadline 内重发受控事件直到任务完成，避免监听建立竞态
func pushUntilCompleted(ctx context.Context, h *Harness, ref *client.RunRef, key string, payload any, scope *string) error {
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		// err 接收 h.Conn.Events 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
		if err := h.Conn.Events().Push(ctx, key, payload, scope); err != nil {
			return err
		}

		// value, err 接收 h.Conn.Runs 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		value, err := h.Conn.Runs().GetStatus(ctx, ref.RunID)
		if err != nil && !strings.Contains(err.Error(), "status 404") {
			return err
		}
		if value == model.Completed {
			return nil
		}
		if value == model.Failed || value == model.Cancelled {
			return fmt.Errorf("durable event run reached %s", value)
		}

		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// hasEviction 从真实运行详情判断是否发生驱逐
func hasEviction(value any) bool {
	// v 保存类型分派后的准确值，各分支只按该类型读取字段或调用方法
	switch v := value.(type) {
	case map[string]any:
		// 逐项处理 v，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for k, item := range v {
			if (k == "isEvicted" || k == "IsEvicted" || k == "is_evicted") && item == true {
				return true
			}
			if hasEviction(item) {
				return true
			}
		}
	case []any:
		// 逐项处理 v，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for _, item := range v {
			if hasEviction(item) {
				return true
			}
		}
	case model.Resource:
		return hasEviction(map[string]any(v))
	}
	return false
}

// waitEvicted 轮询运行直到观察到驱逐或预算到期
func waitEvicted(ctx context.Context, h *Harness, id string) error {
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		// details, err 接收 h.Conn.Runs 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		details, err := h.Conn.Runs().GetDetails(ctx, id)
		if err != nil && !strings.Contains(err.Error(), "status 404") {
			return err
		}
		if hasEviction(details) {
			return nil
		}

		// state, err 接收 h.Conn.Runs 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
		state, err := h.Conn.Runs().GetStatus(ctx, id)
		if err != nil && !strings.Contains(err.Error(), "status 404") {
			return err
		}
		if state == model.Completed || state == model.Failed || state == model.Cancelled {
			return fmt.Errorf("run %s reached %s without observed eviction: %v", id, state, details)
		}

		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Eviction 验证 durable TTL 与容量驱逐，并检查禁止驱逐的配置 每次使用唯一 namespace，成功断言写入报告后清理可删除资源
func Eviction(ctx context.Context, report *Report) (err error) {
	// 逐项处理 []string{ "ttl-sleep", "ttl-event", "disabled", "capacity", }，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, variant := range []string{
		"ttl-sleep",
		"ttl-event",
		"disabled",
		"capacity",
	} {
		// started 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序
		started := make(chan *pb.Reply, 8)
		// policy 明确开启一秒 TTL 与容量驱逐，重放后必须复用原等待记录
		policy := model.EvictionPolicy{TTL: option.Some(time.Second), AllowCapacityEviction: option.Some(true)}
		if variant == "disabled" {
			policy.TTL = option.Some(time.Duration(0))
			policy.AllowCapacityEviction = option.Some(false)
		}
		if variant == "capacity" {
			policy.TTL = option.Some(time.Duration(0))
		}
		// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份
		service := &Service{
			Wait: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
				// out 带当前任务身份的 protobuf 响应，业务字段与运行身份一起返回供断言
				out := Reply(ctx, in)
				started <- out
				// now, e 取得 durable 可重放的时间，重新执行不会生成新的业务时间
				now, e := task.Now(ctx)
				if e != nil {
					return nil, e
				}
				if variant == "ttl-event" {
					_, e = task.WaitForEvent(ctx, "eviction:event", "true", model.EventWait{ConsiderEventsSince: pointer(now)})
				} else {
					e = task.Sleep(ctx, time.Duration(in.DelayMillis)*time.Millisecond)
				}
				if e != nil {
					return nil, e
				}

				out.ProcessedAt = now.Format(time.RFC3339Nano)
				return out, nil
			},
		}
		// h, e 启动实例或 span，并保留返回的完成通道或句柄供后续确认
		h, e := Start(
			ctx,
			"eviction",
			report,
			service,
			[]runtime.Option{runtime.WithDurableSlots(1)},
			worker.WithDurableTask(pb.UnaryGreeter_WaitHello_FullMethodName, task.WithExecutionTimeout(30*time.Second), task.WithEviction(policy)),
		)
		if e != nil {
			return e
		}

		// delay 受控 durable 等待时长，此处为 6000 毫秒，用于给驱逐和重放提供可观察窗口
		delay := int32(6000)
		if variant == "capacity" {
			delay = 16000
		}
		// ref, e 提交任务并取得真实 RunID，后续通过句柄单独等待结果
		ref, e := h.Conn.RunNoWait(ctx, pb.UnaryGreeter_WaitHello_FullMethodName, routedInput(&pb.Request{DelayMillis: delay}))
		if e != nil {
			_ = h.Close()
			return e
		}

		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		case <-started:
		case <-ctx.Done():
			_ = h.Close()
			return ctx.Err()
		}
		// assertion 当前 handler 内部的业务断言错误，执行成功但断言失败时仍必须让场景失败
		var assertion error
		if variant == "capacity" {
			// other, e 提交任务并取得真实 RunID，后续通过句柄单独等待结果
			other, e := h.Conn.RunNoWait(ctx, pb.UnaryGreeter_WaitHello_FullMethodName, routedInput(&pb.Request{DelayMillis: 100}))
			if e != nil {
				assertion = e
			} else {
				assertion = waitEvicted(ctx, h, ref.RunID)
				if assertion == nil {
					_, assertion = other.Result(ctx)
				}
			}
		} else if variant != "disabled" {
			assertion = waitEvicted(ctx, h, ref.RunID)
		}
		if variant == "ttl-event" && assertion == nil {
			assertion = pushUntilCompleted(ctx, h, ref, "eviction:event", map[string]any{}, nil)
		}
		if assertion == nil {
			// result, e 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据
			result, e := ref.Result(ctx)
			if e != nil {
				assertion = e
			} else {
				// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
				var out pb.Reply
				assertion = result.Into(&out)
				if assertion == nil && variant != "disabled" && out.InvocationCount < 2 {
					assertion = fmt.Errorf("eviction did not replay: %v", &out)
				}
				if assertion == nil && variant == "disabled" && out.InvocationCount != 1 {
					assertion = fmt.Errorf("disabled eviction replayed: %v", &out)
				}
				if assertion == nil {
					report.Add(h, "durable "+variant+" resumed without resetting wait", &out)
				}
			}
		}
		// closeErr 释放当前对象拥有的资源，清理错误纳入当前验收结果
		if closeErr := h.Close(); assertion != nil || closeErr != nil {
			return errors.Join(assertion, closeErr)
		}
	}
	return nil
}
