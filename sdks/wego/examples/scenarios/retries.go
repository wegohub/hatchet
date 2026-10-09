package scenarios

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// Retries 验证重试次数、退避和不可重试错误，断言真实执行尝试
func Retries(ctx context.Context, report *Report) (err error) {
	// 逐项处理 []string{ "always-fails", "count", "backoff", "nonretryable", }，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, variant := range []string{
		"always-fails",
		"count",
		"backoff",
		"nonretryable",
	} {
		// mu 保护所属对象的可变状态；读取和写入使用同一把锁
		var mu sync.Mutex
		// attempts 实际尝试的开始时间列表，次数验证重试策略，时间差验证退避
		attempts := []time.Time{}
		// service 是注入重试行为的业务实现，失败次数与最终 RetryCount 共同作为验收断言
		service := &Service{
			Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
				mu.Lock()
				attempts = append(attempts, time.Now())
				mu.Unlock()
				// info, _ 读取实际执行身份，例如 RunID 与 WorkerID；普通网络 context 没有任务身份
				info, _ := task.Info(ctx)
				// failure 构造或读取标准 gRPC 状态，保留 code 与业务 details
				failure := status.Error(codes.Aborted, "intentional failure")
				if variant == "nonretryable" {
					return nil, task.NonRetryable(failure)
				}
				if variant != "count" || info.RetryCount < 2 {
					return nil, failure
				}

				return Reply(ctx, in), nil
			},
		}
		// opts 明确列出本段注册、传输或清理选项，应用顺序决定重复字段的覆盖关系
		opts := []task.Option{task.WithRetries(3)}
		if variant == "backoff" {
			opts = append(opts, task.WithRetryBackoff(2, 2*time.Second))
		}
		// h, e 启动实例或 span，并保留返回的完成通道或句柄供后续确认
		h, e := Start(ctx, "retries", report, service, nil, worker.WithTask(pb.UnaryGreeter_SayHello_FullMethodName, opts...))
		if e != nil {
			return e
		}

		// out, e 通过标准生成桩执行 unary，请求和响应使用 protobuf 载荷
		out, e := h.RPC.SayHello(ctx, &pb.Request{})
		mu.Lock()
		// count 此步骤已经观察到的执行或消息次数，后续与预期重试数、输入数或峰值比较
		count := len(attempts)
		// elapsed 最后一次与第一次尝试的间隔，用于断言重试没有绕过退避等待
		elapsed := attempts[len(attempts)-1].Sub(attempts[0])
		mu.Unlock()
		// want 当前步骤的初始计数 4，后续根据实际执行或数据量更新
		want := 4
		if variant == "count" {
			want = 3
		}
		if variant == "nonretryable" {
			want = 1
		}
		// assertion 当前 handler 内部的业务断言错误，执行成功但断言失败时仍必须让场景失败
		var assertion error
		if count != want {
			assertion = fmt.Errorf("%s attempts %d != %d", variant, count, want)
		} else if variant == "count" && (e != nil || out.RetryCount != 2) {
			assertion = fmt.Errorf("retry count: %v", e)
		} else if variant != "count" && status.Code(e) != codes.Aborted {
			assertion = fmt.Errorf("failure status: %v", e)
		} else if variant == "backoff" && elapsed < time.Second {
			assertion = fmt.Errorf("backoff not observed: %s", elapsed)
		}
		if assertion == nil {
			report.Add(h, variant+" attempts and policy", out)
		}
		// closeErr 释放当前对象拥有的资源，清理错误纳入当前验收结果
		if closeErr := h.Close(); assertion != nil || closeErr != nil {
			return errors.Join(assertion, closeErr)
		}
	}
	return nil
}
