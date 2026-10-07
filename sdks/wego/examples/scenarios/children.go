package scenarios

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
)

// Children 验证顺序与并发子调用，以及借用连接和父子运行身份。
func Children(ctx context.Context, report *Report) (err error) {
	// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份。
	service := &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// 检查 in.Fail；不满足协议或配置约束时返回 FailedPrecondition（child failed）。
			if in.Fail {
				return nil, status.Error(codes.FailedPrecondition, "child failed")
			}

			// out 带当前任务身份的 protobuf 响应，业务字段与运行身份一起返回供断言。
			out := Reply(ctx, in)
			out.Count = in.Count * 2
			return out, nil
		},
	}
	service.Child = func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
		// conn, err 接收 task.Client 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
		conn, err := task.Client(ctx)
		if err != nil {
			return nil, err
		}
		if !errors.Is(conn.Close(), model.ErrBorrowedResource) {
			return nil, fmt.Errorf("borrowed ownership")
		}

		// c 构造 unary 生成桩，使用 wego Conn 时通过任务引擎执行。
		c := pb.NewUnaryGreeterClient(conn)
		// out 带当前任务身份的 protobuf 响应，业务字段与运行身份一起返回供断言。
		out := Reply(ctx, in)
		out.Count = 0
		if in.Fail {
			_, err = c.SayHello(task.WithChildKey(ctx, "failure"), in)
			return nil, err
		}

		// values 分配当前步骤的结果列表，容量只用于聚合，不代表业务已经成功完成。
		values := make([]*pb.Reply, in.Count)
		// errs 分配当前步骤的结果列表，容量只用于聚合，不代表业务已经成功完成。
		errs := make([]error, in.Count)
		// wg 等待本组并发步骤结束，资源关闭前必须确认所有已启动步骤退出。
		var wg sync.WaitGroup
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
		for i := int32(0); i < in.Count; i++ {
			// call 为第 i 个子调用设置稳定 key，并分别记录结果和错误；并发不改写父上下文。
			call := func(i int32) {
				values[i], errs[i] = c.SayHello(task.WithChildKey(ctx, fmt.Sprintf("item-%d", i)), &pb.Request{Count: i + 1})
			}
			if in.Message == "parallel" {
				wg.Add(1)
				go func(i int32) {
					defer wg.Done()

					call(i)
				}(i)
			} else {
				call(i)
			}
		}
		wg.Wait()
		if err = errors.Join(errs...); err != nil {
			return nil, err
		}

		// 逐项处理 values，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
		for _, value := range values {
			if value.ParentRunId != out.RunId {
				return nil, fmt.Errorf("parent identity")
			}

			out.Count += value.Count
		}
		out.ChildRunId = values[0].RunId
		return out, nil
	}
	// h, err 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值。
	h, err := Start(ctx, "children", report, service, nil)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// 逐项处理 []string{"sequential", "parallel"}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, mode := range []string{"sequential", "parallel"} {
		// out, e 调用子任务并保留父子运行关系，后续断言使用真实执行身份。
		out, e := h.RPC.ChildHello(ctx, &pb.Request{Message: mode, Count: 3})
		if e != nil {
			return e
		}
		if out.Count != 12 || out.ChildRunId == "" {
			return fmt.Errorf("child aggregation")
		}

		report.Add(h, mode+" children preserve identity", out)
	}
	_, err = h.RPC.ChildHello(ctx, &pb.Request{Fail: true})
	if status.Code(err) != codes.FailedPrecondition {
		return fmt.Errorf("child error: %v", err)
	}

	report.Add(h, "child error propagation", nil)
	return nil
}
