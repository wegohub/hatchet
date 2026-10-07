package scenarios

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// Idempotency 验证幂等冲突携带已有 RunID，以及状态和 TTL 两种释放策略。
func Idempotency(ctx context.Context, report *Report) (err error) {
	// 逐项处理 []bool{false, true}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, basedOnStatus := range []bool{false, true} {
		// ttl 幂等记录有效期，此处为一分钟；期内重复提交必须冲突，到期后可新建运行。
		ttl := time.Minute
		if basedOnStatus {
			ttl = 10 * time.Second
		}
		// started 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
		started := make(chan struct{}, 2)
		// release 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
		release := make(chan struct{})
		// h, e 启动实例或 span，并保留返回的完成通道或句柄供后续确认。
		h, e := Start(ctx, "idempotency", report, &Service{
			Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
				started <- struct{}{}
				// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
				select {
				case <-release:
					return Reply(ctx, in), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}, nil, worker.WithTask(pb.UnaryGreeter_SayHello_FullMethodName, task.WithIdempotencyTTL("input.routing.id", basedOnStatus, ttl)))
		if e != nil {
			return e
		}

		// triggered 读取本机时钟，用于耗时或超时判断，不参与 durable 重放。
		triggered := time.Now()
		// ref, e 提交任务并取得真实 RunID，后续通过句柄单独等待结果。
		ref, e := h.Conn.RunNoWait(ctx, pb.UnaryGreeter_SayHello_FullMethodName, &pb.Request{Id: h.Namespace + "key"})
		if e != nil {
			_ = h.Close()
			return e
		}

		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
		select {
		case <-started:
		case <-ctx.Done():
			_ = h.Close()
			return ctx.Err()
		}
		// _, collision 提交任务并取得真实 RunID，后续通过句柄单独等待结果。
		_, collision := h.Conn.RunNoWait(ctx, pb.UnaryGreeter_SayHello_FullMethodName, &pb.Request{Id: h.Namespace + "key"})
		// conflict errors.As 的幂等冲突目标，用于读取已存在运行身份。
		var conflict *model.IdempotencyCollisionError
		// assertion 验证冲突返回的已有 RunID，冲突与新运行不能混为同一成功结果。
		assertion := expect(
			errors.As(collision, &conflict) && conflict.ExistingRunID == ref.RunID,
			fmt.Sprintf("idempotency conflict carries existing RunID: %v; expected %s", collision, ref.RunID),
		)
		close(release)
		// result, e 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据。
		result, e := ref.Result(ctx)
		// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
		var out pb.Reply
		if e == nil {
			e = result.Into(&out)
		}
		if e == nil && assertion == nil {
			report.Add(h, fmt.Sprintf("idempotency status=%t", basedOnStatus), &out)
			e = waitStatus(ctx, h, ref.RunID, model.Completed)
			if e == nil && !basedOnStatus {
				_, collision = h.Conn.RunNoWait(ctx, pb.UnaryGreeter_SayHello_FullMethodName, &pb.Request{Id: h.Namespace + "key"})
				if !errors.As(collision, &conflict) || conflict.ExistingRunID != ref.RunID {
					e = fmt.Errorf("TTL key released before expiry: %v", collision)
				}
				if e == nil {
					// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
					select {
					case <-ctx.Done():
						e = ctx.Err()
					case <-time.After(time.Until(triggered.Add(ttl + 2*time.Second))):
					}
				}
			}
			if e == nil {
				// next, x 提交并等待业务结果，失败和取消不得作为有效输出继续使用。
				next, x := h.Conn.Run(ctx, pb.UnaryGreeter_SayHello_FullMethodName, &pb.Request{Id: h.Namespace + "key"})
				e = x
				if e == nil {
					// renewed 幂等 TTL 到期后重新提交得到的 protobuf 输出，确认创建了新的运行。
					var renewed pb.Reply
					e = next.Into(&renewed)
					if e == nil && renewed.RunId == out.RunId {
						e = fmt.Errorf("idempotency key did not create a new run after release")
					}
					if e == nil {
						report.Add(h, fmt.Sprintf("idempotency key released after completion=%t or TTL expiry", basedOnStatus), &renewed)
					}
				}
			}
		}
		// closeErr 释放当前对象拥有的资源，清理错误纳入当前验收结果。
		if closeErr := h.Close(); assertion != nil || e != nil || closeErr != nil {
			return errors.Join(assertion, e, closeErr)
		}
	}
	return nil
}
