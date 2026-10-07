package scenarios

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// Events 验证 Event 用 RPCInput 触发 protobuf handler。
func Events(ctx context.Context, report *Report) (err error) {
	// observed 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	observed := make(chan *pb.Reply, 2)
	// service 本场景的受控业务服务，handler 注入明确行为并记录实际调用结果。
	service := &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// out 带当前任务身份的 protobuf 响应，业务字段与运行身份一起返回供断言。
			out := Reply(ctx, in)
			observed <- out
			return out, nil
		},
	}
	// h, err 接收 worker.WithTask 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	h, err := Start(ctx, "events", report, service, nil, worker.WithTask(pb.UnaryGreeter_SayHello_FullMethodName, task.WithEvents("event:create")))
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	if err = h.Conn.Events().Push(ctx, "event:create", model.RPCInput{
		Method:  pb.UnaryGreeter_SayHello_FullMethodName,
		Message: &pb.Request{Message: "event"},
	}); err != nil {
		return err
	}

	// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
	select {
	// out 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功。
	case out := <-observed:
		if out.Message != "event" {
			return fmt.Errorf("event payload")
		}
		// e 等待只读运行记录可见；handler 通知先于 OLAP 复制完成，首次 404 不能证明没有运行。
		if e := awaitVisibleEventRun(ctx, func(ctx context.Context) error {
			// result、lookupErr 通过真实 RunID 查询，非 404 错误和空成功响应必须立即失败。
			result, lookupErr := h.Conn.Runs().Get(ctx, out.RunId)
			if lookupErr != nil {
				return lookupErr
			}
			if result == nil {
				return errors.New("event run lookup returned empty result")
			}
			return nil
		}); e != nil {
			return fmt.Errorf("event run lookup: %w", e)
		}
		report.Add(h, "event triggers protobuf RPC and result lookup", out)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// awaitVisibleEventRun 只重试异步 OLAP 尚未复制产生的 404，其他错误立即返回；整体预算由场景 ctx 约束。
// 例如第一次查询 404、100ms 后可见即成功；服务 500、空响应或取消不能被解释为暂时不可见。
func awaitVisibleEventRun(ctx context.Context, lookup func(context.Context) error) error {
	// timer 首次立即查询，后续从查询完成再计时，慢查询不会积累过期 tick。
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		// 取消在 select 的两个分支同时就绪时仍优先于发起新的查询。
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// err 保存当前查询结果；现有管理入口使用规范化的 HTTP 状态文本，匹配仅限 404。
		err := lookup(ctx)
		if err == nil || !strings.Contains(err.Error(), "status 404") {
			return err
		}
		timer.Reset(100 * time.Millisecond)
	}
}
