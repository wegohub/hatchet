package scenarios

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	wlog "github.com/hatchet-dev/hatchet/sdks/wego/log"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// Logs 验证任务日志完成上报后可按运行和时间查询
func Logs(ctx context.Context, report *Report) (err error) {
	// since 读取本机时钟，用于耗时或超时判断，不参与 durable 重放
	since := time.Now().UTC().Add(-time.Second)
	// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份
	service := &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// 同一请求的组件字段供本地打印和任务上报共用，输入保持结构化而非拼接 message。
			ctx = wlog.With(ctx, slog.String("component", "log-example"))
			// err 接收 wlog.RInfo 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
			if err := wlog.RInfo(ctx, "Starting task execution"); err != nil {
				return nil, err
			}
			// err 接收 wlog.RInfo 的结果；按当前分支校验错误或有效性，确认成功后才继续本逻辑块
			if err := wlog.RInfo(ctx, "Logging input", "input", in.Message); err != nil {
				return nil, err
			}
			// 首次执行报告两条日志后失败；下一次执行保留原运行身份并报告 retry=1。
			info, _ := task.Info(ctx)
			if info.RetryCount == 0 {
				return nil, errors.New("intentional log retry")
			}

			return Reply(ctx, in), nil
		},
	}
	// h, err 接收 runtime.WithLogReport 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	h, err := Start(ctx, "logs", report, service, []runtime.Option{runtime.WithLogReport(true)},
		worker.WithTask(pb.UnaryGreeter_SayHello_FullMethodName, task.WithRetries(1)))
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// out, err 接收 h.RPC.SayHello 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	out, err := h.RPC.SayHello(ctx, &pb.Request{Message: "fixture"})
	if err != nil {
		return err
	}
	if out.RetryCount != 1 {
		return fmt.Errorf("wego: log fixture did not retry once")
	}
	completed := time.Now().UTC()

	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果
	for {
		// logs, e 查询引擎已确认的任务日志，不能仅断言本地日志回调发生
		logs, e := h.Conn.Logs().List(ctx, out.RunId, model.Query{"since": since})
		if e != nil {
			return e
		}

		// rows, _ 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值
		rows, _ := logs["rows"].([]any)
		if len(rows) >= 4 {
			// 引擎查询必须保留原始 message、共享属性、请求属性及真实执行身份。
			found := map[int]bool{}
			for _, value := range rows {
				row, _ := value.(map[string]any)
				if row["message"] != "Logging input" {
					continue
				}
				metadata, _ := row["metadata"].(map[string]any)
				if metadata["input"] != "fixture" || metadata["component"] != "log-example" || metadata["run_id"] != out.RunId {
					return fmt.Errorf("wego: reported structured log does not match request")
				}
				// 两次日志的 retry、级别及原始时间均从引擎读模型核验，而非本地回调。
				retry, ok := row["retryCount"].(float64)
				text, _ := row["createdAt"].(string)
				created, err := time.Parse(time.RFC3339Nano, text)
				if !ok || retry != 0 && retry != 1 || metadata["retry_count"] != retry || row["level"] != "INFO" || err != nil || created.Before(since) || created.After(completed) {
					return fmt.Errorf("wego: reported log time, level or retry is invalid")
				}
				found[int(retry)] = true
			}
			if !found[0] || !found[1] {
				return fmt.Errorf("wego: expected structured log message is missing")
			}
			report.Add(h, "reported logs and since query", out)
			return nil
		}

		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
