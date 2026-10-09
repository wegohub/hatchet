package backend

import (
	"context"
	"errors"
	"time"

	dispatcherpb "github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// Run 使用调用方预算提交官方协议请求，句柄取得后不再依赖提交 context
func (b *Backend) Run(ctx context.Context, name string, input any, opts model.RunOptions) (ports.Run, error) {
	// refs 已由引擎确认接受的运行句柄，后续分块失败不能清空它们
	refs, err := b.submit(ctx, name, []model.RunManyInput{{Input: input, Options: opts}}, false)
	if err != nil {
		return ports.Run{}, err
	}
	if len(refs) != 1 {
		return ports.Run{}, status.Error(codes.DataLoss, "wego: missing RunID")
	}
	return refs[0], nil
}

// RunMany 保留每个已确认分块的 RunID；例如第二块取消时第一块的 1000 个身份仍可追踪
func (b *Backend) RunMany(ctx context.Context, name string, inputs []model.RunManyInput) ([]ports.Run, error) {
	return b.submit(ctx, name, inputs, true)
}

// runRef 每次 Wait 建立独立订阅，单个等待的取消不会终止其他运行
func (b *Backend) runRef(id string) ports.Run {
	return ports.Run{
		ID: id,
		Wait: func(ctx context.Context) (ports.Result, error) {
			return b.await(ctx, id, func(ctx context.Context) (ports.Result, error) { return b.waitRun(ctx, id) })
		},
	}
}

// waitRun 从独立订阅读取终态；Send 与 Recv 使用同一调用预算，取消即可释放等待
func (b *Backend) waitRun(ctx context.Context, id string) (ports.Result, error) {
	// stream 由当前结果等待独占，不初始化官方的共享结果监听器
	stream, err := b.rpcDispatcher.SubscribeToWorkflowRuns(b.auth(ctx))
	if err != nil {
		return ports.Result{}, &runObservationError{Normalize(err)}
	}
	defer stream.CloseSend()
	// 发送也必须受请求预算约束，不能在进入取消 select 之前无限等待连接流控
	if err := stream.Send(&dispatcherpb.SubscribeToWorkflowRunsRequest{WorkflowRunId: id}); err != nil {
		return ports.Result{}, &runObservationError{Normalize(err)}
	}
	for {
		// event 是此订阅的终态；只有对应 RunID 的结果可以交付业务
		event, err := stream.Recv()
		if err != nil {
			return ports.Result{}, &runObservationError{Normalize(err)}
		}
		if event.WorkflowRunId != id {
			continue
		}
		// 订阅通知可能属于旧执行；只用它唤醒持久终态查询，不能直接交付其结果
		result, done, err := b.persistedResult(ctx, id)
		if err != nil {
			return ports.Result{}, err
		}
		if done {
			return result, nil
		}
	}
}

// await 独立执行辅助状态轮询；HTTP 请求不能占住接收成功结果的 select
func (b *Backend) await(ctx context.Context, id string, wait func(context.Context) (ports.Result, error)) (ports.Result, error) {
	// ctx 当前操作预算，用于传输取消；不得替换为无预算的 Background
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// response 将业务结果及错误一并交付；缓冲使调用方取消后发送者也能退出
	type response struct {
		// result 当前完成结果，只有成功确认后才交付业务
		result ports.Result
		// err 当前操作错误，失败时不继续使用对应结果
		err error
	}
	// resultCh 有缓冲的结果交付通道，取消后发送方仍可退出
	resultCh := make(chan response, 1)
	// statusCh 辅助轮询的终止通知，不能阻塞业务结果交付
	statusCh := make(chan error, 1)
	// done 本次 goroutine 的退出确认，关闭函数返回前需完成资源回收
	done := make(chan struct{})
	go func() {
		defer close(done)
		// result/err 成对交付；缓冲通道保证取消后不会阻塞发送者退出
		delay := 100 * time.Millisecond
		for {
			result, err := wait(ctx)
			// observation 只标识订阅/查询故障；最终业务错误不进入重连循环
			var observation *runObservationError
			if errors.As(err, &observation) && ctx.Err() == nil && retryObservation(observation.err) {
				if waitObservationRetry(ctx, delay) == nil {
					delay = min(delay*2, 2*time.Second)
					continue
				}
			}
			select {
			case resultCh <- response{result, err}:
			case <-ctx.Done():
			}
			return
		}
	}()
	// pollingDone 确认辅助请求在结果交付后随 ctx 取消，不遗留 HTTP goroutine
	pollingDone := make(chan struct{})
	go func() {
		defer close(pollingDone)
		// interval 在查询完成后再计时，慢 HTTP 不会累积 ticker 事件导致密集补发
		interval := b.config.ResultPollInterval
		if interval <= 0 {
			interval = time.Second
		}
		// timer 只有当前查询完成后才重置；调用取消时及时释放计时资源
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				// budget 此步骤独立的网络或资源清理预算，不继承已到期的业务 deadline
				budget, stop := context.WithTimeout(ctx, 500*time.Millisecond)
				// state 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障
				var state model.RunStatus
				// err 当前操作错误，失败时不继续使用对应结果
				err := b.Feature(budget, ports.RunsGetStatus{ID: id}, &state)
				stop()
				if err == nil && (state == model.Completed || state == model.Failed) {
					result, done, terminalErr := b.persistedResult(ctx, id)
					if done || terminalErr != nil {
						// observation 区分瞬时查询失败和已经持久化的业务失败
						var observation *runObservationError
						if !errors.As(terminalErr, &observation) || !retryObservation(observation.err) {
							select {
							case resultCh <- response{result, terminalErr}:
							case <-ctx.Done():
							}
							return
						}
					}
				}
				if err == nil && state == model.Cancelled {
					select {
					case statusCh <- status.Error(codes.Canceled, "wego: run was cancelled"):
					case <-ctx.Done():
					}
					return
				}
				timer.Reset(interval)
			}
		}
	}()
	defer func() {
		cancel()
		<-pollingDone // 独立 gRPC stream 的 Send/Recv 在取消后会退出
		<-done
	}()
	select {
	// case r 取得 <-resultCh: 的结果，确认成功后才进入下一处理阶段
	case r := <-resultCh:
		return r.result, r.err
	// err 当前操作错误，失败时不继续使用对应结果
	case err := <-statusCh:
		return ports.Result{}, err
	case <-ctx.Done():
		return ports.Result{}, ctx.Err()
	}
}
