package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"strings"
	"sync"
	"time"

	adminpb "github.com/hatchet-dev/hatchet/internal/services/admin/contracts"
	dispatcherpb "github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts"
	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	v0 "github.com/hatchet-dev/hatchet/pkg/client"
	"github.com/hatchet-dev/hatchet/pkg/client/types"
	clientconfig "github.com/hatchet-dev/hatchet/pkg/config/client"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// Run 使用调用方预算提交官方协议请求，句柄取得后不再依赖提交 context。
func (b *Backend) Run(ctx context.Context, name string, input any, opts model.RunOptions) (ports.Run, error) {
	// refs 已由引擎确认接受的运行句柄，后续分块失败不能清空它们。
	refs, err := b.submit(ctx, name, []model.RunManyInput{{Input: input, Options: opts}}, false)
	if err != nil {
		return ports.Run{}, err
	}
	if len(refs) != 1 {
		return ports.Run{}, status.Error(codes.DataLoss, "wego: missing RunID")
	}
	return refs[0], nil
}

// RunMany 保留每个已确认分块的 RunID；例如第二块取消时第一块的 1000 个身份仍可追踪。
func (b *Backend) RunMany(ctx context.Context, name string, inputs []model.RunManyInput) ([]ports.Run, error) {
	return b.submit(ctx, name, inputs, true)
}

// submit 在提交前编码全部输入，防止前一项已经运行后才发现后一项无法编码。
func (b *Backend) submit(ctx context.Context, name string, inputs []model.RunManyInput, bulk bool) ([]ports.Run, error) {
	name = clientconfig.ApplyNamespace(strings.ToLower(name), &b.config.Namespace)
	// executionState 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
	var executionState *execution
	// state, ok 取得 callctx.Get 的结果，确认成功后才进入下一处理阶段。
	if state, ok := callctx.Get(ctx); ok {
		executionState, _ = state.Execution.(*execution)
	}
	if executionState != nil {
		// err 当前操作错误，失败时不继续使用对应结果。
		if err := executionState.spawnMu.Lock(ctx); err != nil {
			return nil, err
		}
		defer executionState.spawnMu.Unlock()
	}
	// requests 逐项编码的提交请求；输入下标用于关联部分成功的 RunID。
	requests := make([]*v1.TriggerWorkflowRequest, len(inputs))
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2。
	for i, input := range inputs {
		// opts 取得 input.Options 的结果，确认成功后才进入下一处理阶段。
		opts := input.Options
		opts.Metadata = traceMetadata(ctx, opts.Metadata)
		// data 编码后的业务或协议字节，只有编码成功才可交付。
		data, err := json.Marshal(input.Input)
		if err != nil {
			return nil, err
		}
		// req 当前协议请求，编码完整后才发送，不能使用未初始化的调度字段。
		req := &v1.TriggerWorkflowRequest{Name: name, Input: string(data)}
		// options, err 取得 runOptions 的结果，确认成功后才进入下一处理阶段。
		options, err := runOptions(opts)
		if err != nil {
			return nil, err
		}
		// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2。
		for _, option := range options {
			// err 当前操作错误，失败时不继续使用对应结果。
			if err := option(req); err != nil {
				return nil, err
			}
		}
		if executionState != nil {
			// parent 取得 executionState.original 的结果，确认成功后才进入下一处理阶段。
			parent := executionState.original
			// key 当前事件、memo 或子调用的稳定键，不能随重放随机改变。
			key := opts.Key
			if key == nil {
				key = callctx.ChildKey(ctx)
			}
			// 父身份真实存在时才添加 parent 字段，批处理 handler 没有单一父任务。
			if parent.WorkflowRunId() != "" {
				// id 真实运行或成员身份，例如 run-1，不能使用显示名称代替。
				id := parent.WorkflowRunId()
				req.ParentId = &id
			}
			if parent.StepRunId() != "" {
				// id 真实运行或成员身份，例如 run-1，不能使用显示名称代替。
				id := parent.StepRunId()
				req.ParentTaskRunExternalId = &id
			}
			// index 取得 int32 的结果，确认成功后才进入下一处理阶段。
			index := int32(parent.CurChildIndex())
			parent.IncChildIndex()
			req.ChildIndex = &index
			req.ChildKey = key
			if opts.Sticky != nil && *opts.Sticky {
				// owner 取得 parent.WorkerId 的结果，确认成功后才进入下一处理阶段。
				owner := parent.WorkerId()
				req.DesiredWorkerId = &owner
			}
		}
		requests[i] = req
	}
	if executionState != nil && executionState.durableListener() != nil && executionState.durableSupported() {
		return executionState.submitChildren(ctx, name, requests)
	}
	// refs 已由引擎确认接受的运行句柄，后续分块失败不能清空它们。
	var refs []ports.Run
	// errs 各分块或清理步骤的错误集合，不能丢弃部分成功信息。
	var errs []error
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2。
	for start := 0; start < len(requests); {
		// err 当前操作错误，失败时不继续使用对应结果。
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		// end 取得 start + 1 的结果，确认成功后才进入下一处理阶段。
		end := start + 1
		if bulk {
			// size 取得 proto.Size 的结果，确认成功后才进入下一处理阶段。
			size := proto.Size(requests[start])
			for end < len(requests) && end-start < 1000 && size+proto.Size(requests[end]) <= 3*1024*1024 {
				size += proto.Size(requests[end])
				end++
			}
		}
		// ids 引擎确认的运行身份集合，不通过提交输入猜测身份。
		var ids []string
		// err 当前操作错误，失败时不继续使用对应结果。
		var err error
		if bulk {
			// r 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
			var r *adminpb.BulkTriggerWorkflowResponse
			r, err = b.admin.BulkTriggerWorkflow(b.auth(ctx), &adminpb.BulkTriggerWorkflowRequest{Workflows: requests[start:end]})
			if err == nil {
				ids = r.WorkflowRunIds
			}
		} else {
			// r 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
			var r *adminpb.TriggerWorkflowResponse
			r, err = b.admin.TriggerWorkflow(b.auth(ctx), requests[start])
			if err == nil {
				ids = []string{r.WorkflowRunId}
			}
		}
		if err != nil {
			errs = append(errs, Normalize(err))
		} else if len(ids) != end-start {
			errs = append(errs, status.Error(codes.DataLoss, "wego: submitted count differs from RunIDs"))
		} else {
			// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2。
			for i, id := range ids {
				// ref 取得 b.runRef 的结果，确认成功后才进入下一处理阶段。
				ref := b.runRef(id)
				ref.InputIndex = start + i
				refs = append(refs, ref)
			}
		}
		start = end
	}
	if len(errs) > 0 {
		// cause 取得 errs[0] 的结果，确认成功后才进入下一处理阶段。
		cause := errs[0]
		if len(errs) > 1 {
			cause = errors.Join(errs...)
		}
		// collision 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
		var collision *model.BulkIdempotencyCollisionError
		// merged 累计每个失败块的完整详情；1002 项分两块时不能只返回第一块。
		merged := &model.BulkIdempotencyCollisionError{Err: cause}
		// chunkErr 的详情逐块累计，其余传输错误仍保留在错误链。
		for _, chunkErr := range errs {
			if errors.As(chunkErr, &collision) {
				merged.SuccessfulRunIDs = append(merged.SuccessfulRunIDs, collision.SuccessfulRunIDs...)
				// item 为某个失败块的冲突身份，复制后不借用错误容器。
				for _, item := range collision.Collisions {
					merged.Collisions = append(merged.Collisions, &model.IdempotencyCollisionError{ExistingRunID: item.ExistingRunID})
				}
			}
		}
		if len(merged.Collisions) > 0 || len(merged.SuccessfulRunIDs) > 0 {
			// ref 的成功身份也纳入跨块冲突汇总，不能因其他块冲突而丢失。
			for _, ref := range refs {
				merged.SuccessfulRunIDs = append(merged.SuccessfulRunIDs, ref.ID)
			}
			return refs, merged
		}
		if len(refs) > 0 {
			// ids 引擎确认的运行身份集合，不通过提交输入猜测身份。
			ids := make([]string, len(refs))
			// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2。
			for i, ref := range refs {
				ids[i] = ref.ID
			}
			return refs, &model.PartialSubmissionError{SuccessfulRunIDs: ids, Err: cause}
		}
		return refs, cause
	}
	return refs, nil
}

// runRef 每次 Wait 建立独立订阅，单个等待的取消不会终止其他运行。
func (b *Backend) runRef(id string) ports.Run {
	return ports.Run{
		ID: id,
		Wait: func(ctx context.Context) (ports.Result, error) {
			return b.await(ctx, id, func(ctx context.Context) (ports.Result, error) { return b.waitRun(ctx, id) })
		},
	}
}

// waitRun 从独立订阅读取终态；Send 与 Recv 使用同一调用预算，取消即可释放等待。
func (b *Backend) waitRun(ctx context.Context, id string) (ports.Result, error) {
	// stream 由当前结果等待独占，不初始化官方的共享结果监听器。
	stream, err := b.rpcDispatcher.SubscribeToWorkflowRuns(b.auth(ctx))
	if err != nil {
		return ports.Result{}, Normalize(err)
	}
	defer stream.CloseSend()
	// 发送也必须受请求预算约束，不能在进入取消 select 之前无限等待连接流控。
	if err := stream.Send(&dispatcherpb.SubscribeToWorkflowRunsRequest{WorkflowRunId: id}); err != nil {
		return ports.Result{}, Normalize(err)
	}
	for {
		// event 是此订阅的终态；只有对应 RunID 的结果可以交付业务。
		event, err := stream.Recv()
		if err != nil {
			return ports.Result{}, Normalize(err)
		}
		if event.WorkflowRunId != id {
			continue
		}
		// outputs 保留 task-name→output 形状，例如 {hello:{message:"world"}}。
		outputs := map[string]any{}
		// 每个任务独立解析错误和输出，损坏 JSON 不能作为空结果返回。
		for _, task := range event.Results {
			if task.Error != nil {
				return ports.Result{}, Normalize(errors.New(*task.Error))
			}
			if task.Output == nil {
				continue
			}
			// value 保持 JSON 业务值，RPC envelope 在更外层恢复 protobuf。
			var value any
			// err 当前操作错误，失败时不继续使用对应结果。
			if err := json.Unmarshal([]byte(*task.Output), &value); err != nil {
				return ports.Result{}, err
			}
			outputs[task.TaskName] = value
		}
		return ports.Result{RunID: id, Outputs: outputs}, nil
	}
}

// await 独立执行辅助状态轮询；HTTP 请求不能占住接收成功结果的 select。
func (b *Backend) await(ctx context.Context, id string, wait func(context.Context) (ports.Result, error)) (ports.Result, error) {
	// ctx 当前操作预算，用于传输取消；不得替换为无预算的 Background。
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// response 将业务结果及错误一并交付；缓冲使调用方取消后发送者也能退出。
	type response struct {
		// result 当前完成结果，只有成功确认后才交付业务。
		result ports.Result
		// err 当前操作错误，失败时不继续使用对应结果。
		err error
	}
	// resultCh 有缓冲的结果交付通道，取消后发送方仍可退出。
	resultCh := make(chan response, 1)
	// statusCh 辅助轮询的终止通知，不能阻塞业务结果交付。
	statusCh := make(chan error, 1)
	// done 本次 goroutine 的退出确认，关闭函数返回前需完成资源回收。
	done := make(chan struct{})
	go func() {
		defer close(done)
		// result/err 成对交付；缓冲通道保证取消后不会阻塞发送者退出。
		result, err := wait(ctx)
		resultCh <- response{result, err}
	}()
	// pollingDone 确认辅助请求在结果交付后随 ctx 取消，不遗留 HTTP goroutine。
	pollingDone := make(chan struct{})
	go func() {
		defer close(pollingDone)
		// interval 在查询完成后再计时，慢 HTTP 不会累积 ticker 事件导致密集补发。
		interval := b.config.ResultPollInterval
		if interval <= 0 {
			interval = time.Second
		}
		// timer 只有当前查询完成后才重置；调用取消时及时释放计时资源。
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				// budget 此步骤独立的网络或资源清理预算，不继承已到期的业务 deadline。
				budget, stop := context.WithTimeout(ctx, 500*time.Millisecond)
				// state 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障。
				var state model.RunStatus
				// err 当前操作错误，失败时不继续使用对应结果。
				err := b.Feature(budget, ports.RunsGetStatus{ID: id}, &state)
				stop()
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
		<-pollingDone // 独立 gRPC stream 的 Send/Recv 在取消后会退出。
		<-done
	}()
	select {
	// case r 取得 <-resultCh: 的结果，确认成功后才进入下一处理阶段。
	case r := <-resultCh:
		return r.result, r.err
	// err 当前操作错误，失败时不继续使用对应结果。
	case err := <-statusCh:
		return ports.Result{}, err
	case <-ctx.Done():
		return ports.Result{}, ctx.Err()
	}
}

// traceMetadata 将当前实例 trace 注入调度元数据，复制调用者 map 后再写入。
func traceMetadata(ctx context.Context, values map[string]string) map[string]string {
	values = maps.Clone(values)
	if values == nil {
		values = map[string]string{}
	}
	// carrier 取得 propagation.MapCarrier{} 的结果，确认成功后才进入下一处理阶段。
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2。
	for key, value := range carrier {
		values["wego."+key] = value
	}
	return values
}

// runOptions 转换本次运行的键、优先级、metadata 与亲和性选项。
func runOptions(v model.RunOptions) ([]v0.RunOptFunc, error) {
	// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
	var out []v0.RunOptFunc
	if v.Priority != nil {
		out = append(out, v0.WithPriority(int32(*v.Priority)))
	}
	if v.Metadata != nil {
		out = append(out, v0.WithRunMetadata(v.Metadata))
	}
	if v.Labels != nil {
		// labels 后端亲和性条件集合，例如 owner 的 Required=true 不允许回退到另一实例。
		labels := map[string]*types.DesiredWorkerLabel{}
		// key 是调度标签名，label 的 Value 必须保持 string 或 int32 类型。
		for key, label := range v.Labels {
			if label == nil {
				return nil, fmt.Errorf("wego: nil affinity label %s", key)
			}
			// value 验证数值范围，禁止 JSON roundtrip 把整数变成 float64。
			value, err := affinityValue(label.Value)
			if err != nil {
				return nil, fmt.Errorf("wego: label %s: %w", key, err)
			}
			// converted 拷贝必需性、权重和比较器，避免引用调用方可变指针。
			converted := &types.DesiredWorkerLabel{Value: value, Required: label.Required, Weight: label.Weight}
			if label.Comparator != nil {
				// comparator 与 wego 枚举保持同一数值语义。
				comparator := types.WorkerLabelComparator(*label.Comparator)
				converted.Comparator = &comparator
			}
			labels[key] = converted
		}
		out = append(out, v0.WithDesiredWorkerLabels(labels))
	}
	return out, nil
}

// affinityValue 只接受协议支持的字符串和有符号整数，例如 capacity=8，拒绝 8.5 及溢出值。
func affinityValue(value any) (any, error) {
	// integer 将整数统一到后端 int32；调用方整数类型不影响匹配结果。
	var integer int64
	// v 是原始动态类型，不能通过 JSON 推断数值语义。
	switch v := value.(type) {
	case string:
		return v, nil
	case int:
		integer = int64(v)
	case int32:
		return v, nil
	case int64:
		integer = v
	default:
		return nil, fmt.Errorf("unsupported affinity value type %T", value)
	}
	if integer < math.MinInt32 || integer > math.MaxInt32 {
		return nil, fmt.Errorf("affinity integer exceeds int32 range")
	}
	return int32(integer), nil
}

// submissionGate 串行化父执行的提交确认；排队者可取消而不影响持有者。
type submissionGate struct {
	// once 只初始化一份 token 通道，允许 execution 的零值直接使用。
	once sync.Once
	// token 容量为一，持有期间占用 token，不能同时修改 child 序号。
	token chan struct{}
}

// Lock 等待提交权，预算结束时不消耗下一个稳定 child 序号。
func (g *submissionGate) Lock(ctx context.Context) error {
	// err 优先拒绝已经取消的调用，不能随机获取空闲 token。
	if err := ctx.Err(); err != nil {
		return err
	}
	g.once.Do(func() { g.token = make(chan struct{}, 1) })
	select {
	case g.token <- struct{}{}:
		// 取消和获取 token 同时就绪时，不让已取消的请求占用 child 序号。
		if err := ctx.Err(); err != nil {
			g.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Unlock 在真正提交确认后归还提交权，业务结果等待不持有此 token。
func (g *submissionGate) Unlock() { <-g.token }
