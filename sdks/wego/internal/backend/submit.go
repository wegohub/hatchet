package backend

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	adminpb "github.com/hatchet-dev/hatchet/internal/services/admin/contracts"
	v1 "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	clientconfig "github.com/hatchet-dev/hatchet/pkg/config/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/sdks/wego/internal/binding"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/callctx"
	"github.com/hatchet-dev/hatchet/sdks/wego/internal/ports"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
)

// submit 在提交前编码全部输入，防止前一项已经运行后才发现后一项无法编码
func (b *Backend) submit(ctx context.Context, name string, inputs []model.RunManyInput, bulk bool) ([]ports.Run, error) {
	name, err := b.submissionName(ctx, name, inputs)
	if err != nil {
		return nil, err
	}
	// executionState 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障
	var executionState *execution
	// gatedState 为普通可靠输出任务保留独立的父身份与子索引
	var gatedState *gatedExecution
	// state, ok 取得 callctx.Get 的结果，确认成功后才进入下一处理阶段
	if state, ok := callctx.Get(ctx); ok {
		executionState, _ = state.Execution.(*execution)
		gatedState, _ = state.Execution.(*gatedExecution)
	}
	if executionState != nil {
		// err 当前操作错误，失败时不继续使用对应结果
		if err := executionState.spawnMu.Lock(ctx); err != nil {
			return nil, err
		}
		defer executionState.spawnMu.Unlock()
	}
	if gatedState != nil {
		if err := gatedState.spawnMu.Lock(ctx); err != nil {
			return nil, err
		}
		defer gatedState.spawnMu.Unlock()
	}
	requests, err := b.prepareTriggers(ctx, name, inputs, bulk, executionState, gatedState)
	if err != nil {
		return nil, err
	}
	if executionState != nil && executionState.durableListener() != nil && executionState.durableSupported() {
		return executionState.submitChildren(ctx, name, requests)
	}
	// refs 已由引擎确认接受的运行句柄，后续分块失败不能清空它们
	var refs []ports.Run
	// errs 各分块或清理步骤的错误集合，不能丢弃部分成功信息
	var errs []error
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
	for start := 0; start < len(requests); {
		// err 当前操作错误，失败时不继续使用对应结果
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		// end 取得 start + 1 的结果，确认成功后才进入下一处理阶段
		end := start + 1
		if bulk {
			// repeated-message 的 tag 与长度前缀也必须计费；不能只相加内层消息大小
			size := proto.Size(&adminpb.BulkTriggerWorkflowRequest{Workflows: requests[start:end]})
			for end < len(requests) && end-start < 1000 {
				cost := proto.Size(&adminpb.BulkTriggerWorkflowRequest{Workflows: requests[end : end+1]})
				if size+cost > min(3*1024*1024, b.messageLimit()) {
					break
				}
				size += cost
				end++
			}
		}
		// ids 引擎确认的运行身份集合，不通过提交输入猜测身份
		var ids []string
		// err 当前操作错误，失败时不继续使用对应结果
		var err error
		if bulk {
			// r 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障
			var r *adminpb.BulkTriggerWorkflowResponse
			r, err = b.admin.BulkTriggerWorkflow(b.auth(ctx), &adminpb.BulkTriggerWorkflowRequest{Workflows: requests[start:end]})
			if err == nil {
				ids = r.WorkflowRunIds
			}
		} else {
			// r 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障
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
			// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
			for i, id := range ids {
				// ref 取得 b.runRef 的结果，确认成功后才进入下一处理阶段
				ref := b.runRef(id)
				ref.InputIndex = start + i
				refs = append(refs, ref)
			}
		}
		start = end
	}
	if len(errs) > 0 {
		// cause 取得 errs[0] 的结果，确认成功后才进入下一处理阶段
		cause := errs[0]
		if len(errs) > 1 {
			cause = errors.Join(errs...)
		}
		// collision 属于当前操作的独立状态，读写遵守所属对象的锁与退出屏障
		var collision *model.BulkIdempotencyCollisionError
		// merged 累计每个失败块的完整详情；1002 项分两块时不能只返回第一块
		merged := &model.BulkIdempotencyCollisionError{Err: cause}
		// chunkErr 的详情逐块累计，其余传输错误仍保留在错误链
		for _, chunkErr := range errs {
			if errors.As(chunkErr, &collision) {
				merged.SuccessfulRunIDs = append(merged.SuccessfulRunIDs, collision.SuccessfulRunIDs...)
				// item 为某个失败块的冲突身份，复制后不借用错误容器
				for _, item := range collision.Collisions {
					merged.Collisions = append(merged.Collisions, &model.IdempotencyCollisionError{ExistingRunID: item.ExistingRunID})
				}
			}
		}
		if len(merged.Collisions) > 0 || len(merged.SuccessfulRunIDs) > 0 {
			// ref 的成功身份也纳入跨块冲突汇总，不能因其他块冲突而丢失
			for _, ref := range refs {
				merged.SuccessfulRunIDs = append(merged.SuccessfulRunIDs, ref.ID)
			}
			return refs, merged
		}
		if len(refs) > 0 {
			// ids 引擎确认的运行身份集合，不通过提交输入猜测身份
			ids := make([]string, len(refs))
			// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
			for i, ref := range refs {
				ids[i] = ref.ID
			}
			return refs, &model.PartialSubmissionError{SuccessfulRunIDs: ids, Err: cause}
		}
		return refs, cause
	}
	return refs, nil
}

// submissionName 验证整批绑定的一致性，原生任务只应用其自身的 namespace 规则
func (b *Backend) submissionName(ctx context.Context, name string, inputs []model.RunManyInput) (string, error) {
	// RPC 身份来自输入类型而非 JSON 字段或 workflow 前缀
	if len(inputs) > 0 {
		if rpcInput, ok := inputs[0].Input.(ports.RPCInput); ok {
			method := rpcInput.RPCMethod()
			if err := binding.ValidateName(b.config.Namespace, method); err != nil {
				return "", err
			}
			for _, item := range inputs {
				other, ok := item.Input.(ports.RPCInput)
				if !ok || other.RPCMethod() != method || other.RPCShape() != rpcInput.RPCShape() || other.RPCMode() != rpcInput.RPCMode() {
					return "", status.Error(codes.InvalidArgument, "wego: mixed RPC batch bindings")
				}
			}
			if err := b.validateRPCBinding(ctx, rpcInput); err != nil {
				return "", err
			}
			name = binding.WorkflowName(b.config.Namespace, method)
		} else {
			name = clientconfig.ApplyNamespace(strings.ToLower(name), &b.config.Namespace)
		}
	}
	return name, nil
}

// prepareTriggers 在任何远程提交前冻结全部请求；后项非法时没有前项已经提交的副作用
func (b *Backend) prepareTriggers(ctx context.Context, name string, inputs []model.RunManyInput, bulk bool, executionState *execution, gatedState *gatedExecution) ([]*v1.TriggerWorkflowRequest, error) {
	// requests 逐项编码的提交请求；输入下标用于关联部分成功的 RunID
	requests := make([]*v1.TriggerWorkflowRequest, len(inputs))
	// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
	for i, input := range inputs {
		// opts 取得 input.Options 的结果，确认成功后才进入下一处理阶段
		opts := input.Options
		opts.Metadata = traceMetadata(ctx, opts.Metadata)
		// data 编码后的业务或协议字节，只有编码成功才可交付
		data, err := json.Marshal(input.Input)
		if err != nil {
			return nil, err
		}
		// req 当前协议请求，编码完整后才发送，不能使用未初始化的调度字段
		req := &v1.TriggerWorkflowRequest{Name: name, Input: string(data)}
		// options, err 取得 runOptions 的结果，确认成功后才进入下一处理阶段
		options, err := runOptions(opts)
		if err != nil {
			return nil, err
		}
		// 逐项处理本组定义、成员或协议详情，保留每项身份；例如 run-1 的结果不能交给 run-2
		for _, option := range options {
			// err 当前操作错误，失败时不继续使用对应结果
			if err := option(req); err != nil {
				return nil, err
			}
		}
		b.bindTriggerParent(ctx, req, opts, executionState, gatedState)
		size := proto.Size(req)
		if bulk {
			size = proto.Size(&adminpb.BulkTriggerWorkflowRequest{Workflows: []*v1.TriggerWorkflowRequest{req}})
		}
		if size > b.messageLimit() {
			return nil, status.Error(codes.ResourceExhausted, "wego: complete trigger request exceeds backend message limit")
		}
		requests[i] = req
	}
	return requests, nil
}

// bindTriggerParent 只在持有父执行 spawnMu 时调用，索引递增与批次冻结不可交错
// gatedState 内部身份只读，外部 Info 仍返回独立 metadata 和 labels 快照
func (b *Backend) bindTriggerParent(ctx context.Context, req *v1.TriggerWorkflowRequest, opts model.RunOptions, executionState *execution, gatedState *gatedExecution) {
	if executionState != nil {
		// parent 取得 executionState.original 的结果，确认成功后才进入下一处理阶段
		parent := executionState.original
		// key 当前事件、memo 或子调用的稳定键，不能随重放随机改变
		key := opts.Key
		if key == nil {
			key = callctx.ChildKey(ctx)
		}
		// 父身份真实存在时才添加 parent 字段，批处理 handler 没有单一父任务
		if parent.WorkflowRunId() != "" {
			// id 真实运行或成员身份，例如 run-1，不能使用显示名称代替
			id := parent.WorkflowRunId()
			req.ParentId = &id
		}
		if parent.StepRunId() != "" {
			// id 真实运行或成员身份，例如 run-1，不能使用显示名称代替
			id := parent.StepRunId()
			req.ParentTaskRunExternalId = &id
		}
		// index 取得 int32 的结果，确认成功后才进入下一处理阶段
		index := int32(parent.CurChildIndex())
		parent.IncChildIndex()
		req.ChildIndex = &index
		req.ChildKey = key
		if opts.Sticky != nil && *opts.Sticky {
			// owner 取得 parent.WorkerId 的结果，确认成功后才进入下一处理阶段
			owner := parent.WorkerId()
			req.DesiredWorkerId = &owner
		}
	}
	if gatedState != nil {
		// 内部只读身份在构造执行时冻结；不为三个字符串复制业务 metadata 和 labels
		info := gatedState.info
		req.ParentId, req.ParentTaskRunExternalId = &info.RunID, &info.TaskRunID
		index := gatedState.childIndex
		gatedState.childIndex++
		req.ChildIndex = &index
		req.ChildKey = opts.Key
		if req.ChildKey == nil {
			req.ChildKey = callctx.ChildKey(ctx)
		}
		if opts.Sticky != nil && *opts.Sticky {
			req.DesiredWorkerId = &info.WorkerID
		}
	}
}
