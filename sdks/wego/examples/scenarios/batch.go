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
)

// Stubs 验证官方 stub 场景中的任务定义与结果读取 每次使用唯一 namespace，成功断言写入报告后清理可删除资源
func Stubs(ctx context.Context, report *Report) (err error) {
	// h, err 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值
	h, err := Start(ctx, "stubs", report, &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			// out 带当前任务身份的 protobuf 响应，业务字段与运行身份一起返回供断言
			out := Reply(ctx, in)
			out.Ok = true
			return out, nil
		},
	}, nil)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// out, err 接收 h.RPC.SayHello 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果
	out, err := h.RPC.SayHello(ctx, &pb.Request{Message: "stub"})
	if err != nil {
		return err
	}
	if !out.Ok {
		return fmt.Errorf("stub output")
	}

	report.Add(h, "definition template registered and decoded", out)
	return nil
}

// batchInput 批次输入，包括文本与用于隔离聚合的分组键
type batchInput struct {
	// Message, Group 批输入文本与分组键；不同 Group 应隔离聚合
	Message, Group string
}

// batchOutput 批处理输出，保留聚合大小、分组和汇总信息便于断言
type batchOutput struct {
	// Uppercase, Group 批处理后的大写文本与所属分组
	Uppercase, Group string
	// BatchSize, UniqueKeys, Sum 批次大小、唯一分组数和汇总结果，用于验证聚合边界
	BatchSize, UniqueKeys, Sum int
}

// Batch 验证大小触发、时间窗口、分组隔离以及逐条和广播结果 每次使用唯一 namespace，成功断言写入报告后清理可删除资源
func Batch(ctx context.Context, report *Report) (err error) {
	// h 保存本场景的连接、定义和资源名称，失败路径同样负责关闭与清理
	h := &Harness{
		Scenario:  "batch",
		Namespace: fmt.Sprintf("wego_accept_batch_%d_", time.Now().UnixNano()),
		Report:    report,
		Names:     []string{"batch-simple", "batch-keyed", "batch-broadcast"},
	}
	h.Conn, err = client.New(client.WithRuntime(Runtime(h.Namespace)...))
	if err != nil {
		return err
	}

	// interval 批聚合的时间窗口，未达大小阈值时由此时长触发执行
	interval := 300 * time.Millisecond
	// mapOutput 以每个输入 RunID 为键返回独立输出，不能把整批结果套用到一条输入
	mapOutput := func(ctx context.Context, inputs map[string]batchInput) (map[string]batchOutput, error) {
		// out 当前结果的按键映射，逐项写入转换后的输出；不会把后端对象直接放入公开返回值
		out := map[string]batchOutput{}
		// keys 批次中的唯一分组键集合，用于断言不同分组没有被混入同一批
		keys := map[string]bool{}
		// 逐项处理 inputs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for _, input := range inputs {
			keys[input.Group] = true
		}
		// 逐项处理 inputs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for id, input := range inputs {
			out[id] = batchOutput{
				Uppercase:  strings.ToUpper(input.Message),
				Group:      input.Group,
				BatchSize:  len(inputs),
				UniqueKeys: len(keys),
			}
		}
		return out, nil
	}
	// simple 注册批量聚合定义，例如三项输入各有独立 RunID 与对应输出
	simple := h.Conn.NewStandaloneBatchTask("batch-simple", mapOutput, model.BatchConfig{MaxSize: 3, MaxInterval: &interval})
	// keyed 注册批量聚合定义，例如三项输入各有独立 RunID 与对应输出
	keyed := h.Conn.NewStandaloneBatchTask("batch-keyed", mapOutput, model.BatchConfig{MaxSize: 2, MaxInterval: &interval, GroupKey: pointer("input.Group")})
	// broadcast 注册批量聚合定义，例如三项输入各有独立 RunID 与对应输出
	broadcast := h.Conn.NewStandaloneBatchTask("batch-broadcast", func(ctx context.Context, inputs map[string]batchInput) (batchOutput, error) {
		// sum 当前步骤的初始计数 0，后续根据实际执行或数据量更新
		sum := 0
		// 逐项处理 inputs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for _, input := range inputs {
			sum += len(input.Message)
		}
		return batchOutput{Sum: sum, BatchSize: len(inputs)}, nil
	}, model.BatchConfig{MaxSize: 10, MaxInterval: &interval, BroadcastOutput: true})
	h.NativeWorker, err = h.Conn.NewWorker(h.Namespace+"worker", client.WithWorkflows(simple, keyed, broadcast))
	if err != nil {
		_ = h.Conn.Close()
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	if _, err = h.NativeWorker.Start(); err != nil {
		return err
	}
	if err = h.NativeWorker.WaitReady(ctx); err != nil {
		return err
	}

	// 逐项处理 []string{"simple", "keyed", "broadcast"}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
	for _, variant := range []string{"simple", "keyed", "broadcast"} {
		// inputs 批提交输入列表，每项保存自己的业务数据和运行选项，提交顺序与结果句柄对应
		inputs := []client.RunManyInput{}
		// n 当前步骤的初始计数 3，后续根据实际执行或数据量更新
		n := 3
		if variant == "keyed" {
			n = 4
		}
		if variant == "broadcast" {
			n = 2
		}
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言
		for i := 0; i < n; i++ {
			inputs = append(inputs, client.RunManyInput{
				Input: batchInput{Message: fmt.Sprintf("m%d", i), Group: fmt.Sprintf("group%d", i%2)},
			})
		}
		// refs, e 逐条提交并保留输入顺序，结果句柄与每项输入一一对应
		refs, e := h.Conn.RunMany(ctx, "batch-"+variant, inputs)
		if e != nil {
			return e
		}

		// 逐项处理 refs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组
		for i := range refs {
			// result, e 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据
			result, e := refs[i].Result(ctx)
			if e != nil {
				return e
			}

			// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果
			var out batchOutput
			if e = result.Into(&out); e != nil {
				return e
			}
			if variant == "broadcast" {
				if out.Sum != 4 || out.BatchSize != 2 {
					return fmt.Errorf("batch broadcast: %+v", out)
				}
			} else {
				if out.Uppercase != fmt.Sprintf("M%d", i) {
					return fmt.Errorf("per-item output mapping: %+v", out)
				}
				if variant == "keyed" && (out.Group != fmt.Sprintf("group%d", i%2) || out.UniqueKeys != 1 || out.BatchSize != 2) {
					return fmt.Errorf("group isolation: %+v", out)
				}
				if variant == "simple" && out.BatchSize != 3 {
					return fmt.Errorf("size aggregation: %+v", out)
				}
			}
			report.Add(h, "batch "+variant+" aggregation and result mapping", &pb.Reply{RunId: refs[i].RunID})
		}
	}
	return nil
}
