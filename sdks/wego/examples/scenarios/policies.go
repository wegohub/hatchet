package scenarios

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hatchet-dev/hatchet/sdks/wego/client"
	pb "github.com/hatchet-dev/hatchet/sdks/wego/examples/proto"
	"github.com/hatchet-dev/hatchet/sdks/wego/model"
	"github.com/hatchet-dev/hatchet/sdks/wego/runtime"
	"github.com/hatchet-dev/hatchet/sdks/wego/task"
	"github.com/hatchet-dev/hatchet/sdks/wego/worker"
)

// occupancy 按分组记录当前并发和历史峰值的观测夹具。
type occupancy struct {
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁。
	mu sync.Mutex
	// active, peak 按分组记录当前并发数与历史峰值，验证调度上限。
	active, peak map[string]int
	// starts 按分组记录执行开始时间，验证限流和排队。
	starts map[string][]time.Time
}

// newOccupancy 初始化按分组统计并发峰值的 fixture。
func newOccupancy() *occupancy {
	return &occupancy{active: map[string]int{}, peak: map[string]int{}, starts: map[string][]time.Time{}}
}

// enter 登记业务开始及分组并发量，返回释放回调用于执行结束时归还计数。
func (o *occupancy) enter(keys ...string) func() {
	o.mu.Lock()
	// 逐项处理 keys，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, key := range keys {
		o.active[key]++
		o.peak[key] = max(o.peak[key], o.active[key])
		o.starts[key] = append(o.starts[key], time.Now())
	}
	o.mu.Unlock()
	return func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		// 逐项处理 keys，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
		for _, key := range keys {
			o.active[key]--
		}
	}
}

// handler 执行 o.enter/ctx.Done/ctx.Err 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func (o *occupancy) handler(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
	// leave 登记网络在途调用，排空后拒绝新增入口并等待存量请求结束。
	leave := o.enter("total", "group:"+in.GroupKey, "account:"+in.Account, "tier:"+in.Tier, "user:"+in.UserId)
	defer leave()

	// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Duration(max(in.DelayMillis, 400)) * time.Millisecond):
	}
	return Reply(ctx, in), nil
}

// collect 等待一组运行结果，并逐条关联断言或失败。
func collect(ctx context.Context, h *Harness, refs []client.RunRef, assertion string) error {
	// 逐项处理 refs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for i := range refs {
		// result, err 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据。
		result, err := refs[i].Result(ctx)
		if err != nil {
			return err
		}

		// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
		var out pb.Reply
		if err = result.Into(&out); err != nil {
			return err
		}

		h.Report.Add(h, assertion, &out)
	}
	return nil
}

// Concurrency 验证分组并发策略和动态上限，记录真实执行峰值。 每次使用唯一 namespace，成功断言写入报告后清理可删除资源。
func Concurrency(ctx context.Context, report *Report) error {
	// 逐项处理 []string{ "round-robin", "multiple-keys", "shared", "dynamic", }，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, variant := range []string{
		"round-robin",
		"multiple-keys",
		"shared",
		"dynamic",
	} {
		// 当前步骤失败时终止处理：%s: %w；不把无效结果交给下一步。
		if err := concurrencyQueued(ctx, report, variant); err != nil {
			return fmt.Errorf("%s: %w", variant, err)
		}
	}
	// 分别验收取消正在执行、取消最新，以及只保留最新或最旧排队项；每种策略独立创建运行并断言终态。
	for _, strategy := range []model.ConcurrencyLimitStrategy{
		model.CancelInProgress,
		model.CancelNewest,
		model.CancelQueuedExceptNewest,
		model.CancelQueuedExceptOldest,
	} {
		// 当前步骤失败时终止处理：%s: %w；不把无效结果交给下一步。
		if err := concurrencyCancel(ctx, report, strategy); err != nil {
			return fmt.Errorf("%s: %w", strategy, err)
		}
	}
	return nil
}

// concurrencyQueued 执行 time.Now/worker.WithTask/task.WithConcurrency 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func concurrencyQueued(ctx context.Context, report *Report, variant string) (err error) {
	// o 按分组记录当前并发和峰值的 fixture，所有测试请求共用它进行实际执行统计。
	o := newOccupancy()
	// strategy 本场景需要验证的并发策略，实际执行峰值和终态用于判断其是否生效。
	strategy := model.GroupRoundRobin
	// limit 构造当前步骤的数据对象，字段在提交、编码或断言之前一次性初始化。
	limit := model.Concurrency{
		Expression:    "input.routing.group",
		MaxRuns:       pointer(int32(1)),
		LimitStrategy: &strategy,
	}
	// limits 本场景的并发约束集合，静态与动态上限分别配置，业务峰值按实际执行记录断言。
	limits := []model.Concurrency{limit}
	if variant == "multiple-keys" {
		limits = []model.Concurrency{
			{Expression: "input.routing.account", MaxRuns: pointer(int32(1)), LimitStrategy: &strategy},
			{Expression: "input.routing.tier", MaxRuns: pointer(int32(1)), LimitStrategy: &strategy},
		}
	}
	if variant == "dynamic" {
		limits[0].MaxRunsExpression = pointer("input.routing.tier == 'premium' ? 2 : 1")
	}
	if variant == "shared" {
		limits[0].Name = fmt.Sprintf("wego_shared_%d", time.Now().UnixNano())
		limits[0].IsTenantScoped = true
	}
	// policies 明确列出本段注册、传输或清理选项，应用顺序决定重复字段的覆盖关系。
	policies := []worker.Option{
		worker.WithTask(pb.UnaryGreeter_SayHello_FullMethodName, task.WithConcurrency(limits...)),
	}
	if variant == "shared" {
		policies = append(policies, worker.WithTask(pb.UnaryGreeter_WaitHello_FullMethodName, task.WithConcurrency(limits...)))
	}
	// h, err 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值。
	h, err := Start(ctx, "concurrency", report, &Service{Say: o.handler, Wait: o.handler}, nil, policies...)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// inputs 批提交输入列表，每项保存自己的业务数据和运行选项，提交顺序与结果句柄对应。
	inputs := []client.RunManyInput{}
	// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
	for i := 0; i < 6; i++ {
		// group 生成当前数据的文本表示，用于名称、业务比较或报告，不包含凭证。
		group := fmt.Sprint(i % 2)
		// tier 当前步骤使用的字符串值 "standard"，用于路由、请求或断言。
		tier := "standard"
		if variant == "dynamic" && i%2 == 1 {
			tier = "premium"
		}
		inputs = append(inputs, client.RunManyInput{Input: &pb.Request{GroupKey: group, Account: group, Tier: tier}})
	}
	if variant == "multiple-keys" {
		// 逐项处理 inputs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
		for i := range inputs {
			// input 当前提交的 protobuf 请求，分组、数量与故障标记在此固定，便于核对执行结果。
			input := inputs[i].Input.(*pb.Request)
			input.Tier = fmt.Sprint(i % 3)
		}
	}
	if variant == "shared" {
		// first, e 提交任务并取得真实 RunID，后续通过句柄单独等待结果。
		first, e := h.Conn.RunNoWait(ctx, pb.UnaryGreeter_SayHello_FullMethodName, &pb.Request{GroupKey: "same"})
		if e != nil {
			return e
		}

		// second, e 提交任务并取得真实 RunID，后续通过句柄单独等待结果。
		second, e := h.Conn.RunNoWait(ctx, pb.UnaryGreeter_WaitHello_FullMethodName, &pb.Request{GroupKey: "same"})
		if e != nil {
			return e
		}
		if err = collect(ctx, h, []client.RunRef{*first, *second}, "shared strategy across two workflows"); err != nil {
			return err
		}
		if o.peak["total"] != 1 {
			return fmt.Errorf("shared limit peak %v", o.peak)
		}

		return nil
	}

	// refs, err 接收 h.Conn.RunMany 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	refs, err := h.Conn.RunMany(ctx, pb.UnaryGreeter_SayHello_FullMethodName, inputs)
	if err != nil {
		return err
	}
	if err = collect(ctx, h, refs, "concurrency "+variant); err != nil {
		return err
	}

	// 逐项处理 o.peak，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for key, peak := range o.peak {
		// bound 当前步骤的初始计数 1，后续根据实际执行或数据量更新。
		bound := 1
		// check 是否已经到达需要断言的观察点，避免在尚未注册或执行时读取空结果。
		check := false
		// 按协议、输入类型或状态分派处理；每个分支只应用与该类型匹配的转换和状态更新。
		switch variant {
		case "round-robin":
			check = key == "group:0" || key == "group:1"
		case "multiple-keys":
			check = key == "account:0" || key == "account:1" || key == "tier:0" || key == "tier:1" || key == "tier:2"
		case "dynamic":
			check = key == "group:0" || key == "group:1"
			if key == "group:1" {
				bound = 2
			}
		}
		if check && peak != bound {
			return fmt.Errorf("%s peak %d, expected %d", key, peak, bound)
		}
	}
	if o.peak["total"] < 2 {
		return fmt.Errorf("independent groups did not run concurrently: %v", o.peak)
	}

	return nil
}

// waitStatus 在截止时间前轮询运行状态，避免使用固定 sleep 猜测执行位置。
func waitStatus(ctx context.Context, h *Harness, id string, want model.RunStatus) error {
	// 重复当前步骤直到完成条件或取消信号满足；等待期间重新检查状态，避免把一次唤醒当作最终结果。
	for {
		// state, err 接收 h.Conn.Runs 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
		state, err := h.Conn.Runs().GetStatus(ctx, id)
		if err != nil {
			if !strings.Contains(err.Error(), "status 404") {
				return err
			}
		}
		if state == want {
			return nil
		}
		if state == model.Completed || state == model.Failed || state == model.Cancelled {
			return fmt.Errorf("run %s reached %s, expected %s", id, state, want)
		}

		// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// concurrencyCancel 执行 ctx.Done/ctx.Err/worker.WithTask 对应的场景步骤；检查业务结果后将断言关联到当前报告，失败立即返回错误。
func concurrencyCancel(ctx context.Context, report *Report, strategy model.ConcurrencyLimitStrategy) (err error) {
	// started 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	started := make(chan int32, 3)
	// gate 创建协调通知通道；等待方通过它确认步骤已经发生，而不靠固定 sleep 猜测时序。
	gate := make(chan struct{})
	// release 只执行一次的完成或释放保护，避免重复关闭通知通道。
	var release sync.Once
	// service 注入本场景的业务 handler，实际调用结果用于验证注册策略和任务身份。
	service := &Service{
		Say: func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			started <- in.Count
			// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-gate:
			}
			return Reply(ctx, in), nil
		},
	}
	// h, err 接收 worker.WithTask 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	h, err := Start(
		ctx,
		"concurrency",
		report,
		service,
		nil,
		worker.WithTask(
			pb.UnaryGreeter_SayHello_FullMethodName,
			task.WithConcurrency(model.Concurrency{
				Expression:    "input.routing.group",
				MaxRuns:       pointer(int32(1)),
				LimitStrategy: &strategy,
			}),
		),
	)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()
	defer release.Do(func() {
		close(gate)
	})

	// refs 分配当前步骤的结果列表，容量只用于聚合，不代表业务已经成功完成。
	refs := make([]*client.RunRef, 3)
	// 逐项处理 refs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for i := range refs {
		refs[i], err = h.Conn.RunNoWait(ctx, pb.UnaryGreeter_SayHello_FullMethodName, &pb.Request{GroupKey: "same", Count: int32(i)})
		if err != nil {
			return err
		}
		if i == 0 || strategy == model.CancelInProgress {
			// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
			select {
			// value 从当前通知通道接收结果；后续检查内容或错误，关闭通知不等于业务成功。
			case value := <-started:
				if value != int32(i) {
					return fmt.Errorf("unexpected started task %d", value)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if i > 0 && strategy == model.CancelNewest {
			if err = waitStatus(ctx, h, refs[i].RunID, model.Cancelled); err != nil {
				return err
			}
		}
		if i > 0 && strategy == model.CancelInProgress {
			if err = waitStatus(ctx, h, refs[i-1].RunID, model.Cancelled); err != nil {
				return err
			}
		}
		if i == 1 && (strategy == model.CancelQueuedExceptNewest || strategy == model.CancelQueuedExceptOldest) {
			// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
			select {
			case <-time.After(300 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	// cancelled 被取消的输入序号集合，用来区分保留和被取消运行，而不是只核对总成功数。
	cancelled := map[int]bool{}
	// 按协议、输入类型或状态分派处理；每个分支只应用与该类型匹配的转换和状态更新。
	switch strategy {
	case model.CancelInProgress:
		cancelled[0] = true
		cancelled[1] = true
	case model.CancelNewest:
		cancelled[1] = true
		cancelled[2] = true
	case model.CancelQueuedExceptNewest:
		cancelled[1] = true
	case model.CancelQueuedExceptOldest:
		cancelled[2] = true
	}
	// 逐项处理 cancelled，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for index := range cancelled {
		if err = waitStatus(ctx, h, refs[index].RunID, model.Cancelled); err != nil {
			return err
		}
	}
	release.Do(func() {
		close(gate)
	})
	// 逐项处理 refs，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for i, ref := range refs {
		// result, e 等待此运行的业务结果；等待失败不能作为正常完成，返回的 RunID 可用于关联证据。
		result, e := ref.Result(ctx)
		if cancelled[i] {
			if e == nil {
				return fmt.Errorf("cancelled run returned success")
			}

			report.Add(h, "cancel strategy "+string(strategy), &pb.Reply{RunId: ref.RunID})
			continue
		}
		if e != nil {
			return e
		}

		// out 保存当前步骤的输出或解码目标，初始化后由当前调用填充；错误时不能使用其零值作为成功结果。
		var out pb.Reply
		if e = result.Into(&out); e != nil {
			return e
		}

		report.Add(h, "cancel strategy "+string(strategy), &out)
	}
	return nil
}

// SlotCost 验证任务容量成本，例如 4 槽、成本 2 的峰值不超过 2。 每次使用唯一 namespace，成功断言写入报告后清理可删除资源。
func SlotCost(ctx context.Context, report *Report) (err error) {
	// mu 保护所属对象的可变状态；读取和写入使用同一把锁。
	var mu sync.Mutex
	// occupied, peak, heavy, peakHeavy 保存此步骤的并行结果或状态快照，后续逻辑分别使用对应值。
	occupied, peak, heavy, peakHeavy := 0, 0, 0, 0
	// handler 按传入的容量成本构造业务函数，配合占用计数验证 slot cost 改变并发峰值。
	handler := func(cost int) UnaryHandler {
		return func(ctx context.Context, in *pb.Request) (*pb.Reply, error) {
			mu.Lock()
			occupied += cost
			peak = max(peak, occupied)
			if cost == 5 {
				heavy++
				peakHeavy = max(peakHeavy, heavy)
			}
			mu.Unlock()
			defer func() {
				mu.Lock()
				occupied -= cost
				if cost == 5 {
					heavy--
				}
				mu.Unlock()
			}()

			// 在业务进展和上下文取消之间等待；deadline 或 Stop 到达时结束阻塞并走清理路径。
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(600 * time.Millisecond):
			}
			return Reply(ctx, in), nil
		}
	}
	// h, err 接收 runtime.WithSlots 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	h, err := Start(
		ctx,
		"slot-cost",
		report,
		&Service{Say: handler(5), Wait: handler(1)},
		[]runtime.Option{runtime.WithSlots(10)},
		worker.WithTask(pb.UnaryGreeter_SayHello_FullMethodName, task.WithSlotCost(5)),
		worker.WithTask(pb.UnaryGreeter_WaitHello_FullMethodName, task.WithSlotCost(1)),
	)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// heavies, err 接收 h.Conn.RunMany 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	heavies, err := h.Conn.RunMany(
		ctx,
		pb.UnaryGreeter_SayHello_FullMethodName,
		[]client.RunManyInput{{Input: &pb.Request{}}, {Input: &pb.Request{}}, {Input: &pb.Request{}}},
	)
	if err != nil {
		return err
	}

	// lights, err 接收 h.Conn.RunMany 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	lights, err := h.Conn.RunMany(ctx, pb.UnaryGreeter_WaitHello_FullMethodName, []client.RunManyInput{{Input: &pb.Request{}}, {Input: &pb.Request{}}})
	if err != nil {
		return err
	}
	if err = collect(ctx, h, append(heavies, lights...), "slot costs 5 and 1 share capacity 10"); err != nil {
		return err
	}
	if peak > 10 || peakHeavy != 2 {
		return fmt.Errorf("slot usage peak=%d heavy=%d", peak, peakHeavy)
	}

	return nil
}

// RateLimits 验证静态与动态限流，检查业务实际启动时间。 每次使用唯一 namespace，成功断言写入报告后清理可删除资源。
func RateLimits(ctx context.Context, report *Report) (err error) {
	// key 生成当前数据的文本表示，用于名称、业务比较或报告，不包含凭证。
	key := fmt.Sprintf("wego_limit_%d", time.Now().UnixNano())
	// o 按分组记录当前并发和峰值的 fixture，所有测试请求共用它进行实际执行统计。
	o := newOccupancy()
	// setup, err 接收 client.New 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	setup, err := client.New(client.WithRuntime(Runtime("wego_rate_setup_")...))
	if err != nil {
		return err
	}

	err = setup.RateLimits().Upsert(model.CreateRatelimitOpts{Key: key, Limit: 2, Duration: model.Second})
	err = errors.Join(err, setup.Close())
	if err != nil {
		return err
	}

	// h, err 接收 worker.WithTask 的返回值，同时保存错误供紧接着的分支检查；失败时不继续使用结果。
	h, err := Start(
		ctx,
		"rate-limiting",
		report,
		&Service{Say: o.handler, Wait: o.handler},
		nil,
		worker.WithTask(pb.UnaryGreeter_SayHello_FullMethodName, task.WithRateLimits(model.RateLimit{Key: key, Units: pointer(1)})),
		worker.WithTask(pb.UnaryGreeter_WaitHello_FullMethodName, task.WithRateLimits(model.RateLimit{
			Key:            key + "_dynamic",
			KeyExpr:        pointer("input.routing.id"),
			Units:          pointer(1),
			LimitValueExpr: pointer("1"),
			Duration:       pointer(model.Second),
		})),
	)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, h.Close())
	}()

	// 逐项处理 []string{pb.UnaryGreeter_SayHello_FullMethodName, pb.UnaryGreeter_WaitHello_FullMethodName}，保留各项的身份和独立结果，避免把一个条目的结果套用到整组。
	for _, method := range []string{pb.UnaryGreeter_SayHello_FullMethodName, pb.UnaryGreeter_WaitHello_FullMethodName} {
		// inputs 批提交输入列表，每项保存自己的业务数据和运行选项，提交顺序与结果句柄对应。
		inputs := []client.RunManyInput{}
		// 遍历本组明确的条目，逐项验证结果或登记资源；迭代结束后才汇总这一组断言。
		for i := 0; i < 6; i++ {
			inputs = append(inputs, client.RunManyInput{Input: &pb.Request{Id: h.Namespace + fmt.Sprint(i%2), UserId: method}})
		}
		// refs, e 逐条提交并保留输入顺序，结果句柄与每项输入一一对应。
		refs, e := h.Conn.RunMany(ctx, method, inputs)
		if e != nil {
			return e
		}
		if err = collect(ctx, h, refs, "static/dynamic per-second rate limit"); err != nil {
			return err
		}

		// times 同一分组的真实启动时间，比较相邻间隔以验证限流调度。
		times := o.starts["user:"+method]
		if len(times) != 6 || times[5].Sub(times[0]) < 900*time.Millisecond {
			return fmt.Errorf("rate limit did not delay all six tasks: %v", times)
		}
	}
	return nil
}
