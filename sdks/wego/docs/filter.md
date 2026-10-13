Hatchet 的 **Filter 用来决定“某个事件是否触发 workflow”，并为每次触发附带配置数据**。它在引擎里执行，发生在 Worker 接到任务之前。[官方事件文档](https://docs.hatchet.run/v1/events)

例如：所有订单都发布 `order:paid`，但通知任务只处理“指定店铺、金额达到门槛”的订单。

### 1. 三个概念先分清

| 配置 | 作用 | 示例 |
|---|---|---|
| Event key | workflow 订阅哪类事件 | `order:paid` |
| Filter scope | 精确匹配一组过滤器 | `shop:42` |
| Filter expression | 用 CEL 判断是否触发 | `input.amount >= payload.min_amount` |

匹配过程是：

```text
事件 key 匹配 workflow
→ 找到 scope 相同的 Filter
→ CEL 返回 true
→ 创建一次 workflow run
```

**scope 是独立的事件参数，不会自动从业务输入中的 `shop_id` 提取，也不是 Worker 亲和标签。**[Scope 说明](https://docs.hatchet.run/v1/events#understanding-scopes)

### 2. Go SDK 使用示例

下面使用官方 Hatchet Go SDK。

注册任务时，声明事件和默认过滤器：

```go
type OrderInput struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

notify := client.NewStandaloneTask(
	"order-notify",
	func(ctx hatchet.Context, input OrderInput) (map[string]any, error) {
		// input 是事件发布时提供的订单数据。
		// FilterPayload 是本次命中的过滤器携带的配置。
		fmt.Println(input.OrderID, ctx.FilterPayload())

		return map[string]any{"notified": true}, nil
	},
	hatchet.WithWorkflowEvents("order:paid"),
	hatchet.WithDefaultFilters(hatchet.DefaultFilter{
		Scope:      "shop:42",
		Expression: `input.amount >= payload.min_amount`,
		Payload: map[string]any{
			"min_amount": 100,
			"channel":    "email",
		},
	}),
)
```

将 `notify` 注册到 Worker 后，再发布事件：

```go
scope := "shop:42"

err := client.Events().Push(
	ctx,
	"order:paid",
	OrderInput{
		OrderID: "order-001",
		Amount:  150,
	},
	hatchet.WithFilterScope(&scope),
)
```

这个事件会触发任务，因为 scope 匹配，且 `150 >= 100`。这些接口可对照本地的[官方 Go 示例](/Users/huaanhuang/workspace/src/github.com/wegohub/hatchet/sdks/go/examples/on-event/main.go)。

### 3. `input` 和 `payload` 很容易弄反

在 **Filter 的 CEL 表达式**中：

| 变量 | 内容 |
|---|---|
| `input` | 发布事件时提供的业务数据 |
| `payload` | 创建 Filter 时保存的配置数据 |
| `additional_metadata` | 事件的附加 metadata |
| `event_key` | 事件名称 |
| `event_id` | 事件 ID |

因此，上面的表达式比较的是：

```text
事件的 amount >= Filter 配置的 min_amount
```

Filter payload 不会自动合并进业务输入；handler 通过 `ctx.FilterPayload()` 单独读取。CEL 必须返回布尔值，表达式计算失败也会导致不触发。[官方 CEL 说明](https://docs.hatchet.run/v1/cel-expressions#event-filter-expressions)

### 4. 多个 Filter 不是简单的“满足任意一个就运行一次”

这是最需要注意的行为：

| workflow 的过滤器配置 | 事件匹配情况 | 创建的运行数 |
|---|---|---:|
| 没有 Filter | Event key 匹配 | 1 |
| 有 Filter | scope 缺失或不匹配 | 0 |
| 有 Filter | scope 匹配，但条件不成立 | 0 |
| 有两个 Filter | 两个都匹配 | **2** |

**每个命中的 Filter 都会触发一次运行。** 例如一个 Filter 配置邮件渠道，另一个配置短信渠道，同一个订单事件可以产生两次执行，各自拿到不同的 Filter payload。[官方过滤器规则](https://docs.hatchet.run/v1/events#event-filters)

### 5. 默认 Filter 与动态 Filter

- **默认 Filter**：通过 `WithDefaultFilters` 随任务定义注册，适合固定规则。更新 workflow 定义时会重新同步，Filter ID 不保证稳定。
- **动态 Filter**：通过 `client.Filters().Create(...)` 绑定已有 workflow，适合用户订阅、动态阈值和通知渠道；还可查询、修改、删除。它仍需要 workflow 已经订阅对应事件 key。[官方创建方式](https://docs.hatchet.run/v1/events#event-filters)

结合你的 wego codec 设计：**CEL 在 Hatchet 服务端计算，读取的是服务端收到的 JSON**。如果业务载荷被加密或卸载，过滤条件应读取明文 `input.routing.*`；Filter 配置若也封包，则对应读取 `payload.routing.*`。Handler 内解码后能读到业务数据，并不能让服务端 CEL 同样读到。


配置入口就是你写的这组。**如果只想按 scope 匹配并附带 `target_msg`，Expression 应写 `"true"`，不要留空。**

```go
worker.WithTask(
	pb.Greeter_SayHello_FullMethodName,
	task.WithEvents("say/hello:invoke"),
	task.WithDefaultFilters(
		task.DefaultFilter{
			Scope:      "test-feature", // 发布事件时必须传相同 scope。
			Expression: "true",         // scope 匹配后直接触发。
			Payload: map[string]any{
				"target_msg": "hello-filter", // 附带给 handler 的业务配置。
			},
		},
	),
),
```

客户端对应这样发送：

```go
scope := "test-feature"

err := conn.Events().Push(
	ctx,
	"say/hello:invoke",
	&pb.Request{
		Message: "hello event",
	},
	scope,
)
```

你当前 `Test_Event` 没传 scope，**不会匹配这个 Filter**。`Payload["target_msg"]` 本身也不是过滤条件，只是附加数据。

handler 中读取：

```go
info, ok := task.Info(ctx)
if ok {
	// req.Message 是 "hello event"。
	// FilterPayload["target_msg"] 是 "hello-filter"。
	target := info.FilterPayload["target_msg"]
	log.Info(ctx, "收到过滤事件", "target_msg", target)
}
```

如果希望“事件中的消息等于 Filter 配置的目标消息才触发”，需要通过 **routing** 暴露比较字段，因为 wego 会将业务 Payload 封包并经过 codec，CEL 不能直接读取其中的业务字段。

Filter 改为：

```go
task.DefaultFilter{
	Scope:      "test-feature",
	Expression: `input.routing.message == payload.routing.target_msg`,
	Payload: model.Input{
		// Value 经过 codec，解码后成为 handler 的 FilterPayload。
		Value: map[string]any{
			"target_msg": "hello-filter",
		},
		// Routing 保持明文，供 Hatchet CEL 判断。
		Routing: map[string]any{
			"target_msg": "hello-filter",
		},
	},
}
```

发布事件：

```go
scope := "test-feature"

err := conn.Events().Push(
	ctx,
	"say/hello:invoke",
	model.Input{
		Value: &pb.Request{
			Message: "hello-filter",
		},
		Routing: map[string]any{
			"message": "hello-filter",
		},
	},
	scope,
)
```

对应关系是：

| wego 配置 | CEL / handler 中的用途 |
|---|---|
| 事件 `model.Input.Routing` | CEL 的 `input.routing.*` |
| Filter `model.Input.Routing` | CEL 的 `payload.routing.*` |
| 事件 `model.Input.Value` | 解码后传给 handler 的请求 |
| Filter `model.Input.Value` | 解码后通过 `task.Info(ctx).FilterPayload` 读取 |

当前实现已有同样用法，可参考 [wego Filter 示例](/Users/huaanhuang/workspace/src/github.com/wegohub/hatchet/sdks/wego/examples/scenarios/filters.go:49)。
