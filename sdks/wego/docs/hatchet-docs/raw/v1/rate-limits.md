# Rate Limiting Step Runs in Hatchet

Hatchet allows you to enforce rate limits on task runs, enabling you to control the rate at which your service runs consume resources, such as external API calls, database queries, or other services. By defining rate limits, you can prevent task runs from exceeding a certain number of requests per time window (e.g., per second, minute, or hour), ensuring efficient resource utilization and avoiding overloading external services.

The state of active rate limits can be viewed in the dashboard in the `Rate Limit` resource tab.

## Dynamic vs Static Rate Limits

Hatchet offers two patterns for Rate Limiting task runs:

1. [Dynamic Rate Limits](#dynamic-rate-limits): Allows for complex rate limiting scenarios, such as per-user limits, by using `input` or `additional_metadata` keys to upsert a limit at runtime.
2. [Static Rate Limits](#static-rate-limits): Allows for simple rate limiting for resources known prior to runtime (e.g., external APIs).

## Dynamic Rate Limits

Dynamic rate limits are ideal for complex scenarios where rate limits need to be partitioned by resources that are only known at runtime.

This pattern is especially useful for:

1. Rate limiting individual users or tenants
2. Implementing variable rate limits based on subscription tiers or user roles
3. Dynamically adjusting limits based on real-time system load or other factors

### How It Works

1. Define the dynamic rate limit key with a [CEL (Common Expression Language) expression](/v1/cel-expressions) on the key, referencing either `input` or `additional_metadata`.
2. Provide this key as part of the workflow trigger or event `input` or `additional_metadata` at runtime.
3. Hatchet will create or update the rate limit based on the provided key and enforce it for the step run.

> **Info:** Note: Dynamic keys are a shared resource, this means the same rendered cel on
>   multiple steps will be treated as one global rate limit.

### Declaring and Consuming Dynamic Rate Limits

#### Ruby

> Note: `dynamic_key` must be a CEL expression. `units` and `limits` can be either an integer or a CEL expression.

We can add one or more rate limits to a task by adding the `rate_limits` configuration to the task definition.

```python
@rate_limit_workflow.task(
    rate_limits=[
        RateLimit(
            dynamic_key="input.user_id",
            units=1,
            limit=10,
            duration=RateLimitDuration.MINUTE,
        )
    ]
)
def step_2(input: RateLimitInput, ctx: Context) -> None:
    print("executed step_2")
```

#### Tab 2

> Note: `dynamicKey` must be a CEL expression. `units` and `limit` can be either an integer or a CEL expression.

We can add one or more rate limits to a task by adding the `rate_limits` configuration to the task definition.

```typescript
const task2 = hatchet.task({
  name: 'task2',
  fn: (input: { userId: string }, ctx) => {
    ctx.logger.info(`executed task2 for user: ${input.userId}`);
  },
  rateLimits: [
    {
      dynamicKey: 'input.userId',
      units: 1,
      limit: 10,
      duration: RateLimitDuration.MINUTE,
    },
  ],
});
```

#### Tab 3

> Note: Go requires both a key and KeyExpr be set and the LimitValueExpr must be a CEL.

```go
userUnits := 1
userLimit := "10"
duration := hatchet.Minute
dynamicTask := client.NewStandaloneTask("task2",
	func(ctx hatchet.Context, input APIRequest) (string, error) {
		log.Printf("executed task2 for user: %s", input.UserID)

		return "completed", nil
	},
	hatchet.WithRateLimits(&hatchet.RateLimit{
		Key:            "input.userId",
		Units:          &userUnits,
		LimitValueExpr: &userLimit,
		Duration:       &duration,
	}),
)
```

#### Tab 4

```ruby
RATE_LIMIT_WORKFLOW.task(
  :step_2,
  rate_limits: [
    Hatchet::RateLimit.new(
      dynamic_key: "input.user_id",
      units: 1,
      limit: 10,
      duration: :minute
    )
  ]
) do |input, ctx|
  puts "executed step_2"
end
```

## Static Rate Limits

Static Rate Limits (formerly known as Global Rate Limits) are defined as part of your worker startup lifecycle prior to runtime. This model provides a single "source of truth" for pre-defined resources such as:

1. External API resources that have a rate limit across all users or tenants
2. Database connection pools with a maximum number of concurrent connections
3. Shared computing resources with limited capacity

### How It Works

1. Declare static rate limits using the `put_rate_limit` method in the `Admin` client before starting your worker.
2. Specify the units of consumption for a specific rate limit key in each step definition using the `rate_limits` configuration.
3. Hatchet enforces the defined rate limits by tracking the number of units consumed by each step run across all workflow runs.

If a step run exceeds the rate limit, Hatchet re-queues the step run until the rate limit is no longer exceeded.

### Declaring Static Limits

Define the static rate limits that can be consumed by any step run across all workflow runs using the `put_rate_limit` method in the `Admin` client within your code.

#### Ruby

```python
RATE_LIMIT_KEY = "test-limit"

hatchet.rate_limits.put(RATE_LIMIT_KEY, 2, RateLimitDuration.SECOND)
```

#### Tab 2

{" "}

```typescript
hatchet.ratelimits.upsert({
  key: 'api-service-rate-limit',
  limit: 10,
  duration: RateLimitDuration.SECOND,
});
```

#### Tab 3

```go
err = client.RateLimits().Upsert(features.CreateRatelimitOpts{
	Key:      RATE_LIMIT_KEY,
	Limit:    10,
	Duration: hatchet.Second,
})
if err != nil {
	log.Fatalf("failed to create rate limit: %v", err)
}
```

#### Tab 4

```ruby
def main
  HATCHET.rate_limits.put(RATE_LIMIT_KEY, 2, :second)

  worker = HATCHET.worker(
    "rate-limit-worker", slots: 10, workflows: [RATE_LIMIT_WORKFLOW]
  )
  worker.start
end
```

### Consuming Static Rate Limits

With your rate limit key defined, specify the units of consumption for a specific key in each step definition by adding the `rate_limits` configuration to your step definition in your workflow.

#### Ruby

```python
RATE_LIMIT_KEY = "test-limit"


@rate_limit_workflow.task(rate_limits=[RateLimit(static_key=RATE_LIMIT_KEY, units=1)])
def step_1(input: RateLimitInput, ctx: Context) -> None:
    print("executed step_1")
```

#### Tab 2

```typescript
const RATE_LIMIT_KEY = 'api-service-rate-limit';

const task1 = hatchet.task({
  name: 'task1',
  rateLimits: [
    {
      staticKey: RATE_LIMIT_KEY,
      units: 1,
    },
  ],
  fn: (_input, ctx) => {
    ctx.logger.info('executed task1');
  },
});
```

#### Tab 3

```go
units := 1
staticTask := client.NewStandaloneTask("task1",
	func(ctx hatchet.Context, input APIRequest) (string, error) {
		log.Println("executed task1")

		return "completed", nil
	},
	hatchet.WithRateLimits(&hatchet.RateLimit{
		Key:   RATE_LIMIT_KEY,
		Units: &units,
	}),
)
```

#### Tab 4

```ruby
RATE_LIMIT_KEY = "test-limit"

RATE_LIMIT_WORKFLOW.task(
  :step_1,
  rate_limits: [Hatchet::RateLimit.new(static_key: RATE_LIMIT_KEY, units: 1)]
) do |input, ctx|
  puts "executed step_1"
end
```

### Limiting Workflow Runs

To rate limit an entire workflow run, it's recommended to specify the rate limit configuration on the entry step (i.e., the first step in the workflow). This will gate the execution of all downstream steps in the workflow.
