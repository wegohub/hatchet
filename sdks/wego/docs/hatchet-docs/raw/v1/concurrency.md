# Concurrency Control in Hatchet Tasks

Hatchet provides powerful concurrency control features to help you manage the execution of your tasks. This is particularly useful when you have tasks that may be triggered frequently or have long-running steps, and you want to limit the number of concurrent executions to prevent overloading your system, ensure fairness, or avoid race conditions. Concurrency strategies can be added to both tasks and workflows.

> **Info:** This page will discuss concurrency **keys** often. The key is the result of
>   evaluating a [CEL expression](/v1/cel-expressions) that you provide on
>   your tasks or workflows. The CEL expression you create can reference the
>   `input` to the workflow and the `additional_metadata`.
>
> For instance, the expression `input.user_id + ':' + additional_metadata.foobar` on a workflow run triggered with input `{"user_id": "abc"}` and additional metadata `{"foobar": "bazqux"}` would evaluate to `abc:bazqux`.

## Why use concurrency control?

You should primarily use concurrency control when you need to ensure fair access to resources across your application's users, projects, or organizations. By limiting the number of in-flight tasks for a particular user, you can prevent that user from monopolizing the system.


Concurrency control also lets you limit the number of runs for a workflow globally, if you use a static CEL expression, such as `'global'`. This and [rate limiting](/v1/rate-limits) are the recommended mechanisms for setting per-workflow throughput limits.

> **Info:** Concurrency limits how many runs happen at once. To instead control how much
>   of a worker's capacity a single run consumes, so a heavy task takes up more
>   slots than a light one, see [Task Slot
>   Cost](/v1/advanced-assignment/slot-cost).

## Available Strategies:

- [**Group Round Robin**](#group-round-robin) queues incoming task and workflow runs and only dispatches them to workers and triggers them once an available slot is open.
- [**Cancel In Progress**](#cancel-in-progress) cancels in-progress instances of the task or workflow with matching concurrency keys in order to free up slots for the newly-triggered task or workflow run.
- [**Cancel Newest**](#cancel-newest) cancels any incoming task or workflow runs for a key once the number of runs in a running state for that key has reached a provided limit.
- [**Cancel Queued Except Newest**](#cancel-queued-except-newest) cancels any incoming task or workflow runs for a key once it has reached a provided limit, except for the most recently enqueued.
- [**Cancel Queued Except Oldest**](#cancel-queued-except-oldest) cancels any incoming task or workflow runs for a key once it has reached a provided limit, except for the least recently enqueued.

## Group Round Robin

When a new task instance is triggered, the Group Round Robin strategy will:

1. Determine the key that the run belongs to based on the [CEL expression](/v1/cel-expressions) defined in the task or workflow's concurrency configuration.
2. Check if there are any available slots for the computed concurrency key based on the maximum number of concurrent runs allowed by the concurrency configuration.
3. If a slot is available, the new task or workflow starts executing immediately.
4. If no slots are available, the new task or workflow is added to a queue for its key.
5. When a running task instance completes and a slot becomes available for a group, the next queued instance for that group (in round-robin order) is dequeued and starts executing.

Group round robin ensures that task instances are processed fairly across different groups, preventing any one group from monopolizing the available resources. It also helps to reduce latency for instances within each group, as they are processed in a round-robin fashion rather than strictly in the order they were triggered. Group round robin is also useful as a global concurrency control for the maximum number of runs of a single task or workflow that you want executing at any given time, regardless of any grouping. You can set a constant concurrency key as the expression, such as `'*'`, to enable this global concurrency control behavior.


To use this strategy, set the `GROUP_ROUND_ROBIN` limit strategy along with a `max_runs` limit and a key expression:

#### Python

```python
class WorkflowInput(BaseModel):
    group: str


concurrency_limit_rr_workflow = hatchet.workflow(
    name="ConcurrencyDemoWorkflowRR",
    concurrency=ConcurrencyExpression(
        expression="input.group",
        max_runs=1,
        limit_strategy=ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
    ),
    input_validator=WorkflowInput,
)
```

#### Typescript

```typescript
export const simpleConcurrency = hatchet.workflow({
  name: 'simple-concurrency',
  concurrency: {
    maxRuns: 1,
    limitStrategy: ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
    expression: 'input.GroupKey',
  },
});
```

#### Go

```go
var maxRuns int32 = 1
strategy := hatchet.GroupRoundRobin

return client.NewStandaloneTask("simple-concurrency",
	func(ctx hatchet.Context, input ConcurrencyInput) (*TransformedOutput, error) {
		// Random sleep between 200ms and 1000ms
		time.Sleep(time.Duration(200+rand.Intn(800)) * time.Millisecond)

		return &TransformedOutput{
			TransformedMessage: input.Message,
		}, nil
	},
	hatchet.WithWorkflowConcurrency(hatchet.Concurrency{
		Expression:    "input.GroupKey",
		MaxRuns:       &maxRuns,
		LimitStrategy: &strategy,
	}),
)
```

#### Ruby

```ruby
CONCURRENCY_LIMIT_RR_WORKFLOW = HATCHET.workflow(
  name: "ConcurrencyDemoWorkflowRR",
  concurrency: Hatchet::ConcurrencyExpression.new(
    expression: "input.group",
    max_runs: 1,
    limit_strategy: :group_round_robin
  )
)

CONCURRENCY_LIMIT_RR_WORKFLOW.task(:step1) do |input, ctx|
  puts "starting step1"
  sleep 2
  puts "finished step1"
end
```

## Cancel In Progress

When a new task instance is triggered, the Cancel In Progress strategy will:

1. Determine the key that the run belongs to based on the [CEL expression](/v1/cel-expressions) defined in the task or workflow's concurrency configuration.
2. Check if there are any available slots for the computed concurrency key based on the maximum number of concurrent runs allowed by the concurrency configuration.
3. If a slot is available, the new task or workflow starts executing immediately.
4. If no slots are available, a running instance of the task or workflow with the same concurrency key will be cancelled to free up a slot, and the new instance will start executing immediately.

Cancel In Progress ensures that the most recently triggered runs always take priority over older ones, which is useful when newer inputs supersede older ones and in-progress work becomes stale or irrelevant as soon as a newer run arrives. It's particularly well suited for user-facing interactions where only the latest input matters (such as chat messages, form submissions, or search-as-you-type), for resource-intensive tasks where it's more efficient to abandon an old run than wait for it to complete, and for any scenario where you want to prioritize processing the most recent data or events over older ones.

To use this strategy, set the `CANCEL_IN_PROGRESS` limit strategy along with a `max_runs` limit and a key expression:

#### Python

```python
class WorkflowInput(BaseModel):
    group: str


concurrency_cancel_in_progress_workflow = hatchet.workflow(
    name="ConcurrencyCancelInProgress",
    concurrency=ConcurrencyExpression(
        expression="input.group",
        max_runs=1,
        limit_strategy=ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS,
    ),
    input_validator=WorkflowInput,
)
```

#### Typescript

```typescript
export const concurrencyCancelInProgressWorkflow = hatchet.workflow({
  name: 'concurrencycancelinprogress',
  concurrency: {
    expression: 'input.group',
    maxRuns: 1,
    limitStrategy: ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS,
  },
});
```

#### Go

```go
var maxRuns int32 = 1
strategy := hatchet.CancelInProgress

return client.NewStandaloneTask("cancel-in-progress",
	func(ctx hatchet.Context, input ConcurrencyInput) (*TransformedOutput, error) {
		// Random sleep between 200ms and 1000ms
		time.Sleep(time.Duration(200+rand.Intn(800)) * time.Millisecond)

		return &TransformedOutput{
			TransformedMessage: input.Message,
		}, nil
	},
	hatchet.WithWorkflowConcurrency(hatchet.Concurrency{
		Expression:    "input.GroupKey",
		MaxRuns:       &maxRuns,
		LimitStrategy: &strategy,
	}),
)
```

#### Ruby

```ruby
CONCURRENCY_CANCEL_IN_PROGRESS_WORKFLOW = HATCHET.workflow(
  name: "ConcurrencyCancelInProgress",
  concurrency: Hatchet::ConcurrencyExpression.new(
    expression: "input.group",
    max_runs: 1,
    limit_strategy: :cancel_in_progress
  )
)
```

## Cancel Newest

When a new task instance is triggered, the Cancel Newest strategy will:

1. Determine the key that the run belongs to based on the [CEL expression](/v1/cel-expressions) defined in the task or workflow's concurrency configuration.
2. Check if there are any available slots for the computed concurrency key based on the maximum number of concurrent runs allowed by the concurrency configuration.
3. If a slot is available, the new task or workflow starts executing immediately.
4. If no slots are available, the newly triggered run is cancelled immediately, allowing the in-progress runs to continue uninterrupted.

Cancel Newest is the inverse of Cancel In Progress: rather than preempting running work in favor of new arrivals, it protects in-progress runs from being disrupted by allowing them to complete before any new work for the same key is started. This is useful when you want to guarantee that long-running task instances finish without interference, when the cost of restarting work outweighs the value of processing newer inputs, and when you want to prevent a single group's instances from monopolizing the available slots by rejecting excess runs outright instead of queuing them.

To use this strategy, set the `CANCEL_NEWEST` limit strategy along with a `max_runs` limit and a key expression:

#### Python

```python
class WorkflowInput(BaseModel):
    group: str


concurrency_cancel_newest_workflow = hatchet.workflow(
    name="ConcurrencyCancelNewest",
    concurrency=ConcurrencyExpression(
        expression="input.group",
        max_runs=1,
        limit_strategy=ConcurrencyLimitStrategy.CANCEL_NEWEST,
    ),
    input_validator=WorkflowInput,
)
```

#### Typescript

```typescript
export const concurrencyCancelNewestWorkflow = hatchet.workflow({
  name: 'concurrencycancelnewest',
  concurrency: {
    expression: 'input.group',
    maxRuns: 1,
    limitStrategy: ConcurrencyLimitStrategy.CANCEL_NEWEST,
  },
});
```

#### Go

```go
var maxRuns int32 = 1
strategy := hatchet.CancelNewest

return client.NewStandaloneTask("cancel-newest",
	func(ctx hatchet.Context, input ConcurrencyInput) (*TransformedOutput, error) {
		// Random sleep between 200ms and 1000ms
		time.Sleep(time.Duration(200+rand.Intn(800)) * time.Millisecond)

		return &TransformedOutput{
			TransformedMessage: input.Message,
		}, nil
	},
	hatchet.WithWorkflowConcurrency(hatchet.Concurrency{
		Expression:    "input.GroupKey",
		MaxRuns:       &maxRuns,
		LimitStrategy: &strategy,
	}),
)
```

#### Ruby

```ruby
CONCURRENCY_CANCEL_NEWEST_WORKFLOW = HATCHET.workflow(
  name: "ConcurrencyCancelNewest",
  concurrency: Hatchet::ConcurrencyExpression.new(
    expression: "input.group",
    max_runs: 1,
    limit_strategy: :cancel_newest
  )
)
```

## Cancel Queued Except Newest

When a new task instance is triggered, the Cancel Queued Except Newest strategy will:

1. Determine the key that the run belongs to based on the [CEL expression](/v1/cel-expressions) defined in the task or workflow's concurrency configuration.
2. Check if there are any available slots for the computed concurrency key based on the maximum number of concurrent runs allowed by the concurrency configuration.
3. If a slot is available, the new task or workflow starts executing immediately.
4. If no slots are available, the newly triggered run is cancelled immediately, _except for the most recent `max_runs` tasks_, which will be kept in the queued state until there is available slot capacity.

Cancel Queued Except Newest is very similar to Cancel Newest, except that it will keep `max_runs` of the most recent enqueued tasks available until the in-progress tasks are done running.
At that point, it will enqueue the most recent `max_runs` tasks that would have otherwise been cancelled. This is useful for cases in which you want an in-progress task to block new tasks, but still guarantee a trailing run.

As a more concrete example, take the following scenario with three tasks A, B, and C and `max_runs=1`:

- Task A will start executing immediately.
- Task B will stay in the queued state, waiting for A to finish, or another task to be enqueued.
- Task C, as the most recent, will replace Task B in the queued state, causing Task B to be cancelled.
- Task A finished execution--Task C is waiting in the queued state, and will start running immediately after.

To use this strategy, set the `CANCEL_QUEUED_EXCEPT_NEWEST` limit strategy along with a `max_runs` limit and a key expression:

#### Python

```python
class WorkflowInput(BaseModel):
    group: str


concurrency_cancel_queued_except_newest_workflow = hatchet.workflow(
    name="ConcurrencyCancelQueuedExceptNewest",
    concurrency=ConcurrencyExpression(
        expression="input.group",
        max_runs=1,
        limit_strategy=ConcurrencyLimitStrategy.CANCEL_QUEUED_EXCEPT_NEWEST,
    ),
    input_validator=WorkflowInput,
)
```

#### Typescript

```typescript
export const concurrencyCancelQueuedExceptNewestWorkflow = hatchet.workflow<
  WorkflowInput,
  WorkflowOutput
>({
  name: 'concurrencycancelqueuedexceptnewest',
  concurrency: {
    expression: 'input.group',
    maxRuns: 1,
    limitStrategy: ConcurrencyLimitStrategy.CANCEL_QUEUED_EXCEPT_NEWEST,
  },
});
```

#### Go

```go
var maxRuns int32 = 1
strategy := hatchet.CancelQueuedExceptNewest

return client.NewStandaloneTask("cancel-queued-except-newest",
	func(ctx hatchet.Context, input ConcurrencyInput) (*TransformedOutput, error) {
		// Random sleep between 200ms and 1000ms
		time.Sleep(time.Duration(200+rand.Intn(800)) * time.Millisecond)

		return &TransformedOutput{
			TransformedMessage: input.Message,
		}, nil
	},
	hatchet.WithWorkflowConcurrency(hatchet.Concurrency{
		Expression:    "input.GroupKey",
		MaxRuns:       &maxRuns,
		LimitStrategy: &strategy,
	}),
)
```

#### Ruby

```ruby
CONCURRENCY_CANCEL_QUEUED_EXCEPT_NEWEST_WORKFLOW = HATCHET.workflow(
  name: "ConcurrencyCancelQueuedExceptNewest",
  concurrency: Hatchet::ConcurrencyExpression.new(
    expression: "input.group",
    max_runs: 1,
    limit_strategy: :cancel_queued_except_newest
  )
)
```

## Cancel Queued Except Oldest

When a new task instance is triggered, the Cancel Queued Except Oldest strategy will:

1. Determine the key that the run belongs to based on the [CEL expression](/v1/cel-expressions) defined in the task or workflow's concurrency configuration.
2. Check if there are any available slots for the computed concurrency key based on the maximum number of concurrent runs allowed by the concurrency configuration.
3. If a slot is available, the new task or workflow starts executing immediately.
4. If no slots are available, the newly triggered run is cancelled immediately, _except for the oldest `max_runs` still-queued tasks_, which will be kept in the queued state until there is available slot capacity.

Cancel Queued Except Oldest is the opposite of Cancel Queued Except Newest: instead of queuing the most recently enqueued tasks, it cancels newer tasks, and keeps `max_runs` of the earliest tasks available until the in-progress tasks are done running.
At that point, it will enqueue the oldest `max_runs` tasks that would have otherwise been cancelled. This is useful for cases in which you want to guarantee that the longest-waiting runs for a key are the ones that eventually get to run, while still allowing
the initial task to complete.

As a more concrete example, take the following scenario with three tasks A, B, and C and `max_runs=1`:

- Task A will start executing immediately.
- Task B will stay in the queued state, waiting for A to finish, or another task to be enqueued.
- Task C, as the newer of the two queued tasks, will be cancelled immediately, leaving Task B in the queued state.
- Task A finishes execution--Task B is waiting in the queued state, and will start running immediately after.

To use this strategy, set the `CANCEL_QUEUED_EXCEPT_OLDEST` limit strategy along with a `max_runs` limit and a key expression:

#### Python

```python
class WorkflowInput(BaseModel):
    group: str


concurrency_cancel_queued_except_oldest_workflow = hatchet.workflow(
    name="ConcurrencyCancelQueuedExceptOldest",
    concurrency=ConcurrencyExpression(
        expression="input.group",
        max_runs=1,
        limit_strategy=ConcurrencyLimitStrategy.CANCEL_QUEUED_EXCEPT_OLDEST,
    ),
    input_validator=WorkflowInput,
)
```

#### Typescript

```typescript
export const concurrencyCancelQueuedExceptOldestWorkflow = hatchet.workflow<
  WorkflowInput,
  WorkflowOutput
>({
  name: 'concurrencycancelqueuedexceptoldest',
  concurrency: {
    expression: 'input.group',
    maxRuns: 1,
    limitStrategy: ConcurrencyLimitStrategy.CANCEL_QUEUED_EXCEPT_OLDEST,
  },
});
```

#### Go

```go
var maxRuns int32 = 1
strategy := hatchet.CancelQueuedExceptOldest

return client.NewStandaloneTask("cancel-queued-except-oldest",
	func(ctx hatchet.Context, input ConcurrencyInput) (*TransformedOutput, error) {
		// Random sleep between 200ms and 1000ms
		time.Sleep(time.Duration(200+rand.Intn(800)) * time.Millisecond)

		return &TransformedOutput{
			TransformedMessage: input.Message,
		}, nil
	},
	hatchet.WithWorkflowConcurrency(hatchet.Concurrency{
		Expression:    "input.GroupKey",
		MaxRuns:       &maxRuns,
		LimitStrategy: &strategy,
	}),
)
```

#### Ruby

```ruby
CONCURRENCY_CANCEL_QUEUED_EXCEPT_OLDEST_WORKFLOW = HATCHET.workflow(
  name: "ConcurrencyCancelQueuedExceptOldest",
  concurrency: Hatchet::ConcurrencyExpression.new(
    expression: "input.group",
    max_runs: 1,
    limit_strategy: :cancel_queued_except_oldest
  )
)
```

## Multiple concurrency strategies

You can also combine multiple concurrency strategies to create a more complex concurrency control system. For example, you can use one group key to represent a specific team, and another group to represent a specific resource in that team, giving you more control over the rate at which tasks are executed.

#### Python

```python
class WorkflowInput(BaseModel):
    name: str
    digit: str


concurrency_workflow_level_workflow = hatchet.workflow(
    name="ConcurrencyWorkflowLevel",
    input_validator=WorkflowInput,
    concurrency=[
        ConcurrencyExpression(
            expression="input.digit",
            max_runs=DIGIT_MAX_RUNS,
            limit_strategy=ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
        ),
        ConcurrencyExpression(
            expression="input.name",
            max_runs=NAME_MAX_RUNS,
            limit_strategy=ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
        ),
    ],
)
```

#### Typescript

```typescript
export const multipleConcurrencyKeys = hatchet.workflow({
  name: 'simple-concurrency',
  concurrency: [
    {
      maxRuns: 1,
      limitStrategy: ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
      expression: 'input.Tier',
    },
    {
      maxRuns: 1,
      limitStrategy: ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
      expression: 'input.Account',
    },
  ],
});
```

#### Go

```go
strategy := hatchet.GroupRoundRobin
var maxRuns int32 = 20

return client.NewStandaloneTask("multi-concurrency",
	func(ctx hatchet.Context, input ConcurrencyInput) (*TransformedOutput, error) {
		// Random sleep between 200ms and 1000ms
		time.Sleep(time.Duration(200+rand.Intn(800)) * time.Millisecond)

		return &TransformedOutput{
			TransformedMessage: input.Message,
		}, nil
	},
	hatchet.WithWorkflowConcurrency(
		hatchet.Concurrency{
			Expression:    "input.Tier",
			MaxRuns:       &maxRuns,
			LimitStrategy: &strategy,
		}, hatchet.Concurrency{
			Expression:    "input.Account",
			MaxRuns:       &maxRuns,
			LimitStrategy: &strategy,
		},
	),
)
```

#### Ruby

```ruby
CONCURRENCY_WORKFLOW_LEVEL_WORKFLOW = HATCHET.workflow(
  name: "ConcurrencyWorkflowLevel",
  concurrency: [
    Hatchet::ConcurrencyExpression.new(
      expression: "input.digit",
      max_runs: DIGIT_MAX_RUNS_WL,
      limit_strategy: :group_round_robin
    ),
    Hatchet::ConcurrencyExpression.new(
      expression: "input.name",
      max_runs: NAME_MAX_RUNS_WL,
      limit_strategy: :group_round_robin
    )
  ]
)

CONCURRENCY_WORKFLOW_LEVEL_WORKFLOW.task(:task_1) do |input, ctx|
  sleep SLEEP_TIME_WL
end

CONCURRENCY_WORKFLOW_LEVEL_WORKFLOW.task(:task_2) do |input, ctx|
  sleep SLEEP_TIME_WL
end
```

## Shared concurrency across workflows

> **Info:** Shared concurrency strategies require engine version 0.106.0 or greater.

All of the strategies above are scoped to a single workflow, but you can also share concurrency strategies between workflows. These are shared via the name of the strategy: every task in every workflow that declares the same name consumes the same concurrency limit.

#### Python

```python
# A tenant-scoped strategy is shared across workflows: every task declaring the same name
# consumes the same concurrency limit. The definition rides on workflow registration and
# re-registering the name updates it in place.
shared_limit = ConcurrencyExpression(
    expression="input.group",
    max_runs=1,
    limit_strategy=ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
    name="example-shared-limit",
    is_tenant_scoped=True,
)


@hatchet.task(input_validator=WorkflowInput, concurrency=[shared_limit])
def task_a(input: WorkflowInput, ctx: Context) -> RunWindow:
    return run_window(1.5)


@hatchet.task(input_validator=WorkflowInput, concurrency=[shared_limit])
def task_b(input: WorkflowInput, ctx: Context) -> RunWindow:
    return run_window(1.5)
```

#### Typescript

```typescript
// A tenant-scoped strategy is shared across workflows: every task declaring the same name
// consumes the same concurrency limit. The definition rides on workflow registration and
// re-registering the name updates it in place.
export const sharedLimit: Concurrency = {
  name: 'ts-example-shared-limit',
  isTenantScoped: true,
  expression: 'input.group',
  maxRuns: 1,
  limitStrategy: ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
};

const runWindowTask = async (): Promise => {
  const startMs = Date.now();
  await sleep(SLEEP_TIME_MS);
  return { startMs, endMs: Date.now() };
};

export const concurrencySharedWorkflowA = hatchet.workflow({
  name: 'concurrency-shared-a',
});

concurrencySharedWorkflowA.task({
  name: 'shared-task',
  concurrency: [sharedLimit],
  fn: runWindowTask,
});

export const concurrencySharedWorkflowB = hatchet.workflow({
  name: 'concurrency-shared-b',
});

concurrencySharedWorkflowB.task({
  name: 'shared-task',
  concurrency: [sharedLimit],
  fn: runWindowTask,
});
```

#### Go

```go
var maxRuns int32 = 1
strategy := hatchet.GroupRoundRobin

// A tenant-scoped strategy is shared across workflows: every task declaring the
// same name consumes the same concurrency limit. Re-registering the name updates
// it in place.
sharedLimit := hatchet.Concurrency{
	Name:           "example-shared-limit",
	IsTenantScoped: true,
	Expression:     "input.Account",
	MaxRuns:        &maxRuns,
	LimitStrategy:  &strategy,
}

// two different tasks, in two different workflows, consuming one limit
syncCrm := client.NewStandaloneTask("sync-crm",
	func(ctx hatchet.Context, input ConcurrencyInput) (*TransformedOutput, error) {
		return &TransformedOutput{TransformedMessage: input.Message}, nil
	},
	hatchet.WithWorkflowConcurrency(sharedLimit),
)

generateReport := client.NewStandaloneTask("generate-report",
	func(ctx hatchet.Context, input ConcurrencyInput) (*TransformedOutput, error) {
		return &TransformedOutput{TransformedMessage: input.Message}, nil
	},
	hatchet.WithWorkflowConcurrency(sharedLimit),
)

return syncCrm, generateReport
```

#### Ruby

```ruby
# A tenant-scoped strategy is shared across workflows: every task declaring the same
# name consumes the same concurrency limit. Re-registering the name updates it in place.
SHARED_LIMIT = Hatchet::ConcurrencyExpression.new(
  expression: "input.group",
  max_runs: 1,
  limit_strategy: :group_round_robin,
  name: "example-shared-limit",
  is_tenant_scoped: true
)

# two different workflows consuming one limit
SYNC_WORKFLOW = HATCHET.workflow(
  name: "SyncCrm",
  concurrency: SHARED_LIMIT
)

SYNC_WORKFLOW.task(:sync) do |input, ctx|
  puts "syncing crm"
  sleep 2
end

REPORT_WORKFLOW = HATCHET.workflow(
  name: "GenerateReport",
  concurrency: SHARED_LIMIT
)

REPORT_WORKFLOW.task(:report) do |input, ctx|
  puts "generating report"
  sleep 2
end
```

If you edit a shared concurrency strategy, it will edit in-place on the next worker deployment and re-registration of the workflow.

Shared strategies can be mixed freely with inline (workflow-scoped) entries on the same task: the position in the concurrency list is the chain order, so a shared entry may come before or after an inline one, and every limit in the chain applies at once.

> **Warning:** When multiple workflows chain the same shared strategies, they must order
>   those shared strategies consistently relative to each other. Registrations
>   whose chains order the same shared strategies differently are rejected, since
>   inconsistent orders can deadlock runs that hold one shared slot while waiting
>   for another.

## Dynamic limits with a max runs expression

> **Info:** Max runs expressions require engine version 0.106.0 or greater.

The `max_runs` limit on a concurrency strategy is normally a fixed number that applies to every key. You can instead compute the limit per key with a CEL expression over the task's input, so different groups get different limits from a single strategy. For example, let's say you want different pricing tiers in your application to correspond to different limits: you can use the `max_runs` expression to define something like `input.tier == 'premium' ? 10 : 1`.

#### Python

```python
# max_runs accepts an int or a CEL expression string. With an expression, each
# concurrency group's limit is computed from the task's input.
@hatchet.task(
    input_validator=WorkflowInput,
    concurrency=[
        ConcurrencyExpression(
            expression="input.account",
            max_runs="input.tier == 'premium' ? 10 : 1",
            limit_strategy=ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
        ),
    ],
)
def dynamic_task(input: WorkflowInput, ctx: Context) -> None:
    print("running for account", input.account)
```

#### Typescript

```typescript
// maxRuns accepts a number or a CEL expression string. With an expression, each
// concurrency group's limit is computed from the task's input.
export const concurrencyDynamicWorkflow = hatchet.workflow({
  name: 'concurrency-dynamic',
});

concurrencyDynamicWorkflow.task({
  name: 'dynamic-task',
  concurrency: [
    {
      expression: 'input.account',
      maxRuns: "input.tier == 'premium' ? 10 : 1",
      limitStrategy: ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
    },
  ],
  fn: async (input) => ({ account: input.account }),
});
```

#### Go

```go
var maxRuns int32 = 1
strategy := hatchet.GroupRoundRobin

// MaxRunsExpression computes each concurrency group's limit from the task's
// input; MaxRuns stays the static default.
maxRunsExpression := "input.Tier == 'premium' ? 10 : 1"

return client.NewStandaloneTask("dynamic-concurrency",
	func(ctx hatchet.Context, input ConcurrencyInput) (*TransformedOutput, error) {
		return &TransformedOutput{TransformedMessage: input.Message}, nil
	},
	hatchet.WithWorkflowConcurrency(hatchet.Concurrency{
		Expression:        "input.Account",
		MaxRuns:           &maxRuns,
		LimitStrategy:     &strategy,
		MaxRunsExpression: &maxRunsExpression,
	}),
)
```

#### Ruby

```ruby
# max_runs accepts an Integer or a CEL expression String. With an expression, each
# concurrency group's limit is computed from the task's input.
CONCURRENCY_DYNAMIC_WORKFLOW = HATCHET.workflow(
  name: "ConcurrencyDynamicWorkflow",
  concurrency: Hatchet::ConcurrencyExpression.new(
    expression: "input.account",
    max_runs: "input.tier == 'premium' ? 10 : 1",
    limit_strategy: :group_round_robin
  )
)

CONCURRENCY_DYNAMIC_WORKFLOW.task(:step1) do |input, ctx|
  puts "running for account #{input["account"]}"
  sleep 2
end
```

If an expression fails to evaluate, it will result in a task failure with the evaluation error as the task failure message.

If you edit the expression, the new limit will get picked up on the next triggered task. This does not apply to replays of existing tasks: the task has to be a new task. If the limit gets lowered, the `GROUP_ROUND_ROBIN` strategy and the cancel-queued strategies let running work finish and stop filling until the group drains below the new limit, while `CANCEL_IN_PROGRESS` cancels running work above it.
