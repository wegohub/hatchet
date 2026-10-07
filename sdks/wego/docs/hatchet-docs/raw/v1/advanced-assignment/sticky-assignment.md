# Sticky Worker Assignment (Beta)

> **Info:** This feature is currently in beta and may be subject to change.

Sticky assignment is a task property that allows you to specify that all child tasks should be assigned to the same worker for the duration of its execution. This can be useful in situations like when you need to maintain expensive local memory state across multiple tasks in a workflow or ensure that certain tasks are processed by the same worker for consistency.

> **Warning:** This feature is only compatible with long lived workers, and not webhook
>   workers.

## Setting Sticky Assignment

Sticky assignment is set on the task level by adding the `sticky` property to the task definition. When a task is marked as sticky, all steps within that task will be assigned to the same worker for the duration of the task execution.

> **Warning:** While sticky assignment can be useful in certain scenarios, it can also
>   introduce potential bottlenecks if the assigned worker becomes unavailable, or
>   if local state is not maintained when the job is picked up. Be sure to
>   consider the implications of sticky assignment when designing your tasks and
>   have a plan in place to handle local state issues.

There are two strategies for setting sticky assignment for [DAG](/v1/directed-acyclic-graphs) workflows:

- `SOFT`: All tasks in the workflow will attempt to be assigned to the same worker, but if that worker is unavailable, it will be assigned to another worker.
- `HARD`: All tasks in the workflow will only be assigned to the same worker. If that worker is unavailable, the workflow run will not be assigned to another worker and will remain in a pending state until the original worker becomes available or timeout is reached. (See [Scheduling Timeouts](/v1/timeouts#task-level-timeouts))

#### Ruby

```python
sticky_workflow = hatchet.workflow(
    name="StickyWorkflow",
    # 👀 Specify a sticky strategy when declaring the workflow
    sticky=StickyStrategy.SOFT,
)


@sticky_workflow.task()
def step1a(input: EmptyModel, ctx: Context) -> dict[str, str | None]:
    return {"worker": ctx.worker_id}


@sticky_workflow.task()
def step1b(input: EmptyModel, ctx: Context) -> dict[str, str | None]:
    return {"worker": ctx.worker_id}
```

#### Tab 2

```typescript
export const sticky = hatchet.task({
  name: 'sticky',
  retries: 3,
  sticky: StickyStrategy.SOFT,
  fn: async (_, ctx) => {
    // specify a child workflow to run on the same worker
    const result = await child.run(
      {
        N: 1,
      },
      { sticky: true }
    );

    return {
      result,
    };
  },
});
```

#### Tab 3

```go
func StickyDag(client *hatchet.Client) *hatchet.Workflow {
	stickyDag := client.NewWorkflow("sticky-dag",
		hatchet.WithWorkflowStickyStrategy(hatchet.StickyStrategySoft),
	)

	_ = stickyDag.NewTask("sticky-task",
		func(ctx hatchet.Context, input StickyInput) (interface{}, error) {
			workerId := ctx.Worker().ID()

			return &StickyResult{
				Result: workerId,
			}, nil
		},
	)

	_ = stickyDag.NewTask("sticky-task-2",
		func(ctx hatchet.Context, input StickyInput) (interface{}, error) {
			workerId := ctx.Worker().ID()

			return &StickyResult{
				Result: workerId,
			}, nil
		},
	)

	return stickyDag
}
```

#### Tab 4

```ruby
STICKY_WORKFLOW = HATCHET.workflow(
  name: "StickyWorkflow",
  # Specify a sticky strategy when declaring the workflow
  sticky: :soft
)

STEP1A = STICKY_WORKFLOW.task(:step1a) do |input, ctx|
  { "worker" => ctx.worker.id }
end

STEP1B = STICKY_WORKFLOW.task(:step1b) do |input, ctx|
  { "worker" => ctx.worker.id }
end
```

In this example, the `sticky` property is set to `SOFT`, which means that the task will attempt to be assigned to the same worker for the duration of its execution. If the original worker is unavailable, the task will be assigned to another worker.

## Sticky Child Tasks

It is possible to spawn child tasks on the same worker as the parent task by setting the `sticky` property to `true` in the `run` method options. This can be useful when you need to maintain local state across multiple tasks or ensure that child tasks are processed by the same worker for consistency.

However, the child task must:

1. Specify a `sticky` strategy in the child task's definition
2. Be registered with the same worker as the parent task

If either condition is not met, an error will be thrown when the child task is spawned.

#### Ruby

```python
sticky_child_workflow = hatchet.workflow(
    name="StickyChildWorkflow", sticky=StickyStrategy.SOFT
)


@sticky_workflow.task(parents=[step1a, step1b])
async def step2(input: EmptyModel, ctx: Context) -> dict[str, str | None]:
    ref = await sticky_child_workflow.aio_run(
        sticky=True,
        wait_for_result=False,
    )

    await ref.aio_result()

    return {"worker": ctx.worker_id}


@sticky_child_workflow.task()
def child(input: EmptyModel, ctx: Context) -> dict[str, str | None]:
    return {"worker": ctx.worker_id}
```

#### Tab 2

```typescript
export const sticky = hatchet.task({
  name: 'sticky',
  retries: 3,
  sticky: StickyStrategy.SOFT,
  fn: async (_, ctx) => {
    // specify a child workflow to run on the same worker
    const result = await child.run(
      {
        N: 1,
      },
      { sticky: true }
    );

    return {
      result,
    };
  },
});
```

#### Tab 3

```go
func Sticky(client *hatchet.Client) *hatchet.StandaloneTask {
	sticky := client.NewStandaloneTask("sticky-task",
		func(ctx hatchet.Context, input StickyInput) (*StickyResult, error) {
			// Run a child workflow on the same worker
			childWorkflow := Child(client)
			childResult, err := childWorkflow.Run(ctx, ChildInput{N: 1}, hatchet.WithRunSticky(true))

			if err != nil {
				return nil, err
			}

			var childOutput ChildResult
			err = childResult.Into(&childOutput)
			if err != nil {
				return nil, err
			}

			return &StickyResult{
				Result: fmt.Sprintf("child-result-%s", childOutput.Result),
			}, nil
		},
	)

	return sticky
}
```

#### Tab 4

```ruby
STICKY_CHILD_WORKFLOW = HATCHET.workflow(
  name: "StickyChildWorkflow",
  sticky: :soft
)

STICKY_WORKFLOW.task(:step2, parents: [STEP1A, STEP1B]) do |input, ctx|
  ref = STICKY_CHILD_WORKFLOW.run_no_wait(
    options: Hatchet::TriggerWorkflowOptions.new(sticky: true)
  )

  ref.result

  { "worker" => ctx.worker.id }
end

STICKY_CHILD_WORKFLOW.task(:child) do |input, ctx|
  { "worker" => ctx.worker.id }
end
```
