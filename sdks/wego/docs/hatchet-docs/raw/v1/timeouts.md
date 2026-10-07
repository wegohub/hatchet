# Timeouts in Hatchet

Timeouts are an important concept in Hatchet that allow you to control how long a task is allowed to run before it is considered to have failed. This is useful for ensuring that your tasks don't run indefinitely and consume unnecessary resources. Timeouts in Hatchet are treated as failures and the task will be [retried](/v1/retry-policies) if specified.

There are two types of timeouts in Hatchet:

1. **Scheduling Timeouts** (Default 5m) - the time a task is allowed to wait in the queue before it is cancelled
2. **Execution Timeouts** (Default 60s) - the time a task is allowed to run before it is considered to have failed

## Timeout Format

In Hatchet, timeouts are duration strings: a sequence of decimal numbers, each with an optional fraction and a unit suffix. Valid units are:

- `ms` for milliseconds
- `s` for seconds
- `m` for minutes
- `h` for hours

Components are summed. For example:

- `10s` means 10 seconds
- `4m` means 4 minutes
- `1h` means 1 hour
- `1h30m` means 1 hour 30 minutes
- `42m30s` means 42 minutes 30 seconds
- `1.5h` means 1 hour 30 minutes

A unit is required and values must not be signed; bare numbers like `42` and negative values like `-30s` are rejected.

> **Info:** In the Python SDK, timeouts can also be specified as a `datetime.timedelta`
>   object.

### Task-Level Timeouts

You can specify execution and scheduling timeouts for a task using the `execution_timeout` and `schedule_timeout` parameters when creating a task.

#### Ruby

```python
# 👀 Specify an execution timeout on a task
@timeout_wf.task(
    execution_timeout=timedelta(seconds=5), schedule_timeout=timedelta(minutes=10)
)
def timeout_task(input: EmptyModel, ctx: Context) -> dict[str, str]:
    time.sleep(30)
    return {"status": "success"}
```

#### Tab 2

```typescript
export const withTimeouts = hatchet.task({
  name: 'with-timeouts',
  // time the task can wait in the queue before it is cancelled
  scheduleTimeout: '10s',
  // time the task can run before it is cancelled
  executionTimeout: '10s',
  fn: async (input: SimpleInput, ctx) => {
    // wait 15 seconds
    await sleep(15000);

    // get the abort controller
    const { abortController } = ctx;

    // if the abort controller is aborted, throw an error
    if (abortController.signal.aborted) {
      throw new Error('cancelled');
    }

    return {
      TransformedMessage: input.Message.toLowerCase(),
    };
  },
});
```

#### Tab 3

```go
// Task that will timeout - sleeps for 10 seconds but has 3 second timeout
_ = timeoutWorkflow.NewTask("timeout-task",
	func(ctx hatchet.Context, input TimeoutInput) (TimeoutOutput, error) {
		log.Printf("Starting task that will timeout. Message: %s", input.Message)

		// Sleep for 10 seconds (will be interrupted by timeout)
		time.Sleep(10 * time.Second)

		// This should not be reached due to timeout
		log.Println("Task completed successfully (this shouldn't be reached)")
		return TimeoutOutput{
			Status:    "completed",
			Completed: true,
		}, nil
	},
	hatchet.WithExecutionTimeout(3*time.Second), // 3 second timeout
)
```

#### Tab 4

```ruby
# Specify an execution timeout on a task
TIMEOUT_WF.task(:timeout_task, execution_timeout: 5, schedule_timeout: 600) do |input, ctx|
  sleep 30
  { "status" => "success" }
end

REFRESH_TIMEOUT_WF = HATCHET.workflow(name: "RefreshTimeoutWorkflow")
```

In these tasks, both timeouts are specified, meaning:

1. If the task is not scheduled before the `schedule_timeout` is reached, it will be cancelled.
2. If the task does not complete before the `execution_timeout` is reached (after starting), it will be cancelled.

> **Warning:** A timed out task does not guarantee that the task will be stopped immediately.
>   The task will be stopped as soon as the worker is able to stop the task. See
>   [cancellation](/v1/cancellation) for more information.

## Refreshing Timeouts

In some cases, you may need to extend the timeout for a task while it is running. This can be done by using the task context.

For example:

#### Ruby

```python
@refresh_timeout_wf.task(execution_timeout=timedelta(seconds=4))
def refresh_task(input: EmptyModel, ctx: Context) -> dict[str, str]:
    ctx.refresh_timeout(timedelta(seconds=10))
    time.sleep(5)

    return {"status": "success"}
```

#### Tab 2

```typescript
export const refreshTimeout = hatchet.task({
  name: 'refresh-timeout',
  executionTimeout: '10s',
  scheduleTimeout: '10s',
  fn: async (input: SimpleInput, ctx) => {
    // adds 15 seconds to the execution timeout
    ctx.refreshTimeout('15s');
    await sleep(15000);

    // get the abort controller
    const { abortController } = ctx;

    // now this condition will not be met
    // if the abort controller is aborted, throw an error
    if (abortController.signal.aborted) {
      throw new Error('cancelled');
    }

    return {
      TransformedMessage: input.Message.toLowerCase(),
    };
  },
});
```

#### Tab 3

```go
// Create workflow with timeout refresh example
refreshTimeoutWorkflow := client.NewWorkflow("refresh-timeout-demo",
	hatchet.WithWorkflowDescription("Demonstrates timeout refresh functionality"),
	hatchet.WithWorkflowVersion("1.0.0"),
)

// Task that refreshes its timeout to avoid timing out
_ = refreshTimeoutWorkflow.NewTask("refresh-timeout-task",
	func(ctx hatchet.Context, input TimeoutInput) (TimeoutOutput, error) {
		log.Printf("Starting task with timeout refresh. Message: %s", input.Message)

		// Refresh timeout by 10 seconds
		log.Println("Refreshing timeout by 10 seconds...")
		err := ctx.RefreshTimeout("10s")
		if err != nil {
			log.Printf("Failed to refresh timeout: %v", err)
			return TimeoutOutput{
				Status:    "failed",
				Completed: false,
			}, err
		}

		// Now sleep for 5 seconds (should complete successfully)
		log.Println("Sleeping for 5 seconds...")
		time.Sleep(5 * time.Second)

		log.Println("Task completed successfully after timeout refresh")
		return TimeoutOutput{
			Status:    "completed",
			Completed: true,
		}, nil
	},
	hatchet.WithExecutionTimeout(3*time.Second), // Initial 3 second timeout
)
```

#### Tab 4

```ruby
REFRESH_TIMEOUT_WF.task(:refresh_task, execution_timeout: 4) do |input, ctx|
  ctx.refresh_timeout(10)
  sleep 5

  { "status" => "success" }
end
```

In this example, the task initially would exceed its execution timeout. But before it does, we call the `refreshTimeout` method, which extends the timeout and allows it to complete. Importantly, refreshing a timeout is an additive operation - the new timeout is added to the existing timeout. So for instance, if the task originally had a timeout of `30s` and we call `refreshTimeout("15s")`, the new timeout will be `45s`.

The task timeout can be refreshed multiple times within a task to further extend the timeout as needed.
