# Cancellation in Hatchet Tasks

Hatchet provides a mechanism for canceling task executions gracefully, allowing you to signal to running tasks that they should stop running. Cancellation can be triggered on graceful termination of a worker or automatically through concurrency control strategies like [`CANCEL_IN_PROGRESS`](/v1/concurrency#cancel-in-progress), which cancels currently running task instances to free up slots for new instances when the concurrency limit is reached.

When a task is canceled, Hatchet sends a cancellation signal to the task. The task can then check for the cancellation signal and take appropriate action, such as cleaning up resources, aborting network requests, or gracefully terminating their execution.

## Cancellation Mechanisms

#### Python

```python
@cancellation_workflow.task()
def check_flag(input: EmptyModel, ctx: Context) -> dict[str, str]:
    for i in range(3):
        time.sleep(1)

        # Note: Checking the status of the exit flag is mostly useful for cancelling
        # sync tasks without needing to forcibly kill the thread they're running on.
        if ctx.exit_flag:
            print("Task has been cancelled")
            raise ValueError("Task has been cancelled")

    return {"error": "Task should have been cancelled"}
```
```python
@cancellation_workflow.task()
async def self_cancel(input: EmptyModel, ctx: Context) -> dict[str, str]:
    await asyncio.sleep(2)

    ## Cancel the task
    await ctx.aio_cancel()

    await asyncio.sleep(10)

    return {"error": "Task should have been cancelled"}
```

#### Typescript

```typescript
export const cancellation = hatchet.task({
  name: 'cancellation',
  fn: async (_, ctx) => {
    await sleep(10 * 1000);

    if (ctx.cancelled) {
      throw new Error('Task was cancelled');
    }

    return {
      Completed: true,
    };
  },
});
```
```typescript
export const abortSignal = hatchet.task({
  name: 'abort-signal',
  fn: async (_, ctx) => {
    try {
      const response = await axios.get('https://api.example.com/data', {
        signal: ctx.abortController.signal,
      });
      // Handle the response
    } catch (error) {
      if (axios.isCancel(error)) {
        // Request was canceled
        ctx.logger.info('Request canceled');
      } else {
        // Handle other errors
      }
    }
  },
});
```

#### Go

```go
// Add a long-running task that can be cancelled
_ = workflow.NewTask("long-running-task", func(ctx hatchet.Context, input CancellationInput) (CancellationOutput, error) {
	log.Printf("Starting long-running task with message: %s", input.Message)

	// Simulate long-running work with cancellation checking
	for i := 0; i < 10; i++ {
		select {
		case <-ctx.Done():
			log.Printf("Task cancelled after %d steps", i)
			return CancellationOutput{
				Status:    "cancelled",
				Completed: false,
			}, nil
		default:
			log.Printf("Working... step %d/10", i+1)
			time.Sleep(1 * time.Second)
		}
	}

	log.Println("Task completed successfully")
	return CancellationOutput{
		Status:    "completed",
		Completed: true,
	}, nil
}, hatchet.WithExecutionTimeout(30*time.Second))
```

#### Ruby

```ruby
CANCELLATION_WORKFLOW.task(:check_flag) do |input, ctx|
  3.times do
    sleep 1

    # Note: Checking the status of the exit flag is mostly useful for cancelling
    # sync tasks without needing to forcibly kill the thread they're running on.
    if ctx.cancelled?
      puts "Task has been cancelled"
      raise "Task has been cancelled"
    end
  end

  { "error" => "Task should have been cancelled" }
end
```
```ruby
CANCELLATION_WORKFLOW.task(:self_cancel) do |input, ctx|
  sleep 2

  ## Cancel the task
  ctx.cancel

  sleep 10

  { "error" => "Task should have been cancelled" }
end
```

## Cancellation Best Practices

When working with cancellation in Hatchet tasks, consider the following best practices:

1. **Graceful Termination**: When a task receives a cancellation signal, aim to terminate its execution gracefully. Clean up any resources, abort pending operations, and perform any necessary cleanup tasks before returning from the task function.

2. **Cancellation Checks**: Regularly check for cancellation signals within long-running tasks or loops. This allows the task to respond to cancellation in a timely manner and avoid unnecessary processing.

3. **Cancellation Propagation**: If a task invokes other functions or libraries, consider propagating the cancellation signal to those dependencies. This ensures that cancellation is handled consistently throughout the task.

4. **Error Handling**: Handle cancellation errors appropriately. Distinguish between cancellation errors and other types of errors to provide meaningful error messages and take appropriate actions.

## Additional Features

In addition to the methods of cancellation listed here, Hatchet also supports [bulk cancellation](/v1/bulk-retries-and-cancellations), which allows you to cancel many tasks in bulk using either their IDs or a set of filters, which is often the easiest way to cancel many things at once.

## Conclusion

Cancellation is a powerful feature in Hatchet that allows you to gracefully stop task executions when needed. Remember to follow best practices when implementing cancellation in your tasks, such as graceful termination, regular cancellation checks, handling asynchronous operations, proper error handling, and cancellation propagation.

By incorporating cancellation into your Hatchet tasks and workflows, you can build more resilient and responsive systems that can adapt to changing circumstances and user needs.
