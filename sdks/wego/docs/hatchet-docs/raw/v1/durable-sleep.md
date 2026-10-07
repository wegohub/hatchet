# Durable Sleep

A durable task can elect to **sleep** for either a specified period of time or until a provided time, which pauses task execution. While the task is sleeping, no resources are consumed, and the task can also be [evicted](/v1/task-eviction) in order to free the worker slot.


Unlike a language-level sleep (e.g. `time.sleep` in Python or `setTimeout` in Node), durable sleep is guaranteed to respect the original duration across interruptions of the durable task, worker crashes, and so on. A language-level sleep ties the wait to the local process, so if the process restarts, the sleep starts over from scratch.

### Using durable sleep

The `DurableContext` exposes helper methods to allow you to sleep for either a specific duration of time, or until a certain time.

#### Python

```python
@hatchet.durable_task(name="DurableSleepTask")
async def durable_sleep_task(input: EmptyModel, ctx: DurableContext) -> None:
    res = await ctx.aio_sleep_for(timedelta(seconds=5))

    print("got result", res)
```

#### Typescript

```typescript
durableSleep.durableTask({
  name: 'durable-sleep',
  executionTimeout: '10m',
  fn: async (_, ctx) => {
    ctx.logger.info('sleeping for 5s');
    const sleepRes = await ctx.sleepFor('5s');
    ctx.logger.info('done sleeping for 5s', { sleepRes });

    return {
      Value: 'done',
    };
  },
});
```

#### Go

```go
task := client.NewStandaloneDurableTask("long-running-task", func(ctx hatchet.DurableContext, input DurableInput) (DurableOutput, error) {
	log.Printf("Starting task, will sleep for %d seconds", input.Delay)

	if _, err := ctx.SleepFor(time.Duration(input.Delay) * time.Second); err != nil {
		return DurableOutput{}, err
	}

	log.Printf("Finished sleeping, processing message: %s", input.Message)

	return DurableOutput{
		ProcessedAt: time.Now().Format(time.RFC3339),
		Message:     "Processed: " + input.Message,
	}, nil
})
```

#### Ruby

```ruby
DURABLE_SLEEP_TASK = HATCHET.durable_task(name: "DurableSleepTask") do |input, ctx|
  res = ctx.sleep_for(duration: 5)

  puts "got result #{res}"
end
```
