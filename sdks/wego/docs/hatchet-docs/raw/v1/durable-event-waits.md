# Durable Event Waits

Tasks can pause until an external event arrives before continuing. This is the foundation for human-in-the-loop workflows, webhook-driven pipelines, and any process that depends on signals from outside the task.


Events are delivered by pushing events into Hatchet either [using one of the SDKs](/v1/events#pushing-events-to-hatchet) or via an [incoming webhook](/v1/webhooks). The event key you push must match the key your task is waiting for.

Waiting for an event lets a durable task pause until an event arrives. Even if the task is interrupted and requeued while waiting, the event will still be processed. When it resumes, it reads the event from the durable event log and continues.

## Establishing an event wait

Your durable tasks can establish an event wait by using helper methods on the `DurableContext`.

#### Python

```python
@hatchet.durable_task(name="DurableEventTask")
async def durable_event_task(input: EmptyModel, ctx: DurableContext) -> None:
    res = await ctx.aio_wait_for_event(
        "user:update",
    )

    print("got event", res)
```

#### Typescript

```typescript
export const durableEvent = hatchet.durableTask({
  name: 'durable-event',
  executionTimeout: '10m',
  fn: async (_, ctx) => {
    const res = await ctx.waitForEvent(EVENT_KEY);

    ctx.logger.info('res', { res });

    return {
      Value: 'done',
    };
  },
});
```

#### Go

```go
task := client.NewStandaloneDurableTask("durable-event-task", func(ctx hatchet.DurableContext, input DurableInput) (DurableOutput, error) {
	log.Printf("Waiting for user:update event, message: %s", input.Message)

	if _, err := ctx.WaitForEvent("user:update", ""); err != nil {
		return DurableOutput{}, err
	}

	log.Printf("Got event, processing message: %s", input.Message)

	return DurableOutput{
		ProcessedAt: time.Now().Format(time.RFC3339),
		Message:     "Processed: " + input.Message,
	}, nil
})
```

#### Ruby

```ruby
DURABLE_EVENT_TASK = HATCHET.durable_task(name: "DurableEventTask") do |input, ctx|
  res = ctx.wait_for(
    "event",
    Hatchet::UserEventCondition.new(event_key: "user:update")
  )

  puts "got event #{res}"
end

DURABLE_EVENT_TASK_WITH_FILTER = HATCHET.durable_task(name: "DurableEventWithFilterTask") do |input, ctx|
```

## Matching CEL expressions

In general, you'll want to match the payload of the incoming event to some data that your task knows about to make sure the event you receive is the _right_ one. For instance, you might want to make sure that the event payload contains the correct user or organization id in order to consider the event wait as having been completed. To do this, you can provide a [CEL expression](/v1/cel-expressions) in addition to the event key when establishing the wait.

#### Python

```python
res = await ctx.aio_wait_for_event("user:update", "input.user_id == '1234'")
```

#### Typescript

```typescript
const res = await ctx.waitForEvent(EVENT_KEY, "input.userId == '1234'");
```

#### Go

```go
if _, err := ctx.WaitForEvent("user:update", "input.user_id == '1234'"); err != nil {
	return DurableOutput{}, err
}
```

#### Ruby

```ruby
res = ctx.wait_for(
    "event",
    Hatchet::UserEventCondition.new(
      event_key: "user:update",
      expression: "input.user_id == '1234'"
    )
  )

  puts "got event #{res}"
end
```

## Lookback Windows

Event waits can also optionally look back in time for recent events that have come in, which is often useful for preventing race conditions. An important caveat is that in order look up previous events, the wait must also be established alongside a **scope**, which must also be pushed with the event itself. The scope serves as a hint to help Hatchet narrow down the pool of candidate events.

As a simple example, if you're establishing an event wait with a CEL expression like `input.user_id == 1234`, then you know that `{"user_id": 1234}` is present on the event payload, so you might use the string `user_id:1234` as the scope, **both when establishing the wait and pushing the event.**

#### Python

```python
event = await ctx.aio_wait_for_event(
    key="user:create",
    expression=f"input.user_id == {input.user_id}",
    scope=f"user_id:{input.user_id}",
    lookback_window=timedelta(minutes=1),
    payload_validator=LookbackEventPayload,
)
```

#### Typescript

```typescript
const event = await ctx.waitForEvent(
  'user:create',
  `input.user_id == ${input.userId}`,
  lookbackEventPayloadSchema,
  `user_id:${input.userId}`,
  '1m'
);
```

#### Go

```go
now, err := ctx.Now()
if err != nil {
	return DurableOutput{}, err
}

// Matches a "user:update" event scoped to this user, including events
// pushed up to a minute before this wait was registered.
_, err = ctx.WaitForEvent("user:update", "",
	hatchet.WithEventScope("user_id:1234"),
	hatchet.WithConsiderEventsSince(now.Add(-time.Minute)),
)
if err != nil {
	return DurableOutput{}, err
}
```

#### Ruby

> **Info:** Lookback window support for the Ruby SDK is coming soon. Join our
>       [Discord](https://hatchet.run/discord) to stay up to date.
