# Task Eviction

Since durable tasks are often waiting for sleeps, events, or child runs, they spend much of their time inactive. When this happens, the durable task no longer needs to hold a slot on the worker until the thing it's waiting for completes. Hatchet has the option to **evict** durable tasks from workers when they're in one of these waiting states, in order to free up slots on the worker. This allows the worker to pick up additional durable tasks, and then resume the original one from where it left off when the wait is satisfied, without keeping the idle task around.

> **Info:** It's important to note that eviction is a fundamental difference between
>   durable and non-durable tasks. Only durable tasks can be evicted from and
>   restored on workers, as opposed to non-durable tasks, which will hold slots
>   throughout the entirety of their execution, even while they're inactive.

### Configuring Eviction Policies

Every durable task can configure an **eviction policy**, which tells Hatchet how to respond when that task hits a wait. There are a few configuration options available to you to choose from:

1. You can set a TTL, which tells Hatchet that the durable task should only be evicted after some amount of uninterrupted time has been spent waiting.
2. You can enable or disable **capacity-based eviction**, which allows Hatchet to optimistically evict the durable task if the worker is running out of slots and wants to pick up a new durable task run, but would be unable to if the waiting task could not be evicted.
3. Finally, you can provide a **priority**, which tells Hatchet what order candidate durable tasks should be evicted in (lower priority values will be evicted first when choosing between multiple candidate durable tasks to evict).

Start by declaring a policy:

#### Python

```python
EVICTION_POLICY = EvictionPolicy(
    ttl=timedelta(seconds=EVICTION_TTL_SECONDS),
    allow_capacity_eviction=True,
    priority=0,
)
```

#### Typescript

```typescript
const EVICTION_POLICY: EvictionPolicy = {
  ttl: `${EVICTION_TTL_SECONDS}s`,
  allowCapacityEviction: true,
  priority: 0,
};
```

#### Go

```go
evictionPolicy := &hatchet.EvictionPolicy{
	TTL:                   evictionTTLSeconds * time.Second,
	AllowCapacityEviction: true,
	Priority:              0,
}
```

#### Ruby

```ruby
EVICTION_POLICY = Hatchet::EvictionPolicy.new(
  ttl: EVICTION_TTL_SECONDS,
  allow_capacity_eviction: true,
  priority: 0,
)
```

Then attach it to a durable task. Any time the task enters a wait (sleep, event wait, child spawn) the policy's TTL and capacity-eviction settings are honored:

#### Python

```python
@hatchet.durable_task(
    execution_timeout=timedelta(minutes=5),
    eviction_policy=EVICTION_POLICY,
)
async def evictable_sleep(input: EmptyModel, ctx: DurableContext) -> dict[str, Any]:
    """Sleeps long enough for the TTL-based eviction to kick in."""
    await ctx.aio_sleep_for(timedelta(seconds=LONG_SLEEP_SECONDS))
    return {"status": "completed"}
```

#### Typescript

```typescript
export const evictableSleep = hatchet.durableTask({
  name: 'evictable-sleep',
  executionTimeout: '5m',
  evictionPolicy: EVICTION_POLICY,
  fn: async (_input, ctx) => {
    await ctx.sleepFor(`${LONG_SLEEP_SECONDS}s`);
    return { status: 'completed' };
  },
});
```

#### Go

```go
evictableSleep := client.NewStandaloneDurableTask("evictable-sleep",
	func(ctx hatchet.DurableContext, input EmptyInput) (EvictionOutput, error) {
		if _, err := ctx.SleepFor(longSleepSeconds * time.Second); err != nil {
			return EvictionOutput{}, err
		}
		return EvictionOutput{Status: "completed"}, nil
	},
	hatchet.WithExecutionTimeout(5*time.Minute),
	hatchet.WithEvictionPolicy(evictionPolicy),
)
```

#### Ruby

```ruby
EVICTABLE_SLEEP = HATCHET.durable_task(
  name: "evictable_sleep",
  execution_timeout: 300,
  eviction_policy: EVICTION_POLICY,
) do |_input, ctx|
  ctx.sleep_for(duration: LONG_SLEEP_SECONDS)
  { "status" => "completed" }
end
```

To opt a durable task out of eviction entirely, set `allowCapacityEviction` to `false` and leave the TTL unset. The task will hold its slot through waits:

#### Python

```python
@hatchet.durable_task(
    execution_timeout=timedelta(minutes=5),
    eviction_policy=EvictionPolicy(
        ttl=None,
        allow_capacity_eviction=False,
        priority=0,
    ),
)
async def non_evictable_sleep(input: EmptyModel, ctx: DurableContext) -> dict[str, Any]:
    """Has eviction disabled -- should never be evicted."""
    await ctx.aio_sleep_for(timedelta(seconds=30))
    return {"status": "completed"}
```

#### Typescript

```typescript
export const nonEvictableSleep = hatchet.durableTask({
  name: 'non-evictable-sleep',
  executionTimeout: '5m',
  evictionPolicy: {
    ttl: undefined,
    allowCapacityEviction: false,
    priority: 0,
  },
  fn: async (_input, ctx) => {
    await ctx.sleepFor('10s');
    return { status: 'completed' };
  },
});
```

#### Go

```go
nonEvictablePolicy := &hatchet.EvictionPolicy{
	AllowCapacityEviction: false,
	Priority:              0,
}

nonEvictableSleep := client.NewStandaloneDurableTask("non-evictable-sleep",
	func(ctx hatchet.DurableContext, input EmptyInput) (EvictionOutput, error) {
		if _, err := ctx.SleepFor(10 * time.Second); err != nil {
			return EvictionOutput{}, err
		}
		return EvictionOutput{Status: "completed"}, nil
	},
	hatchet.WithExecutionTimeout(5*time.Minute),
	hatchet.WithEvictionPolicy(nonEvictablePolicy),
)
```

#### Ruby

```ruby
NON_EVICTABLE_SLEEP = HATCHET.durable_task(
  name: "non_evictable_sleep",
  execution_timeout: 300,
  eviction_policy: NON_EVICTABLE_POLICY,
) do |_input, ctx|
  ctx.sleep_for(duration: 10)
  { "status" => "completed" }
end
```

### Task Resumption

Hatchet will **resume** a task in an evicted state when the thing it was waiting for is satisfied. When this happens, the durable task is re-triggered on the worker, and its event log is replayed up to the checkpoint where it left off, at which point it continues making progress.

> **Warning:** Since durable tasks can be evicted or resumed, it's important to make sure
>   that a durable task doesn't perform any operations that you wouldn't want to
>   occur multiple times. For instance, non-idempotent writes to a database,
>   sending emails, or computationally expensive work are generally not good
>   candidates for the kinds of things to do in a durable task, since that logic
>   will be rerun every time the task is resumed.
