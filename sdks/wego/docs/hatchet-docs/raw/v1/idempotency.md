# Idempotency

> **Info:** Idempotency is currently in beta and may be subject to change.

If you need to prevent more than one run of a task from occurring within a given time window, for instance because of duplicate event sends from a webhook that can trigger duplicate runs in Hatchet, you can achieve this by adding **idempotency** configuration to your workflow or standalone task.

## Types of Idempotency in Hatchet

Hatchet supports two different types of idempotency behavior, which you can choose between depending on the needs of your application:

1. **TTL-based** idempotency, which says that there can only be one run for a given idempotency key within a specified amount of time after the first run for that key is seen.
2. **Status-based** idempotency, which clears the idempotency key when the run that claimed it reaches a terminal status (success, failure, or cancellation). This acts similarly to `CANCEL_NEWEST` concurrency, where any time there's a running task holding an idempotency key, any incoming tasks with the same key will be dropped. But once the running task reaches a terminal state, a new one that's triggered can immediately claim the key once again, with no fixed wait time.

## The Idempotency Key Expression

Every idempotency configuration, regardless of strategy, requires an **expression**, which is a [CEL expression](/v1/cel-expressions) that is evaluated against the input and additional metadata of the run that's about to be triggered to produce the idempotency key. Two runs with the same computed key are considered duplicates.

> **Warning:** The idempotency key expression must evaluate to a string.

## TTL-based Idempotency

TTL-based idempotency requires two parameters: the **expression** described above, and a **TTL**, which determines how long the key should live for. Only one run for a given key will occur in the time window from when the first trigger comes in until the TTL expires.

#### Python

```python
EVENT_KEY = "idempotency:example"


class IdempotencyInput(BaseModel):
    id: str
    desired_status: Literal["success", "cancel", "fail"] = "success"


@hatchet.task(
    idempotency=TTLBasedIdempotencyConfig(
        key_expression="input.id", ttl=timedelta(minutes=1)
    ),
    input_validator=IdempotencyInput,
    on_events=[EVENT_KEY],
)
async def idempotent_task(input: IdempotencyInput, ctx: Context) -> dict[str, str]:
    return {"result": f"Hello, world from task {input.id}"}
```

#### Typescript

```typescript
export const idempotentTask = hatchet.task({
  name: 'ts-e2e-idempotent-task',
  idempotency: {
    strategy: 'ttl',
    expression: 'input.id',
    ttlMs: 60_000,
  },
  onEvents: [EVENT_KEY],
  fn: async (input) => {
    return { result: `Hello, world from task ${input.id}` };
  },
});
```

#### Go

```go
func IdempotentTask(client *hatchet.Client) *hatchet.StandaloneTask {
	return client.NewStandaloneTask(
		"idempotent-task",
		func(ctx hatchet.Context, input IdempotencyInput) (*IdempotencyOutput, error) {
			return &IdempotencyOutput{
				Result: fmt.Sprintf("Hello, world from task %s", input.ID),
			}, nil
		},
		hatchet.WithWorkflowIdempotency(hatchet.IdempotencyConfig{
			Expression: "input.id",
			TTL:        time.Minute,
		}),
	)
}
```

#### Ruby

```ruby
IDEMPOTENT_TASK = HATCHET.task(
  name: 'ruby-e2e-idempotent-task',
  idempotency: Hatchet::TTLBasedIdempotencyConfig.new(expression: 'input.id', ttl_ms: 60_000),
  on_events: [EVENT_KEY]
) do |input, _ctx|
  { 'result' => "Hello from task #{input['id']}" }
end

IDEMPOTENT_TASK_SHORT_WINDOW = HATCHET.task(
  name: 'ruby-e2e-idempotent-task-short-window',
  idempotency: Hatchet::TTLBasedIdempotencyConfig.new(expression: 'input.id', ttl_ms: 2_000)
) do |input, _ctx|
  { 'result' => "Hello from task #{input['id']}" }
end
```

The TTL window is _sliding_: each accepted run resets the clock. For instance, if you trigger a workflow at `00:00:00 UTC` (midnight) with a TTL of five minutes, and then the same workflow is triggered again with the same inputs and metadata at `00:02:00 UTC`, `00:04:00 UTC`, and `00:06:00 UTC`, only the first one (at midnight) and the final one (at `00:06:00 UTC`) will run. After the second run occurs, the lock on the key will be held until `00:11:00 UTC` (five minutes after the final run was triggered).

TTL-based idempotency is a good fit when you want to _debounce_ duplicate triggers over a fixed period of time, regardless of how long the run itself takes.

## Status-based Idempotency

Status-based idempotency keeps the idempotency key claimed only while the run that owns it is still active. Once that run reaches a terminal status (success, failure, or cancellation), the key is released immediately, and the next trigger with the same key can claim it and start a fresh run with no fixed wait time.

This behaves similarly to `CANCEL_NEWEST` concurrency: while a run is holding the key, any incoming run with the same key is dropped (raising an idempotency collision), but once the running task finishes, a new one can immediately take its place.

Instead of a TTL, status-based idempotency takes a **fallback TTL**. Because the key is normally released when the run reaches a terminal state, the fallback TTL exists only as a safety net: it caps the longest the key can remain claimed if, for some reason, the run never reaches a terminal status. You should generally set the fallback TTL comfortably longer than you expect the run to take.

#### Python

```python
@hatchet.task(
    idempotency=StatusBasedIdempotencyConfig(
        key_expression="input.id", fallback_ttl=timedelta(seconds=10)
    ),
    input_validator=IdempotencyInput,
)
async def idempotent_status_based_task(
    input: IdempotencyInput,
    ctx: Context,
) -> dict[str, str]:
    if input.desired_status == "success":
        await asyncio.sleep(2)
        return {"result": f"Hello, world from task {input.id}"}

    if input.desired_status == "fail":
        await asyncio.sleep(2)
        raise Exception(f"Task {input.id} failed as requested.")

    if input.desired_status == "cancel":
        await asyncio.sleep(1)
        await ctx.aio_cancel()
        for _ in range(10):
            await asyncio.sleep(1)

    raise Exception(f"Task {input.id} should have been cancelled, but was not.")
```

#### Typescript

```typescript
export const idempotentStatusBasedTask = hatchet.task({
  name: 'ts-e2e-idempotent-status-based-task',
  idempotency: {
    strategy: 'status',
    expression: 'input.id',
    fallbackTtlMs: 10_000,
  },
  fn: async (input) => {
    return { result: `Hello, world from task ${input.id}` };
  },
});
```

#### Go

```go
func IdempotentStatusBasedTask(client *hatchet.Client) *hatchet.StandaloneTask {
	return client.NewStandaloneTask(
		"idempotent-status-based-task",
		func(ctx hatchet.Context, input IdempotencyInput) (*IdempotencyOutput, error) {
			return &IdempotencyOutput{
				Result: fmt.Sprintf("Hello, world from task %s", input.ID),
			}, nil
		},
		hatchet.WithWorkflowIdempotency(hatchet.IdempotencyConfig{
			Expression: "input.id",
			Method:     hatchet.IdempotencyMethodStatus,
			TTL:        10 * time.Second,
		}),
	)
}
```

#### Ruby

```ruby
IDEMPOTENT_STATUS_BASED_TASK = HATCHET.task(
  name: 'ruby-e2e-idempotent-status-based-task',
  idempotency: Hatchet::StatusBasedIdempotencyConfig.new(expression: 'input.id', fallback_ttl_ms: 10_000)
) do |input, _ctx|
  { 'result' => "Hello from task #{input['id']}" }
end
```

For example, if you trigger a run at `00:00:00 UTC` that takes thirty seconds to complete, any duplicate triggers that come in before `00:00:30 UTC` will collide and be rejected. But a duplicate that arrives at `00:00:31 UTC`, just after the first run has finished, will be accepted and start a new run, because the key was released as soon as the first run reached its terminal status.

Status-based idempotency is a good fit when you want to guarantee that only one run for a given key is _in flight_ at any moment, without imposing a cooldown after it completes.

## Handling Collisions

When a collision occurs, the engine will reject the workflow run, and, if the run was triggered from an SDK, then the SDK will raise an exception indicating that there was an idempotency collision. This exception will contain the id of the workflow run that already existed that had claimed the key already, so you can retrieve its output if you like.

#### Python

```python
ref_1 = await idempotent_task.aio_run(
    input=IdempotencyInput(id="123"),
    wait_for_result=False,
)

try:
    ref_2 = await idempotent_task.aio_run(
        input=IdempotencyInput(id="123"),
        wait_for_result=False,
    )
    run_id_2 = ref_2.workflow_run_id
except IdempotencyCollisionError as e:
    print(
        f"Run with external ID {e.existing_run_external_id} already exists for this idempotency key"
    )
    run_id_2 = e.existing_run_external_id

res_1 = await ref_1.aio_result()
res_2 = await idempotent_task.aio_get_result(run_id_2)

assert res_1 == res_2
assert ref_1.workflow_run_id == run_id_2
```

#### Typescript

```typescript
const ref1 = await idempotentTask.runNoWait({ id: '123' });

let runId2: string;
try {
  const ref2 = await idempotentTask.runNoWait({ id: '123' });
  runId2 = await ref2.getWorkflowRunId();
} catch (e) {
  if (e instanceof IdempotencyCollisionError) {
    console.log(
      `Run with external ID ${e.existingRunExternalId} already exists for this idempotency key`
    );
    runId2 = e.existingRunExternalId;
  } else {
    throw e;
  }
}

const res1 = await ref1.result();
console.log(`Result: ${JSON.stringify(res1)}, run ID: ${runId2}`);
```

#### Go

```go
ref1, err := idempotentTask.RunNoWait(ctx, IdempotencyInput{ID: "123"})
if err != nil {
	log.Fatalf("unexpected error on first run: %v", err)
}

ref2, err := idempotentTask.RunNoWait(ctx, IdempotencyInput{ID: "123"})

var runID2 string

if err != nil {
	if idempErr, ok := hatchet.IsIdempotencyCollisionError(err); ok {
		fmt.Printf("Run %s already exists for this idempotency key\n", idempErr.ExistingRunExternalId)
		runID2 = idempErr.ExistingRunExternalId
	} else {
		log.Fatalf("unexpected error on second run: %v", err)
	}
} else {
	runID2 = ref2.RunId
}
```

#### Ruby

```ruby
first_ref = IDEMPOTENT_TASK.run_no_wait({ 'id' => '123' })

second_run_id = begin
  second_ref = IDEMPOTENT_TASK.run_no_wait({ 'id' => '123' })
  second_ref.workflow_run_id
rescue Hatchet::IdempotencyCollisionError => e
  puts "Run #{e.existing_run_external_id} already exists for this idempotency key"
  e.existing_run_external_id
end
```

In other cases, such as triggering by events, the idempotency collision will be swallowed, and no additional runs will be created, but the event will still be ingested correctly without an error being raised.
