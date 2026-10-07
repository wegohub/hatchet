# Batch Tasks

> **Warning:** Batch tasks are in beta and may change in future releases.
For tasks that do not need to be executed immediately, you can use batch tasks
to automatically accumulate tasks and execute them in a single batch. The input
to a batch task is a map that associates the ID of the originating task with
it's input. The output *must* be the same shape, using the same keys, to send
outputs back to the original call site (unless output broadcasting is used, see
[Broadcasting outputs](#broadcasting-outputs)). For the caller of a batch task,
the batched execution is hidden. It is called, and receives output, the same way
as a normal task. Though each batch will be executed in one handler function,
the call site only receives output corresponding to its input, not the output
for the entire batch.


## Batch flushing behavior

Batches will accumulate tasks continuously, preventing them from executing until one of the following conditions is true:

- The max size for the batch is reached.
- The maximum interval has elapsed since the last batch flush.
- The size of the payloads for the buffered tasks has exceeded the 4Mb gRPC message limit.

In the following example, the batch task will be executed once 3 tasks have accumulated, or every 200ms, whichever comes first.

#### Python

```python
@hatchet.batch_task(
    batch_max_size=3,
    batch_max_interval=timedelta(milliseconds=200),
    input_validator=SimpleInput,
)
async def batch_simple(
    tasks: dict[BatchMemberId, SimpleInput], context: Context
) -> dict[BatchMemberId, SimpleOutput]:
    return {
        id: SimpleOutput(transformed_message=inp.message.upper())
        for id, inp in tasks.items()
    }
```

#### Typescript

```typescript
export const batchSimple = hatchet.batchTask({
  name: 'batch-simple',
  batch: { maxSize: 3, maxInterval: 200 },
  fn: async (tasks: Record<string, SimpleInput>) => {
    const out: Record<string, SimpleOutput> = {};
    Object.entries(tasks).forEach(([id, input]) => {
      out[id] = { transformed_message: input.message.toUpperCase() };
    });
    return out;
  },
});
```

#### Go

```go
batchSimple := client.NewStandaloneBatchTask("batch-simple",
	func(ctx hatchet.Context, tasks map[string]SimpleInput) (map[string]SimpleOutput, error) {
		out := make(map[string]SimpleOutput, len(tasks))
		for id, inp := range tasks {
			out[id] = SimpleOutput{TransformedMessage: strings.ToUpper(inp.Message)}
		}
		return out, nil
	},
	hatchet.BatchConfig{
		MaxSize:     3,
		MaxInterval: durationPtr(200 * time.Millisecond),
	},
)
```

#### Ruby

```ruby
BATCH_SIMPLE = HATCHET.batch_task(
  name: 'ruby-e2e-batch-simple',
  batch: Hatchet::BatchTaskConfig.new(max_size: 3, max_interval_ms: 200)
) do |inputs, _ctx|
  inputs.transform_values { |input| { 'transformed_message' => input['message'].upcase } }
end
```

## Batch keys

To allow for accumulation of multiple batches simultaneously using the same workflow, batch keys can be used.
For example, if you had multiple tenants, and wanted to ensure that each batch execution did not mix inputs from multiple
tenants, batch keys can be used to partition the inputs. The batch key is a [CEL expression](/v1/cel-expressions) evaluated against the run's input and additional metadata. In the following example, the batches will flush independently based on the `group` value.

#### Python

```python
@hatchet.batch_task(
    batch_max_size=2,
    batch_max_interval=timedelta(milliseconds=200),
    batch_group_key="input.group",
    input_validator=KeyedInput,
)
async def batch_keyed(
    tasks: dict[BatchMemberId, KeyedInput], context: Context
) -> dict[BatchMemberId, KeyedOutput]:
    unique_keys = len({inp.group for _, inp in tasks.items()})
    return {
        id: KeyedOutput(
            batch_key=inp.group,
            batch_size=len(tasks),
            unique_keys=unique_keys,
            uppercase=inp.message.upper(),
        )
        for id, inp in tasks.items()
    }
```

#### Typescript

```typescript
export const batchKeyed = hatchet.batchTask({
  name: 'batch-keyed',
  batch: { maxSize: 2, maxInterval: 200, groupKey: 'input.group' },
  fn: async (tasks: Record<string, KeyedInput>) => {
    const uniqueKeys = new Set(Object.values(tasks).map((i) => i.group)).size;
    const batchSize = Object.keys(tasks).length;
    const out: Record<string, KeyedOutput> = {};
    Object.entries(tasks).forEach(([id, input]) => {
      out[id] = {
        batch_key: input.group,
        batch_size: batchSize,
        unique_keys: uniqueKeys,
        uppercase: input.message.toUpperCase(),
      };
    });
    return out;
  },
});
```

#### Go

```go
batchKeyed := client.NewStandaloneBatchTask("batch-keyed",
	func(ctx hatchet.Context, tasks map[string]KeyedInput) (map[string]KeyedOutput, error) {
		uniqueGroups := make(map[string]struct{})
		for _, inp := range tasks {
			uniqueGroups[inp.Group] = struct{}{}
		}

		out := make(map[string]KeyedOutput, len(tasks))
		for id, inp := range tasks {
			out[id] = KeyedOutput{
				BatchKey:   inp.Group,
				BatchSize:  len(tasks),
				UniqueKeys: len(uniqueGroups),
				Uppercase:  strings.ToUpper(inp.Message),
			}
		}
		return out, nil
	},
	hatchet.BatchConfig{
		MaxSize:     2,
		MaxInterval: durationPtr(200 * time.Millisecond),
		GroupKey:    stringPtr("input.group"),
	},
)
```

#### Ruby

```ruby
BATCH_KEYED = HATCHET.batch_task(
  name: 'ruby-e2e-batch-keyed',
  batch: Hatchet::BatchTaskConfig.new(max_size: 2, max_interval_ms: 200, group_key: 'input.group')
) do |inputs, _ctx|
  unique_keys = inputs.values.map { |i| i['group'] }.uniq.length
  batch_size = inputs.length

  inputs.transform_values do |input|
    {
      'batch_key' => input['group'],
      'batch_size' => batch_size,
      'unique_keys' => unique_keys,
      'uppercase' => input['message'].upcase,
    }
  end
end
```

## Broadcasting outputs

In the default case, batch outputs are mapped 1-1 back to the callers. So if you called a batch task with input `a` and `b` at separate
call sites, you would receive back outputs `a'` and `b'` respectively, despite the fact they would both be executed at the same time, in the same handler.
However, if you wish to return the same input back to all callsites, you can use output broadcasting.
In the following example, every task will receive the same output regardless of call site. Note, however, that this will only return
the same output to every task buffered into the same batch. See the batch flushing rules above for more details.

#### Python

```python
@hatchet.batch_task(
    batch_max_size=10,
    batch_max_interval=timedelta(seconds=2),
    input_validator=SimpleInput,
    broadcast_output=True,
)
async def batch_broadcast(
    tasks: dict[BatchMemberId, SimpleInput], context: Context
) -> BroadcastOutput:
    return BroadcastOutput(sum=sum(len(i.message) for _, i in tasks.items()))
```

#### Typescript

```typescript
export const batchBroadcast = hatchet.batchTask({
  name: 'batch-broadcast',
  batch: { maxSize: 10, maxInterval: 2_000, broadcastOutput: true },
  fn: async (tasks: Record<string, SimpleInput>): Promise => {
    const sum = Object.values(tasks).reduce((acc, i) => acc + i.message.length, 0);
    return { sum };
  },
});
```

#### Go

```go
batchBroadcast := client.NewStandaloneBatchTask("batch-broadcast",
	func(ctx hatchet.Context, tasks map[string]SimpleInput) (BroadcastSumOutput, error) {
		sum := 0
		for _, inp := range tasks {
			sum += len(inp.Message)
		}
		return BroadcastSumOutput{Sum: sum}, nil
	},
	hatchet.BatchConfig{
		MaxSize:         10,
		MaxInterval:     durationPtr(2 * time.Second),
		BroadcastOutput: true,
	},
)
```

#### Ruby

```ruby
BATCH_BROADCAST = HATCHET.batch_task(
  name: 'ruby-e2e-batch-broadcast',
  batch: Hatchet::BatchTaskConfig.new(max_size: 10, max_interval_ms: 2_000, broadcast_output: true)
) do |inputs, _ctx|
  { 'sum' => inputs.values.sum { |i| i['message'].length } }
end
```
