# Child Spawning

A task can spawn child tasks at runtime — including other durable tasks or entire DAG workflows. Children run independently on any available worker, and the parent can wait for their results before continuing. You can spawn children out of any task (durable or not).

> **Info:** While child spawning is not unique to durable tasks, we often recommend using
>   durable tasks when spawning children is the main responsibility (or one of the
>   main responsibilities) of a task. For instance, an agent might spawn many
>   children (i.e. tool calls) as its main responsibility, making a durable task a
>   good fit.

### Spawning a child task

You can spawn child tasks similarly to how you run tasks normally, but the implementation details differ slightly by language. Any task can spawn child tasks.

#### Python

```python
from examples.fanout.worker import ChildInput, child_wf

# 👀 example: run this inside of a parent task to spawn a child
child_wf.run(
    ChildInput(a="b"),
)
```

#### Typescript

```typescript
export const parentSingleChild = hatchet.task({
  name: 'parent-single-child',
  fn: async () => {
    const childRes = await child.run({ N: 1 });

    return {
      Result: childRes.Value,
    };
  },
});
```

#### Go

```go
// Inside a parent task
childResult, err := childWorkflow.Run(hCtx, ChildInput{
	Value: 1,
})
if err != nil {
	return err
}
```

#### Ruby

```ruby
FANOUT_CHILD_WF.run({ "a" => "b" })
```

### Spawning many children at once

You can also spawn children in bulk, exactly the same as you can spawn any other tasks in bulk.

#### Python

```python
async def run_child_workflows(n: int) -> list[dict[str, Any]]:
    return await child_wf.aio_run_many(
        [
            child_wf.create_bulk_run_item(
                input=ChildInput(a=str(i)),
            )
            for i in range(n)
        ]
    )
```

#### Typescript

```typescript
type ParentInput = {
  N: number;
};

export const parent = hatchet.task({
  name: 'parent',
  fn: async (input: ParentInput, ctx) => {
    const n = input.N;
    const promises = [];

    for (let i = 0; i < n; i++) {
      promises.push(child.run({ N: i }));
    }

    const childRes = await Promise.all(promises);
    const sum = childRes.reduce((acc, curr) => acc + curr.Value, 0);

    return {
      Result: sum,
    };
  },
});
```

#### Go

```go
// Run multiple child tasks in parallel using goroutines
var wg sync.WaitGroup
var mu sync.Mutex
results := make([]*ChildOutput, 0, n)

wg.Add(n)
for i := 0; i < n; i++ {
	go func(index int) {
		defer wg.Done()
		result, err := childWorkflow.Run(hCtx, ChildInput{Value: index})
		if err != nil {
			return
		}

		var childOutput ChildOutput
		err = result.Into(&childOutput)
		if err != nil {
			return
		}

		mu.Lock()
		results = append(results, &childOutput)
		mu.Unlock()
	}(i)
}
wg.Wait()
```

#### Ruby

```ruby
def run_child_workflows(n)
  FANOUT_CHILD_WF.run_many(
    n.times.map do |i|
      FANOUT_CHILD_WF.create_bulk_run_item(
        input: { "a" => i.to_s }
      )
    end
  )
end
```

### What you can spawn

A durable task can spawn any runnable:

Child type, Example

**Regular task**, Spawn a stateless task for a quick computation or API call.
**Durable task**, Spawn another durable task that has its own checkpoints, sleeps, and event waits.
**DAG workflow**, Spawn an entire multi-task workflow and wait for its final output.

### Error handling

#### Python

```python
try:
    child_wf.run(
        ChildInput(a="b"),
    )
except Exception as e:
    print(f"Child workflow failed: {e}")
```

#### Typescript

```typescript
export const withErrorHandling = hatchet.task({
  name: 'parent-error-handling',
  fn: async () => {
    try {
      const childRes = await child.run({ N: 1 });

      return {
        Result: childRes.Value,
      };
    } catch (error) {
      // decide how to proceed here
      return {
        Result: -1,
      };
    }
  },
});
```

#### Go

```go
result, err := childWorkflow.Run(hCtx, ChildInput{Value: 1})
if err != nil {
	// Handle error from child workflow
	fmt.Printf("Child workflow failed: %v\n", err)
	// Decide how to proceed - retry, skip, or fail the parent
}
```

#### Ruby

```ruby
begin
  FANOUT_CHILD_WF.run({ "a" => "b" })
rescue StandardError => e
  puts "Child workflow failed: #{e.message}"
end
```

## Ways to use child spawning

### Fan-out: spawning many children in parallel

Process a list of items whose length is only known at runtime. Spawn one child per item, collect all results, then continue. Document processing and batch processing are the canonical examples: when a batch of files arrives, a parent fans out to one child per document; each child parses, extracts, and validates its document in parallel across your worker fleet.

[Concurrency](/v1/concurrency) controls how many children run at once. Hatchet distributes child tasks across available workers, so adding workers increases throughput without code changes. For rate-limited external services (OCR, LLM APIs), combine with [Rate Limits](/v1/rate-limits) to throttle child execution across all workers.

### Agent reasoning loops

An agent loop runs by having a durable task spawn a new child run of itself with updated input until a termination condition is met. Each iteration is a separate child task, so you get full observability in the dashboard. AI agents use this when they reason about what to do next, spawn a subtask (or a sub-workflow), inspect the result, and decide whether to continue, branch, or stop.

### Spawning trees of work

A durable task can spawn child durable tasks, each of which may spawn their own children. This creates a tree of work that's entirely driven by runtime logic — useful for crawlers, recursive search, and tree-structured computations.
