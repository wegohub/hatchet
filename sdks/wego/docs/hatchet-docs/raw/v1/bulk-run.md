# Running Tasks in Bulk

Often you may want to run a task multiple times with different inputs. There is significant overhead (i.e. network roundtrips) to write the task, so if you're running multiple tasks, it's best to use the bulk run methods.

#### Python

You can use the `aio_run_many` method to bulk run a task. This will return a list of results.

```python
greetings = ["Hello, World!", "Hello, Moon!", "Hello, Mars!"]

results = await child_task.aio_run_many(
    [
        # run each greeting as a task in parallel
        child_task.create_bulk_run_item(
            input=SimpleInput(message=greeting),
        )
        for greeting in greetings
    ]
)

# this will await all results and return a list of results
print(results)
```

> **Info:** `Workflow.create_bulk_run_item` is a typed helper to create the inputs for
>   each task.

There are additional bulk methods available on the `Workflow` object.

- `aio_run_many`
- `aio_run_many_no_wait`

And blocking variants:

- `run_many`
- `run_many_no_wait`

As with the run methods, you can call bulk methods from within a task and the runs will be associated with the parent task in the dashboard.

#### Typescript

You can use the `run` method directly to bulk run tasks by passing an array of inputs. This will return a list of results.

```typescript
const res = await simple.run([
  {
    Message: 'HeLlO WoRlD',
  },
  {
    Message: 'Hello MoOn',
  },
]);

// 👀 Access the results of the Task
console.log(res[0].TransformedMessage);
console.log(res[1].TransformedMessage);
```

There are additional bulk methods available on the `Task` object.

- `run`
- `runNoWait`

You can also use `runMany` and `runManyNoWait` for per-run options while keeping the same bulk execution behavior.

```typescript
const runManyRes = await simple.runMany([
  {
    input: {
      Message: 'HeLlO WoRlD',
    },
  },
  {
    input: {
      Message: 'Hello MoOn',
    },
    opts: {
      priority: 3,
    },
  },
]);

console.log(runManyRes[0].TransformedMessage);
console.log(runManyRes[1].TransformedMessage);
```

Additional `Task` bulk methods:

- `runMany`
- `runManyNoWait`

As with the run methods, you can call bulk methods on the task fn context parameter within a task and the runs will be associated with the parent task in the dashboard.

```typescript
const parent = hatchet.task({
  name: 'simple',
  fn: async (input: SimpleInput, ctx) => {
    // Bulk run two tasks in parallel
    const child = await ctx.bulkRunChildren([
      {
        workflow: simple,
        input: {
          Message: 'Hello, World!',
        },
      },
      {
        workflow: simple,
        input: {
          Message: 'Hello, Moon!',
        },
      },
    ]);

    return {
      TransformedMessage: `${child[0].TransformedMessage} ${child[1].TransformedMessage}`,
    };
  },
});
```

Available bulk methods on the `Context` object are: - `bulkRunChildren` - `bulkRunChildrenNoWait`

#### Go

You can use the `RunMany` method directly on the `Workflow` or `StandaloneTask` instance to bulk run tasks by passing an array of inputs. This will return a list of run IDs.

```go
// Prepare inputs as []RunManyOpt for bulk run
inputs := make([]hatchet.RunManyOpt, len(bulkInputs))
for i, input := range bulkInputs {
	inputs[i] = hatchet.RunManyOpt{
		Input: input,
	}
}

// Run workflows in bulk
ctx := context.Background()
runRefs, err := workflow.RunMany(ctx, inputs)
if err != nil {
	log.Fatalf("failed to run bulk workflows: %v", err)
}
```

Additional bulk methods are coming soon for the Go SDK. Join our [Discord](https://hatchet.run/discord) to stay up to date.

#### Ruby

```ruby
greetings = ["Hello, World!", "Hello, Moon!", "Hello, Mars!"]

results = CHILD_TASK_WF.run_many(
  greetings.map do |greeting|
    CHILD_TASK_WF.create_bulk_run_item(
      input: { "message" => greeting }
    )
  end
)

puts results
```
