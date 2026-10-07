# Task Slot Cost

Every worker has a fixed number of slots that limit how many tasks it runs at once, set with the `slots` option on the worker and defaulting to 100. By default a task consumes one slot while it runs. Slot cost lets a task consume more than one slot, so a task that needs more memory or CPU takes up more of a worker's capacity and the worker runs fewer of them at the same time.


## Setting slot cost

Set the slot cost when you define a task. A cost of 5 means the task reserves five slots while it runs.

#### Python

```python
@hatchet.task(slot_cost=5)
def omega(input: EmptyModel, ctx: Context) -> None:
    print("heavy work")


@hatchet.task(slot_cost=1)
def weenie(input: EmptyModel, ctx: Context) -> None:
    print("light work")
```

#### Typescript

```typescript
import { hatchet } from '../hatchet-client';

export const omega = hatchet.task({
  name: 'omega',
  slotCost: 5,
  fn: async () => {
    console.log('heavy work');
  },
});

export const weenie = hatchet.task({
  name: 'weenie',
  slotCost: 1,
  fn: async () => {
    console.log('light work');
  },
});
```

#### Go

```go
omega := client.NewStandaloneTask("omega", func(ctx hatchet.Context, input any) (any, error) {
	log.Println("heavy work")
	return nil, nil
}, hatchet.WithSlotCost(5))

weenie := client.NewStandaloneTask("weenie", func(ctx hatchet.Context, input any) (any, error) {
	log.Println("light work")
	return nil, nil
}, hatchet.WithSlotCost(1))
```

On a worker with 100 slots, the omega task at cost 5 and the weenie task at cost 1 draw from the same 100 slots. The worker runs at most 20 omega tasks, or 100 weenie tasks, or any mix whose costs sum to 100.

## How the reservation works

Slot capacity is local to a single worker. A task's slot cost is charged against the one worker that runs it, and a reservation cannot span two workers. A task with cost 5 needs a single worker with 5 free slots. It cannot take 3 slots from one worker and 2 from another, even when the worker pool has capacity in total.

Set the slot count on each worker to at least the largest slot cost you use. If a task's cost is greater than every worker's slot count, the task can never be scheduled. It waits in the queue until its [schedule timeout](/v1/timeouts) and is then cancelled.

> **Info:** Changing a task's slot cost changes its workflow version, so the next
>   registration creates a new version. Leaving the cost unset, or setting it to
>   1, keeps the task at one slot and does not change the version.

## Slot cost and concurrency limits

Slot cost and [concurrency limits](/v1/concurrency) solve different problems. A concurrency limit caps how many runs of a key execute at once, and with a static key the limit applies across the whole worker pool. Slot cost does not limit the number of runs. It changes how much of a worker one run consumes.

If you want at most five heavy runs at once, use a concurrency limit. If you want each heavy run to take five times the worker capacity of a light run, use slot cost. The two can be combined on the same task.
