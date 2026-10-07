# Assigning priority to tasks in Hatchet

Hatchet allows you to assign different `priority` values to your tasks depending on how soon you want them to run. `priority` can be set to either `1`, `2`, or `3`, (`low`, `medium`, and `high`, respectively) with relatively higher values resulting in that task being picked up before others of the same type. **By default, runs in Hatchet have a priority of 1 (low) unless otherwise specified.**


Priority only affects multiple runs of a _single_ workflow. If you have two different workflows (A and B) and set A to globally have a priority of 3, and B to globally have a priority of 1, this does _not_ guarantee that if there is one task from A and one from B in the queue, that A's task will be run first.

However, _within_ A, if you enqueue one task with priority 3 and one with priority 1, the priority 3 task will be run first.


A couple of common use cases for assigning priorities are things like:

1. Having high-priority (e.g. paying, new, etc.) customers be prioritized over lower-priority ones, allowing them to get faster turnaround times on their tasks.
2. Having tasks triggered via your API run with higher priority than the same tasks triggered by a cron.

## Setting priority for a task or workflow

There are a few different ways to set priorities for tasks or workflows in Hatchet.

### Workflow-level default priority

First, you can set a default priority at the workflow level:

#### Ruby

```python
DEFAULT_PRIORITY = Priority.LOW
SLEEP_TIME = 0.25

priority_workflow = hatchet.workflow(
    name="PriorityWorkflow",
    default_priority=DEFAULT_PRIORITY,
)
```

#### Tab 2

```typescript
export const priorityWf = hatchet.workflow({
  name: 'priority-wf',
  defaultPriority: Priority.LOW,
});
```

#### Tab 3

```go
workflow := client.NewWorkflow(
	"priority",
	hatchet.WithWorkflowDefaultPriority(features.RunPriorityLow),
)
```

#### Tab 4

```ruby
DEFAULT_PRIORITY = 1
SLEEP_TIME = 0.25

PRIORITY_WORKFLOW = HATCHET.workflow(
  name: "PriorityWorkflow",
  default_priority: DEFAULT_PRIORITY
)

PRIORITY_WORKFLOW.task(:priority_task) do |input, ctx|
  puts "Priority: #{ctx.priority}"
  sleep SLEEP_TIME
end
```

This will assign the same default priority to all runs of this workflow (and all of the workflow's corresponding tasks), but will have no effect without also setting run-level priorities, since every run will use the same default.

### Priority-on-trigger

When you trigger a run, you can set the priority of the triggered run to override its default priority.

#### Ruby

```python
low_prio = priority_workflow.run(
    ## 👀 Adding priority and key to metadata to show them in the dashboard
    priority=Priority.LOW,
    additional_metadata={"priority": "low", "key": 1},
    wait_for_result=False,
)

high_prio = priority_workflow.run(
    ## 👀 Adding priority and key to metadata to show them in the dashboard
    priority=Priority.HIGH,
    additional_metadata={"priority": "high", "key": 1},
    wait_for_result=False,
)
```

#### Tab 2

```typescript
const run = priority.run(new Date(Date.now() + 60 * 60 * 1000), { priority: Priority.HIGH });
```

#### Tab 3

```go
ref, err := client.RunNoWait(
	context.Background(),
	workflow.GetName(),
	PriorityInput{},
	hatchet.WithRunPriority(features.RunPriorityLow),
)
if err != nil {
	return err
}
```

#### Tab 4

```ruby
low_prio = PRIORITY_WORKFLOW.run_no_wait(
  {},
  options: Hatchet::TriggerWorkflowOptions.new(
    priority: 1,
    additional_metadata: { "priority" => "low", "key" => 1 }
  )
)

high_prio = PRIORITY_WORKFLOW.run_no_wait(
  {},
  options: Hatchet::TriggerWorkflowOptions.new(
    priority: 3,
    additional_metadata: { "priority" => "high", "key" => 1 }
  )
)
```

Similarly, you can also assign a priority to scheduled and cron workflows.

#### Ruby

```python
schedule = priority_workflow.schedule(
    run_at=datetime.now(tz=timezone.utc) + timedelta(minutes=1),
    priority=Priority.HIGH,
)

cron = priority_workflow.create_cron(
    cron_name="my-scheduled-cron",
    expression="0 * * * *",
    priority=Priority.HIGH,
)
```

#### Tab 2

```typescript
const scheduled = priority.schedule(
  new Date(Date.now() + 60 * 60 * 1000),
  {},
  { priority: Priority.HIGH }
);
const delayed = priority.delay(60 * 60 * 1000, {}, { priority: Priority.HIGH });
const cron = priority.cron(
  `daily-cron-${Math.random()}`,
  '0 0 * * *',
  {},
  { priority: Priority.HIGH }
);
```

#### Tab 3

```go
priority := features.RunPriorityHigh

schedule, err := client.Schedules().Create(
	context.Background(),
	workflow.GetName(),
	features.CreateScheduledRunTrigger{
		Priority: &priority,
	},
)
if err != nil {
	return err
}

cron, err := client.Crons().Create(
	context.Background(),
	workflow.GetName(),
	features.CreateCronTrigger{
		Priority: &priority,
	},
)
if err != nil {
	return err
}
```

#### Tab 4

```ruby
schedule = PRIORITY_WORKFLOW.schedule(
  Time.now + 60,
  options: Hatchet::TriggerWorkflowOptions.new(priority: 3)
)

cron = PRIORITY_WORKFLOW.create_cron(
  "my-scheduled-cron",
  "0 * * * *",
  input: {},
)
```

In these cases, the priority set on the trigger will override the default priority, so these runs will be processed ahead of lower-priority ones.
