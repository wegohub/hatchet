# Scheduled Runs

Scheduled runs allow you to trigger a task at a specific time in the future. Some example use cases of scheduling runs might include:

- Sending a reminder email at a specific time after a user took an action.
- Running a one-time maintenance task at a predetermined time as determined by your application. For instance, you might want to run a database vacuum during a maintenance window any time a task matches a certain criteria.
- Allowing a customer to decide when they want your application to perform a specific task. For instance, if your application is a simple alarm app that sends a customer a notification at a time that they specify, you might create a scheduled run for each alarm that the customer sets.

Hatchet supports scheduled runs to run on a schedule defined in a few different ways:

- [Programmatically](/v1/scheduled-runs#programmatically-creating-scheduled-runs): Use the Hatchet SDKs to dynamically set the schedule of a task.
- [Hatchet Dashboard](/v1/scheduled-runs#managing-scheduled-runs-in-the-hatchet-dashboard): Manually create scheduled runs from the Hatchet Dashboard.

> **Warning:** The scheduled time is when Hatchet **enqueues** the task, not when the run
>   starts. Scheduling constraints like concurrency limits, rate limits, and retry
>   policies can affect run start times.

## Programmatically Creating Scheduled Runs

### Create a Scheduled Run

You can create dynamic scheduled runs programmatically via the API to run tasks at a specific time in the future.

Here's an example of creating a scheduled run to trigger a task tomorrow at noon:

#### Python

```python
from datetime import datetime

from examples.simple.worker import simple

schedule = simple.schedule(datetime(2025, 3, 14, 15, 9, 26))

## 👀 do something with the id
print(schedule.scheduled_workflows[0].id)
```

#### Typescript

```typescript
const runAt = new Date(new Date().setHours(12, 0, 0, 0) + 24 * 60 * 60 * 1000);

const scheduled = await simple.schedule(runAt, {
  Message: 'hello',
});

// 👀 Get the scheduled run ID of the workflow
// it may be helpful to store the scheduled run ID of the workflow
// in a database or other persistent storage for later use
const scheduledRunId = scheduled.metadata.id;
console.log(scheduledRunId);
```

#### Go

```go
scheduledRun, err := client.Schedules().Create(
	context.Background(),
	"scheduled",
	features.CreateScheduledRunTrigger{
		TriggerAt: time.Now().Add(1 * time.Minute),
		Input:     map[string]interface{}{"message": "Hello, World!"},
	},
)
if err != nil {
	log.Fatalf("failed to create scheduled run: %v", err)
}
```

#### Ruby

```ruby
schedule = SIMPLE.schedule(Time.now + 86_400, input: { "message" => "Hello, World!" })

## do something with the id
puts schedule.scheduled_workflows.first.id
```

In this example you can have different scheduled times for different customers, or dynamically set the scheduled time based on some other business logic.

When creating a scheduled run via the API, you will receive a scheduled run object with a metadata property containing the id of the scheduled run. This id can be used to reference the scheduled run when deleting the scheduled run and is often stored in a database or other persistence layer.

> **Info:** Note: Be mindful of the time zone of the scheduled run. Scheduled runs are
>   **always** stored and returned in UTC.

### Deleting a Scheduled Run

You can delete a scheduled run by calling the `delete` method on the scheduled client.

#### Python

```python
hatchet.scheduled.delete(scheduled_id=scheduled_run.metadata.id)
```

#### Typescript

```typescript
await hatchet.scheduled.delete(scheduled);
```

#### Go

```go
err = client.Schedules().Delete(
	context.Background(),
	scheduledRun.Metadata.Id,
)
if err != nil {
	log.Fatalf("failed to delete scheduled run: %v", err)
}
```

#### Ruby

```ruby
hatchet.scheduled.delete(scheduled_run.metadata.id)
```

### Listing Scheduled Runs

You can list all scheduled runs for a task by calling the `list` method on the scheduled client.

#### Python

```python
scheduled_runs = hatchet.scheduled.list()
```

#### Typescript

```typescript
const scheduledRuns = await hatchet.scheduled.list({
  workflow: simple,
});
console.log(scheduledRuns);
```

#### Go

```go
scheduledRuns, err := client.Schedules().List(
	context.Background(),
	rest.WorkflowScheduledListParams{},
)
if err != nil {
	log.Fatalf("failed to list scheduled runs: %v", err)
}
```

#### Ruby

```ruby
scheduled_runs = hatchet.scheduled.list
```

### Rescheduling a Scheduled Run

If you need to change the trigger time for an existing scheduled run, you can reschedule it by updating its `triggerAt`.

#### Python

```python
hatchet.scheduled.update(
    scheduled_id=scheduled_run.metadata.id,
    trigger_at=datetime.now(tz=timezone.utc) + timedelta(hours=1),
)
```

#### Typescript

```typescript
await hatchet.scheduled.update(scheduledRunId, {
  triggerAt: new Date(Date.now() + 60 * 60 * 1000),
});
```

#### Ruby

```ruby
hatchet.scheduled.update(
  scheduled_run.metadata.id,
  trigger_at: Time.now + 3600
)
```

> **Warning:** You can only reschedule scheduled runs created via the API (not runs created
>   via a code-defined schedule), and Hatchet may reject rescheduling if the run
>   has already triggered.

### Bulk operations (delete / reschedule)

Hatchet supports bulk operations for scheduled runs. You can bulk delete scheduled runs, and you can bulk reschedule scheduled runs by providing a list of updates.

#### Python

```python
hatchet.scheduled.bulk_delete(scheduled_ids=[id])

hatchet.scheduled.bulk_delete(
    workflow_id="workflow_id",
    statuses=[ScheduledRunStatus.SCHEDULED],
    additional_metadata={"customer_id": "customer-a"},
)
```
```python
hatchet.scheduled.bulk_update(
    [
        (id, datetime.now(tz=timezone.utc) + timedelta(hours=2)),
    ]
)
```

#### Typescript

```typescript
await hatchet.scheduled.bulkDelete({
  scheduledRuns: [scheduledRunId],
});
```
```typescript
await hatchet.scheduled.bulkUpdate([
  { scheduledRun: scheduledRunId, triggerAt: new Date(Date.now() + 2 * 60 * 60 * 1000) },
]);
```

#### Ruby

```ruby
hatchet.scheduled.bulk_delete(scheduled_ids: [id])
```
```ruby
hatchet.scheduled.bulk_update(
  [[id, Time.now + 7200]]
)
```

## Managing Scheduled Runs in the Hatchet Dashboard

In the Hatchet Dashboard, you can view and manage scheduled runs for your tasks.

Navigate to "Triggers" > "Scheduled Runs" in the left sidebar and click "Create Scheduled Run" at the top right.

You can specify run parameters such as Input, Additional Metadata, and the Scheduled Time.

![Create Scheduled Run](/schedule-dash.gif)

You can also manage existing scheduled runs:

- **Single-run actions**: Use the per-row actions menu to **Reschedule** or **Delete** an individual scheduled run.
- **Bulk actions**: Use the **Actions** menu to bulk **Delete** or **Reschedule** either:
  - The selected rows, or
  - All rows matching the current filters (including “all” if no filters are set).

> **Info:** In the dashboard, reschedule/delete actions may be disabled for runs that were
>   created via a code-defined schedule, and rescheduling may be disabled for runs
>   that have already triggered.

## Scheduled Run Considerations

When using scheduled runs, there are a few considerations to keep in mind:

1. **Time Zone**: Scheduled runs are stored and returned in UTC. Make sure to consider the time zone when defining your scheduled time.

2. **Execution Time**: The actual execution time of a scheduled run may vary slightly from the scheduled time. Hatchet makes a best-effort attempt to enqueue the task as close to the scheduled time as possible, but there may be slight delays due to system load or other factors.

3. **Missed Schedules**: If a scheduled task is missed (e.g., due to system downtime), Hatchet will not automatically run the missed instances when the service comes back online.

4. **Overlapping Schedules**: If a task is still running when a second scheduled run is scheduled to start, Hatchet will start a new instance of the task or respect [concurrency](/v1/concurrency) policy.
