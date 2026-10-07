# Recurring Runs with Cron

A [Cron](https://en.wikipedia.org/wiki/Cron) is a time-based job scheduler that allows you to define when a task should be executed automatically on a pre-determined schedule.

Some example use cases for cron-style tasks might include:

1. Running a daily report at a specific time.
2. Sending weekly digest emails to users about their activity from the past week.
3. Running a monthly billing process to generate invoices for customers.

Hatchet supports cron triggers to run on a schedule defined in a few different ways:

- [Task Definitions](/v1/cron-runs#defining-a-cron-in-your-task-definition): Define a cron expression in your task definition to trigger the task on a predefined schedule.
- [Dynamic Programmatically](/v1/cron-runs#programmatically-creating-cron-triggers): Use the Hatchet SDKs to dynamically set the cron schedule of a task.
- [Hatchet Dashboard](/v1/cron-runs#managing-cron-jobs-in-the-hatchet-dashboard): Manually create cron triggers from the Hatchet Dashboard.

> **Warning:** The expression is when Hatchet **enqueues** the task, not when the run starts.
>   Scheduling constraints like concurrency limits, rate limits, and retry
>   policies can affect run start times.

### Cron Expression Syntax

Cron expressions in Hatchet follow the standard cron syntax. Hatchet supports both 5-field and 6-field expressions.
A cron expression consists of 5 to 6 fields separated by spaces. If there are 6 fields, the first field is seconds; if
there are 5 fields, the first field is minutes.

```
┌───────────── second (0 - 59) (optional)
│ ┌───────────── minute (0 - 59)
│ │ ┌───────────── hour (0 - 23)
│ │ │ ┌───────────── day of the month (1 - 31)
│ │ │ │ ┌───────────── month (1 - 12)
│ │ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday)
* * * * * *
```

Each field can contain a specific value, an asterisk (`*`) to represent all possible values, or a range of values. Here are some examples of cron expressions:

- `0 0 * * *`: Run every day at midnight
- `*/15 * * * *`: Run every 15 minutes
- `0 9 * * 1`: Run every Monday at 9 AM
- `0 0 1 * *`: Run on the first day of every month at midnight
- `30 * * * * *`: Run at 30 seconds past every minute (6-field)

> **Info:** Keep in mind, Hatchet Cloud meters by Task runs so use seconds wisely. Have
>   questions about pricing? [Contact us](https://cal.com/team/hatchet/talk-to-us)

## Defining a Cron in Your Task Definition

You can define a task with a cron schedule by configuring the cron expression as part of the task definition:

#### Python-Sync

```python
# Adding a cron trigger to a workflow is as simple
# as adding a `cron expression` to the `on_cron`
# prop of the workflow definition

cron_workflow = hatchet.workflow(name="CronWorkflow", on_crons=["* * * * *"])


@cron_workflow.task()
def step1(input: EmptyModel, ctx: Context) -> dict[str, str]:
    return {
        "time": "step1",
    }
```

#### Python-Async

```python
# Adding a cron trigger to a workflow is as simple
# as adding a `cron expression` to the `on_cron`
# prop of the workflow definition

cron_workflow = hatchet.workflow(name="CronWorkflow", on_crons=["* * * * *"])


@cron_workflow.task()
def step1(input: EmptyModel, ctx: Context) -> dict[str, str]:
    return {
        "time": "step1",
    }
```

#### Typescript

```typescript
export const onCron = hatchet.workflow({
  name: 'on-cron-workflow',
  on: {
    // 👀 add a cron expression to run the workflow every 15 minutes
    cron: '*/15 * * * *',
  },
});
```

#### Go

```go
dailyCleanup := client.NewStandaloneTask("cleanup-temp-files", func(ctx hatchet.Context, input CronInput) (CronOutput, error) {
	log.Printf("Running daily cleanup at %s", input.Timestamp)

	time.Sleep(2 * time.Second)

	return CronOutput{
		JobName:    "daily-cleanup",
		ExecutedAt: time.Now().Format(time.RFC3339),
		NextRun:    "Next run: tomorrow at 2 AM",
	}, nil
},
	hatchet.WithWorkflowCron("0 2 * * *"),
	hatchet.WithWorkflowCronInput(CronInput{
		Timestamp: time.Now().Format(time.RFC3339),
	}),
	hatchet.WithWorkflowDescription("Daily cleanup and maintenance tasks"),
)
```

#### Ruby

```ruby
CRON_WORKFLOW = HATCHET.workflow(
  name: "CronWorkflow",
  on_crons: ["*/5 * * * *"]
)

CRON_WORKFLOW.task(:cron_task) do |input, ctx|
  puts "Cron task executed at #{Time.now}"
  { "status" => "success" }
end
```

In the examples above, we set the `on cron` property of the task. The property specifies the cron expression that determines when the task should be triggered.


  Note: When modifying a cron in your task definition, it will override any cron
  schedule for previous crons defined in previous task definitions, but crons
  created via the API or Dashboard will still be respected.


## Programmatically Creating Cron Triggers

### Create a Cron Trigger

You can create dynamic cron triggers programmatically via the API. This is useful if you want to create a cron trigger that is not known at the time of task definition,

Here's an example of creating a a cron to trigger a report for a specific customer every day at noon:

#### Python-Sync

```python
cron_trigger = dynamic_cron_workflow.create_cron(
    cron_name="customer-a-daily-report",
    expression="0 12 * * *",
    input=DynamicCronInput(name="John Doe"),
    additional_metadata={
        "customer_id": "customer-a",
    },
)


id = cron_trigger.metadata.id  # the id of the cron trigger
```

#### Python-Async

```python
cron_trigger = await dynamic_cron_workflow.aio_create_cron(
    cron_name="customer-a-daily-report",
    expression="0 12 * * *",
    input=DynamicCronInput(name="John Doe"),
    additional_metadata={
        "customer_id": "customer-a",
    },
)

cron_trigger.metadata.id  # the id of the cron trigger
```

#### Typescript

```typescript
const cron = await simple.cron('simple-daily', '0 0 * * *', {
  Message: 'hello',
});

// it may be useful to save the cron id for later
const cronId = cron.metadata.id;
```

#### Go

```go
createdCron, err := client.Crons().Create(context.Background(), "cleanup-temp-files", features.CreateCronTrigger{
	Name:       "daily-cleanup",
	Expression: "0 0 * * *",
	Input: map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339),
	},
	AdditionalMetadata: map[string]interface{}{
		"description": "Daily cleanup and maintenance tasks",
	},
})
if err != nil {
	return err
}
```

#### Ruby

```ruby
cron_trigger = dynamic_cron_workflow.create_cron(
  "customer-a-daily-report",
  "0 12 * * *",
  input: { "name" => "John Doe" }
)

id = cron_trigger.metadata.id
```

In this example you can have different expressions for different customers, or dynamically set the expression based on some other business logic.

When creating a cron via the API, you will receive a cron trigger object with a metadata property containing the id of the cron trigger. This id can be used to reference the cron trigger when deleting the cron trigger and is often stored in a database or other persistence layer.


  Note: Cron Name and Expression are required fields when creating a cron
  trigger and we enforce a unique constraint on the two.


### Delete a Cron Trigger

You can delete a cron trigger by passing the cron object or a cron trigger id to the delete method.

#### Python-Sync

```python
hatchet.cron.delete(cron_id=cron_trigger.metadata.id)
```

#### Python-Async

```python
await hatchet.cron.aio_delete(cron_id=cron_trigger.metadata.id)
```

#### Typescript

```typescript
await hatchet.crons.delete(cronId);
```

#### Go

```go
err = client.Crons().Delete(context.Background(), createdCron.Metadata.Id)
if err != nil {
	return err
}
```

#### Ruby

```ruby
hatchet.cron.delete(cron_trigger.metadata.id)
```


  Note: Deleting a cron trigger will not cancel any currently running instances
  of the task. It will simply stop the cron trigger from triggering the task
  again.


### List Cron Triggers

Retrieves a list of all task cron triggers matching the criteria.

#### Python-Sync

```python
cron_triggers = hatchet.cron.list()
```

#### Python-Async

```python
await hatchet.cron.aio_list()
```

#### Typescript

```typescript
const crons = await hatchet.crons.list({
  workflow: simple,
});
```

#### Go

```go
cronList, err := client.Crons().List(context.Background(), rest.CronWorkflowListParams{
	AdditionalMetadata: &[]string{"description:Daily cleanup and maintenance tasks"},
})
if err != nil {
	return err
}
```

#### Ruby

```ruby
cron_triggers = hatchet.cron.list
```

## Managing Cron Triggers in the Hatchet Dashboard

In the Hatchet Dashboard, you can view and manage cron triggers for your tasks.

Navigate to "Triggers" > "Cron Jobs" in the left sidebar and click "Create Cron Job" at the top right.

You can specify run parameters such as Input, Additional Metadata, and the Expression.

![Create Cron Job](/cron-dash.gif)

## Cron Considerations

When using cron triggers, there are a few considerations to keep in mind:

1. **Time Zone**: Cron schedules are UTC. Make sure to consider the time zone when defining your cron expressions.

2. **Execution Time**: The actual execution time of a cron-triggered task may vary slightly from the scheduled time. Hatchet makes a best-effort attempt to enqueue the task as close to the scheduled time as possible, but there may be slight delays due to system load or other factors.

3. **Missed Schedules**: If a scheduled task is missed (e.g., due to system downtime), Hatchet will **not** automatically run the missed instances. It will wait for the next scheduled time to trigger the task.

4. **Overlapping Schedules**: If a task is still running when the next scheduled time arrives, Hatchet will start a new instance of the task or respect the [concurrency](/v1/concurrency) policy.
