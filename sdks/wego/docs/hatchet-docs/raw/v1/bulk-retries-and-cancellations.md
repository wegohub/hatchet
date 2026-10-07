# Bulk Cancellations and Replays

V1 adds the ability to cancel or replay task runs in bulk, which you can now do either in the Hatchet Dashboard or programmatically via the SDKs and the REST API.

There are two ways of bulk cancelling or replaying tasks in both cases:

1. You can provide a list of task run ids to cancel or replay, which will cancel or replay all of the tasks in the list.
2. You can provide a list of filters, similar to the list of filters on task runs in the Dashboard, and cancel or replay runs matching those filters. For instance, if you wanted to replay all failed runs of a `SimpleTask` from the past fifteen minutes that had the `foo` field in `additional_metadata` set to `bar`, you could apply those filters and replay all of the matching runs.

### Bulk Operations by Run Ids

The first way to bulk cancel or replay runs is by providing a list of run ids. This is the most straightforward way to cancel or replay runs in bulk.

#### Python

> **Info:** In the Python SDK, the mechanics of bulk replaying and bulk cancelling tasks
>   are exactly the same. The only change would be replacing e.g.
>   `hatchet.runs.bulk_cancel` with `hatchet.runs.bulk_replay`.

First, we'll start by fetching a task via the REST API.

```python
from datetime import datetime, timedelta, timezone

from hatchet_sdk import BulkCancelReplayOpts, Hatchet, RunFilter, V1TaskStatus

hatchet = Hatchet()

workflows = hatchet.workflows.list()

assert workflows.rows

workflow = workflows.rows[0]
```

Now that we have a task, we'll get runs for it, so that we can use them to bulk cancel by run id.

```python
workflow_runs = hatchet.runs.list(workflow_ids=[workflow.metadata.id])
```

And finally, we can cancel the runs in bulk.

```python
workflow_run_ids = [workflow_run.metadata.id for workflow_run in workflow_runs.rows]

bulk_cancel_by_ids = BulkCancelReplayOpts(ids=workflow_run_ids)

hatchet.runs.bulk_cancel(bulk_cancel_by_ids)
```

> **Info:** Note that the Python SDK also exposes async versions of each of these methods:
>
>       - `workflows.list` -> `await workflows.aio_list`
>       - `runs.list` -> `await runs.aio_list`
>       - `runs.bulk_cancel` -> `await runs.aio_bulk_cancel`

#### Typescript

> **Info:** The mechanics of bulk replaying and bulk cancelling tasks are exactly the
>   same. The only change would be replacing e.g. `hatchet.runs.cancel` with
>   `hatchet.runs.replay`.

First, we'll start by fetching a task via the REST API.

```typescript
const workflows = await hatchet.workflows.list();

if (!workflows.rows?.length) {
  throw new Error('no workflows found');
}

const [workflow] = workflows.rows;
```

Now that we have a task, we'll get runs for it, so that we can use them to bulk cancel by run id.

```typescript
const workflowRuns = await hatchet.runs.list({
  workflowNames: [workflow.name],
});
```

And finally, we can cancel the runs in bulk.

```typescript
const runIds = workflowRuns.rows?.map((run) => run.metadata.id) ?? [];

// to replay runs by their ids, use `hatchet.runs.replay` instead
await hatchet.runs.cancel({ ids: runIds });
```

#### Go

> **Info:** The mechanics of bulk replaying and bulk cancelling tasks are exactly the
>   same. The only change would be replacing e.g. `client.Runs().Cancel` with
>   `client.Runs().Replay`.

First, we'll start by fetching a task via the REST API.

```go
client, err := hatchet.NewClient()
if err != nil {
	log.Fatalf("failed to create hatchet client: %v", err)
}

ctx := context.Background()

workflows, err := client.Workflows().List(ctx, nil)
if err != nil {
	log.Fatalf("failed to list workflows: %v", err)
}

if workflows.Rows == nil || len(*workflows.Rows) == 0 {
	log.Fatal("no workflows found")
}

workflow := (*workflows.Rows)[0]
workflowId := uuid.MustParse(workflow.Metadata.Id)
```

Now that we have a task, we'll get runs for it, so that we can use them to bulk cancel by run id.

```go
workflowRuns, err := client.Runs().List(ctx, rest.V1WorkflowRunListParams{
	Since:       time.Now().Add(-24 * time.Hour),
	WorkflowIds: &[]types.UUID{workflowId},
})
if err != nil {
	log.Fatalf("failed to list workflow runs: %v", err)
}
```

And finally, we can cancel the runs in bulk.

```go
runIds := make([]types.UUID, len(workflowRuns.Rows))
for i, run := range workflowRuns.Rows {
	runIds[i] = uuid.MustParse(run.Metadata.Id)
}

// to replay runs by their ids, use `client.Runs().Replay` with a
// `rest.V1ReplayTaskRequest` instead
_, err = client.Runs().Cancel(ctx, rest.V1CancelTaskRequest{
	ExternalIds: &runIds,
})
if err != nil {
	log.Fatalf("failed to bulk cancel by run ids: %v", err)
}
```

#### Ruby

> **Info:** The mechanics of bulk replaying and bulk cancelling tasks are exactly the
>   same. The only change would be replacing e.g. `hatchet.runs.bulk_cancel` with
>   `hatchet.runs.bulk_replay`.

First, we'll start by fetching a task via the REST API.

```ruby
hatchet = Hatchet::Client.new

workflows = hatchet.workflows.list

workflow = workflows.rows.first
```

Now that we have a task, we'll get runs for it, so that we can use them to bulk cancel by run id.

```ruby
workflow_runs = hatchet.runs.list(workflow_ids: [workflow.metadata.id])
```

And finally, we can cancel the runs in bulk.

```ruby
workflow_run_ids = workflow_runs.rows.map { |run| run.metadata.id }

hatchet.runs.bulk_cancel(ids: workflow_run_ids)
```

### Bulk Operations by Filters

The second way to bulk cancel or replay runs is by providing a list of filters. This is the most powerful way to cancel or replay runs in bulk, as it allows you to cancel or replay all runs matching a set of arbitrary filters without needing to provide IDs for the runs in advance.

#### Python

The example below provides some filters you might use to cancel or replay runs in bulk. Importantly, these filters are very similar to the filters you can use in the Hatchet Dashboard to filter which task runs are displaying.

```python
bulk_cancel_by_filters = BulkCancelReplayOpts(
    filters=RunFilter(
        since=datetime.today() - timedelta(days=1),
        until=datetime.now(tz=timezone.utc),
        statuses=[V1TaskStatus.RUNNING],
        workflow_ids=[workflow.metadata.id],
        additional_metadata={"key": "value"},
    )
)

hatchet.runs.bulk_cancel(bulk_cancel_by_filters)
```

Running this request will cancel all task runs matching the filters provided.

#### Typescript

The example below provides some filters you might use to cancel or replay runs in bulk. Importantly, these filters are very similar to the filters you can use in the Hatchet Dashboard to filter which task runs are displaying.

```typescript
// to replay runs matching filters, use `hatchet.runs.replay` instead
await hatchet.runs.cancel({
  filters: {
    since: new Date(Date.now() - 24 * 60 * 60 * 1000),
    until: new Date(),
    statuses: [V1TaskStatus.RUNNING],
    workflowNames: [workflow.name],
    additionalMetadata: { key: 'value' },
  },
});
```

Running this request will cancel all task runs matching the filters provided.

#### Go

The example below provides some filters you might use to cancel or replay runs in bulk. Importantly, these filters are very similar to the filters you can use in the Hatchet Dashboard to filter which task runs are displaying.

```go
until := time.Now()

// to replay runs matching filters, use `client.Runs().Replay` with a
// `rest.V1ReplayTaskRequest` instead
_, err = client.Runs().Cancel(ctx, rest.V1CancelTaskRequest{
	Filter: &rest.V1TaskFilter{
		Since:              time.Now().Add(-24 * time.Hour),
		Until:              &until,
		Statuses:           &[]rest.V1TaskStatus{rest.V1TaskStatusRUNNING},
		WorkflowIds:        &[]types.UUID{workflowId},
		AdditionalMetadata: &[]string{"key:value"},
	},
})
if err != nil {
	log.Fatalf("failed to bulk cancel by filters: %v", err)
}
```

Running this request will cancel all task runs matching the filters provided.

#### Ruby

The example below provides some filters you might use to cancel or replay runs in bulk. Importantly, these filters are very similar to the filters you can use in the Hatchet Dashboard to filter which task runs are displaying.

```ruby
hatchet.runs.bulk_cancel(
  since: Time.now - 86_400,
  until_time: Time.now,
  statuses: ["RUNNING"],
  workflow_ids: [workflow.metadata.id],
  additional_metadata: { "key" => "value" }
)
```

Running this request will cancel all task runs matching the filters provided.

# Manual Retries

Hatchet provides a manual retry mechanism that allows you to handle failed task instances flexibly from the Hatchet dashboard.

Navigate to the specific task in the Hatchet dashboard and click on the failed run. From there, you can inspect the details of the run, including the input data and the failure reason for each task.

To retry a failed task, simply click on the task in the run details view and then click the "Replay" button. This will create a new instance of the task, starting from the failed task, and using the same input data as the original run.

Manual retries give you full control over when and how to reprocess failed instances. For example, you may choose to wait until an external service is back online before retrying instances that depend on that service, or you may need to deploy a bug fix to your task code before retrying instances that were affected by the bug.

## A Note on Dead Letter Queues

A dead letter queue (DLQ) is a messaging concept used to handle messages that cannot be processed successfully. In the context of task management, a DLQ can be used to store failed task instances that require manual intervention or further analysis.

While Hatchet does not have a built-in dead letter queue feature, the persistence of failed task instances in the dashboard serves a similar purpose. By keeping a record of failed instances, Hatchet allows you to track and manage failures, perform root cause analysis, and take appropriate actions, such as modifying input data or updating your task code before manually retrying the failed instances.

It's important to note that the term "dead letter queue" is more commonly associated with messaging systems like Apache Kafka or Amazon SQS, where unprocessed messages are automatically moved to a separate queue for manual handling. In Hatchet, the failed instances are not automatically moved to a separate queue but are instead persisted in the dashboard for manual management.
