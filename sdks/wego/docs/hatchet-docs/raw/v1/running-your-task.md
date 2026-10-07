# Running Tasks

Once you've [defined some tasks](/v1/tasks) and registered them on a [worker](/v1/workers), you're ready to run them!

Hatchet lets you run tasks in a number of ways, which support different application needs. In broad strokes, these are [fire-and-wait](#fire-and-wait), various forms of [fire-and-forget-style triggering](#fire-and-forget), and scheduling tasks to run either [periodically](#crons) or at some [specific time in the future](#scheduled-runs).

## Fire-and-wait

Fire-and-wait is a common way of triggering a task which blocks until it completes and returns a result. This is particularly useful for situations where you want to do something with the result of your task. For instance, if your task generates some LLM output which you want to return to the user or persist in the database, you might trigger a task, wait for it to complete, collect its result, and then continue on with your application logic.

#### Python

Call `run` or `aio_run` on a `Task` or `Workflow` object to invoke it. These methods block until the task or workflow completes and return the result.

```python
from examples.child.worker import SimpleInput, child_task

child_task.run(SimpleInput(message="Hello, World!"))
```

The run methods in Hatchet's Python SDK also have async flavors you can `await`. These are prefixed with `aio_`, such as `aio_run`.

```python
result = await child_task.aio_run(SimpleInput(message="Hello, World!"))
```

Note that the type of `input` here is a Pydantic model that matches the input schema of the task or workflow being triggered.

#### Typescript

Call `run` on the `Task` object to invoke it. This returns a promise that resolves when the task completes and returns the result.

```typescript
const res = await parent.run(
  {
    Message: 'HeLlO WoRlD',
  },
  {
    additionalMetadata: {
      test: 'test',
    },
  }
);

const res3 = await simpleWithZod.run({
  Message: 'HeLlO WoRlD',
});
console.log(res3.TransformedMessage);

// 👀 Access the results of the Task
console.log(res.TransformedMessage);
```

#### Go

Call `Run` on the `Task` object to invoke it. This blocks until the task completes and returns the result.

```go
result, err := task.Run(context.Background(), SimpleInput{Message: "Hello, World!"})
if err != nil {
	return err
}
```

#### Ruby

Call `run` on the `Task` object to invoke it. This blocks until the task completes and returns the result.

```ruby
result = CHILD_TASK_WF.run({ "message" => "Hello, World!" })
```

## Fire-and-forget

On the other hand, fire-and-forget-style triggering enqueues a task without waiting for the result. This is useful for background jobs like sending emails, processing uploads, or kicking off long-running pipelines where the application does _not_ need to wait for the result to continue along.

#### Python

Call `run` with `wait_for_result=False` on a `Task` or `Workflow` object to enqueue it fire-and-forget. This returns a `WorkflowRunRef` you can use to access the run ID and result later.

```python
ref = say_hello.run(input=HelloInput(name="World"), wait_for_result=False)
```

There's also an async flavor:

```python
ref = await say_hello.aio_run(
    input=HelloInput(name="Async World"), wait_for_result=False
)
```

Note that the type of `input` here is a Pydantic model that matches the input schema of the task.

#### Typescript

Call `runNoWait` on the `Task` object to enqueue it without waiting for the result. This returns a `WorkflowRunRef`.

```typescript
import { simple } from './workflow';
// ...

async function main() {
  // 👀 Enqueue the workflow
  const run = await simple.runNoWait({
    Message: 'hello',
  });

  // 👀 Get the run ID of the workflow
  const runId = await run.getWorkflowRunId();
  // It may be helpful to store the run ID of the workflow
  // in a database or other persistent storage for later use
  console.log(runId);
```

#### Go

Call `RunNoWait` on the `Task` object to enqueue it without waiting for the result. This returns a `WorkflowRunRef`.

```go
runRef, err := task.RunNoWait(context.Background(), SimpleInput{Message: "Hello, World!"})
if err != nil {
	return err
}

fmt.Println(runRef.RunId)
```

#### Ruby

Call `run_no_wait` on the `Task` object to enqueue it without waiting for the result. This returns a `WorkflowRunRef`.

```ruby
ref = SAY_HELLO.run_no_wait({ "name" => "World" })
```

When running a task fire-and-forget-style, you can also always retrieve the result later on. The workflow run ref that's returned from these trigger methods has a result method, which lets you retrieve the result of the triggered task if you need it later.

#### Python

Use `ref.result()` to block until the result is available:

```python
result = ref.result()
```

or await `aio_result`:

```python
result = await ref.aio_result()
```

#### Typescript

```typescript
// the return object of the enqueue method is a WorkflowRunRef which includes a listener for the result of the workflow
const result = await run.output;
console.log(result);

// if you need to subscribe to the result of the workflow at a later time, you can use the runRef method and the stored runId
const ref = hatchet.runRef(runId);
const result2 = await ref.output;
console.log(result2);
```

#### Go

```go
result, err := runRef.Result()
if err != nil {
	return err
}

var resultOutput SimpleOutput
err = result.TaskOutput("process-message").Into(&resultOutput)
if err != nil {
	return err
}

fmt.Println(resultOutput.Result)
```

#### Ruby

```ruby
result = ref.result
```

### Triggering from events

Another method of running tasks fire-and-forget style is via [pushing events](/v1/events#pushing-events-to-hatchet) to Hatchet. If a task is configured to be triggered by an event, then when the event is pushed to Hatchet, a corresponding run will be triggered using the payload of the event as the input to the run.

You can push events to Hatchet directly using the SDKs, or via [webhooks](/v1/webhooks), which are converted into Hatchet events internally on ingestion.

## Crons

Another common way to run tasks is on a cron schedule, which Hatchet [supports natively](/v1/cron-runs). Crons are useful for running tasks that are expected to run at the same time every day, such as data processing pipelines, reconciliation jobs, and so on.

Note that Hatchet supports second-level cron granularity, although in most cases using the minutes as the most granular level is perfectly fine.

## Scheduled runs

Finally, Hatchet also supports [scheduling a run to be triggered at a specific time in the future](/v1/scheduled-runs). This is particularly useful for situations where you want to wait a known amount of time before running some task, such as sending a follow up email or a welcome email to a new user, or allowing your customers to choose when they want something to run, such as sending a reminder, for instance.

## Triggering from the dashboard

Finally, there are a number of pages in the dashboard that have a `Trigger Run` button. You can provide run parameters such as input, additional metadata, and a scheduled time.

![Create Scheduled Run](/schedule-dash.gif)
