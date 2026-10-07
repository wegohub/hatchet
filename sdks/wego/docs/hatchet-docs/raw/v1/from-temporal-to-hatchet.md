# Migrating from Temporal to Hatchet

Temporal and Hatchet are both platforms which allow developers to write [_durable workflows_](/v1/durable-execution). These are workflows whose intermediate state is persisted, which means that if your worker crashes or fails halfway through a workflow, it automatically picks up where it left off. This can be particularly useful for AI agents, long-running jobs, or business-critical workflows.

This guide assumes you have already decided to migrate a project from Temporal to Hatchet and want to understand what code and configuration need to change. Each section starts with a common Temporal pattern, then shows what replaces it in Hatchet and what to watch out for.

> **Info:** Code examples are given in Python, TypeScript, Go and Ruby, the four Hatchet
>   SDKs.

## Conceptual similarities and differences

Let's start with what's the same. The Temporal and Hatchet long-lived worker models are very similar. They both utilize client-instantiated, long-lived gRPC connections to connect to the Temporal or Hatchet engine, respectively. They also both implement primarily push-based scheduling: work is assigned from the engine to the worker, not the other way around.

However, there are a few important conceptual differences when you decide to migrate to Hatchet. In Temporal, everything operates as either a _workflow_ or an _activity_. The equivalent concepts in Hatchet are _durable tasks_ and _tasks_:

- A [**task**](/v1/tasks) is ordinary code. It runs on a [worker](/v1/workers), retries on failure, and has no determinism constraints. This is what most Temporal activities and most short Temporal workflows become. It is `@hatchet.task()` in Python, `hatchet.task()` in TypeScript, `client.NewStandaloneTask` in Go, and `HATCHET.task` in Ruby.
- A [**durable task**](/v1/durable-tasks) is the durable-execution primitive. It checkpoints every time it waits or spawns a child, and it carries the same determinism rules a Temporal workflow does. It is `@hatchet.durable_task()`, `hatchet.durableTask()`, `client.NewStandaloneDurableTask`, and `HATCHET.durable_task` respectively.

Second, workers in Hatchet have a built-in notion of [slots](/v1/workers#slots), instead of per-activity or per-workflow rate limits. This makes it very easy to reason about the amount of work that a worker is able to accept.

## Migration Steps

Step, Temporal, Hatchet replacement, Migration category

[1](#step-1-dependencies-and-connection), `temporalio` + namespace / mTLS config, `hatchet-sdk` + `HATCHET_CLIENT_TOKEN`, Operational change
[2](#step-2-replace-the-client-and-worker-bootstrap), `Client.connect(...)` + `Worker(task_queue=...)`, `Hatchet()` + `hatchet.worker(...)`, Small rewrite
[3](#step-3-convert-activities-to-tasks), `@activity.defn`, `@hatchet.task()`, Small rewrite
[4](#step-4-convert-workflows-to-durable-tasks), `@workflow.defn` + `execute_activity`, `@hatchet.durable_task()` that calls tasks, Small rewrite
[5](#step-5-invoke-work), `client.execute_workflow(...)`, `task.run()` / `await task.aio_run()`, Direct API swap
[5](#step-5-invoke-work), `client.start_workflow(...)`, `task.run(wait_for_result=False)`, Direct API swap
[6](#step-6-retries-and-timeouts), `RetryPolicy(...)`, `retries` / `backoff_factor` / `backoff_max_seconds`, Small rewrite
[6](#step-6-retries-and-timeouts), `ApplicationError(non_retryable=True)`, `NonRetryableException`, Direct API swap
[6](#step-6-retries-and-timeouts), `start_to_close_timeout`, `execution_timeout`, Direct API swap
[6](#step-6-retries-and-timeouts), `schedule_to_start_timeout`, `schedule_timeout`, Direct API swap
[7](#step-7-timers-and-sleeps), `await asyncio.sleep(...)` inside a workflow, `await ctx.aio_sleep_for(...)`, Direct API swap
[8](#step-8-signals-queries-and-updates), `@workflow.signal` + `handle.signal(...)`, Events + `await ctx.aio_wait_for_event(...)`, Conceptual redesign
[8](#step-8-signals-queries-and-updates), `@workflow.query`, Run history / dashboard / `ctx.aio_put_stream`, Conceptual redesign
[8](#step-8-signals-queries-and-updates), `workflow.wait_condition(...)`, `wait_for=[...]` conditions, or a durable event wait, Conceptual redesign
[9](#step-9-child-workflows-and-fan-out), `workflow.execute_child_workflow(...)`, `await child.aio_run(...)` from a durable task, Small rewrite
[10](#step-10-schedules-and-crons), `client.create_schedule(...)`, `on_crons=["..."]` or `hatchet.scheduled.aio_create(...)`, Small rewrite
[11](#step-11-versioning-and-determinism), `workflow.patched(...)` / `GetVersion`, Fewer durable surfaces + additive DAG changes, Conceptual redesign
[12](#step-12-flow-control), Task queue partitioning / custom rate limiting, `concurrency` / `rate_limits` / `priority` / worker slots, Simplification
[13](#step-13-simplification), A workflow that is a fixed sequence, or a wrapper, A `parents=[...]` DAG, or a plain task, Simplification
[14](#step-14-observability), Temporal Web UI + your own logging/tracing, Hatchet dashboard + built-in log sink and OTel collector, Simplification

## Step 1: Dependencies and connection

Install the Hatchet SDK:

#### Python

```bash
pip install hatchet-sdk
```

#### Typescript

```bash
npm install @hatchet-dev/typescript-sdk
```

#### Go

```bash
go get github.com/hatchet-dev/hatchet/sdks/go
```

#### Ruby

```bash
gem install hatchet-sdk
```

Temporal clients are configured with a server address, a namespace, and, on Temporal Cloud, mTLS certificates or an API key. Hatchet replaces all of that with a single token, read from the environment by every SDK:

```bash
export HATCHET_CLIENT_TOKEN="your-token-here"
```

The token encodes the tenant and the engine address, so there is no separate namespace or endpoint setting for [Hatchet Cloud](https://cloud.onhatchet.run). For [self-hosted](/self-hosting) deployments you may need environment-specific host and TLS settings.

Temporal and Hatchet can run side by side during a migration. They share no runtime, so each workflow must be moved as a unit: once a workflow runs on Hatchet, update the callers that start it.

## Step 2: Replace the client and worker bootstrap

Temporal separates the client from the worker, and routes work with a task queue name:

#### Python

```python
from temporalio.client import Client
from temporalio.worker import Worker

client = await Client.connect("localhost:7233", namespace="default")

worker = Worker(
    client,
    task_queue="my-task-queue",
    workflows=[ProcessOrderWorkflow],
    activities=[validate_order, charge_order, fulfill_order],
)
await worker.run()
```

#### Typescript

```typescript
import { NativeConnection, Worker } from "@temporalio/worker";
import * as activities from "./activities";

const connection = await NativeConnection.connect({
  address: "localhost:7233",
});

const worker = await Worker.create({
  connection,
  namespace: "default",
  taskQueue: "my-task-queue",
  // Workflows are registered by path — they run in a separate JS context.
  workflowsPath: require.resolve("./workflows"),
  activities,
});

await worker.run();
```

#### Go

```go
import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

c, err := client.Dial(client.Options{HostPort: client.DefaultHostPort})
if err != nil {
	log.Fatalln("Unable to create client", err)
}
defer c.Close()

w := worker.New(c, "my-task-queue", worker.Options{})

w.RegisterWorkflow(ProcessOrderWorkflow)
w.RegisterActivity(ChargeOrder)

if err := w.Run(worker.InterruptCh()); err != nil {
	log.Fatalln("Unable to start worker", err)
}
```

#### Ruby

```ruby
require "temporalio/client"
require "temporalio/worker"

client = Temporalio::Client.connect("localhost:7233", "default")

worker = Temporalio::Worker.new(
  client:,
  task_queue: "my-task-queue",
  workflows: [ProcessOrderWorkflow],
  activities: [ChargeOrder]
)

worker.run(shutdown_signals: ["SIGINT"])
```

In Hatchet, one client object is both the client and the registry, and workers are named rather than addressed through a queue:

#### Python

The client belongs in a shared module that the rest of your code imports:

```python
from hatchet_sdk import Hatchet

hatchet = Hatchet()
```

The worker imports that client and registers what it is allowed to run:

```python
def main() -> None:
    worker = hatchet.worker(
        "order-worker",
        slots=10,
        workflows=[validate_order, charge_order, fulfill_order, process_order],
    )
    worker.start()
```

#### Typescript

```typescript
import { HatchetClient } from '@hatchet/v1';

export const hatchet = HatchetClient.init();

async function main() {
  const worker = await hatchet.worker('order-worker', {
    workflows: [validateOrder, chargeOrder, fulfillOrder, processOrder],
    slots: 10,
  });

  await worker.start();
}
```

#### Go

```go
client, err := hatchet.NewClient()
if err != nil {
	return fmt.Errorf("creating hatchet client: %w", err)
}

validateOrder := NewValidateOrder(client)
chargeOrder := NewChargeOrder(client)
fulfillOrder := NewFulfillOrder(client)
processOrder := NewProcessOrder(client, validateOrder, chargeOrder, fulfillOrder)

worker, err := client.NewWorker("order-worker",
	hatchet.WithWorkflows(validateOrder, chargeOrder, fulfillOrder, processOrder),
	hatchet.WithSlots(10),
)
if err != nil {
	return fmt.Errorf("creating worker: %w", err)
}

interruptCtx, cancel := cmdutils.NewInterruptContext()
defer cancel()

return worker.StartBlocking(interruptCtx)
```

#### Ruby

```ruby
require "hatchet-sdk"

HATCHET = Hatchet::Client.new(debug: true) unless defined?(HATCHET)

def main
  worker = HATCHET.worker(
    "order-worker",
    slots: 10,
    workflows: [VALIDATE_ORDER, CHARGE_ORDER, FULFILL_ORDER, PROCESS_ORDER]
  )
  worker.start
end
```

Two differences worth noting:

- You register both durable tasks and tasks in the same `workflows` argument
- `slots` bounds how many task runs this worker will accept at once. Temporal's nearest equivalents are its worker-side executor limits (`max_concurrent_activity_task_executions` and friends); in Hatchet slot control is a first-class scheduling input, not just a worker-side limit. See [workers](/v1/workers).

Routing that you would have done with multiple task queues is done with separate workers and, when you need finer control, [worker affinity](/v1/advanced-assignment/worker-affinity). **Note that each separate task definition in Hatchet get its own queue**.

## Step 3: Convert activities to tasks

A Temporal activity:

#### Python

```python
from temporalio import activity


@activity.defn
async def charge_order(order_id: str) -> bool:
    return await payments.charge(order_id)
```

#### Typescript

```typescript
// activities.ts — activities are plain exported functions.
export async function chargeOrder(orderId: string): Promise<boolean> {
  return payments.charge(orderId);
}
```

#### Go

```go
import "context"

func ChargeOrder(ctx context.Context, orderID string) (bool, error) {
	return payments.Charge(ctx, orderID)
}
```

#### Ruby

```ruby
require "temporalio/activity"

class ChargeOrder < Temporalio::Activity::Definition
  def execute(order_id)
    Payments.charge(order_id)
  end
end
```

becomes a Hatchet task. Inputs are a single structured value rather than positional arguments, and every task receives a context:

#### Python

```python
class OrderInput(BaseModel):
    order_id: str


class ChargeOutput(BaseModel):
    charged: bool
    charge_id: str


@hatchet.task(name="charge-order", input_validator=OrderInput)
async def charge_order(input: OrderInput, ctx: Context) -> ChargeOutput:
    charge_id = await submit_charge(input.order_id)

    return ChargeOutput(charged=True, charge_id=charge_id)
```

#### Typescript

```typescript
export type ChargeOutput = {
  charged: boolean;
  amountCents: number;
};

export const chargeOrder = hatchet.task({
  name: 'charge-order',
  fn: async (input: OrderInput): Promise => {
    const amountCents = await payments.charge(input.orderId);

    return { charged: amountCents > 0, amountCents };
  },
});
```

#### Go

```go
// OrderInput is the structured input every task in the order flow receives.
// Temporal activities take positional arguments; Hatchet tasks take one value.
type OrderInput struct {
	OrderID       string `json:"order_id"`
	CorrelationID string `json:"correlation_id"`
}

// ChargeOutput is returned by the charge-order task.
type ChargeOutput struct {
	Charged     bool  `json:"charged"`
	AmountCents int64 `json:"amount_cents"`
}

// NewChargeOrder declares the charge-order task: ordinary code, retried by the
// engine, and runnable on its own without a workflow to orchestrate it.
func NewChargeOrder(client *hatchet.Client) *hatchet.StandaloneTask {
	chargeOrder := client.NewStandaloneTask("charge-order",
		func(ctx hatchet.Context, input OrderInput) (ChargeOutput, error) {
			amount, err := capturePayment(input.OrderID)
			if err != nil {
				return ChargeOutput{}, fmt.Errorf("charging order %q: %w", input.OrderID, err)
			}

			return ChargeOutput{Charged: true, AmountCents: amount}, nil
		},
	)

	return chargeOrder
}
```

#### Ruby

```ruby
CHARGE_ORDER = HATCHET.task(name: "charge-order", execution_timeout: 30) do |input, _ctx|
  charge = Payments.charge(input["order_id"], input["amount_cents"])

  {
    "charged" => true,
    "transaction_id" => charge["transaction_id"],
    "amount_cents" => charge["amount_cents"]
  }
end
```

Note that a Hatchet task is directly runnable. Running `charge_order` with an order id is a valid, retryable, observable unit of work with no workflow wrapper around it. In Temporal an activity cannot be invoked on its own; it needs a workflow to orchestrate it, which is why Temporal projects accumulate single-activity workflows. Those wrappers can eventually be deleted, though not yet: migrate them in step 4 like any other workflow, then collapse them in [step 13](#step-13-simplification).

Serialization moves from Temporal's converters (JSON by default, pickle or custom converters if configured) to JSON. Any workflow argument that relied on a custom data converter needs a JSON-serializable representation. The shape of the input type differs by SDK: Python uses Pydantic models, TypeScript uses TypeScript types (with optional Zod validation via `inputValidator`), Go uses structs with `json` tags, and Ruby passes plain string-keyed hashes.

## Step 4: Convert workflows to durable tasks

Next, we'll convert every Temporal workflow to a Hatchet durable task. The workflow body becomes the durable task body, and each `execute_activity(...)` call becomes a call to the Hatchet task you defined in step 3.

A Temporal workflow that runs three activities in order:

#### Python

```python
@workflow.defn
class ProcessOrderWorkflow:
    @workflow.run
    async def run(self, order_id: str) -> dict:
        valid = await workflow.execute_activity(
            validate_order, order_id, start_to_close_timeout=timedelta(seconds=30)
        )
        charged = await workflow.execute_activity(
            charge_order, order_id, start_to_close_timeout=timedelta(seconds=30)
        )
        return await workflow.execute_activity(
            fulfill_order, order_id, start_to_close_timeout=timedelta(seconds=30)
        )
```

#### Typescript

```typescript
import { proxyActivities } from "@temporalio/workflow";
// Only import the activity types
import type * as activities from "./activities";

const { validateOrder, chargeOrder, fulfillOrder } = proxyActivities<
  typeof activities
>({
  startToCloseTimeout: "30 seconds",
});

export async function processOrder(orderId: string): Promise<boolean> {
  await validateOrder(orderId);
  await chargeOrder(orderId);
  return fulfillOrder(orderId);
}
```

#### Go

```go
func ProcessOrderWorkflow(ctx workflow.Context, orderID string) (bool, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	if err := workflow.ExecuteActivity(ctx, ValidateOrder, orderID).Get(ctx, nil); err != nil {
		return false, err
	}

	if err := workflow.ExecuteActivity(ctx, ChargeOrder, orderID).Get(ctx, nil); err != nil {
		return false, err
	}

	var fulfilled bool
	err := workflow.ExecuteActivity(ctx, FulfillOrder, orderID).Get(ctx, &fulfilled)

	return fulfilled, err
}
```

#### Ruby

```ruby
require "temporalio/workflow"

class ProcessOrderWorkflow < Temporalio::Workflow::Definition
  def execute(order_id)
    Temporalio::Workflow.execute_activity(
      ValidateOrder, order_id, start_to_close_timeout: 30
    )
    Temporalio::Workflow.execute_activity(
      ChargeOrder, order_id, start_to_close_timeout: 30
    )
    Temporalio::Workflow.execute_activity(
      FulfillOrder, order_id, start_to_close_timeout: 30
    )
  end
end
```

becomes a durable task that runs three tasks in order:

#### Python

```python
@hatchet.durable_task(name="ProcessOrder", input_validator=OrderInput)
async def process_order(input: OrderInput, ctx: DurableContext) -> FulfillOutput:
    await validate_order.aio_run(input)
    await charge_order.aio_run(input)

    return await fulfill_order.aio_run(input)
```

#### Typescript

```typescript
export const processOrder = hatchet.durableTask({
  name: 'ProcessOrder',
  executionTimeout: '10m',
  fn: async (input: OrderInput): Promise => {
    await validateOrder.run(input);
    await chargeOrder.run(input);

    return fulfillOrder.run(input);
  },
});
```

#### Go

```go
processOrder := client.NewStandaloneDurableTask("ProcessOrder",
	func(ctx hatchet.DurableContext, input OrderInput) (FulfillOutput, error) {
		if _, err := validateOrder.Run(ctx, input); err != nil {
			return FulfillOutput{}, fmt.Errorf("validating order: %w", err)
		}

		if _, err := chargeOrder.Run(ctx, input); err != nil {
			return FulfillOutput{}, fmt.Errorf("charging order: %w", err)
		}

		result, err := fulfillOrder.Run(ctx, input)
		if err != nil {
			return FulfillOutput{}, fmt.Errorf("fulfilling order: %w", err)
		}

		var fulfilled FulfillOutput
		if err := result.Into(&fulfilled); err != nil {
			return FulfillOutput{}, fmt.Errorf("decoding fulfillment: %w", err)
		}

		return fulfilled, nil
	},
)
```

#### Ruby

```ruby
PROCESS_ORDER = HATCHET.durable_task(name: "ProcessOrder", execution_timeout: 300) do |input, _ctx|
  validated = VALIDATE_ORDER.run(input)
  raise Hatchet::NonRetryableError, "order #{input["order_id"]} failed validation" unless validated["valid"]

  charged = CHARGE_ORDER.run(input)
  fulfilled = FULFILL_ORDER.run(input.merge("transaction_id" => charged["transaction_id"]))

  {
    "order_id" => input["order_id"],
    "transaction_id" => charged["transaction_id"],
    "shipment_id" => fulfilled["shipment_id"]
  }
end
```

Every guarantee of the Temporal workflow is preserved in Hatchet. The durable task has a run id and a history, it survives a worker crash, and it resumes from its last checkpoint rather than from the top. `workflow.execute_activity(fn, arg, ...)` becomes `fn.aio_run(input)` (`.run(...)` in TypeScript, Go and Ruby), results come back as ordinary return values rather than futures, and the per-call retry and timeout options move onto the task definition instead, which is [step 6](#step-6-retries-and-timeouts).

Workflows that wait, rather than only calling activities, translate the same way. A workflow that sends an email, sleeps for three days, then sends another:

#### Python

```python
@hatchet.durable_task(
    name="OnboardingFlow",
    input_validator=OnboardingInput,
    # The timeout has to cover the whole wall-clock span of the run, sleeps included.
    execution_timeout=timedelta(days=7),
)
async def onboarding_flow(input: OnboardingInput, ctx: DurableContext) -> None:
    await send_welcome_email.aio_run(input)

    await ctx.aio_sleep_for(timedelta(days=3))

    await send_followup_email.aio_run(input)
```

#### Typescript

```typescript
export const onboardingFlow = hatchet.durableTask({
  name: 'OnboardingFlow',
  // The timeout has to cover the whole wall-clock span of the run, sleeps included.
  executionTimeout: '168h',
  fn: async (input: SignupInput, ctx): Promise => {
    await sendWelcomeEmail.run(input);

    await ctx.sleepFor('72h');

    return sendFollowupEmail.run(input);
  },
});
```

#### Go

```go
onboardingFlow := client.NewStandaloneDurableTask("OnboardingFlow",
	func(ctx hatchet.DurableContext, input OnboardingInput) (OnboardingOutput, error) {
		if _, err := sendWelcomeEmail.Run(ctx, input); err != nil {
			return OnboardingOutput{}, fmt.Errorf("sending welcome email: %w", err)
		}

		if _, err := ctx.SleepFor(72 * time.Hour); err != nil {
			return OnboardingOutput{}, fmt.Errorf("sleeping between emails: %w", err)
		}

		if _, err := sendFollowupEmail.Run(ctx, input); err != nil {
			return OnboardingOutput{}, fmt.Errorf("sending follow-up email: %w", err)
		}

		return OnboardingOutput{EmailsSent: 2}, nil
	},
	// The execution timeout has to cover the sleep, not just the work.
	hatchet.WithExecutionTimeout(168*time.Hour),
)
```

#### Ruby

```ruby
# The execution timeout has to cover the whole wall-clock span of the run,
# sleeps included, so a three-day sleep needs a timeout longer than three days.
ONBOARDING_FLOW = HATCHET.durable_task(
  name: "OnboardingFlow",
  execution_timeout: 604_800
) do |input, ctx|
  welcome = SEND_WELCOME_EMAIL.run(input)

  ctx.sleep_for(duration: 259_200)

  followup = SEND_FOLLOWUP_EMAIL.run(input)

  {
    "user_id" => input["user_id"],
    "welcome_message_id" => welcome["message_id"],
    "followup_message_id" => followup["message_id"]
  }
end
```

The determinism rules here are the same: a durable task may only wait on the durable context or spawn children, and the code between checkpoints must be reproducible on replay. Anything that touches the outside world belongs in a child task. See [durable tasks](/v1/durable-tasks).

Note that a durable task's execution timeout has to cover the whole wall-clock span of the run, including its sleeps, so set it accordingly.

> **Info:** Some of these durable tasks will not need to be durable, and a few will not
>   need to exist at all. Leave them as they are until the migration is running,
>   then see [step 13](#step-13-simplification).

## Step 5: Invoke work

The mapping is the same in every SDK; names below use the Python spelling.

Temporal, Hatchet

`await client.execute_workflow(Wf.run, arg, id=..., task_queue=...)`, `await my_task.aio_run(MyInput(...))`
`await client.start_workflow(Wf.run, arg, ...)`, `await my_task.aio_run(MyInput(...), wait_for_result=False)`
`handle.result()`, `await ref.aio_result()`
Synchronous client, `my_task.run(...)`

Fire and forget, then collect the result later:

#### Python

```python
async def trigger_process_order(order_id: str) -> FulfillOutput:
    ref = await process_order.aio_run(
        OrderInput(order_id=order_id),
        wait_for_result=False,
    )

    # Available immediately. Store it if you need to reattach to the run later.
    print(ref.workflow_run_id)

    return await ref.aio_result()
```

#### Typescript

```typescript
const run = await processOrder.runNoWait(input);

// It may be helpful to store this run id somewhere durable.
const runId = await run.getWorkflowRunId();

const result = await run.output;
```

#### Go

```go
runRef, err := processOrder.RunNoWait(context.Background(), OrderInput{OrderID: "123"})
if err != nil {
	return FulfillOutput{}, fmt.Errorf("triggering process-order: %w", err)
}

// Store this somewhere durable if you need to reattach to the run later.
runID := runRef.RunId

result, err := runRef.Result()
if err != nil {
	return FulfillOutput{}, fmt.Errorf("waiting for run %s: %w", runID, err)
}

var output FulfillOutput
if err := result.TaskOutput("ProcessOrder").Into(&output); err != nil {
	return FulfillOutput{}, fmt.Errorf("decoding run %s output: %w", runID, err)
}
```

#### Ruby

```ruby
def enqueue_order(order_id, amount_cents)
  ref = PROCESS_ORDER.run_no_wait({ "order_id" => order_id, "amount_cents" => amount_cents })

  # The run is enqueued and its id is available before any work starts.
  puts "enqueued run #{ref.workflow_run_id}"

  ref.result
end
```

> **Info:** The Ruby SDK has no separate async trigger API: `run_no_wait` is the enqueue
>   path and `ref.result` is a blocking read.

Note what is _not_ required: there is no workflow id and no task queue at the call site. Deduplication that you would have achieved with a Temporal workflow id reuse policy is expressed with an [idempotency key](/v1/idempotency) instead.

## Step 6: Retries and timeouts

Temporal attaches a `RetryPolicy` to activity and workflow options. Hatchet attaches retry settings to the task definition:

#### Python

```python
@hatchet.task(
    name="charge-order-with-retries",
    input_validator=OrderInput,
    retries=10,
    backoff_factor=2.0,
    backoff_max_seconds=10,
    execution_timeout=timedelta(seconds=30),
    schedule_timeout=timedelta(minutes=10),
)
async def charge_order_with_retries(input: OrderInput, ctx: Context) -> ChargeOutput:
    # Raising `NonRetryableException` here would stop Hatchet from retrying at all.
    if ctx.retry_count < 2:
        raise RuntimeError(f"payment provider unavailable for {input.order_id}")

    return ChargeOutput(charged=True, charge_id=await submit_charge(input.order_id))
```

#### Typescript

```typescript
export const chargeOrderWithRetries = hatchet.task({
  name: 'charge-order-with-retries',
  retries: 10,
  backoff: {
    // Factor to increase the wait time between retries.
    factor: 2,
    // Maximum number of seconds to wait between retries.
    maxSeconds: 10,
  },
  executionTimeout: '30s',
  scheduleTimeout: '10m',
  fn: async (input: OrderInput): Promise => {
    const amountCents = await payments.charge(input.orderId);

    return { charged: amountCents > 0, amountCents };
  },
});
```

#### Go

```go
chargeOrderWithRetries := client.NewStandaloneTask("charge-order-with-retries",
	func(ctx hatchet.Context, input OrderInput) (ChargeOutput, error) {
		amount, err := capturePayment(input.OrderID)
		if err != nil {
			return ChargeOutput{}, fmt.Errorf("charging order %q: %w", input.OrderID, err)
		}

		return ChargeOutput{Charged: true, AmountCents: amount}, nil
	},
	// retries counts retries, not attempts: maximum_attempts=11 becomes 10.
	hatchet.WithRetries(10),
	// factor, maxBackoffSeconds
	hatchet.WithRetryBackoff(2, 10),
	hatchet.WithExecutionTimeout(30*time.Second),
	hatchet.WithScheduleTimeout(10*time.Minute),
)
```

#### Ruby

```ruby
CHARGE_ORDER_WITH_RETRIES = HATCHET.task(
  name: "charge-order-with-retries",
  # Hatchet counts retries, not attempts: this is 11 attempts in total.
  retries: 10,
  backoff_factor: 2.0,
  backoff_max_seconds: 10,
  execution_timeout: 30,
  schedule_timeout: 600
) do |input, ctx|
  charge = Payments.charge(input["order_id"], input["amount_cents"])

  { "transaction_id" => charge["transaction_id"], "attempt" => ctx.retry_count + 1 }
end
```

The examples give this task its own name, `charge-order-with-retries`, only so that it can be registered alongside the plain `charge-order` task from [step 3](#step-3-convert-activities-to-tasks). In your own code these options go straight onto the existing task and the name does not change.

The concept mapping is the same in every SDK. Names below use the Python spelling; the code above shows each SDK's exact form.

Temporal, Hatchet

`maximum_attempts=N`, `retries=N-1` (Hatchet counts retries, not attempts)
`backoff_coefficient`, `backoff_factor`
`maximum_interval`, `backoff_max_seconds`
`non_retryable_error_types=[...]`, raise a non-retryable error
`start_to_close_timeout`, `execution_timeout`
`schedule_to_start_timeout`, `schedule_timeout`
`activity.info().attempt`, retry count on the context

The non-retryable error type is `NonRetryableException` in Python (from `hatchet_sdk.exceptions`), `NonRetryableError` in TypeScript, `worker.NewNonRetryableError` in Go, and `Hatchet::NonRetryableError` in Ruby. The retry count is `ctx.retry_count` in Python and Ruby, `ctx.retryCount()` in TypeScript, and `ctx.RetryCount()` in Go.

> **Warning:** `retries` counts retries, not total attempts: `maximum_attempts=3` in Temporal
>   is `retries=2` in Hatchet. Copying the number across unchanged gives you one
>   extra attempt.

Defaults for every task in a workflow go in the workflow's task defaults (`task_defaults` in Python and Ruby, `taskDefaults` in TypeScript). See [retry policies](/v1/retry-policies) and [timeouts](/v1/timeouts).

## Step 7: Timers and sleeps

A durable sleep inside a Temporal workflow becomes a durable sleep on the Hatchet durable context:

**Temporal:**

#### Python

```python
await asyncio.sleep(60 * 60 * 24)
```

#### Typescript

```typescript
import { sleep } from "@temporalio/workflow";

await sleep(24 * 60 * 60 * 1000);
```

#### Go

```go
workflow.Sleep(ctx, 24*time.Hour)
```

#### Ruby

```ruby
Temporalio::Workflow.sleep(86_400)
```

**Hatchet:**

#### Python

```python
await ctx.aio_sleep_for(timedelta(days=1))
```

#### Typescript

```typescript
await ctx.sleepFor('24h');
```

#### Go

```go
if _, err := ctx.SleepFor(24 * time.Hour); err != nil {
	return OnboardingOutput{}, fmt.Errorf("sleeping for a day: %w", err)
}
```

#### Ruby

```ruby
# A sleeping durable task is evicted and releases its worker slot, so the wait
# costs no capacity however long it is.
WAIT_A_DAY = HATCHET.durable_task(name: "WaitADay", execution_timeout: 90_000) do |input, ctx|
  ctx.sleep_for(duration: 86_400)

  { "order_id" => input["order_id"], "resumed_at" => Time.now.utc.to_i }
end
```

While a Hatchet durable task is sleeping it is [evicted](/v1/task-eviction) and its worker slot is released, so a million sleeping runs cost no worker capacity. See [durable sleep](/v1/durable-sleep).

For a _task_ (not a durable task) that should simply start later, create a scheduled run rather than sleeping. There is no reason to hold a durable task open for a fixed future start.

## Step 8: Signals, queries, and updates

Temporal signals become Hatchet [events](/v1/events), which are pushed to the tenant rather than to a specific run:

**Temporal:**

#### Python

```python
handle = client.get_workflow_handle(workflow_id)
await handle.signal(ApprovalWorkflow.approve, "approved")
```

#### Typescript

```typescript
const handle = client.workflow.getHandle(workflowId);
await handle.signal(approveSignal, "approved");
```

#### Go

```go
err := c.SignalWorkflow(context.Background(), workflowID, runID, "approve", "approved")
```

#### Ruby

```ruby
handle = client.workflow_handle(workflow_id)
handle.signal(ApprovalWorkflow.approve, "approved")
```

**Hatchet:**

#### Python

```python
async def grant_approval(correlation_id: str) -> None:
    await hatchet.event.aio_push(
        "approval:granted",
        {"correlation_id": correlation_id},
    )
```

#### Typescript

```typescript
await hatchet.events.push('approval:granted', {
  correlationId: input.correlationId,
});
```

#### Go

```go
err := client.Events().Push(context.Background(), "approval:granted", map[string]any{
	"correlation_id": correlationID,
	"approved_by":    "finance@example.com",
})
if err != nil {
	return fmt.Errorf("pushing approval:granted: %w", err)
}
```

#### Ruby

```ruby
def grant_approval(correlation_id)
  HATCHET.events.push(
    "approval:granted",
    { "correlation_id" => correlation_id }
  )
end
```

The waiting side uses a durable event wait, with a CEL expression to select the event that belongs to this run:

#### Python

```python
class ApprovalInput(BaseModel):
    order_id: str
    # Generate this yourself, and keep it CEL-safe: the expression below is compiled
    # as-is, so a value containing a quote would fail to compile and the run would
    # wait until it timed out.
    correlation_id: str


class ApprovalOutput(BaseModel):
    approved: bool


@hatchet.durable_task(
    name="ApprovalFlow",
    input_validator=ApprovalInput,
    execution_timeout=timedelta(minutes=10),
)
async def approval_flow(input: ApprovalInput, ctx: DurableContext) -> ApprovalOutput:
    await ctx.aio_wait_for_event(
        "approval:granted",
        f"input.correlation_id == '{input.correlation_id}'",
    )

    return ApprovalOutput(approved=True)
```

#### Typescript

```typescript
export const approvalFlow = hatchet.durableTask({
  name: 'ApprovalFlow',
  executionTimeout: '10m',
  fn: async (input: OrderInput, ctx): Promise => {
    // The expression is compiled as CEL, so correlate on an id that cannot contain a quote.
    await ctx.waitForEvent('approval:granted', `input.correlationId == '${input.correlationId}'`);

    return fulfillOrder.run(input);
  },
});
```

#### Go

```go
approvalFlow := client.NewStandaloneDurableTask("ApprovalFlow",
	func(ctx hatchet.DurableContext, input OrderInput) (ApprovalOutput, error) {
		// The expression is compiled as CEL and passed through unescaped, so
		// correlate on an opaque id you generate rather than on user input.
		expr := fmt.Sprintf("input.correlation_id == '%s'", input.CorrelationID)

		event, err := ctx.WaitForEvent("approval:granted", expr)
		if err != nil {
			return ApprovalOutput{}, fmt.Errorf("waiting for approval: %w", err)
		}

		var payload ApprovalEvent
		if err := hatchet.EventInto(event, &payload); err != nil {
			return ApprovalOutput{}, fmt.Errorf("decoding approval event: %w", err)
		}

		return ApprovalOutput{Approved: true, ApprovedBy: payload.ApprovedBy}, nil
	},
	hatchet.WithExecutionTimeout(24*time.Hour),
)
```

#### Ruby

```ruby
APPROVAL_FLOW = HATCHET.durable_task(name: "ApprovalFlow", execution_timeout: 86_400) do |input, ctx|
  # The CEL expression does the job a Temporal workflow id used to do, so
  # correlate on an opaque id you generate rather than user-supplied text.
  ctx.wait_for(
    "event",
    Hatchet::UserEventCondition.new(
      event_key: "approval:granted",
      expression: "input.correlation_id == '#{input["correlation_id"]}'"
    )
  )

  FULFILL_ORDER.run(input)
end
```

Note that anything can emit a Hatchet event, and any number of durable tasks can wait on the same event. The filter expression is doing the work that the workflow id used to do in Temporal, so give runs a stable, CEL-safe correlation id in their input and filter on that.

Three related mappings:

- **A wait-for-condition inside a workflow** (`workflow.wait_condition`, `wf.condition`, `workflow.Await`) is usually a wait condition on a DAG task instead: a sleep condition, a user-event condition, or a parent condition, composed with an or-group. That moves the wait out of your code and into the workflow definition, where the engine can see it. See [conditions](/v1/directed-acyclic-graphs#waiting-on-conditions-with-or-groups).
- **Queries** have no direct equivalent, because Hatchet does not run your code to answer a question about a run. Run inputs, outputs, and history are already queryable through the API and dashboard. For live progress, stream it from the task (`ctx.aio_put_stream` in Python, `ctx.putStream` in TypeScript, `ctx.PutStream` in Go, `ctx.put_stream` in Ruby) and consume it from the caller. See [streaming](/v1/streaming).
- **Updates**, a synchronous and validated mutation of running workflow state, have no equivalent. Model this as an event the durable task waits on, plus a child task that performs the mutation and returns a result.

## Step 9: Child workflows and fan-out

Convert workflows that spawn one child per item into a durable task that runs one child task per item:

**Temporal:**

#### Python

```python
results = await asyncio.gather(*[
    workflow.execute_child_workflow(ProcessItem.run, item) for item in items
])
```

#### Typescript

```typescript
import { executeChild } from "@temporalio/workflow";

const results = await Promise.all(
  items.map((item) => executeChild(processItem, { args: [item] })),
);
```

#### Go

```go
// Collect the child futures, then await them.
var futures []workflow.Future
for _, item := range items {
	futures = append(futures, workflow.ExecuteChildWorkflow(ctx, ProcessItem, item))
}

for _, f := range futures {
	var result ItemOutput
	if err := f.Get(ctx, &result); err != nil {
		return err
	}
}
```

#### Ruby

```ruby
results = items.map do |item|
  Temporalio::Workflow.execute_child_workflow(
    ProcessItem, item, schedule_to_close_timeout: 300
  )
end
```

**Hatchet:**

#### Python

```python
class ItemInput(BaseModel):
    item_id: str


class ItemOutput(BaseModel):
    item_id: str
    result: str


class FanOutInput(BaseModel):
    item_ids: list[str]


class FanOutOutput(BaseModel):
    results: list[ItemOutput]


@hatchet.task(name="process-item", input_validator=ItemInput)
async def process_item(input: ItemInput, ctx: Context) -> ItemOutput:
    return ItemOutput(item_id=input.item_id, result=await handle_item(input.item_id))


@hatchet.durable_task(name="ProcessItems", input_validator=FanOutInput)
async def process_items(input: FanOutInput, ctx: DurableContext) -> FanOutOutput:
    results = await asyncio.gather(
        *[
            process_item.aio_run(ItemInput(item_id=item_id))
            for item_id in input.item_ids
        ]
    )

    return FanOutOutput(results=results)
```

#### Typescript

```typescript
export const packOrder = hatchet.durableTask({
  name: 'PackOrder',
  executionTimeout: '30m',
  fn: async (input: { items: string[] }): Promise<{ packed: number }> => {
    const results = await Promise.all(input.items.map((item) => processItem.run({ item })));

    return { packed: results.filter((result) => result.packed).length };
  },
});
```

#### Go

```go
var wg sync.WaitGroup

outputs := make([]ItemOutput, len(items))
errs := make([]error, len(items))

wg.Add(len(items))

for i, item := range items {
	go func(i int, item Item) {
		defer wg.Done()

		result, err := processItem.Run(ctx, ItemInput{Item: item})
		if err != nil {
			errs[i] = fmt.Errorf("running item %d: %w", i, err)
			return
		}

		if err := result.Into(&outputs[i]); err != nil {
			errs[i] = fmt.Errorf("decoding result for item %d: %w", i, err)
		}
	}(i, item)
}

wg.Wait()

if err := errors.Join(errs...); err != nil {
	return ShipmentOutput{}, err
}
```

#### Ruby

```ruby
PROCESS_ITEM = HATCHET.task(name: "process-item") do |input, _ctx|
  { "item" => input["item"], "processed" => true }
end

FAN_OUT_ORDER_ITEMS = HATCHET.durable_task(name: "FanOutOrderItems", execution_timeout: 300) do |input, _ctx|
  items = input["items"] || []

  # Ruby's trigger methods are synchronous, so fan out with the bulk API rather
  # than one call per child.
  results = PROCESS_ITEM.run_many(
    items.map { |item| PROCESS_ITEM.create_bulk_run_item(input: { "item" => item }) }
  )

  { "results" => results }
end
```

> **Info:** The Ruby example above spawns children in bulk rather than concurrently,
>   because the Ruby SDK's trigger methods are synchronous. Python
>   (`aio_run_many`), TypeScript (`ctx.bulkRunChildren`) and Go
>   (`workflow.RunMany`) have the same bulk API if you prefer one call over N.

Spawning from a durable task checkpoints each child, so a crash mid-fan-out resumes without re-running completed children. See [child spawning](/v1/child-spawning).

Hatchet places no per-run cap on the number of children spawned from a durable run. Temporal enforces a limit on pending child executions and pending activities per run (51,200 by default, tunable in server dynamic config), which is the constraint behind most Temporal "continue-as-new to keep the history small" batching code. That batching code can usually be deleted. For large fan-outs, see [bulk run](/v1/bulk-run).

## Step 10: Schedules and crons

A Temporal schedule created through the schedule client is usually a declaration on the task itself:

#### Python

```python
class ReportInput(BaseModel):
    kind: str


class ReportOutput(BaseModel):
    kind: str
    rows: int


@hatchet.task(
    name="weekly-report",
    input_validator=ReportInput,
    on_crons=["0 9 * * 1"],
    cron_input=ReportInput(kind="weekly"),
)
async def weekly_report(input: ReportInput, ctx: Context) -> ReportOutput:
    return ReportOutput(kind=input.kind, rows=await count_report_rows(input.kind))
```

#### Typescript

```typescript
export const weeklyReport = hatchet.workflow({
  name: 'weekly-report',
  on: {
    cron: '0 9 * * 1',
  },
});

weeklyReport.task({
  name: 'generate',
  fn: async (input) => {
    return { rows: await reports.build(input.kind) };
  },
});
```

#### Go

```go
weeklyReport := client.NewStandaloneTask("weekly-report",
	func(ctx hatchet.Context, input ReportInput) (ReportOutput, error) {
		rows, err := generateReport(input.Kind)
		if err != nil {
			return ReportOutput{}, fmt.Errorf("generating %q report: %w", input.Kind, err)
		}

		return ReportOutput{Rows: rows}, nil
	},
	hatchet.WithWorkflowCron("0 9 * * 1"),
	hatchet.WithWorkflowCronInput(ReportInput{Kind: "weekly"}),
)
```

#### Ruby

```ruby
# Ruby declares crons on a workflow rather than on a standalone task.
WEEKLY_REPORT = HATCHET.workflow(
  name: "WeeklyReport",
  on_crons: ["0 9 * * 1"]
)

WEEKLY_REPORT.task(:generate) do |input, _ctx|
  { "kind" => input["kind"] || "weekly", "generated_at" => Time.now.utc.to_i }
end
```

For schedules created at runtime (the equivalent of programmatic Temporal schedules), use the API:

#### Python

```python
async def create_schedules(customer_id: str) -> tuple[str, str]:
    cron = await hatchet.cron.aio_create(
        workflow_name=weekly_report.name,
        cron_name=f"weekly-report-{customer_id}",
        expression="0 9 * * 1",
        input={"kind": "weekly"},
        additional_metadata={"customer_id": customer_id},
    )

    scheduled = await hatchet.scheduled.aio_create(
        workflow_name=weekly_report.name,
        trigger_at=datetime.now(tz=timezone.utc) + timedelta(days=1),
        input={"kind": "weekly"},
        additional_metadata={"customer_id": customer_id},
    )

    return cron.metadata.id, scheduled.metadata.id
```

#### Typescript

```typescript
// A recurring schedule, created at runtime.
await hatchet.crons.create('weekly-report', {
  name: 'weekly-report-acme',
  expression: '0 9 * * 1',
  input: { kind: 'weekly' },
});

// A one-shot future run.
await hatchet.schedules.create('weekly-report', {
  triggerAt: new Date(Date.now() + 24 * 60 * 60 * 1000),
  input: { kind: 'weekly' },
});
```

#### Go

```go
// A recurring schedule, created at runtime.
_, err := client.Crons().Create(ctx, "weekly-report", features.CreateCronTrigger{
	Name:       "weekly-report-acme",
	Expression: "0 9 * * 1",
	Input:      map[string]interface{}{"kind": "weekly"},
	AdditionalMetadata: map[string]interface{}{
		"customer_id": "acme",
	},
})
if err != nil {
	return fmt.Errorf("creating cron: %w", err)
}

// A one-shot future run.
_, err = client.Schedules().Create(ctx, "weekly-report", features.CreateScheduledRunTrigger{
	TriggerAt: time.Now().Add(24 * time.Hour),
	Input:     map[string]interface{}{"kind": "weekly"},
})
if err != nil {
	return fmt.Errorf("creating scheduled run: %w", err)
}
```

#### Ruby

```ruby
def schedule_weekly_report
  HATCHET.cron.create(
    workflow_name: "WeeklyReport",
    cron_name: "weekly-report-acme",
    expression: "0 9 * * 1",
    input: { "kind" => "weekly" }
  )

  HATCHET.scheduled.create(
    workflow_name: "WeeklyReport",
    trigger_at: Time.now + 86_400,
    input: { "kind" => "weekly" },
    additional_metadata: { "customer_id" => "acme" }
  )
end
```

See [cron runs](/v1/cron-runs) and [scheduled runs](/v1/scheduled-runs).

## Step 11: Versioning and determinism

Temporal gives you patching APIs (`workflow.patched` / `GetVersion` and their per-SDK equivalents) to branch on workflow version, so a deployment can change the code path of workflows that are already running. Hatchet has no equivalent: for breaking changes to durable tasks, we recommend deploying a new durable task definition and letting the old work drain.

## Step 12: Flow control

Several things that Temporal projects build by hand (partitioned task queues, worker-side semaphores, external rate limiters in front of activities) are engine features in Hatchet:

#### Python

```python
class SyncInput(BaseModel):
    customer_id: str


class SyncOutput(BaseModel):
    records_synced: int


# One in-flight run per customer, newest cancels the oldest.
sync_customer = hatchet.workflow(
    name="SyncCustomer",
    input_validator=SyncInput,
    concurrency=ConcurrencyExpression(
        expression="input.customer_id",
        max_runs=1,
        limit_strategy=ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS,
    ),
)


@sync_customer.task()
async def sync(input: SyncInput, ctx: Context) -> SyncOutput:
    return SyncOutput(records_synced=await sync_customer_data(input.customer_id))


class PromptInput(BaseModel):
    prompt: str


class ModelOutput(BaseModel):
    completion: str


# A global budget shared by every worker, not per-process. The static key has to be
# declared once with `hatchet.rate_limits.put` before a task can consume it.
@hatchet.task(
    name="call-model",
    input_validator=PromptInput,
    rate_limits=[RateLimit(static_key="openai", units=1)],
)
async def call_model(input: PromptInput, ctx: Context) -> ModelOutput:
    return ModelOutput(completion=await complete_prompt(input.prompt))
```

#### Typescript

```typescript
import { ConcurrencyLimitStrategy } from '@hatchet/v1';

// One in-flight run per customer, newest cancels the oldest.
export const syncCustomer = hatchet.workflow({
  name: 'SyncCustomer',
  concurrency: {
    expression: 'input.customerId',
    maxRuns: 1,
    limitStrategy: ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS,
  },
});

syncCustomer.task({
  name: 'sync',
  fn: async (input) => {
    return { synced: await crm.sync(input.customerId) };
  },
});

// A global budget shared by every worker, not per-process.
export const callModel = hatchet.task({
  name: 'call-model',
  rateLimits: [
    {
      staticKey: 'openai',
      units: 1,
    },
  ],
  fn: async (input: PromptInput) => {
    return { completion: await models.complete(input.prompt) };
  },
});
```

#### Go

```go
// One in-flight run per customer, newest cancels the oldest.
var maxRuns int32 = 1
strategy := types.CancelInProgress

syncCustomer = client.NewStandaloneTask("SyncCustomer",
	func(ctx hatchet.Context, input SyncInput) (SyncOutput, error) {
		records, err := syncCustomerRecords(input.CustomerID)
		if err != nil {
			return SyncOutput{}, fmt.Errorf("syncing customer %q: %w", input.CustomerID, err)
		}

		return SyncOutput{Records: records}, nil
	},
	hatchet.WithWorkflowConcurrency(types.Concurrency{
		Expression:    "input.customer_id",
		MaxRuns:       &maxRuns,
		LimitStrategy: &strategy,
	}),
)

// A global budget shared by every worker, not per-process.
units := 1

callModel = client.NewStandaloneTask("call-model",
	func(ctx hatchet.Context, input PromptInput) (ModelOutput, error) {
		completion, err := completePrompt(input.Prompt)
		if err != nil {
			return ModelOutput{}, fmt.Errorf("calling model: %w", err)
		}

		return ModelOutput{Completion: completion}, nil
	},
	hatchet.WithRateLimits(&types.RateLimit{
		Key:   "openai",
		Units: &units,
	}),
)
```

#### Ruby

```ruby
# One in-flight run per customer, newest cancels the oldest.
SYNC_CUSTOMER = HATCHET.workflow(
  name: "SyncCustomer",
  concurrency: Hatchet::ConcurrencyExpression.new(
    expression: "input.customer_id",
    max_runs: 1,
    limit_strategy: :cancel_in_progress
  )
)

SYNC_CUSTOMER.task(:sync) do |input, _ctx|
  { "customer_id" => input["customer_id"], "synced" => true }
end

# A global budget shared by every worker, not per-process.
CALL_MODEL = HATCHET.task(
  name: "call-model",
  rate_limits: [Hatchet::RateLimit.new(static_key: "openai", units: 1)]
) do |input, _ctx|
  { "prompt" => input["prompt"], "completion" => "..." }
end
```

See [concurrency](/v1/concurrency), [rate limits](/v1/rate-limits), [priority](/v1/priority), and [worker slots](/v1/workers). Auditing what your Temporal deployment does with task queues and external limiters usually deletes a meaningful amount of infrastructure.

## Step 13: Simplification

Now that workflows have been converted to durable tasks, you can simplify them in two ways. Both are optional. Neither changes what your code does, so nothing here has to happen on the migration's critical path.

### A fixed sequence of task calls becomes a DAG

The `ProcessOrder` durable task from step 4 never waits and never decides at runtime what to run next. It calls three tasks in a fixed order. That shape is a DAG, and Hatchet can express it declaratively with `parents=[...]`, which deletes the orchestration code entirely:

#### Python

```python
order_workflow = hatchet.workflow(name="ProcessOrderDag", input_validator=OrderInput)


@order_workflow.task(execution_timeout=timedelta(seconds=30))
async def validate(input: OrderInput, ctx: Context) -> ValidateOutput:
    return ValidateOutput(valid=await check_inventory(input.order_id))


@order_workflow.task(parents=[validate], execution_timeout=timedelta(seconds=30))
async def charge(input: OrderInput, ctx: Context) -> ChargeOutput:
    validated = ctx.task_output(validate)

    if not validated.valid:
        raise ValueError(f"order {input.order_id} failed validation")

    return ChargeOutput(charged=True, charge_id=await submit_charge(input.order_id))


@order_workflow.task(parents=[charge], execution_timeout=timedelta(seconds=30))
async def fulfill(input: OrderInput, ctx: Context) -> FulfillOutput:
    charged = ctx.task_output(charge)

    return FulfillOutput(
        order_id=input.order_id,
        tracking_number=await ship(charged.charge_id),
    )
```

#### Typescript

```typescript
export const orderWorkflow = hatchet.workflow({
  name: 'ProcessOrderDag',
});

const validate = orderWorkflow.task({
  name: 'validate',
  executionTimeout: '30s',
  fn: async (input) => {
    return { valid: await warehouse.reserve(input.orderId) };
  },
});

const charge = orderWorkflow.task({
  name: 'charge',
  parents: [validate],
  executionTimeout: '30s',
  fn: async (input, ctx) => {
    const validated = await ctx.parentOutput(validate);

    if (!validated.valid) {
      return { charged: false, amountCents: 0 };
    }

    const amountCents = await payments.charge(input.orderId);

    return { charged: true, amountCents };
  },
});

orderWorkflow.task({
  name: 'fulfill',
  parents: [charge],
  executionTimeout: '30s',
  fn: async (input, ctx) => {
    const charged = await ctx.parentOutput(charge);

    if (!charged.charged) {
      return { fulfilled: false, shipmentId: '' };
    }

    return { fulfilled: true, shipmentId: await warehouse.ship(input.orderId) };
  },
});
```

#### Go

```go
workflow := client.NewWorkflow("ProcessOrderDag")

validate := workflow.NewTask("validate",
	func(ctx hatchet.Context, input OrderInput) (ValidateOutput, error) {
		valid, err := validateOrderRecord(input.OrderID)
		if err != nil {
			return ValidateOutput{}, fmt.Errorf("validating order %q: %w", input.OrderID, err)
		}

		return ValidateOutput{Valid: valid}, nil
	},
	hatchet.WithExecutionTimeout(30*time.Second),
)

charge := workflow.NewTask("charge",
	func(ctx hatchet.Context, input OrderInput) (ChargeOutput, error) {
		var validated ValidateOutput
		if err := ctx.ParentOutput(validate, &validated); err != nil {
			return ChargeOutput{}, fmt.Errorf("reading validate output: %w", err)
		}

		if !validated.Valid {
			return ChargeOutput{}, fmt.Errorf("order %q failed validation", input.OrderID)
		}

		amount, err := capturePayment(input.OrderID)
		if err != nil {
			return ChargeOutput{}, fmt.Errorf("charging order %q: %w", input.OrderID, err)
		}

		return ChargeOutput{Charged: true, AmountCents: amount}, nil
	},
	hatchet.WithParents(validate),
	hatchet.WithExecutionTimeout(30*time.Second),
)

_ = workflow.NewTask("fulfill",
	func(ctx hatchet.Context, input OrderInput) (FulfillOutput, error) {
		var charged ChargeOutput
		if err := ctx.ParentOutput(charge, &charged); err != nil {
			return FulfillOutput{}, fmt.Errorf("reading charge output: %w", err)
		}

		trackingID, err := shipOrder(input.OrderID)
		if err != nil {
			return FulfillOutput{}, fmt.Errorf("fulfilling order %q: %w", input.OrderID, err)
		}

		return FulfillOutput{
			Fulfilled:   true,
			TrackingID:  trackingID,
			AmountCents: charged.AmountCents,
		}, nil
	},
	hatchet.WithParents(charge),
	hatchet.WithExecutionTimeout(30*time.Second),
)
```

#### Ruby

```ruby
ORDER_WORKFLOW = HATCHET.workflow(name: "ProcessOrderDag")

VALIDATE = ORDER_WORKFLOW.task(:validate, execution_timeout: 30) do |input, _ctx|
  { "order_id" => input["order_id"], "valid" => input["amount_cents"].to_i.positive? }
end

CHARGE = ORDER_WORKFLOW.task(:charge, parents: [VALIDATE], execution_timeout: 30) do |input, ctx|
  validated = ctx.task_output(VALIDATE)
  raise Hatchet::NonRetryableError, "order #{input["order_id"]} failed validation" unless validated["valid"]

  charge = Payments.charge(validated["order_id"], input["amount_cents"])

  { "transaction_id" => charge["transaction_id"], "amount_cents" => charge["amount_cents"] }
end

ORDER_WORKFLOW.task(:fulfill, parents: [CHARGE], execution_timeout: 30) do |input, ctx|
  charged = ctx.task_output(CHARGE)

  { "shipment_id" => Fulfillment.ship(input["order_id"], charged["transaction_id"]) }
end
```

The examples register the DAG as `ProcessOrderDag` so that it and the `ProcessOrder` durable task it replaces can both be served while the cutover is in progress. When you make the switch for real, give the DAG the name the durable task had and delete the durable task, so callers do not have to change.

Upstream results are read off the context (`ctx.task_output` in Python and Ruby, `ctx.parentOutput` in TypeScript, `ctx.ParentOutput` in Go) rather than returned through local variables. Tasks with no parents run in parallel; that is the replacement for whatever you used to run activities concurrently inside a workflow. See [DAGs](/v1/directed-acyclic-graphs).

There is no determinism constraint on a DAG task. Each one is ordinary code that can read a database, call an API, or use the clock.

That is worth doing for three reasons. The orchestration code is gone, leaving only task bodies and their parent declarations. The parallelism is free: you declare the graph, and Hatchet runs whatever it does not order concurrently, with no `asyncio.gather`, `Promise.all` or goroutines of your own. And the dashboard renders the graph before the run starts, so a stuck run points at the node it is stuck on rather than at a flat list of events.

### A wrapper around a single task becomes that task

A Temporal workflow that existed only to invoke one activity became, in step 4, a durable task that does nothing but call one task. Delete the wrapper and call the task directly.

Nothing is lost, because a Hatchet task is independently runnable. The call site changes from the wrapper's name to the task's name, and the run in the dashboard is the work itself rather than a wrapper around it. You get one run per invocation instead of two, no durable checkpoint on a call that never needed one, and no determinism constraint on code that never had one to respect.

## Step 14: Observability

Hatchet contains a fully-featured web UI which aims to be a superset of the Temporal Web UI. It is available in both Hatchet Cloud and self-hosted deployments. The UI provides run history, workflow event views, and additional observability features such as:

- Logging from the context writes to a built-in, searchable log sink attached to the run. See [logging](/v1/logging).
- An OpenTelemetry collector ships with the platform; you can also instrument it yourself. See [OpenTelemetry](/v1/opentelemetry).
- Prometheus metrics are exposed per tenant. See [Prometheus metrics](/v1/prometheus-metrics).

#### Python

```python
ctx.log("charging order")
```

#### Typescript

```typescript
ctx.logger.info(`validating order ${input.orderId}`);
```

#### Go

```go
ctx.Log("charging order " + input.OrderID)
```

#### Ruby

```ruby
LOG_CHARGE = HATCHET.task(name: "log-charge") do |input, ctx|
  ctx.log("charging order #{input["order_id"]}")

  { "order_id" => input["order_id"], "logged" => true }
end
```

## Where to go next

- [Tasks](/v1/tasks) and [running your task](/v1/running-your-task) for the basics
- [Durable execution](/v1/durable-execution) to understand the durable context and its guarantees
- [DAGs](/v1/directed-acyclic-graphs) for the declarative replacement for orchestration code
- [Architecture and guarantees](/v1/architecture-and-guarantees) if you are evaluating rather than migrating
