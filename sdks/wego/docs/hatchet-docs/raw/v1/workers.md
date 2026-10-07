# Workers

Workers in Hatchet are the long-running processes that execute [tasks](/v1/tasks). In the broadest sense, it may be helpful to think of a worker as a simple `while` loop that receives a new task assignment from Hatchet, executes the task, and reports the results back.

When workers are spun up - in any environment, be it locally, on a VM, etc. - they will register themselves with Hatchet to start receiving and executing tasks.

## Declaring a worker

A worker needs a name and a set of tasks (or workflows, more on this later) to register:

#### Python

```python
def main() -> None:
    worker = hatchet.worker("dag-worker", workflows=[dag_workflow])

    worker.start()
```

#### Typescript

```typescript
import { hatchet } from '../hatchet-client';
import { simple } from './workflow';
import { parent, child } from './workflow-with-child';
import { simpleWithZod } from './zod';

async function main() {
  const worker = await hatchet.worker('simple-worker', {
    // 👀 Declare the workflows that the worker can execute
    workflows: [simple, simpleWithZod, parent, child],
    // 👀 Declare the number of concurrent task runs the worker can accept
    slots: 100,
  });

  await worker.start();
}

if (require.main === module) {
  main();
}
```

#### Go

```go
worker, err := client.NewWorker("simple-worker", hatchet.WithWorkflows(task))
if err != nil {
	log.Fatalf("failed to create worker: %v", err)
}

interruptCtx, cancel := cmdutils.NewInterruptContext()
defer cancel()

err = worker.StartBlocking(interruptCtx)
if err != nil {
	log.Fatalf("failed to start worker: %v", err)
}
```

#### Ruby

```ruby
def main
  worker = HATCHET.worker("dag-worker", workflows: [DAG_WORKFLOW])
  worker.start
end
```

When a worker starts, it registers each of its tasks and workflows with Hatchet. From that point on, Hatchet knows to route matching tasks to that worker.

One important note is that multiple workers can register the same task. In this scenario, Hatchet distributes work across all of them, allowing for simple horizontal scaling.

## Starting a worker

#### CLI (recommended)

The fastest way to run a worker during development is with the Hatchet CLI. This handles authentication and hot reloads on code changes:

```bash
hatchet worker dev
```

#### Script

You can also run workers without the CLI, which you're likely to do in a production setting, for instance. To do this, you'll first need to set a `HATCHET_CLIENT_TOKEN` environment variable, or provide it via parameters when creating the Hatchet client.

> **Info:** If you don't already have a token, you can generate one in the "API Tokens" section under "Settings" in the dashboard.

```bash
export HATCHET_CLIENT_TOKEN="<your-client-token>"
```

If you're running a self-hosted engine without TLS enabled, also set:

```bash
export HATCHET_CLIENT_TLS_STRATEGY=none
```

Then run the worker:

#### Python

```bash
python worker.py
```

#### Typescript

Add a script to your `package.json`:

```json
"scripts": {
  "start:worker": "ts-node src/worker.ts"
}
```

Then run it:

```bash
npm run start:worker
```

#### Go

```bash
go run main.go
```

#### Ruby

```bash
bundle exec ruby worker.rb
```

Once the worker starts, you will see logs confirming it is connected:

```
[INFO]  🪓 -- starting hatchet...
[DEBUG] 🪓 -- 'test-worker' waiting for ['simpletask:step1']
[DEBUG] 🪓 -- acquired action listener: efc4aaf2-...
[DEBUG] 🪓 -- sending heartbeat
```

> **Info:** For self-hosted engines, there may be additional gRPC configuration options
>   needed. See the [Self-Hosting](/self-hosting/worker-configuration-options)
>   docs for details.

## Slots

Every worker has a fixed number of **slots** that control how many tasks it can run concurrently, which can be configured with the `slots` option on the worker. For instance, if `slots` is set to 5, the worker will run up to five tasks concurrently at any time. Any additional tasks wait in the queue until a slot opens up. Slots are a **local** limit. They protect the individual worker from attempting to run more tasks concurrently than desired, which can help control resource usage by the worker.


To set the slot count, pass the `slots` option when declaring the worker:

#### Python

```python
def main() -> None:
    worker = hatchet.worker(
        "concurrency-demo-worker", slots=10, workflows=[concurrency_limit_workflow]
    )

    worker.start()
```

#### Typescript

```typescript
async function main() {
  const worker = await hatchet.worker('timeout-worker', {
    workflows: [timeoutTask, refreshTimeoutTask],
    slots: 50,
  });

  await worker.start();
}
```

#### Go

```go
worker, err := client.NewWorker("concurrency-worker",
	hatchet.WithWorkflows(
		ConcurrencyRoundRobin(client),
		MultipleConcurrencyKeys(client),
		ConcurrencyCancelInProgress(client),
		ConcurrencyCancelNewest(client),
		ConcurrencyCancelQueuedExceptNewest(client),
		ConcurrencyCancelQueuedExceptOldest(client),
		syncCrm,
		generateReport,
		DynamicMaxRuns(client),
	),
	hatchet.WithSlots(10),
)
if err != nil {
	log.Fatalf("failed to create worker: %v", err)
}
```

#### Ruby

```ruby
def main
  worker = HATCHET.worker(
    "concurrency-demo-worker", slots: 10, workflows: [CONCURRENCY_LIMIT_WORKFLOW]
  )
  worker.start
end
```

The default slot count for workers in Hatchet is 100. In many cases, leaving the default as-is will be perfectly fine, especially when first getting set up with Hatchet.

By default each running task consumes one slot. A task can be configured to consume more than one, so a task that needs more memory or CPU takes up more of a worker's capacity. See [Task Slot Cost](/v1/advanced-assignment/slot-cost).
