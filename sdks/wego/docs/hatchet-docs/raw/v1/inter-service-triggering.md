## Invoking Tasks From Other Services

While Hatchet recommends importing your workflows and standalone tasks directly to use for triggering runs, this only works in a monorepo or similar setups where you have access to those objects. However, it's common to have a polyrepo, have code written in multiple languages, or otherwise not be able to import your workflows and standalone tasks directly.
Hatchet provides stub tasks for these cases, allowing you to trigger your tasks from anywhere in a type-safe way with only minor code duplication.

### Creating a "Stub" Task on your External Service (Recommended)

The recommended way to trigger a run from a service where you _cannot_ import the workflow or standalone task definition directly is to create a "stub" task or workflow on your external service. This is a Hatchet task or workflow that has the same name and input/output types as the task you want to trigger on your Hatchet worker, but without the function or other configuration.

This allows you to have a polyglot, fully typed interface with full SDK support.

#### Typescript

```typescript
import { hatchet } from '../hatchet-client';

// (optional) Define the input type for the workflow
export type SimpleInput = {
  Message: string;
};

// (optional) Define the output type for the workflow
export type SimpleOutput = {
  'to-lower': {
    TransformedMessage: string;
  };
};

// declare the workflow with the same name as the
// workflow name on the worker
export const simple = hatchet.workflow({
  name: 'simple',
});

// you can use all the same run methods on the stub
// with full type-safety
simple.run({ Message: 'Hello, World!' });
simple.runNoWait({ Message: 'Hello, World!' });
simple.schedule(new Date(), { Message: 'Hello, World!' });
simple.cron('my-cron', '0 0 * * *', { Message: 'Hello, World!' });
```

#### Python

Consider a task with an implementation like this:

```python
from pydantic import BaseModel

from hatchet_sdk import Context, Hatchet


class TaskInput(BaseModel):
    user_id: int


class TaskOutput(BaseModel):
    ok: bool


hatchet = Hatchet()


@hatchet.task(name="externally-triggered-task", input_validator=TaskInput)
async def externally_triggered_task(input: TaskInput, ctx: Context) -> TaskOutput:
    return TaskOutput(ok=True)
```

To trigger this task from a separate service where the code is not shared, start by defining models that match the input and output types of the task defined above.

```python
class TaskInput(BaseModel):
    user_id: int


class TaskOutput(BaseModel):
    ok: bool
```

Next, create the stub task.

```python
stub = hatchet.stubs.task(
    # make sure the name and schemas exactly match the implementation
    name="externally-triggered-task",
    input_validator=TaskInput,
    output_validator=TaskOutput,
)
```

Finally, use the stub to trigger the underlying task, and (optionally) retrieve the result.

```python
# input type checks properly
result = await stub.aio_run(input=TaskInput(user_id=1234))

# `result.ok` type checks properly
print("Is successful:", result.ok)
```

#### Go

```go
package main

import (
	"context"
	"fmt"
	"log"

	hatchet "github.com/hatchet-dev/hatchet/sdks/go"
)

type StubInput struct {
	Message string `json:"message"`
}

type StubOutput struct {
	Ok bool `json:"ok"`
}

func StubWorkflow(client *hatchet.Client) *hatchet.StandaloneTask {
	return client.NewStandaloneTask("stub-workflow", func(ctx hatchet.Context, input StubInput) (StubOutput, error) {
		return StubOutput{
			Ok: true,
		}, nil
	})
}

func main() {
	client, err := hatchet.NewClient()
	if err != nil {
		log.Fatalf("failed to create hatchet client: %v", err)
	}

	task := StubWorkflow(client)

	// we are simply running the task here, but it can be implemented in another service / worker
	// and in another language with the same name and input-output types
	result, err := task.Run(context.Background(), StubInput{Message: "Hello, World!"})
	if err != nil {
		log.Fatalf("failed to run task: %v", err)
	}

	fmt.Println(result)
}
```

#### Ruby


  Note that this approach requires code duplication, which can break type
  safety. For instance, if the input type to your workflow changes, you need to
  remember to also change the type passed to the stub. Some ways to mitigate
  risks here are helpful comments reminding developers to keep these types in
  sync, code generation tools, and end-to-end tests.
