# DAGs as Durable Workflows

A directed acyclic graph (DAG) is a workflow where every task, along with the dependencies between them, is declared upfront, before the workflow runs. At runtime, Hatchet schedules the tasks in the right order, runs tasks that don't depend on each other in parallel, and passes the outputs of parent tasks to their children automatically.


DAGs in Hatchet are a form of [durable execution](/v1/durable-execution) by definition. Every time a task in a DAG completes, Hatchet persists its status and result so that the DAG can be retried without re-executing succeeded parts. This gives DAGs similar exactly-once-style guarantees you'd get from a [durable task](/v1/durable-tasks), without having to write any explicit durable execution code yourself.

On top of those durable properties, DAGs come bundled with a set of workflow-building features you'd otherwise have to construct by hand: automatic parallelism between independent tasks, typed inputs and outputs flowing from parents to children, [branching](#branching-with-parent-conditions), [or groups](#waiting-on-conditions-with-or-groups) for expressing complex wait conditions, and a clear visual representation of the workflow in the Hatchet dashboard.

Common examples of good fits for DAGs are ETL pipelines such as for document or image processing, and CI/CD-style workflows.

## Defining a workflow

To define a DAG, start by declaring a workflow.

#### Python

```python
dag_workflow = hatchet.workflow(name="DAGWorkflow", input_validator=DAGWorkflowInput)
```

#### Typescript

```typescript
// First, we declare the workflow
export const dag = hatchet.workflow({
  name: 'simple',
});
```

#### Go

```go
workflow := client.NewWorkflow("dag-workflow")
```

#### Ruby

```ruby
DAG_WORKFLOW = HATCHET.workflow(name: "DAGWorkflow")
```

### Defining a task

Once you have a workflow, you can add tasks to it. Each task is a function that receives the workflow's input and optionally returns an output. Just like a standalone task, every task in a DAG can declare its own retries, timeouts, concurrency settings, and so on. For more on how tasks work, see the [tasks documentation](/v1/tasks).

#### Python

```python
@dag_workflow.task(execution_timeout=timedelta(seconds=5))
def step1(input: DAGWorkflowInput, ctx: Context) -> StepOutput:
    if input.should_fail:
        raise Exception("intentional error")

    return StepOutput(random_number=random.randint(1, 100))
```

#### Typescript

```typescript
// Next, we declare the tasks bound to the workflow
const toLower = dag.task({
  name: 'to-lower',
  fn: (input) => {
    return {
      TransformedMessage: input.Message.toLowerCase(),
    };
  },
});
```

#### Go

```go
step1 := workflow.NewTask("step-1", func(ctx hatchet.Context, input Input) (StepOutput, error) {
	return StepOutput{
		Step:   1,
		Result: input.Value * 2,
	}, nil
})
```

#### Ruby

```ruby
STEP1 = DAG_WORKFLOW.task(:step1, execution_timeout: 5) do |input, ctx|
  { "random_number" => rand(1..100) }
end

STEP2 = DAG_WORKFLOW.task(:step2, execution_timeout: 5) do |input, ctx|
  { "random_number" => rand(1..100) }
end
```

### Adding task dependencies

Once you have more than one task on a workflow, you can start to declare dependencies between them. A task can declare one or more **parent** tasks, meaning that those parent tasks must complete successfully before the child task is allowed to run. Tasks that don't depend on each other will be run in parallel. When a task runs, the outputs of its parents are available on its context object, so data flows naturally from one part of the DAG to the next.


#### Python

```python
@dag_workflow.task(execution_timeout=timedelta(seconds=5))
async def step2(input: DAGWorkflowInput, ctx: Context) -> StepOutput:
    return StepOutput(random_number=random.randint(1, 100))


@dag_workflow.task(parents=[step1, step2])
async def step3(input: DAGWorkflowInput, ctx: Context) -> RandomSum:
    one = ctx.task_output(step1).random_number
    two = ctx.task_output(step2).random_number

    return RandomSum(sum=one + two)
```

#### Typescript

```typescript
dag.task({
  name: 'reverse',
  parents: [toLower],
  fn: async (input, ctx) => {
    const lower = await ctx.parentOutput(toLower);
    return {
      Original: input.Message,
      Transformed: lower.TransformedMessage.split('').reverse().join(''),
    };
  },
});
```

#### Go

```go
step2 := workflow.NewTask("step-2", func(ctx hatchet.Context, input Input) (StepOutput, error) {
	// Get output from step 1
	var step1Output StepOutput
	if err := ctx.ParentOutput(step1, &step1Output); err != nil {
		return StepOutput{}, err
	}

	return StepOutput{
		Step:   2,
		Result: step1Output.Result + 10,
	}, nil
}, hatchet.WithParents(step1))
```

#### Ruby

```ruby
DAG_WORKFLOW.task(:step3, parents: [STEP1, STEP2]) do |input, ctx|
  one = ctx.task_output(STEP1)["random_number"]
  two = ctx.task_output(STEP2)["random_number"]

  { "sum" => one + two }
end

DAG_WORKFLOW.task(:step4, parents: [STEP1, :step3]) do |input, ctx|
  puts(
    "executed step4",
    Time.now.strftime("%H:%M:%S"),
    input.inspect,
    ctx.task_output(STEP1).inspect,
    ctx.task_output(:step3).inspect
  )

  { "step4" => "step4" }
end
```

## Running a workflow

Once you've defined a workflow and registered it on a [worker](/v1/workers), you can trigger it in all of the same ways you can trigger a standalone task. You can run it and wait for the result, enqueue it and let it run in the background, schedule it for the future, and so on. See the [Running Tasks](/v1/running-your-task) documentation for the full set of options.

## Branching with parent conditions

Even though the structure of a DAG is fixed when you define it, individual tasks can still decide at runtime whether or not they should run, based on data produced earlier in the workflow. **Parent conditions** let a task inspect the output of one of its parents and either skip itself or cancel itself based on what that output contains. There are two operators available:

- **`skip_if`**: Skip this task if the parent's output matches the condition. Downstream tasks can [check whether a parent was skipped](#checking-if-a-parent-was-skipped) and behave accordingly.
- **`cancel_if`**: Cancel this task if the parent's output matches the condition.

Conditions are [CEL expressions](/v1/cel-expressions#task-condition-expressions) evaluated against the parent's `output`.

> **Warning:** A task cancelled by `cancel_if` behaves like any other cancellation in
>   Hatchet, meaning its downstream dependents will be cancelled as well.

A common way to use parent conditions is to express sibling branches in a DAG. You declare a base task that returns some data, and then add two sibling tasks with complementary `skip_if` conditions, so that exactly one of them runs on any given workflow execution.

First, declare the base task that returns the value the branches will key off of:

#### Python

```python
@task_condition_workflow.task()
def start(input: EmptyModel, ctx: Context) -> StepOutput:
    return StepOutput(random_number=random.randint(1, 100))
```

#### Typescript

```typescript
const start = taskConditionWorkflow.task({
  name: 'start',
  fn: () => {
    return {
      randomNumber: Math.floor(Math.random() * 100) + 1,
    };
  },
});
```

#### Go

```go
start := workflow.NewTask("start", func(ctx hatchet.Context, _ any) (StepOutput, error) {
	return StepOutput{RandomNumber: rand.Intn(100) + 1}, nil //nolint:gosec
})
```

#### Ruby

```ruby
COND_START = TASK_CONDITION_WORKFLOW.task(:start) do |input, ctx|
  { "random_number" => rand(1..100) }
end
```

Then add two branches that each use a `skip_if` parent condition to decide whether they should run, based on the output of the base task:

#### Python

```python
@task_condition_workflow.task(
    parents=[wait_for_sleep],
    skip_if=[
        ParentCondition(
            parent=wait_for_sleep,
            expression="output.random_number > 50",
        )
    ],
)
def left_branch(input: EmptyModel, ctx: Context) -> StepOutput:
    return StepOutput(random_number=random.randint(1, 100))


@task_condition_workflow.task(
    parents=[wait_for_sleep],
    skip_if=[
        ParentCondition(
            parent=wait_for_sleep,
            expression="output.random_number <= 50",
        )
    ],
)
def right_branch(input: EmptyModel, ctx: Context) -> StepOutput:
    return StepOutput(random_number=random.randint(1, 100))
```

#### Typescript

```typescript
const leftBranch = taskConditionWorkflow.task({
  name: 'leftBranch',
  parents: [waitForSleep],
  skipIf: [new ParentCondition(waitForSleep, 'output.randomNumber > 50')],
  fn: () => {
    return {
      randomNumber: Math.floor(Math.random() * 100) + 1,
    };
  },
});

const rightBranch = taskConditionWorkflow.task({
  name: 'rightBranch',
  parents: [waitForSleep],
  skipIf: [new ParentCondition(waitForSleep, 'output.randomNumber <= 50')],
  fn: () => {
    return {
      randomNumber: Math.floor(Math.random() * 100) + 1,
    };
  },
});
```

#### Go

```go
leftBranch := workflow.NewTask("left-branch", func(ctx hatchet.Context, _ any) (StepOutput, error) {
	return StepOutput{RandomNumber: rand.Intn(100) + 1}, nil //nolint:gosec
},
	hatchet.WithParents(waitForSleep),
	hatchet.WithSkipIf(hatchet.ParentCondition(waitForSleep, "output.random_number > 50")),
)

rightBranch := workflow.NewTask("right-branch", func(ctx hatchet.Context, _ any) (StepOutput, error) {
	return StepOutput{RandomNumber: rand.Intn(100) + 1}, nil //nolint:gosec
},
	hatchet.WithParents(waitForSleep),
	hatchet.WithSkipIf(hatchet.ParentCondition(waitForSleep, "output.random_number <= 50")),
)
```

#### Ruby

```ruby
LEFT_BRANCH = TASK_CONDITION_WORKFLOW.task(
  :left_branch,
  parents: [WAIT_FOR_SLEEP],
  skip_if: [
    Hatchet::ParentCondition.new(
      parent: WAIT_FOR_SLEEP,
      expression: "output.random_number > 50"
    )
  ]
) do |input, ctx|
  { "random_number" => rand(1..100) }
end

RIGHT_BRANCH = TASK_CONDITION_WORKFLOW.task(
  :right_branch,
  parents: [WAIT_FOR_SLEEP],
  skip_if: [
    Hatchet::ParentCondition.new(
      parent: WAIT_FOR_SLEEP,
      expression: "output.random_number <= 50"
    )
  ]
) do |input, ctx|
  { "random_number" => rand(1..100) }
end
```

### Checking if a parent was skipped

When two sibling branches both feed into a common downstream task, the downstream task often needs to know which of its parents actually ran, as opposed to which one was skipped. You can check this on the context with `ctx.was_skipped`:

#### Python

```python
@task_condition_workflow.task(
    parents=[
        start,
        wait_for_sleep,
        wait_for_event,
        skip_on_event,
        left_branch,
        right_branch,
    ],
)
def sum(input: EmptyModel, ctx: Context) -> RandomSum:
    one = ctx.task_output(start).random_number
    two = ctx.task_output(wait_for_event).random_number
    three = ctx.task_output(wait_for_sleep).random_number
    four = (
        ctx.task_output(skip_on_event).random_number
        if not ctx.was_skipped(skip_on_event)
        else 0
    )

    five = (
        ctx.task_output(left_branch).random_number
        if not ctx.was_skipped(left_branch)
        else 0
    )
    six = (
        ctx.task_output(right_branch).random_number
        if not ctx.was_skipped(right_branch)
        else 0
    )

    return RandomSum(sum=one + two + three + four + five + six)
```

#### Typescript

```typescript
taskConditionWorkflow.task({
  name: 'sum',
  parents: [start, waitForSleep, waitForEvent, skipOnEvent, leftBranch, rightBranch],
  fn: async (_, ctx: Context<any, any>) => {
    const one = (await ctx.parentOutput(start)).randomNumber;
    const two = (await ctx.parentOutput(waitForEvent)).randomNumber;
    const three = (await ctx.parentOutput(waitForSleep)).randomNumber;
    const four = (await ctx.parentOutput(skipOnEvent))?.randomNumber || 0;
    const five = (await ctx.parentOutput(leftBranch))?.randomNumber || 0;
    const six = (await ctx.parentOutput(rightBranch))?.randomNumber || 0;

    return {
      sum: one + two + three + four + five + six,
    };
  },
});
```

#### Go

```go
_ = workflow.NewTask("sum", func(ctx hatchet.Context, _ any) (RandomSum, error) {
	var startOut StepOutput
	err := ctx.ParentOutput(start, &startOut)
	if err != nil {
		return RandomSum{}, err
	}

	var waitForEventOut StepOutput
	err = ctx.ParentOutput(waitForEvent, &waitForEventOut)
	if err != nil {
		return RandomSum{}, err
	}

	var waitForSleepOut StepOutput
	err = ctx.ParentOutput(waitForSleep, &waitForSleepOut)
	if err != nil {
		return RandomSum{}, err
	}

	total := startOut.RandomNumber + waitForEventOut.RandomNumber + waitForSleepOut.RandomNumber

	if !ctx.WasSkipped(skipOnEvent) {
		var out StepOutput
		err = ctx.ParentOutput(skipOnEvent, &out)
		if err == nil {
			total += out.RandomNumber
		}
	}

	if !ctx.WasSkipped(leftBranch) {
		var out StepOutput
		err = ctx.ParentOutput(leftBranch, &out)
		if err == nil {
			total += out.RandomNumber
		}
	}

	if !ctx.WasSkipped(rightBranch) {
		var out StepOutput
		err = ctx.ParentOutput(rightBranch, &out)
		if err == nil {
			total += out.RandomNumber
		}
	}

	return RandomSum{Sum: total}, nil
}, hatchet.WithParents(
	start,
	waitForSleep,
	waitForEvent,
	skipOnEvent,
	leftBranch,
	rightBranch,
))
```

#### Ruby

```ruby
TASK_CONDITION_WORKFLOW.task(
  :sum,
  parents: [COND_START, WAIT_FOR_SLEEP, WAIT_FOR_EVENT, SKIP_ON_EVENT, LEFT_BRANCH, RIGHT_BRANCH]
) do |input, ctx|
  one = ctx.task_output(COND_START)["random_number"]
  two = ctx.task_output(WAIT_FOR_EVENT)["random_number"]
  three = ctx.task_output(WAIT_FOR_SLEEP)["random_number"]
  four = ctx.was_skipped?(SKIP_ON_EVENT) ? 0 : ctx.task_output(SKIP_ON_EVENT)["random_number"]
  five = ctx.was_skipped?(LEFT_BRANCH) ? 0 : ctx.task_output(LEFT_BRANCH)["random_number"]
  six = ctx.was_skipped?(RIGHT_BRANCH) ? 0 : ctx.task_output(RIGHT_BRANCH)["random_number"]

  { "sum" => one + two + three + four + five + six }
end
```

## Waiting on conditions with or groups

In addition to parent conditions, a task in a DAG can wait for external signals before it starts running. It might wait for a sleep timer to expire, for a [user event](/v1/events) to arrive, or for some combination of these alongside parent conditions. You compose conditions like these using **or groups**.

An or group is a set of conditions combined with an `Or` operator, which evaluates to `True` as soon as **at least one** of the conditions inside it is satisfied. If you declare more than one or group on the same task, the groups are combined with `AND`, meaning that every group must have at least one of its conditions satisfied before the task is allowed to run. Between these two operators, you can express arbitrarily complex wait conditions in [conjunctive normal form](https://en.wikipedia.org/wiki/Conjunctive_normal_form) (CNF).

### Sleep + event example

A common pattern is to combine a sleep condition and an event condition in the same or group, so that the task proceeds as soon as either an external signal arrives _or_ a timeout expires, whichever happens first. This is a natural fit for human-in-the-loop workflows, where you want to put a deadline on how long you'll wait for a response before moving on.

#### Python

```python
@task_condition_workflow.task(
    parents=[start],
    wait_for=[
        or_(
            SleepCondition(duration=timedelta(minutes=1)),
            UserEventCondition(event_key="wait_for_event:start"),
        )
    ],
)
def wait_for_event(input: EmptyModel, ctx: Context) -> StepOutput:
    return StepOutput(random_number=random.randint(1, 100))
```

#### Typescript

```typescript
const waitForEvent = taskConditionWorkflow.task({
  name: 'waitForEvent',
  parents: [start],
  waitFor: [Or(new SleepCondition('1m'), new UserEventCondition('wait_for_event:start', 'true'))],
  fn: () => {
    return {
      randomNumber: Math.floor(Math.random() * 100) + 1,
    };
  },
});
```

#### Go

```go
waitForEvent := workflow.NewTask("wait-for-event", func(ctx hatchet.Context, _ any) (StepOutput, error) {
	return StepOutput{RandomNumber: rand.Intn(100) + 1}, nil //nolint:gosec
},
	hatchet.WithParents(start),
	hatchet.WithWaitFor(hatchet.OrCondition(
		hatchet.SleepCondition(1*time.Minute),
		hatchet.UserEventCondition("wait_for_event:start", ""),
	)),
)
```

#### Ruby

```ruby
WAIT_FOR_EVENT = TASK_CONDITION_WORKFLOW.task(
  :wait_for_event,
  parents: [COND_START],
  wait_for: [
    Hatchet.or_(
      Hatchet::SleepCondition.new(60),
      Hatchet::UserEventCondition.new(event_key: "wait_for_event:start")
    )
  ]
) do |input, ctx|
  { "random_number" => rand(1..100) }
end
```

### Combining multiple or groups

For more complicated wait logic, you can declare more than one or group on the same task. As an example, consider these three conditions:

- **Condition A**: A parent task's output is greater than 50.
- **Condition B**: A 30 second sleep timer expires.
- **Condition C**: A `payment:processed` event arrives.

If you want the task to proceed when `(A or B)` **and** `(A or C)` are both satisfied, you'd declare two separate or groups on the task: one containing `A or B`, and the other containing `A or C`. The task will only start once both groups have been satisfied. If `A` is true, both groups pass immediately. If `A` is false, the task needs both `B` (the sleep expires) and `C` (the event arrives) before it can run.

#### Python

```python
@task_condition_workflow.task(
    parents=[start],
    wait_for=[
        or_(
            SleepCondition(duration=timedelta(seconds=30), readable_data_key="first"),
        ),
        or_(
            SleepCondition(duration=timedelta(seconds=30), readable_data_key="second"),
            UserEventCondition(event_key="payment:processed"),
        ),
    ],
)
def wait_for_or_groups(input: EmptyModel, ctx: Context) -> StepOutput:
    return StepOutput(random_number=random.randint(1, 100))
```

#### Typescript

```typescript
taskConditionWorkflow.task({
  name: 'waitForOrGroups',
  parents: [start],
  waitFor: [
    Or(new ParentCondition(start, 'output.randomNumber > 50'), new SleepCondition('30s')),
    Or(
      new ParentCondition(start, 'output.randomNumber > 50'),
      new UserEventCondition('payment:processed', 'true')
    ),
  ],
  fn: () => {
    return {
      randomNumber: Math.floor(Math.random() * 100) + 1,
    };
  },
});
```
