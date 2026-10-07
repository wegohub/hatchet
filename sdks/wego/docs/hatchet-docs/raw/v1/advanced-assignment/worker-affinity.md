# Worker Affinity Assignment (Beta)

> **Info:** This feature is currently in beta and may be subject to change.

It is often desirable to assign workflows to specific workers based on certain criteria, such as worker capabilities, resource availability, or location. Worker affinity allows you to specify that a workflow should be assigned to a specific worker based on worker label state. Labels can be set dynamically on workers to reflect their current state, such as a specific model loaded into memory or specific disk requirements.

Specific tasks can then specify desired label state to ensure that workflows are assigned to workers that meet specific criteria. If no worker meets the specified criteria, the task run will remain in a pending state until a suitable worker becomes available or the task is cancelled. (See [Scheduling Timeouts](/v1/timeouts#task-level-timeouts))

## Specifying Worker Labels

Labels can be set on workers when they are registered with Hatchet. Labels are key-value pairs that can be used to specify worker capabilities, resource availability, or other criteria that can be used to match workflows to workers. Values can be strings or numbers, and multiple labels can be set on a worker.

#### Python

```python
worker = hatchet.worker(
    "affinity-worker",
    slots=10,
    labels={
        "model": "fancy-ai-model-v2",
        "memory": 512,
    },
    workflows=[affinity_worker_workflow],
)
worker.start()
```

#### Typescript

```typescript
const workflow = hatchet.workflow({
  name: 'affinity-workflow',
  description: 'test',
});

workflow.task({
  name: 'step1',
  fn: async (_, ctx) => {
    const results = [];

    for (let i = 0; i < 50; i++) {
      const result = await childWorkflow.run({});
      results.push(result);
    }
    ctx.logger.info('Spawned 50 child workflows');
    ctx.logger.info('Results', { results });

    return { step1: 'step1 results!' };
  },
});
```

#### Go

```go
worker, err := client.NewWorker("affinity-worker",
	hatchet.WithWorkflows(affinityWorkflow),
	hatchet.WithSlots(10),
	hatchet.WithLabels(map[string]any{
		"model":  "fancy-ai-model-v2",
		"memory": 512,
	}),
)
```

#### Ruby

```ruby
def main
  worker = HATCHET.worker(
    "affinity-worker",
    slots: 10,
    labels: {
      "model" => "fancy-ai-model-v2",
      "memory" => 512
    },
    workflows: [AFFINITY_WORKER_WORKFLOW]
  )
  worker.start
end
```

## Specifying Step Desired Labels

You can specify desired worker label state for specific tasks in a workflow by setting the `desired_worker_labels` property on the task definition. This property is an object where the keys are the label keys and the values are objects with the following properties:

- `value`: The desired value of the label
- `comparator` (default: `EQUAL`): The comparison operator to use when matching the label value.
  - `EQUAL`: The label value must be equal to the desired value
  - `NOT_EQUAL`: The label value must not be equal to the desired value
  - `GREATER_THAN`: The label value must be greater than the desired value
  - `GREATER_THAN_OR_EQUAL`: The label value must be greater than or equal to the desired value
  - `LESS_THAN`: The label value must be less than the desired value
  - `LESS_THAN_OR_EQUAL`: The label value must be less than or equal to the desired value
- `required` (default: `true`): Whether the label is required for the task to run. If `true`, the task will remain in a pending state until a worker with the desired label state becomes available. If `false`, the worker will be prioritized based on the sum of the highest matching weights.
- `weight` (optional, default: `100`): The weight of the label. Higher weights are prioritized over lower weights when selecting a worker for the task. If multiple workers have the same highest weight, the worker with the highest sum of weights will be selected. Ignored if `required` is `true`.

#### Ruby

```python
affinity_worker_workflow = hatchet.workflow(name="AffinityWorkflow")


@affinity_worker_workflow.task(
    desired_worker_labels=[
        DesiredWorkerLabel(key="model", value="fancy-ai-model-v2", weight=10),
        DesiredWorkerLabel(
            key="memory",
            value=256,
            required=True,
            comparator=WorkerLabelComparator.LESS_THAN,
        ),
    ],
)
```

#### Tab 2

```typescript
const workflow = hatchet.workflow({
  name: 'affinity-workflow',
  description: 'test',
});

workflow.task({
  name: 'step1',
  fn: async (_, ctx) => {
    const results = [];

    for (let i = 0; i < 50; i++) {
      const result = await childWorkflow.run({});
      results.push(result);
    }
    ctx.logger.info('Spawned 50 child workflows');
    ctx.logger.info('Results', { results });

    return { step1: 'step1 results!' };
  },
});
```

#### Tab 3

```go
	err = w.RegisterWorkflow(
		&worker.WorkflowJob{
			On:          worker.Events("user:create:affinity"),
			Name:        "affinity",
			Description: "affinity",
			Steps: []*worker.WorkflowStep{
				worker.Fn(func(ctx worker.HatchetContext) (result *taskOneOutput, err error) {
					return &taskOneOutput{
						Message: ctx.Worker().ID(),
					}, nil
				}).
					SetName("task-one").
					SetDesiredLabels(map[string]*types.DesiredWorkerLabel{
						"model": {
							Value:  "fancy-ai-model-v2",
							Weight: 10,
						},
						"memory": {
							Value:      512,
							Required:   true,
							Comparator: types.ComparatorPtr(types.WorkerLabelComparator_GREATER_THAN),
						},
					}),
			},
		},
	)
```

#### Tab 4

```ruby
AFFINITY_WORKER_WORKFLOW = HATCHET.workflow(name: "AffinityWorkflow")
```

> **Warning:** Use extra care when using worker affinity with [sticky assignment `HARD`
>   strategy](/v1/advanced-assignment/sticky-assignment). In this case, it is
>   recommended to set desired labels on the first task of the workflow to ensure
>   that the workflow is assigned to a worker that meets the desired criteria and
>   remains on that worker for the duration of the workflow.

### Dynamic Worker Labels

Labels can also be set dynamically on workers using the `upsertLabels` method. This can be useful when worker state changes over time, such as when a new model is loaded into memory or when a worker's resource availability changes.

#### Ruby

```python
async def step(input: EmptyModel, ctx: Context) -> dict[str, str | None]:
    if ctx.worker_labels.get("model") != "fancy-ai-model-v2":
        ctx.worker.upsert_labels({"model": "unset"})
        # DO WORK TO EVICT OLD MODEL / LOAD NEW MODEL
        ctx.worker.upsert_labels({"model": "fancy-ai-model-v2"})

    return {"worker": ctx.worker_id}
```

#### Tab 2

```typescript
const childWorkflow = hatchet.workflow({
  name: 'child-affinity-workflow',
  description: 'test',
});

childWorkflow.task({
  name: 'child-step1',
  desiredWorkerLabels: {
    model: {
      value: 'xyz',
      required: true,
    },
  },
  fn: async (ctx) => {
    return { childStep1: 'childStep1 results!' };
  },
});
```

#### Tab 3

```go
	err = w.RegisterWorkflow(
		&worker.WorkflowJob{
			On:          worker.Events("user:create:affinity"),
			Name:        "affinity",
			Description: "affinity",
			Steps: []*worker.WorkflowStep{
				worker.Fn(func(ctx worker.HatchetContext) (result *taskOneOutput, err error) {

    				model := ctx.Worker().GetLabels()["model"]

    				if model != "fancy-vision-model" {
    					ctx.Worker().UpsertLabels(map[string]interface{}{
    						"model": nil,
    					})
    					// Do something to load the model
            evictModel();
            loadNewModel("fancy-vision-model");
    					ctx.Worker().UpsertLabels(map[string]interface{}{
    						"model": "fancy-vision-model",
    					})
    				}

    				return &taskOneOutput{
    					Message: ctx.Worker().ID(),
    				}, nil
    			}).
    				SetName("task-one").
    				SetDesiredLabels(map[string]*types.DesiredWorkerLabel{
    					"model": {
    						Value:  "fancy-vision-model",
    						Weight: 10,
    					},
    					"memory": {
    						Value:      512,
    						Required:   true,
    						Comparator: types.WorkerLabelComparator_GREATER_THAN,
    					},
    				}),
    		},
    	},
    )

```

#### Tab 4

```ruby
AFFINITY_WORKER_WORKFLOW.task(
  :step,
  desired_worker_labels: {
    "model" => Hatchet::DesiredWorkerLabel.new(value: "fancy-ai-model-v2", weight: 10),
    "memory" => Hatchet::DesiredWorkerLabel.new(
      value: 256,
      required: true,
      comparator: :less_than
    )
  }
) do |input, ctx|
  if ctx.worker.labels["model"] != "fancy-ai-model-v2"
    ctx.worker.upsert_labels("model" => "unset")
    # DO WORK TO EVICT OLD MODEL / LOAD NEW MODEL
    ctx.worker.upsert_labels("model" => "fancy-ai-model-v2")
  end

  { "worker" => ctx.worker.id }
end
```
