# Triggering Runs from Events

Run-on-event allows you to trigger one or more tasks when a specific event occurs. This is useful when you need to execute a task in response to an ephemeral event where the result is not important. A few common use cases for event-triggered task runs are:

1. Running a task when an ephemeral event is received, such as a webhook or a message from a queue.
2. When you want to run multiple independent tasks in response to a single event. For instance, if you wanted to run a `send_welcome_email` task, and you also wanted to run a `grant_new_user_credits` task, and a `reward_referral` task, all triggered by the signup. In this case, you might declare all three of those tasks with an event trigger for `user:signup`, and then have them all kick off when that event happens.


> **Info:** Event triggers evaluate tasks to run at the time of the event. If an event is
>   received before the task is registered, the task will not be run.

## Declaring Event Triggers

To run a task on an event, you need to declare the event that will trigger the task. This is done by declaring the `on_events` property in the task declaration.

#### Python

```python
EVENT_KEY = "user:create"
SECONDARY_KEY = "foobarbaz"
WILDCARD_KEY = "subscription:*"


class EventWorkflowInput(BaseModel):
    should_skip: bool


event_workflow = hatchet.workflow(
    name="EventWorkflow",
    on_events=[EVENT_KEY, SECONDARY_KEY, WILDCARD_KEY],
    input_validator=EventWorkflowInput,
)
```

#### Typescript

```typescript
export const lower = hatchet.workflow({
  name: 'lower',
  // 👀 Declare the event that will trigger the workflow
  onEvents: ['simple-event:create'],
});
```

#### Go

```go
const SimpleEvent = "simple-event:create"

func Lower(client *hatchet.Client) *hatchet.StandaloneTask {
	return client.NewStandaloneTask(
		"lower", func(ctx hatchet.Context, input EventInput) (*LowerTaskOutput, error) {
			return &LowerTaskOutput{
				TransformedMessage: strings.ToLower(input.Message),
			}, nil
		},
		hatchet.WithWorkflowEvents(SimpleEvent),
	)
}
```

#### Ruby

```ruby
EVENT_KEY = "user:create"
SECONDARY_KEY = "foobarbaz"
WILDCARD_KEY = "subscription:*"

EVENT_WORKFLOW = HATCHET.workflow(
  name: "EventWorkflow",
  on_events: [EVENT_KEY, SECONDARY_KEY, WILDCARD_KEY]
)
```

> **Info:** Note: Multiple tasks can be triggered by the same event.

> **Info:** As of engine version 0.65.0, Hatchet supports wildcard event triggers using
>   the `*` wildcard pattern. For example, you could register `subscription:*` as
>   your event key, which would match incoming events like `subscription:create`,
>   `subscription:renew`, `subscription:cancel`, and so on.

## Pushing Events to Hatchet

You can push an event to Hatchet by calling the `push` method on the Hatchet event client and providing the event name and payload. Any tasks that have registered an event trigger with a matching event key will be run.

#### Python

```python
hatchet.event.push("user:create", {"should_skip": False})
```

#### Typescript

```typescript
const res = await hatchet.events.push('simple-event:create', {
  Message: 'hello',
  ShouldSkip: false,
});
```

#### Go

```go
err := client.Events().Push(
	context.Background(),
	"simple-event:create",
	EventInput{
		Message: "Hello, World!",
	},
)
if err != nil {
	return err
}
```

#### Ruby

```ruby
HATCHET.event.push("user:create", { "should_skip" => false })
```

> **Info:** Event triggers evaluate tasks to run at the time of the event. If an event is
>   received before the task is registered, the task will not be run.

## Event Filters

Events can be _filtered_ in Hatchet, which allows you to push events to Hatchet and only trigger task runs from them in certain cases. **If you enable filters on a workflow, your workflow will be triggered once for each matching filter on any incoming event with a matching scope** (see [Understanding scopes](#understanding-scopes) below).

### Understanding scopes

Every filter has a required `scope`, which is an arbitrary string that acts as a grouping key for filters. Its purpose is to narrow down the set of candidate filters an event is evaluated against: when you push an event with a `scope`, Hatchet only considers filters whose scope is an exact match for the event's scope, and then triggers the workflow once for each of those filters whose expression evaluates to `true`.

Concretely, for a workflow with event triggers:

- If the workflow has **no filters**, matching events trigger it as usual, and scopes have no effect.
- If the workflow has **one or more filters**, an incoming event will only trigger it if the event's scope exactly matches the scope of at least one filter, _and_ that filter's expression evaluates to `true` for the event. An event pushed without a scope, or with a scope that doesn't match any of the workflow's filters, will not trigger the workflow at all.

Scopes are most useful when you want per-entity filtering, such as in a multi-tenant application where each of your customers should have their own filtering rules. For instance, you might create one filter per customer with a scope like `customer:1234`, each with its own expression and payload. When an event arrives for that customer, you push it with `scope="customer:1234"`, and it's evaluated only against that customer's filters — not against every filter registered for the workflow.

### Basic Usage

There are two ways to create filters in Hatchet.

### Default filters on the workflow

The simplest way to create a filter is to register it declaratively with your workflow when it's created. For example:

#### Python

```python
event_workflow_with_filter = hatchet.workflow(
    name="EventWorkflow",
    on_events=[EVENT_KEY, SECONDARY_KEY, WILDCARD_KEY],
    input_validator=EventWorkflowInput,
    default_filters=[
        DefaultFilter(
            expression="true",
            scope="example-scope",
            payload={
                "main_character": "Anna",
                "supporting_character": "Stiva",
                "location": "Moscow",
            },
        )
    ],
)
```

#### Typescript

```typescript
export const lowerWithFilter = hatchet.workflow({
  name: 'lower',
  // 👀 Declare the event that will trigger the workflow
  onEvents: ['simple-event:create'],
  defaultFilters: [
    {
      expression: 'true',
      scope: 'example-scope',
      payload: {
        mainCharacter: 'Anna',
        supportingCharacter: 'Stiva',
        location: 'Moscow',
      },
    },
  ],
});
```

#### Go

```go
func LowerWithFilter(client *hatchet.Client) *hatchet.StandaloneTask {
	return client.NewStandaloneTask(
		"lower", accessFilterPayload,
		hatchet.WithWorkflowEvents(SimpleEvent),
		hatchet.WithDefaultFilters(hatchet.DefaultFilter{
			Expression: "true",
			Scope:      "example-scope",
			Payload: map[string]interface{}{
				"main_character":       "Anna",
				"supporting_character": "Stiva",
				"location":             "Moscow"},
		}),
	)
}
```

#### Ruby

```ruby
EVENT_WORKFLOW_WITH_FILTER = HATCHET.workflow(
  name: "EventWorkflow",
  on_events: [EVENT_KEY, SECONDARY_KEY, WILDCARD_KEY],
  default_filters: [
    Hatchet::DefaultFilter.new(
      expression: "true",
      scope: "example-scope",
      payload: {
        "main_character" => "Anna",
        "supporting_character" => "Stiva",
        "location" => "Moscow"
      }
    )
  ]
)

EVENT_WORKFLOW.task(:task) do |input, ctx|
  puts "event received"
  ctx.filter_payload
end
```

In each of these cases, we register a filter with the workflow. Note that these "declarative" filters are overwritten each time your workflow is updated, so the ids associated with them will not be stable over time. This allows you to modify a filter in-place or remove a filter, and not need to manually delete it over the API.

### Filters feature client

You also can create event filters by using the `filters` clients on the SDKs:

#### Python

```python
hatchet.filters.create(
    workflow_id=event_workflow.id,
    expression="input.should_skip == false",
    # the scope groups filters: only events pushed with a matching
    # scope are evaluated against this filter. in a real app, this is
    # often an id, e.g. a customer, user, or organization id
    scope="foobarbaz",
    payload={
        "main_character": "Anna",
        "supporting_character": "Stiva",
        "location": "Moscow",
    },
)
```

#### Typescript

```typescript
hatchet.filters.create({
  workflowId: lower.id,
  expression: 'input.ShouldSkip == false',
  scope: 'foobarbaz',
  payload: {
    main_character: 'Anna',
    supporting_character: 'Stiva',
    location: 'Moscow',
  },
});
```

#### Go

```go
_, err = client.Filters().Create(
	context.Background(),
	rest.V1CreateFilterRequest{
		WorkflowId: uuid.MustParse("bb866b59-5a86-451b-8023-10d451db11d3"),
		Expression: "true",
		Scope:      "example-scope",
	},
)
if err != nil {
	return err
}
```

#### Ruby

```ruby
HATCHET_CLIENT.filters.create(
  workflow_id: EVENT_WORKFLOW.id,
  expression: "input.should_skip == false",
  scope: "foobarbaz",
  payload: {
    "main_character" => "Anna",
    "supporting_character" => "Stiva",
    "location" => "Moscow"
  }
)
```

> **Warning:** Note the [`scope`](#understanding-scopes) argument to the filter is required
>   **both when creating a filter, and when pushing events**. If the scope on
>   filter creation does not match the scope provided when pushing events, the
>   filter will not apply.

Then, push an event that uses the filter to determine whether to run. For instance, this run will be skipped, since the payload does not match the expression:

#### Python

```python
hatchet.event.push(
    event_key=EVENT_KEY,
    payload={
        "should_skip": True,
    },
    scope="foobarbaz",
)
```

#### Typescript

```typescript
hatchet.events.push(
  SIMPLE_EVENT,
  {
    Message: 'hello',
    ShouldSkip: true,
  },
  {
    scope: 'foobarbaz',
  }
);
```

#### Go

```go
skipPayload := map[string]interface{}{
	"shouldSkip": true,
}
skipScope := "foobarbaz"
err = client.Events().Push(
	context.Background(),
	"simple-event:create",
	skipPayload,
	hatchet.WithFilterScope(&skipScope),
)
if err != nil {
	return err
}
```

#### Ruby

```ruby
HATCHET_CLIENT.event.push(
  EVENT_KEY,
  { "should_skip" => true },
  scope: "foobarbaz"
)
```

But this one will be triggered since the payload _does_ match the expression:

#### Python

```python
hatchet.event.push(
    event_key=EVENT_KEY,
    payload={
        "should_skip": False,
    },
    scope="foobarbaz",
)
```

#### Typescript

```typescript
hatchet.events.push(
  SIMPLE_EVENT,
  {
    Message: 'hello',
    ShouldSkip: false,
  },
  {
    scope: 'foobarbaz',
  }
);
```

#### Go

```go
triggerPayload := map[string]interface{}{
	"shouldSkip": false,
}
triggerScope := "foobarbaz"
err = client.Events().Push(
	context.Background(),
	"simple-event:create",
	triggerPayload,
	hatchet.WithFilterScope(&triggerScope),
)
if err != nil {
	return err
}
```

#### Ruby

```ruby
HATCHET_CLIENT.event.push(
  EVENT_KEY,
  { "should_skip" => false },
  scope: "foobarbaz"
)
```

> **Info:** In Hatchet, filters are "positive", meaning that we look for _matches_ to the
>   filter to determine which tasks to trigger.

### Accessing the filter payload

You can access the filter payload by using the `Context` in the task that was triggered by your event:

#### Python

```python
@event_workflow_with_filter.task()
def filtered_task(input: EventWorkflowInput, ctx: Context) -> None:
    print(ctx.filter_payload)
```

#### Typescript

```typescript
lowerWithFilter.task({
  name: 'lowerWithFilter',
  fn: (input, ctx) => {
    ctx.logger.info('filterPayload', { filterPayload: ctx.filterPayload() });
  },
});
```

#### Go

```go
func accessFilterPayload(ctx hatchet.Context, input EventInput) (*LowerTaskOutput, error) {
	fmt.Println(ctx.FilterPayload())
	return &LowerTaskOutput{
		TransformedMessage: strings.ToLower(input.Message),
	}, nil
}
```

#### Ruby

```ruby
EVENT_WORKFLOW_WITH_FILTER.task(:filtered_task) do |input, ctx|
  puts ctx.filter_payload.inspect
end
```

### Advanced Usage

In addition to referencing `input` in the expression (which corresponds to the _event_ payload), you can also reference the following fields:

1. `payload` corresponds to the _filter_ payload (which was part of the request when the filter was created).
2. `additional_metadata` allows for filtering based on `additional_metadata` sent with the event.
3. `event_key` allows for filtering based on the key of the event, such as `user:created`.

See [CEL Expressions](/v1/cel-expressions#event-filter-expressions) for the full set of variables and functions available in filter expressions.
