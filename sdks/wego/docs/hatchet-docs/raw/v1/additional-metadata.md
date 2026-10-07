# Additional Metadata

Hatchet allows you to attach arbitrary key-value string pairs to events and task runs, which can be used for filtering, searching, or any other lookup purposes. This additional metadata is not part of the event payload or task input data but provides supplementary information for better organization and discoverability.

> **Info:** Additional metadata can be added to `Runs`, `Scheduled Runs`, `Cron Runs`, and
>   `Events`. The data is propagated from parents to children or from events to
>   runs.

You can attach additional metadata when pushing events or triggering task runs using the Hatchet client libraries:

#### Event Push

#### Ruby

```python
hatchet.event.push(
    "user:create",
    {"userId": "1234", "should_skip": False},
    additional_metadata={"source": "api"},  # Arbitrary key-value pair
)
```

#### Tab 2

```typescript
const withMetadata = await hatchet.events.push(
  'user:create',
  {
    test: 'test',
  },
  {
    additionalMetadata: {
      source: 'api', // Arbitrary key-value pair
    },
  }
);
```

#### Tab 3

```go
err = client.Events().Push(
	context.Background(),
	"user:create",
	Input{Message: "hello"},
	hatchet.WithEventMetadata(
		map[string]string{"version": "1.0.0"},
	),
)
if err != nil {
	log.Fatalf("failed to push event: %v", err)
}
```

#### Tab 4

```ruby
HATCHET.event.push(
  "user:create",
  { "userId" => "1234", "should_skip" => false },
  additional_metadata: { "source" => "api" }
)
```

#### Task Run Trigger

#### Ruby

```python
simple.run(
    additional_metadata={"source": "api"},  # Arbitrary key-value pair
)
```

#### Tab 2

```typescript
const withMetadata = simple.run(
  {
    Message: 'HeLlO WoRlD',
  },
  {
    additionalMetadata: {
      source: 'api', // Arbitrary key-value pair
    },
  }
);
```

#### Tab 3

```go
_, err = client.Run(
	context.Background(),
	"my-workflow",
	Input{Message: "hello"},
	hatchet.WithRunMetadata(
		map[string]string{"version": "1.0.0"},
	),
)
if err != nil {
	log.Fatalf("failed to run workflow: %v", err)
}
```

#### Tab 4

```ruby
SIMPLE.run(
  {},
  options: Hatchet::TriggerWorkflowOptions.new(
    additional_metadata: { "source" => "api" }
  )
)
```

## Filtering in the Dashboard

Once you've attached additional metadata to events or task runs, this data will be available in the Event and Task Run list views in the Hatchet dashboard. You can use the filter input field to search for events or task runs based on the additional metadata key-value pairs you've attached.

For example, you can filter events by the `source` metadata keys to quickly find events originating from a specific source or environment.

![Blocks](/addl-meta.gif)

## Filtering Semantics

When filtering **runs** with `additional_metadata`, Hatchet returns runs that match **any** of the provided pairs (logical `OR`), not all of them.

> **Info:** For example, filtering runs with `{ first_name: "john", last_name: "doe" }`
>   returns runs whose metadata matches *either* `first_name: "john"` *or*
>   `last_name: "doe"`, not only runs that match both.

Event filtering behaves differently: filtering **events** with multiple `additional_metadata` pairs requires all pairs to match (logical `AND`), since events use `JSONB` containment under the hood.

> **Info:** For example, filtering events with `{ first_name: "john", last_name: "doe" }`
>   returns only events whose metadata matches *both* `first_name: "john"` *and*
>   `last_name: "doe"`.

## Use Cases

Some common use cases for additional metadata include:

- Tagging events or task runs with environment information (e.g., `production`, `staging`, `development`)
- Specifying the source or origin of events (e.g., `api`, `webhook`, `manual`)
- Categorizing events or task runs based on business-specific criteria (e.g., `priority`, `region`, `product`)

By leveraging additional metadata, you can enhance the organization, searchability, and discoverability of your events and task runs within Hatchet.
