# Lifespans


  Lifespans are an **experimental feature** in Hatchet, and are subject to
  change.


Hatchet's Python SDK allows you define a **_lifespan_**, which is an async generator that runs when your worker starts up and cleans up when it exits, which lets you share state across all of the tasks running on the worker. This behaves almost identically to [FastAPI's lifespans](https://fastapi.tiangolo.com/advanced/events/), and is intended to be used in the same way. Lifespans are useful for sharing state like connection pools across all tasks on a single worker. They also work great for loading expensive machine learning models into memory before the worker starts.


  We recommend only using lifespans for storing **_immutable_** state to share
  between tasks running on your worker. The intention is not to e.g. store a
  counter of the number of tasks that a worker has run and increment that
  counter on each task run. This is prone to unexpected behavior due to
  concurrency in Hatchet.


## Usage

To use Hatchet's `lifespan` feature, define an async generator and pass it into your `worker`:

```python
class Lifespan(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    foo: str
    pool: ConnectionPool


async def lifespan() -> AsyncGenerator[Lifespan, None]:
    print("Running lifespan!")
    with ConnectionPool("postgres://hatchet:hatchet@localhost:5431/hatchet") as pool:
        yield Lifespan(
            foo="bar",
            pool=pool,
        )

    print("Cleaning up lifespan!")


worker = hatchet.worker(
    "test-worker", slots=1, workflows=[lifespan_workflow], lifespan=lifespan
)
```

When the worker starts, it will run the lifespan up to the `yield`. Then, on worker shutdown, it will clean up by running everything after the `yield` (the same as with any other generator).


  Your lifespan must only `yield` **_once_**.


Then, to use your lifespan in a task, you can extract it from the context with `Context.lifespan`.

```python
class TaskOutput(BaseModel):
    num_rows: int
    external_ids: list[UUID]


lifespan_workflow = hatchet.workflow(name="LifespanWorkflow")


@lifespan_workflow.task()
def sync_lifespan_task(input: EmptyModel, ctx: Context) -> TaskOutput:
    pool = cast(Lifespan, ctx.lifespan).pool

    with pool.connection() as conn:
        query = conn.execute("SELECT * FROM v1_lookup_table_olap LIMIT 5;")
        rows = query.fetchall()

        for row in rows:
            print(row)

        print("executed sync task with lifespan", ctx.lifespan)

        return TaskOutput(
            num_rows=len(rows),
            external_ids=[cast(UUID, row[0]) for row in rows],
        )
```


  For type checking, cast the `Context.lifespan` to whatever type your lifespan
  generator yields.


And that's it! Now, any task running on the worker with the lifespan provided will have access to the lifespan data.
