# Dependency Injection


  Dependency injection is an **experimental feature** in Hatchet, and is subject
  to change.


Hatchet's Python SDK allows you to inject **_dependencies_** into your tasks, FastAPI style. These dependencies can be either synchronous or asynchronous functions. They are executed before the task is triggered, and their results are injected into the task as parameters.

This behaves almost identically to [FastAPI's dependency injection](https://fastapi.tiangolo.com/tutorial/dependencies/), and is intended to be used in the same way. Dependencies are useful for sharing logic between tasks that you'd like to avoid repeating, or would like to factor out of the task logic itself (e.g. to make testing easier).


Since dependencies are run before tasks are executed, having many dependencies (or any that take a long time to evaluate) can cause tasks to experience significantly delayed start times, as they must wait for all dependencies to finish evaluating.


## Usage

To add dependencies to your tasks, import `Depends` from the `hatchet_sdk`. Then:

```python
async def async_dep(input: EmptyModel, ctx: Context) -> str:
    return ASYNC_DEPENDENCY_VALUE


def sync_dep(input: EmptyModel, ctx: Context) -> str:
    return SYNC_DEPENDENCY_VALUE


@asynccontextmanager
async def async_cm_dep(
    input: EmptyModel, ctx: Context, async_dep: Annotated[str, Depends(async_dep)]
) -> AsyncGenerator[str, None]:
    try:
        yield ASYNC_CM_DEPENDENCY_VALUE + "_" + async_dep
    finally:
        pass


@contextmanager
def sync_cm_dep(
    input: EmptyModel, ctx: Context, sync_dep: Annotated[str, Depends(sync_dep)]
) -> Generator[str, None, None]:
    try:
        yield SYNC_CM_DEPENDENCY_VALUE + "_" + sync_dep
    finally:
        pass


@contextmanager
def base_cm_dep(input: EmptyModel, ctx: Context) -> Generator[str, None, None]:
    try:
        yield CHAINED_CM_VALUE
    finally:
        pass


def chained_dep(
    input: EmptyModel, ctx: Context, base_cm: Annotated[str, Depends(base_cm_dep)]
) -> str:
    return "chained_" + base_cm


@asynccontextmanager
async def base_async_cm_dep(
    input: EmptyModel, ctx: Context
) -> AsyncGenerator[str, None]:
    try:
        yield CHAINED_ASYNC_CM_VALUE
    finally:
        pass


async def chained_async_dep(
    input: EmptyModel,
    ctx: Context,
    base_async_cm: Annotated[str, Depends(base_async_cm_dep)],
) -> str:
    return "chained_" + base_async_cm
```

In this example, we've declared two dependencies: one synchronous and one asynchronous. You can do anything you like in your dependencies, such as creating database sessions, managing configuration, sharing instances of service-layer logic, and more.

Once you've defined your dependency functions, inject them into your tasks as follows:

```python
@hatchet.task()
async def async_task_with_dependencies(
    _i: EmptyModel,
    ctx: Context,
    async_dep: Annotated[str, Depends(async_dep)],
    sync_dep: Annotated[str, Depends(sync_dep)],
    async_cm_dep: Annotated[str, Depends(async_cm_dep)],
    sync_cm_dep: Annotated[str, Depends(sync_cm_dep)],
    chained_dep: Annotated[str, Depends(chained_dep)],
    chained_async_dep: Annotated[str, Depends(chained_async_dep)],
) -> Output:
    return Output(
        sync_dep=sync_dep,
        async_dep=async_dep,
        async_cm_dep=async_cm_dep,
        sync_cm_dep=sync_cm_dep,
        chained_dep=chained_dep,
        chained_async_dep=chained_async_dep,
    )
```


  Important note: Your dependency functions must take two positional arguments:
  the workflow input and the `Context` (the same as any other task).


That's it! Now, whenever your task is triggered, its dependencies will be evaluated, and the results will be injected into the task at runtime for you to use as needed.
