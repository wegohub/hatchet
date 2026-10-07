# Simple Task Retries

Hatchet provides a simple and effective way to handle failures in your tasks using a retry policy. This feature allows you to specify the number of times a task should be retried if it fails, helping to improve the reliability and resilience of your tasks.

> **Info:** Task-level retries can be added to both `Standalone Tasks` and `Workflow
>   Tasks`.

## How it works

When a task fails (i.e. throws an error or returns a non-zero exit code), Hatchet can automatically retry the task based on the `retries` configuration defined in the task object. Here's how it works:

1. If a task fails and `retries` is set to a value greater than 0, Hatchet will catch the error and retry the task.
2. The task will be retried up to the specified number of times, with each retry being executed after a short delay to avoid overwhelming the system.
3. If the task succeeds during any of the retries, the task will continue as normal.
4. If the task continues to fail after exhausting all the specified retries, the task will be marked as failed.

This simple retry mechanism can help to mitigate transient failures, such as network issues or temporary unavailability of external services, without requiring complex error handling logic in your task code.

## How to use task-level retries

To enable retries for a task, simply add the `retries` property to the task object in your task definition:

#### Python

```python
@simple_workflow.task(retries=3)
def always_fail(input: EmptyModel, ctx: Context) -> dict[str, str]:
    raise Exception("simple task failed")
```

#### Typescript

```typescript
export const retries = hatchet.task({
  name: 'retries',
  retries: 3,
  fn: async (_, ctx) => {
    throw new Error('intentional failure');
  },
});
```

#### Go

```go
retries := client.NewStandaloneTask("retries-task", func(ctx hatchet.Context, input RetriesInput) (*RetriesResult, error) {
	return nil, errors.New("intentional failure")
}, hatchet.WithRetries(3))
```

#### Ruby

```ruby
SIMPLE_RETRY_WORKFLOW.task(:always_fail, retries: 3) do |input, ctx|
  raise "simple task failed"
end
```

You can add the `retries` property to any task, and Hatchet will handle the retry logic automatically.

It's important to note that task-level retries are not suitable for all types of failures.
For example, if a task fails due to a programming error or an invalid configuration, retrying the task will likely not resolve the issue.
In these cases, you should fix the underlying problem in your code or configuration rather than relying on retries. See [Bypassing retry logic](#bypassing-retry-logic).

Additionally, if a task interacts with external services or databases, you should ensure that the operation is idempotent (i.e. can be safely repeated without changing the result) before enabling retries. Otherwise, retrying the task could lead to unintended side effects or inconsistencies in your data.

## Accessing the Retry Count in a Running Task

You can access the current retry count on the task's context object:

#### Python

```python
@simple_workflow.task(retries=3)
def fail_twice(input: EmptyModel, ctx: Context) -> dict[str, str]:
    if ctx.retry_count < 2:
        raise Exception("simple task failed")

    return {"status": "success"}
```

#### Typescript

```typescript
export const retriesWithCount = hatchet.task({
  name: 'retries-with-count',
  retries: 3,
  fn: async (_, ctx) => {
    // > Get the current retry count
    const retryCount = ctx.retryCount();

    ctx.logger.info(`Retry count: ${retryCount}`);

    if (retryCount < 2) {
      throw new Error('intentional failure');
    }

    return {
      message: 'success',
    };
  },
});
```

#### Go

```go
retriesWithCount := client.NewStandaloneTask("fail-twice-task", func(ctx hatchet.Context, input RetriesWithCountInput) (*RetriesWithCountResult, error) {
	// Get the current retry count
	retryCount := ctx.RetryCount()

	fmt.Printf("Retry count: %d\n", retryCount)

	if retryCount < 2 {
		return nil, errors.New("intentional failure")
	}

	return &RetriesWithCountResult{
		Message: "success",
	}, nil
}, hatchet.WithRetries(3))
```

#### Ruby

```ruby
SIMPLE_RETRY_WORKFLOW.task(:fail_twice, retries: 3) do |input, ctx|
  raise "simple task failed" if ctx.retry_count < 2

  { "status" => "success" }
end
```

## Exponential Backoff

Hatchet also supports exponential backoff for retries, which can be useful for handling failures in a more resilient manner. Exponential backoff increases the delay between retries exponentially, giving the failing service more time to recover before the next retry.

#### Python

```python
@backoff_workflow.task(
    retries=10,
    # 👀 Maximum number of seconds to wait between retries
    backoff_max_seconds=10,
    # 👀 Factor to increase the wait time between retries.
    # This sequence will be 2s, 4s, 8s, 10s, 10s, 10s... due to the maxSeconds limit
    backoff_factor=2.0,
)
def backoff_task(input: EmptyModel, ctx: Context) -> dict[str, str]:
    if ctx.retry_count < 3:
        raise Exception("backoff task failed")

    return {"status": "success"}
```

#### Typescript

```typescript
export const withBackoff = hatchet.task({
  name: 'with-backoff',
  retries: 10,
  backoff: {
    // 👀 Maximum number of seconds to wait between retries
    maxSeconds: 10,
    // 👀 Factor to increase the wait time between retries.
    // This sequence will be 2s, 4s, 8s, 10s, 10s, 10s... due to the maxSeconds limit
    factor: 2,
  },
  fn: async () => {
    throw new Error('intentional failure');
  },
});
```

#### Go

```go
withBackoff := client.NewStandaloneTask("with-backoff-task", func(ctx hatchet.Context, input BackoffInput) (*BackoffResult, error) {
	return nil, errors.New("intentional failure")
}, hatchet.WithRetries(3), hatchet.WithRetryBackoff(2, 10))
```

#### Ruby

```ruby
BACKOFF_WORKFLOW.task(
  :backoff_task,
  retries: 10,
  # Maximum number of seconds to wait between retries
  backoff_max_seconds: 10,
  # Factor to increase the wait time between retries.
  # This sequence will be 2s, 4s, 8s, 10s, 10s, 10s... due to the maxSeconds limit
  backoff_factor: 2.0
) do |input, ctx|
  raise "backoff task failed" if ctx.retry_count < 3

  { "status" => "success" }
end
```

## Bypassing Retry logic

The Hatchet SDKs each expose a `NonRetryable` exception, which allows you to bypass pre-configured retry logic for the task. **If your task raises this exception, it will not be retried.** This allows you to circumvent the default retry behavior in instances where you don't want to or cannot safely retry. Some examples in which this might be useful include:

1. A task that calls an external API which returns a 4XX response code.
2. A task that contains a single non-idempotent operation that can fail but cannot safely be rerun on failure, such as a billing operation.
3. A failure that requires manual intervention to resolve.

#### Python

```python
@non_retryable_workflow.task(retries=1)
def should_not_retry(input: EmptyModel, ctx: Context) -> None:
    raise NonRetryableException("This task should not retry")
```

#### Typescript

```typescript
const shouldNotRetry = nonRetryableWorkflow.task({
  name: 'should-not-retry',
  fn: () => {
    throw new NonRetryableError('This task should not retry');
  },
  retries: 1,
});
```

#### Go

```go
retries := client.NewStandaloneTask("non-retryable-task", func(ctx hatchet.Context, input NonRetryableInput) (*NonRetryableResult, error) {
	return nil, hatchet.NewNonRetryableError(errors.New("intentional failure"))
}, hatchet.WithRetries(3))
```

#### Ruby

```ruby
NON_RETRYABLE_WORKFLOW.task(:should_not_retry, retries: 1) do |input, ctx|
  raise Hatchet::NonRetryableError, "This task should not retry"
end

NON_RETRYABLE_WORKFLOW.task(:should_retry_wrong_exception_type, retries: 1) do |input, ctx|
  raise TypeError, "This task should retry because it's not a NonRetryableError"
end

NON_RETRYABLE_WORKFLOW.task(:should_not_retry_successful_task, retries: 1) do |input, ctx|
  # no-op
end
```

In these cases, even though `retries` is set to a non-zero number (meaning the task would ordinarily retry), Hatchet will not retry.

## Python SDK Client Retry Behavior

The retry behavior described above is for task execution inside Hatchet. The Python SDK also has separate retry behavior for certain client-side REST and gRPC calls made by the SDK itself.

These client retries are configured separately from task retries and do not control whether a task is retried after failing in a worker.

> **Info:** Task retries and SDK client retries are separate mechanisms. Task retries
>   control whether Hatchet retries a task after task failure. SDK client retries
>   control whether the Python SDK retries certain API calls to Hatchet.

### Default client retry behavior

By default, the Python SDK retries certain client calls with exponential backoff, with `max_attempts` defaulting to 5.

**REST API calls**

Error Type, Retried by Default

HTTP 5xx (server errors), Yes
HTTP 404 (not found), Yes
HTTP 429 (too many requests), No
HTTP 400, 401, 403, 409, 422 (client errors), No
Transport errors (timeout, connection, TLS, protocol), No

**gRPC calls**

Status Code, Retried

`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`, Yes
`RESOURCE_EXHAUSTED`, `ABORTED`, `UNKNOWN`, Yes
`UNIMPLEMENTED`, `NOT_FOUND`, `INVALID_ARGUMENT`, No
`ALREADY_EXISTS`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, No

> **Info:** REST 404 responses are retried by default because some REST reads can observe
>   replication lag between the core database and the OLAP database.

### Configuring Python SDK client retries

The Python SDK exposes client retry configuration through `TenacityConfig`, either directly in `ClientConfig` or via environment variables.

```python
import os

from hatchet_sdk import Hatchet
from hatchet_sdk.config import ClientConfig, HTTPMethod, TenacityConfig

hatchet = Hatchet(
    config=ClientConfig(
        token=os.environ["HATCHET_CLIENT_TOKEN"],
        tenacity=TenacityConfig(
            max_attempts=5,
            retry_429=False,
            retry_transport_errors=False,
            retry_transport_methods=[HTTPMethod.GET, HTTPMethod.DELETE],
        ),
    )
)
```

Name, Type, Description, Default

`max_attempts`, `int`, Maximum number of retry attempts. Set to 0 to disable retries., `5`
`retry_429`, `bool`, Enable retries for HTTP 429 Too Many Requests responses., `False`
`retry_transport_errors`, `bool`, Enable retries for REST transport-level errors (timeout, connection, TLS)., `False`
`retry_transport_methods`, `list[HTTPMethod]`, HTTP methods to retry on transport errors when `retry_transport_errors` is enabled., `[GET, DELETE]`

You can also configure these via environment variables:

Environment Variable, Description

`HATCHET_CLIENT_TENACITY_MAX_ATTEMPTS`, Maximum retry attempts
`HATCHET_CLIENT_TENACITY_RETRY_429`, Enable 429 retries (`true`/`false`)
`HATCHET_CLIENT_TENACITY_RETRY_TRANSPORT_ERRORS`, Enable transport error retries (`true`/`false`)

### Idempotency considerations

> **Warning:** When `retry_transport_errors` is enabled, only idempotent HTTP methods (`GET`,
>   `DELETE`) are retried by default. Non-idempotent methods (`POST`, `PUT`,
>   `PATCH`) are excluded because retrying them after a transport error could
>   result in duplicate operations if the original request succeeded but the
>   response was lost.

You can add non-idempotent methods to `retry_transport_methods`, but only do so if:

1. Your operations are idempotent (for example, because they use idempotency keys), or
2. You understand and accept the risk of duplicate operations

### Retry timing

Python SDK client retries use exponential backoff with jitter. Fine-grained backoff timing is not currently configurable through `TenacityConfig`.

## Go SDK Client Retry Behavior

The retry behavior described above is for task execution inside Hatchet. The Go SDK also retries some REST and gRPC calls that the SDK itself makes to Hatchet.

These SDK client retries are configured separately from task retries. They do not control whether Hatchet retries a task after it fails in a worker.

> **Info:** Task retries and SDK client retries are separate mechanisms. Task retries
>   control whether Hatchet retries a task after task failure. SDK client retries
>   control whether the Go SDK retries certain API calls to Hatchet.

### Default Go client retry behavior

By default, the Go SDK retries certain client calls with exponential backoff. REST reads use up to 5 total attempts: the initial attempt plus up to 4 retries. gRPC calls keep the existing 5 attempt retry limit.

REST read retries use bounded jittered backoff. When the caller request context has no deadline, each REST attempt uses a response-header timeout without cutting off response body reads. If the caller context already has a deadline, that deadline governs the whole request.

**REST API calls (bodyless `GET` and `HEAD` only)**

Error Type, Retried by Default

HTTP 502, 503, 504 (gateway errors), Yes
HTTP 404 (not found), No
HTTP 429 (too many requests), Yes
HTTP 400, 401, 403, 409, 422 (client errors), No
Transport errors (timeout, connection, TLS, protocol), Yes

For HTTP 429 responses on idempotent reads, the Go SDK honors a valid `Retry-After` header when it fits the client retry cap. When `Retry-After` is missing, invalid, or oversized, it falls back to the same bounded jittered backoff used for other retriable errors.

> **Info:** Unlike the Python SDK, the Go SDK does not retry HTTP 404 responses on REST
>   reads in this release. Python retries some 404 reads to account for
>   replication lag between the core database and the OLAP database.

**gRPC calls**

Status Code, Retried

`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`, Yes
`RESOURCE_EXHAUSTED`, Yes
`FAILED_PRECONDITION`, No
`UNIMPLEMENTED`, `NOT_FOUND`, `INVALID_ARGUMENT`, No
`ALREADY_EXISTS`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, No

> **Info:** `FAILED_PRECONDITION` is not retried because Hatchet uses it for non-transient
>   control-plane signals such as inactive listeners. The unary interceptor still
>   retries all unary RPCs, including writes, in this release.

### Configuring Go SDK client retries

Use environment variables to disable SDK client retries:

Environment Variable, Description

`HATCHET_CLIENT_NO_RETRY`, Disables both REST and gRPC SDK client retries when set to a truthy value.
`HATCHET_CLIENT_NO_GRPC_RETRY`, Legacy gRPC-only retry control. Disables gRPC SDK retries only. REST read retries remain enabled unless `HATCHET_CLIENT_NO_RETRY` is set.

If both variables are set, all SDK client retries are disabled.

### Idempotency considerations

> **Warning:** Go SDK REST retries apply only to bodyless `GET` and `HEAD` requests. `POST`,
>   `PUT`, `PATCH`, and `DELETE` requests are never retried by the SDK client in
>   this release. Bodied requests are excluded because Go `http.Request` bodies
>   are one-shot unless `GetBody` is set or the SDK buffers and rebuilds the body.
>   The generated REST clients do not set `GetBody`.

## Conclusion

Hatchet's task-level retry feature is a simple and effective way to handle transient failures in your tasks, improving the reliability and resilience of your tasks. By specifying the number of retries for each task, you can ensure that your tasks can recover from temporary issues without requiring complex error handling logic.

Remember to use retries judiciously and only for tasks that are idempotent. For more advanced retry strategies, such as exponential backoff or circuit breaking, stay tuned for future updates to Hatchet's retry capabilities.
