# Logging

Hatchet comes with a built-in logging view where you can push logs from your workflows. This is useful for debugging and monitoring your workflows.

#### Ruby

You can use either Python's built-in `logging` package, or the `context.log` method for more control over the logs that are sent.

## Using the built-in `logging` package

You can pass a custom logger to the `Hatchet` class when initializing it. For example:

```python
import logging

from hatchet_sdk import ClientConfig, Hatchet

logging.basicConfig(level=logging.INFO)

root_logger = logging.getLogger()

hatchet = Hatchet(
    config=ClientConfig(
        logger=root_logger,
    ),
)
```

It's recommended that you pass the root logger to the `Hatchet` class, as this will ensure that all logs are captured by the Hatchet logger. If you have workflows defined in multiple files, they should be children of the root logger. For example, with the following file structure:

```
workflows/
  workflow.py
client.py
worker.py
workflow.py
```

You should pass the root logger to the `Hatchet` class in `client.py`:

```python
import logging

from hatchet_sdk import ClientConfig, Hatchet

logging.basicConfig(level=logging.INFO)

root_logger = logging.getLogger()

hatchet = Hatchet(
    config=ClientConfig(
        logger=root_logger,
    ),
)
```

And then in `workflows/workflow.py`, you should create a child logger:

```python
import logging
import time

from examples.logger.client import hatchet
from hatchet_sdk import Context, EmptyModel

logger = logging.getLogger(__name__)

logging_workflow = hatchet.workflow(
    name="LoggingWorkflow",
)


@logging_workflow.task()
def root_logger(input: EmptyModel, ctx: Context) -> dict[str, str]:
    for i in range(12):
        logger.info(f"executed step1 - {i}")
        logger.info({"step1": "step1"})

        time.sleep(0.1)

    return {"status": "success"}
```

## Using the `context.log` method

You can also use the `context.log` method to log messages from your workflows. This method is available on the `Context` object that is passed to each task in your workflow. For example:

```python
@logging_workflow.task()
def context_logger(input: EmptyModel, ctx: Context) -> dict[str, str]:
    for i in range(12):
        ctx.log(f"executed step1 - {i}")
        ctx.log({"step1": "step1"})

        time.sleep(0.1)

    return {"status": "success"}
```

Each task is currently limited to 1000 log lines.

#### Tab 2

In TypeScript, there are two options for logging from your tasks. The first is to use the `ctx.log()` method (from the `Context`) to send logs:

```typescript
const workflow = hatchet.workflow({
  name: 'logger-example',
  description: 'test',
  on: {
    event: 'user:create',
  },
});

workflow.task({
  name: 'logger-step1',
  fn: async (_, ctx) => {
    // log in a for loop

    for (let i = 0; i < 10; i++) {
      ctx.logger.info(`log message ${i}`);
      await sleep(200);
    }

    return { step1: 'completed step run' };
  },
});
```

This has the benefit of being easy to use out of the box (no setup required!), but it's limited in its flexibiliy and how pluggable it is with your existing logging setup.

Hatchet also allows you to "bring your own" logger when you define a workflow:

```typescript
const logger = pino();

class PinoLogger implements Logger {
  logLevel: LogLevel;
  context: string;

  constructor(context: string, logLevel: LogLevel = 'DEBUG') {
    this.logLevel = logLevel;
    this.context = context;
  }

  debug(message: string, extra?: JsonObject): void {
    logger.debug(extra, message);
  }

  info(message: string, extra?: JsonObject): void {
    logger.info(extra, message);
  }

  green(message: string, extra?: JsonObject): void {
    logger.info(extra, `%c${message}`);
  }

  warn(message: string, error?: Error, extra?: JsonObject): void {
    logger.warn(extra, `${message} ${error}`);
  }

  error(message: string, error?: Error, extra?: JsonObject): void {
    logger.error(extra, `${message} ${error}`);
  }

  // optional util method
  util(key: string, message: string, extra?: JsonObject): void {
    // for example you may want to expose a trace method
    if (key === 'trace') {
      logger.info(extra, 'trace');
    }
  }
}

const hatchet = Hatchet.init({
  log_level: 'DEBUG',
  logger: (ctx, level) => new PinoLogger(ctx, level),
});
```

In this example, we create Pino logger that implement's Hatchet's `Logger` interface and pass it to the Hatchet client constructor. We can then use that logger in our steps:

```typescript
const workflow = hatchet.workflow({
  name: 'logger-example',
  description: 'test',
  on: {
    event: 'user:create',
  },
});

workflow.task({
  name: 'logger-step1',
  fn: async (_, ctx) => {
    // log in a for loop

    for (let i = 0; i < 10; i++) {
      ctx.logger.info(`log message ${i}`);
      await sleep(200);
    }

    return { step1: 'completed step run' };
  },
});
```

#### Tab 3

```ruby
require "hatchet-sdk"
require "logger"

logger = Logger.new($stdout)
logger.level = Logger::INFO

HATCHET = Hatchet::Client.new(debug: true) unless defined?(HATCHET)

LOGGING_WORKFLOW = HATCHET.workflow(name: "LoggingWorkflow")

LOGGING_WORKFLOW.task(:root_logger) do |input, ctx|
  12.times do |i|
    logger.info("executed step1 - #{i}")
    logger.info({ "step1" => "step1" }.inspect)

    sleep 0.1
  end

  { "status" => "success" }
end
```
```ruby
LOGGING_WORKFLOW.task(:context_logger) do |input, ctx|
  12.times do |i|
    ctx.log("executed step1 - #{i}")
    ctx.log({ "step1" => "step1" }.inspect)

    sleep 0.1
  end

  { "status" => "success" }
end
```
