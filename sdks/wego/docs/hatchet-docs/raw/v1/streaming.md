# Streaming in Hatchet

Hatchet tasks can stream data back to a consumer in real-time. This has a number of valuable uses, such as streaming the results of an LLM call back from a Hatchet worker to a frontend or sending progress updates as a task chugs along.


## Publishing Stream Events

You can stream data out of a task run by using the `put_stream` (or equivalent) method on the `Context`.

#### Python

```python
anna_karenina = """
Happy families are all alike; every unhappy family is unhappy in its own way.

Everything was in confusion in the Oblonskys' house. The wife had discovered that the husband was carrying on an intrigue with a French girl, who had been a governess in their family, and she had announced to her husband that she could not go on living in the same house with him.
"""


def create_chunks(content: str, n: int) -> Generator[str, None, None]:
    for i in range(0, len(content), n):
        yield content[i : i + n]


chunks = list(create_chunks(anna_karenina, 10))


@hatchet.task()
async def stream_task(input: EmptyModel, ctx: Context) -> None:
    # 👀 Sleeping to avoid race conditions
    await asyncio.sleep(2)

    for chunk in chunks:
        await ctx.aio_put_stream(chunk)
        await asyncio.sleep(0.20)
```

#### Typescript

```typescript
const annaKarenina = `
Happy families are all alike; every unhappy family is unhappy in its own way.

Everything was in confusion in the Oblonskys' house. The wife had discovered that the husband was carrying on an intrigue with a French girl, who had been a governess in their family, and she had announced to her husband that she could not go on living in the same house with him.
`;

function* createChunks(content: string, n: number): Generator<string, void, unknown> {
  for (let i = 0; i < content.length; i += n) {
    yield content.slice(i, i + n);
  }
}

export const streamingTask = hatchet.task({
  name: 'stream-example',
  fn: async (_, ctx) => {
    await sleep(2000);

    for (const chunk of createChunks(annaKarenina, 10)) {
      ctx.putStream(chunk);
      await sleep(200);
    }
  },
});
```

#### Go

```go
const annaKarenina = `
Happy families are all alike; every unhappy family is unhappy in its own way.

Everything was in confusion in the Oblonskys' house. The wife had discovered that the husband was carrying on an intrigue with a French girl, who had been a governess in their family, and she had announced to her husband that she could not go on living in the same house with him.
`

func createChunks(content string, n int) []string {
	var chunks []string
	for i := 0; i < len(content); i += n {
		end := i + n
		if end > len(content) {
			end = len(content)
		}
		chunks = append(chunks, content[i:end])
	}
	return chunks
}

func StreamTask(ctx hatchet.Context, input StreamTaskInput) (*StreamTaskOutput, error) {
	time.Sleep(2 * time.Second)

	chunks := createChunks(annaKarenina, 10)

	for _, chunk := range chunks {
		ctx.PutStream(chunk)
		time.Sleep(200 * time.Millisecond)
	}

	return &StreamTaskOutput{
		Message: "Streaming completed",
	}, nil
}
```

#### Ruby

```ruby
ANNA_KARENINA = <<~TEXT
  Happy families are all alike; every unhappy family is unhappy in its own way.

  Everything was in confusion in the Oblonskys' house. The wife had discovered that the husband was carrying on an intrigue with a French girl, who had been a governess in their family, and she had announced to her husband that she could not go on living in the same house with him.
TEXT

STREAM_CHUNKS = ANNA_KARENINA.scan(/.{1,10}/)

STREAM_TASK = HATCHET.task(name: "stream_task") do |input, ctx|
  # Sleeping to avoid race conditions
  sleep 2

  STREAM_CHUNKS.each do |chunk|
    ctx.put_stream(chunk)
    sleep 0.20
  end
end
```

This task will stream small chunks of content through Hatchet, which can then be consumed elsewhere. Here we use some text as an example, but this is intended to replicate streaming the results of an LLM call back to a consumer.

## Consuming Streams

You can easily consume stream events by using the stream method on the workflow run ref that the various [fire-and-forget](/v1/running-your-task#fire-and-forget) methods return.

#### Python

```python
ref = await stream_task.aio_run(wait_for_result=False)

async for chunk in hatchet.runs.subscribe_to_stream(ref.workflow_run_id):
    print(chunk, flush=True, end="")
```

#### Typescript

```typescript
const ref = await streamingTask.runNoWait({});
const id = await ref.getWorkflowRunId();

for await (const content of hatchet.runs.subscribeToStream(id)) {
  process.stdout.write(content);
}
```

#### Go

```go
func main() {
	client, err := hatchet.NewClient()
	if err != nil {
		log.Fatalf("Failed to create Hatchet client: %v", err)
	}

	ctx := context.Background()

	streamingWorkflow := shared.StreamingWorkflow(client)

	workflowRun, err := streamingWorkflow.RunNoWait(ctx, shared.StreamTaskInput{})
	if err != nil {
		log.Fatalf("Failed to run workflow: %v", err)
	}

	id := workflowRun.RunId
	stream := client.Runs().SubscribeToStream(ctx, id)

	for content := range stream {
		fmt.Print(content)
	}

	fmt.Println("\nStreaming completed!")
}
```

#### Ruby

```ruby
ref = STREAM_TASK.run_no_wait

HATCHET.runs.subscribe_to_stream(ref.workflow_run_id) do |chunk|
  print chunk
end
```

In the examples above, this will result in the famous text below being gradually printed to the console, bit by bit.

```
Happy families are all alike; every unhappy family is unhappy in its own way.

Everything was in confusion in the Oblonskys' house. The wife had discovered that the husband was carrying on an intrigue with a French girl, who had been a governess in their family, and she had announced to her husband that she could not go on living in the same house with him.
```


  You must begin consuming the stream before any events are published. Any
  events published before a consumer is initialized will be dropped. In
  practice, this will not be an issue in most cases, but adding a short sleep
  before beginning streaming results back can help.


## Streaming to a Web Application

It's common to want to stream events out of a Hatchet task and back to the frontend of your application, for consumption by an end user. As mentioned before, some clear cases where this is useful would be for streaming back progress of some long-running task for a customer to monitor, or streaming back the results of an LLM call.

In both cases, we recommend using your application's backend as a proxy for the stream, where you would subscribe to the stream of events from Hatchet, and then stream events through to the frontend as they're received by the backend.

#### Python

For example, with FastAPI, you'd do the following:

```python
hatchet = Hatchet()
app = FastAPI()


@app.get("/stream")
async def stream() -> StreamingResponse:
    ref = await stream_task.aio_run(wait_for_result=False)

    return StreamingResponse(
        hatchet.runs.subscribe_to_stream(ref.workflow_run_id), media_type="text/plain"
    )
```

#### Typescript

For example, with NextJS backend-as-frontend, you'd do the following:

```typescript
export async function GET(): Promise {
  try {
    const ref = await streamingTask.runNoWait({});
    const workflowRunId = await ref.getWorkflowRunId();

    const stream = Readable.from(hatchet.runs.subscribeToStream(workflowRunId));

    return new Response(Readable.toWeb(stream), {
      headers: {
        'Content-Type': 'text/plain',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      },
    });
  } catch (error) {
    return new Response('Internal Server Error', { status: 500 });
  }
}
```

#### Go

For example, with Go's built-in HTTP server, you'd do the following:

```go
func main() {
	client, err := hatchet.NewClient()
	if err != nil {
		log.Fatalf("Failed to create Hatchet client: %v", err)
	}

	streamingWorkflow := shared.StreamingWorkflow(client)

	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()

		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		workflowRun, err := streamingWorkflow.RunNoWait(ctx, shared.StreamTaskInput{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		stream := client.Runs().SubscribeToStream(ctx, workflowRun.RunId)

		flusher, _ := w.(http.Flusher)
		for content := range stream {
			fmt.Fprint(w, content)
			if flusher != nil {
				flusher.Flush()
			}
		}
	})

	server := &http.Server{
		Addr:         ":8000",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		log.Println("Failed to start server:", err)
	}
}
```

#### Ruby


Then, assuming you run the server on port `8000`, running `curl -N http://localhost:8000/stream` would result in the text streaming back to your console from Hatchet through your FastAPI proxy.
