# GitHub Webhooks

GitHub can send webhooks for repository events — pushes, pull requests, issues, releases, and so on. This guide walks through connecting GitHub webhooks to Hatchet.

## Setup


### Create the webhook in Hatchet

In the Hatchet dashboard, go to **Webhooks** and create a new webhook with the following settings:

Field, Value

**Name**, `github` (or whatever you'd like)
**Source**, GitHub
**Event Key Expression**, `'github:' + headers['x-github-event'] + ':' + input.action`
**Secret**, A secret string of your choosing (you'll use the same one in GitHub)

A quick note on the event key expression: GitHub sends the event type (like `pull_request` or `issues`) in the `x-github-event` header, and the specific action (like `opened` or `closed`) in the payload's `action` field. The expression above combines them to produce keys like `github:pull_request:opened`.

Not all GitHub events have an `action` field, though. Push events, for instance, don't. If you want to handle events that might not have an `action`, you could use a simpler expression like `'github:' + headers['x-github-event']` and handle action-level routing in your task logic instead. Or you could create two separate webhooks — one for action-based events and one for action-less events.

Once you've created the webhook, copy the URL.

### Configure the webhook in GitHub

Go to your repository (or organization) settings, find **Webhooks**, and add a new webhook. See [GitHub's webhook docs](https://docs.github.com/en/webhooks/using-webhooks/creating-webhooks) for the full walkthrough.

1. **Payload URL**: Paste the Hatchet webhook URL.
2. **Content type**: Select `application/json`.
3. **Secret**: Enter the same secret you used when creating the webhook in Hatchet.
4. **Events**: Choose "Let me select individual events" and pick the ones you care about, or select "Send me everything" if you prefer.

> **Warning:** Make sure you set the content type to `application/json`. GitHub defaults to
>   `application/x-www-form-urlencoded`, which won't work with Hatchet's JSON
>   payload parsing.

### Write a task that listens for GitHub events

Here's an example that triggers when a pull request is opened:

#### Python

```python
class GitHubPullRequest(BaseModel):
    number: int
    title: str


class GitHubRepository(BaseModel):
    full_name: str


class GitHubPRInput(BaseModel):
    action: str
    pull_request: GitHubPullRequest
    repository: GitHubRepository


class GitHubPROutput(BaseModel):
    repo: str
    pr: int


@hatchet.task(
    input_validator=GitHubPRInput,
    on_events=["github:pull_request:opened"],
)
def handle_github_pr(input: GitHubPRInput, ctx: Context) -> GitHubPROutput:
    repo = input.repository.full_name
    pr_number = input.pull_request.number
    title = input.pull_request.title
    print(f"PR #{pr_number} opened on {repo}: {title}")
    return GitHubPROutput(repo=repo, pr=pr_number)
```

#### Typescript

```typescript
type GitHubPRInput = {
  action: string;
  pull_request: {
    number: number;
    title: string;
  };
  repository: {
    full_name: string;
  };
};

export const handleGitHubPR = hatchet.task({
  name: 'handle-github-pr',
  on: {
    event: 'github:pull_request:opened',
  },
  fn: async (input: GitHubPRInput, ctx) => {
    const repo = input.repository.full_name;
    const prNumber = input.pull_request.number;
    const { title } = input.pull_request;
    ctx.logger.info(`PR #${prNumber} opened on ${repo}: ${title}`);
    return { repo, pr: prNumber };
  },
});
```

#### Go

```go
type GitHubPRInput struct {
	Action      string `json:"action"`
	PullRequest struct {
		Number int    `json:"number"`
		Title  string `json:"title"`
	} `json:"pull_request"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

githubPR := client.NewStandaloneTask(
	"handle-github-pr",
	func(ctx hatchet.Context, input GitHubPRInput) (*struct {
		Repo string `json:"repo"`
		PR   int    `json:"pr"`
	}, error) {
		fmt.Printf("PR #%d opened on %s: %s\n", input.PullRequest.Number, input.Repository.FullName, input.PullRequest.Title)
		return &struct {
			Repo string `json:"repo"`
			PR   int    `json:"pr"`
		}{
			Repo: input.Repository.FullName,
			PR:   input.PullRequest.Number,
		}, nil
	},
	hatchet.WithWorkflowEvents("github:pull_request:opened"),
)
```

#### Ruby

```ruby
HANDLE_GITHUB_PR = HATCHET.task(
  name: "handle-github-pr",
  on_events: ["github:pull_request:opened"]
) do |input, ctx|
  repo = input["repository"]["full_name"]
  pr_number = input["pull_request"]["number"]
  title = input["pull_request"]["title"]
  puts "PR ##{pr_number} opened on #{repo}: #{title}"
  { "repo" => repo, "pr" => pr_number }
end
```

### Test it

After saving the webhook in GitHub, GitHub will send a `ping` event to verify the connection. You can also use the "Redeliver" button in GitHub's webhook settings to replay past events, or just open a PR to trigger a real event.
