# Slack Webhooks

Slack has several different ways to send data to your app — slash commands, interactive components (buttons, modals, etc.), and event subscriptions. They each have different payload formats and authentication mechanisms, which makes the setup a bit more involved than other webhook integrations. This guide walks through each one.

> **Info:** Slack's different interaction modes use different content types. Event
>   subscriptions send JSON, but slash commands and interactive components send
>   form-encoded data. Hatchet handles both, but it's good to be aware of the
>   difference when writing your CEL expressions and task logic.

## Slack App Setup

Before configuring anything in Hatchet, you'll need a Slack app. If you don't already have one:

1. Go to [api.slack.com/apps](https://api.slack.com/apps) and click **Create New App**. See [Slack's getting started guide](https://api.slack.com/quickstart) if this is your first time.
2. Choose **From scratch**, give it a name, and select your workspace.
3. Once created, go to **Basic Information** and note the **Signing Secret** — you'll need this for Hatchet. See [Slack's signing secret docs](https://api.slack.com/authentication/verifying-requests-from-slack) for more on how request verification works.

## Event Subscriptions

Event subscriptions are what Slack uses to notify your app about things happening in the workspace — messages being posted, channels being created, users joining, and so on.


### Create the webhook in Hatchet

Field, Value

**Name**, `slack-events`
**Source**, Slack
**Event Key Expression**, `'slack:event:' + input.event.type`
**Secret**, Your Slack app's signing secret

Copy the generated URL.

### Enable Event Subscriptions in Slack

In your Slack app settings, go to [**Event Subscriptions**](https://api.slack.com/events), toggle it on, and paste the Hatchet webhook URL into the **Request URL** field.

> **Info:** Slack will send a challenge request to verify the URL. Hatchet handles this
>   automatically — you should see a green checkmark confirming the URL is
>   verified.

Then, under **Subscribe to bot events**, add the events you want to listen for (e.g., `message.channels`, `app_mention`, `member_joined_channel`).

### Write a task

#### Python

```python
class SlackEvent(BaseModel):
    type: str
    user: str
    text: str
    channel: str


class SlackEventInput(BaseModel):
    event: SlackEvent


class SlackEventOutput(BaseModel):
    handled: bool


@hatchet.task(
    input_validator=SlackEventInput,
    on_events=["slack:event:app_mention"],
)
def handle_slack_mention(input: SlackEventInput, ctx: Context) -> SlackEventOutput:
    print(
        f"Mentioned by {input.event.user} in {input.event.channel}: {input.event.text}"
    )
    return SlackEventOutput(handled=True)
```

#### Typescript

```typescript
type SlackEventInput = {
  event: {
    type: string;
    user: string;
    text: string;
    channel: string;
  };
};

export const handleSlackMention = hatchet.task({
  name: 'handle-slack-mention',
  on: {
    event: 'slack:event:app_mention',
  },
  fn: async (input: SlackEventInput, ctx) => {
    const { user, text, channel } = input.event;
    ctx.logger.info(`Mentioned by ${user} in ${channel}: ${text}`);
    return { handled: true };
  },
});
```

#### Go

```go
type SlackEventInput struct {
	Event struct {
		Type    string `json:"type"`
		User    string `json:"user"`
		Text    string `json:"text"`
		Channel string `json:"channel"`
	} `json:"event"`
}

slackMention := client.NewStandaloneTask(
	"handle-slack-mention",
	func(ctx hatchet.Context, input SlackEventInput) (*struct {
		Handled bool `json:"handled"`
	}, error) {
		fmt.Printf("Mentioned by %s in %s: %s\n", input.Event.User, input.Event.Channel, input.Event.Text)
		return &struct {
			Handled bool `json:"handled"`
		}{Handled: true}, nil
	},
	hatchet.WithWorkflowEvents("slack:event:app_mention"),
)
```

#### Ruby

```ruby
HANDLE_SLACK_MENTION = HATCHET.task(
  name: "handle-slack-mention",
  on_events: ["slack:event:app_mention"]
) do |input, ctx|
  event = input["event"]
  puts "Mentioned by #{event["user"]} in #{event["channel"]}: #{event["text"]}"
  { "handled" => true }
end
```


## Slash Commands

Slash commands work differently from event subscriptions. When a user types something like `/deploy production`, Slack sends a form-encoded POST to your configured URL. The payload includes the command, the text after it, the user, the channel, and a `response_url` you can use to send a response back.


### Create the webhook in Hatchet

Field, Value

**Name**, `slack-commands`
**Source**, Slack
**Event Key Expression**, `'slack:command:' + input.command`
**Secret**, Your Slack app's signing secret

Copy the generated URL.

> **Info:** Even though slash commands send form-encoded payloads, Hatchet parses them
>   into a JSON object so you can use the same `input.field` syntax in your CEL
>   expressions.

### Add the slash command in Slack

In your Slack app settings, go to [**Slash Commands**](https://api.slack.com/interactivity/slash-commands) and create a new command. Set the **Request URL** to the Hatchet webhook URL you just copied.

### Write a task

The `input.command` field includes the leading slash (e.g., `/deploy`), so your event key will look like `slack:command:/deploy`.

#### Python

```python
class SlackCommandInput(BaseModel):
    command: str
    text: str
    user_name: str
    response_url: str


class SlackCommandOutput(BaseModel):
    command: str
    args: str


@hatchet.task(
    input_validator=SlackCommandInput,
    on_events=["slack:command:/deploy"],
)
def handle_slack_command(input: SlackCommandInput, ctx: Context) -> SlackCommandOutput:
    print(f"{input.user_name} ran {input.command} {input.text}")
    return SlackCommandOutput(command=input.command, args=input.text)
```

#### Typescript

```typescript
type SlackCommandInput = {
  command: string;
  text: string;
  user_name: string;
  response_url: string;
};

export const handleSlackCommand = hatchet.task({
  name: 'handle-slack-command',
  on: {
    event: 'slack:command:/deploy',
  },
  fn: async (input: SlackCommandInput, ctx) => {
    ctx.logger.info(`${input.user_name} ran ${input.command} ${input.text}`);
    return { command: input.command, args: input.text };
  },
});
```

#### Go

```go
type SlackCommandInput struct {
	Command     string `json:"command"`
	Text        string `json:"text"`
	UserName    string `json:"user_name"`
	ResponseURL string `json:"response_url"`
}

slackCommand := client.NewStandaloneTask(
	"handle-slack-command",
	func(ctx hatchet.Context, input SlackCommandInput) (*struct {
		Command string `json:"command"`
		Args    string `json:"args"`
	}, error) {
		fmt.Printf("%s ran %s %s\n", input.UserName, input.Command, input.Text)
		return &struct {
			Command string `json:"command"`
			Args    string `json:"args"`
		}{
			Command: input.Command,
			Args:    input.Text,
		}, nil
	},
	hatchet.WithWorkflowEvents("slack:command:/deploy"),
)
```

#### Ruby

```ruby
HANDLE_SLACK_COMMAND = HATCHET.task(
  name: "handle-slack-command",
  on_events: ["slack:command:/deploy"]
) do |input, ctx|
  puts "#{input["user_name"]} ran #{input["command"]} #{input["text"]}"
  { "command" => input["command"], "args" => input["text"] }
end
```


## Interactive Components

Interactive components — buttons, menus, modals — send payloads to an **Interactivity Request URL** when a user interacts with them. These are also form-encoded, with the actual payload nested inside a `payload` field as a JSON string.


### Create the webhook in Hatchet

Field, Value

**Name**, `slack-interactions`
**Source**, Slack
**Event Key Expression**, `'slack:interaction:' + input.type`
**Secret**, Your Slack app's signing secret

### Enable Interactivity in Slack

In your Slack app settings, go to [**Interactivity & Shortcuts**](https://api.slack.com/interactivity/handling), toggle it on, and paste the Hatchet webhook URL into the **Request URL** field.

### Write a task

#### Python

```python
class SlackAction(BaseModel):
    action_id: str


class SlackUser(BaseModel):
    username: str


class SlackInteractionInput(BaseModel):
    type: str
    actions: list[SlackAction]
    user: SlackUser


class SlackInteractionOutput(BaseModel):
    action: str


@hatchet.task(
    input_validator=SlackInteractionInput,
    on_events=["slack:interaction:block_actions"],
)
def handle_slack_interaction(
    input: SlackInteractionInput, ctx: Context
) -> SlackInteractionOutput:
    action = input.actions[0]
    print(f"{input.user.username} clicked button: {action.action_id}")
    return SlackInteractionOutput(action=action.action_id)
```

#### Typescript

```typescript
type SlackInteractionInput = {
  type: string;
  actions: Array<{ action_id: string }>;
  user: { username: string };
};

export const handleSlackInteraction = hatchet.task({
  name: 'handle-slack-interaction',
  on: {
    event: 'slack:interaction:block_actions',
  },
  fn: async (input: SlackInteractionInput, ctx) => {
    const [action] = input.actions;
    ctx.logger.info(`${input.user.username} clicked button: ${action.action_id}`);
    return { action: action.action_id };
  },
});
```

#### Go

```go
type SlackInteractionInput struct {
	Type    string `json:"type"`
	Actions []struct {
		ActionID string `json:"action_id"`
	} `json:"actions"`
	User struct {
		Username string `json:"username"`
	} `json:"user"`
}

slackInteraction := client.NewStandaloneTask(
	"handle-slack-interaction",
	func(ctx hatchet.Context, input SlackInteractionInput) (*struct {
		Action string `json:"action"`
	}, error) {
		action := input.Actions[0]
		fmt.Printf("%s clicked button: %s\n", input.User.Username, action.ActionID)
		return &struct {
			Action string `json:"action"`
		}{Action: action.ActionID}, nil
	},
	hatchet.WithWorkflowEvents("slack:interaction:block_actions"),
)
```

#### Ruby

```ruby
HANDLE_SLACK_INTERACTION = HATCHET.task(
  name: "handle-slack-interaction",
  on_events: ["slack:interaction:block_actions"]
) do |input, ctx|
  action = input["actions"][0]
  puts "#{input["user"]["username"]} clicked button: #{action["action_id"]}"
  { "action" => action["action_id"] }
end
```
