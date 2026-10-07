McpUrl,
  CursorDeeplinkButton,
  CursorMcpConfig,
  ClaudeCodeCommand,
  CursorTabLabel,
  ClaudeCodeTabLabel,
  OtherAgentsTabLabel,
} from "@/components/McpSetup";
import Keywords from "@/components/Keywords";

# Using Coding Agents

Hatchet is designed to work well with AI coding agents. This page covers how to give your agent access to Hatchet documentation and step-by-step skills for common CLI operations.

> **Info:** **Prerequisite:** The `hatchet skills install` and `hatchet docs install`
>   commands require the Hatchet CLI. See the [CLI reference](/cli) for
>   installation instructions.

## Agent Skills

Agent skills are reference documents that teach AI coding agents how to use the Hatchet CLI — triggering workflows, starting workers, debugging runs, and more.

Run the following command in your project root to install the skill package:

```bash copy
hatchet skills install
```

This creates a `skills/hatchet-cli/` directory with step-by-step reference files and appends a section to your project's `AGENTS.md` (and `CLAUDE.md`) pointing agents to the right file for each task.

**Install to a custom directory:**

```bash copy
hatchet skills install --dir ./my-project
```

After installation, commit the `skills/` directory and `AGENTS.md` to version control so all agents working in the repo benefit automatically.

### Available references

Reference, When to use

`references/setup-cli.md`, Installing the CLI, creating or listing profiles
`references/local-dev-embedded.md`, Running Hatchet locally with no token, server, or Docker
`references/start-worker.md`, Starting a dev worker for local development
`references/trigger-and-watch.md`, Triggering a workflow and polling for completion
`references/debug-run.md`, Diagnosing a failed, stuck, or unexpected run
`references/replay-run.md`, Re-running a previous workflow with same or new input

## Local Development with Embedded Mode

For local development, an agent does not need an API token or Docker. [Embedded mode](/v1/embedded) runs a full Hatchet engine locally, so an agent can start a worker, trigger tasks, and inspect results without any account setup.

Each SDK has an embedded entry point: Python uses `Hatchet.from_embedded()`, TypeScript uses `HatchetEmbeddedClient.init()` from `@hatchet-dev/typescript-sdk/v1/embedded`, and Go uses `hatchet.WithEmbedded()` with a blank import of `github.com/hatchet-dev/hatchet-embedded`:

#### Python

```python
hatchet = Hatchet.from_embedded()
```

#### Typescript

```typescript
const hatchet = await HatchetEmbeddedClient.init();
```

#### Go

```go
import (
	hatchet "github.com/hatchet-dev/hatchet/sdks/go"

	_ "github.com/hatchet-dev/hatchet-embedded"
)
```

See [Embedded Mode](/v1/embedded) for configuration options, stopping the engine, and serving the dashboard.

## MCP Server

Hatchet documentation is available as an **MCP (Model Context Protocol) server**, so AI coding assistants like Cursor and Claude Code can search and reference Hatchet docs directly.

MCP endpoint: 


  
  

    #### Hatchet CLI

```bash copy
        hatchet docs install claude-code
```

If `claude` is on your PATH, this runs the command automatically. Otherwise it prints it for you to copy.

#### Command

Run this command in your terminal:


  
  

    For any AI tool that supports [llms.txt](https://llmstxt.org/), Hatchet docs are available at:

    | Resource | URL |
    |----------|-----|
    | **llms.txt** (index) | [docs.hatchet.run/llms.txt](https://docs.hatchet.run/llms.txt) |
    | **llms-full.txt** (all docs) | [docs.hatchet.run/llms-full.txt](https://docs.hatchet.run/llms-full.txt) |
    | **Per-page markdown** | `docs.hatchet.run/llms/{section}/{page}.md` |
    | **MCP endpoint** |  |

    Every documentation page also includes a `<link rel="alternate" type="text/markdown">` header
    pointing to its markdown version, and a "View as Markdown" link at the top of the page.

  


## llms.txt

For any AI tool that supports [llms.txt](https://llmstxt.org/), Hatchet docs are available at:

Resource, URL

**llms.txt** (index), [docs.hatchet.run/llms.txt](https://docs.hatchet.run/llms.txt)
**llms-full.txt** (all docs), [docs.hatchet.run/llms-full.txt](https://docs.hatchet.run/llms-full.txt)
**Per-page markdown**, `docs.hatchet.run/llms/{section}/{page}.md`
**MCP endpoint**, 

Every documentation page also includes a `<link rel="alternate" type="text/markdown">` header
pointing to its markdown version, and a "View as Markdown" link at the top of the page.
