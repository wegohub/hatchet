# Running Workers Locally

The Hatchet CLI provides the `hatchet worker` commands to run Hatchet workers locally for development and testing purposes.

## Setting up a hatchet.yaml file

> **Info:** If you've set up a project using `hatchet quickstart`, a `hatchet.yaml` file
>   is already created for you in the project directory.

The `hatchet worker` commands rely on a `hatchet.yaml` configuration file to define the worker settings. You can create a `hatchet.yaml` file in your project directory which resembles the following (you will need to adjust the `preCmds` and `runCmd` fields to match your project's setup):

#### Python

```yaml
dev:
  preCmds: ["poetry install"]
  runCmd: "poetry run python src/worker.py"
  files:
    - "**/*.py"
    - "!**/__pycache__/**"
    - "!**/.venv/**"
  reload: true
```

#### Typescript

```yaml
dev:
  preCmds: ["pnpm install"]
  runCmd: "pnpm start"
  files:
    - "**/*.ts"
    - "!**/node_modules/**"
  reload: true
```

#### Go

```yaml
dev:
  preCmds: ["go mod download"]
  runCmd: "go run ./cmd/worker"
  files:
    - "**/*.go"
  reload: true
```

## Running a worker

Once you have a `hatchet.yaml` file set up, you can run a worker locally using the following command:

```sh
hatchet worker dev
```

To run a worker with a specific profile, you can run:

```sh
hatchet worker dev --profile <profile-name>
```

### Disabling auto-reload

If you want to run the worker without auto-reloading on file changes, you can set the `dev.reload` field to `false` in your `hatchet.yaml` file:

```yaml
dev:
  reload: false
```

Or you can pass the `--no-reload` flag when running the worker:

```sh
hatchet worker dev --no-reload
```

### Overriding the run command

You can override the `runCmd` specified in the `hatchet.yaml` file by using the `--run-cmd` flag:

```sh
hatchet worker dev --run-cmd "npm run dev"
```
