#!/usr/bin/env bash

set -euo pipefail
# Environment files may contain credentials, including when invoked with bash -x.
set +x

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

component="${1:-}"
case "$component" in
  api|controller|engine|web) ;;
  *) echo "Usage: $0 {api|controller|engine|web}" >&2; exit 2 ;;
esac
shift

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

case "$component" in
  api)
    command=(go run ./cmd/hatchet-api "$@")
    ;;
  controller)
    export SERVER_SERVICES=controllers
    export SERVER_HEALTHCHECK_PORT="${CONTROLLER_HEALTHCHECK_PORT:-8734}"
    export SERVER_PROMETHEUS_ADDRESS="${CONTROLLER_PROMETHEUS_ADDRESS:-:9091}"
    # SDK capability checks require the semantic version maintained by the engine.
    command=(go run ./cmd/hatchet-engine "$@")
    ;;
  engine)
    export SERVER_SERVICES="scheduler grpc-api"
    export SERVER_HEALTHCHECK_PORT="${ENGINE_HEALTHCHECK_PORT:-${SERVER_HEALTHCHECK_PORT:-8733}}"
    export SERVER_PROMETHEUS_ADDRESS="${ENGINE_PROMETHEUS_ADDRESS:-${SERVER_PROMETHEUS_ADDRESS:-:9090}}"
    command=(go run ./cmd/hatchet-engine "$@")
    ;;
  web)
    if [[ ! -f frontend/app/src/lib/generated/docs/index.ts ]]; then
      (cd frontend/snippets && python3 -X utf8 -c 'from generate import write_doc_index_to_app; write_doc_index_to_app()')
    fi
    cd frontend/app
    command=(pnpm run dev --port "${WEB_PORT:-5173}" --strictPort "$@")
    ;;
esac

component_pid=""
cleanup() {
  local exit_code=$?
  trap - EXIT
  trap '' INT TERM

  if [[ -n "$component_pid" ]]; then
    # go run and pnpm create descendants; signal the owned group, not just the launcher.
    kill -TERM -- "-$component_pid" 2>/dev/null || true
    # Task force-kills cancelled commands after two seconds. Finish group cleanup first.
    local attempt
    for ((attempt = 0; attempt < 10; attempt++)); do
      if ! kill -0 -- "-$component_pid" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
    kill -KILL -- "-$component_pid" 2>/dev/null || true
    wait "$component_pid" 2>/dev/null || true
  fi
  exit "$exit_code"
}
trap cleanup EXIT
signal_exit_code=0
# Defer cancellation until the child PID is recorded so startup cannot orphan it.
trap 'signal_exit_code=130' INT
trap 'signal_exit_code=143' TERM

# Bash job control assigns a separate process group on both macOS and Linux.
set -m
"${command[@]}" < /dev/null &
component_pid=$!
set +m
trap 'exit 130' INT
trap 'exit 143' TERM
if ((signal_exit_code != 0)); then
  exit "$signal_exit_code"
fi

wait "$component_pid"
# A foreground server must keep running; some entrypoints return zero on startup errors.
echo "$component exited unexpectedly" >&2
exit 1
