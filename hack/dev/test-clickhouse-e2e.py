#!/usr/bin/env python3
"""Run the repository's official loadtest against the configured PG/CK infrastructure."""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request
import urllib.parse
import uuid

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port-offset", type=int, default=0,
                        help="Offset all owned service ports to coexist with local services")
    args = parser.parse_args()
    if not 0 <= args.port_offset <= 50000:
        parser.error("port offset must be between 0 and 50000")
    api_port, grpc_port = 8080 + args.port_offset, 7070 + args.port_offset
    engine_health, controller_health = 8733 + args.port_offset, 8734 + args.port_offset
    controller_metrics, engine_metrics = 9091 + args.port_offset, 9092 + args.port_offset
    api_url = f"http://127.0.0.1:{api_port}"
    grpc_address = f"127.0.0.1:{grpc_port}"
    def interrupt(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)

    raw = subprocess.check_output(
        ["bash", "-c", "set +x; set -a; if [[ -f .env ]]; then source .env; fi; env -0"],
        cwd=ROOT,
    )
    env = dict(entry.decode().split("=", 1) for entry in raw.split(b"\0") if b"=" in entry)
    for name in ("DATABASE_CLICKHOUSE_ADDRESSES", "DATABASE_CLICKHOUSE_KEEPER_ADDRESSES"):
        if not env.get(name):
            raise RuntimeError(f"{name} is required")
    for port in (api_port, grpc_port, engine_health, controller_health, controller_metrics, engine_metrics):
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                raise RuntimeError(f"Port {port} is already occupied")
    namespace = "ck_e2e_" + uuid.uuid4().hex[:12]
    # A new namespace prevents historical PG-only runs from entering CK assertions.
    env.update(
        DATABASE_OLAP_BACKEND="clickhouse",
        SERVER_SECURITY_CHECK_ENABLED="false",
        SERVER_OBSERVABILITY_ENABLED="true",
        HATCHET_CLIENT_NAMESPACE=namespace,
        HATCHET_CLIENT_HOST_PORT=grpc_address,
        HATCHET_CLIENT_SERVER_URL=api_url,
        SERVER_PORT=str(api_port),
        SERVER_URL=api_url,
        SERVER_GRPC_PORT=str(grpc_port),
        SERVER_GRPC_BROADCAST_ADDRESS=grpc_address,
        SERVER_INTERNAL_CLIENT_INTERNAL_GRPC_BROADCAST_ADDRESS=grpc_address,
        HATCHET_CLIENT_TLS_STRATEGY="none",
    )
    tenant = env.get("HATCHET_CK_E2E_TENANT_ID", "707d0855-80ab-4e1f-a156-f1c4546cbf52")
    uuid.UUID(tenant)
    processes = []
    with tempfile.TemporaryDirectory(prefix="hatchet-ck-e2e-") as directory:
        work = Path(directory)
        os.chmod(work, 0o700)
        streams = []

        def run(args, name, **kwargs):
            with (work / f"{name}.log").open("w") as log:
                return subprocess.run(args, cwd=ROOT, env=env, stdout=log, stderr=log, **kwargs)

        def boot(binary, name, overrides=None):
            log = (work / f"{name}.log").open("w")
            streams.append(log)
            child = subprocess.Popen(
                [str(work / binary)], cwd=ROOT, env={**env, **(overrides or {})},
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
            )
            processes.append(child)

        def api(path):
            request = urllib.request.Request(
                api_url + path,
                headers={"Authorization": "Bearer " + env["HATCHET_CLIENT_TOKEN"]},
            )
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)

        try:
            for binary, package in (
                ("api", "hatchet-api"), ("engine", "hatchet-engine"),
                ("migrate", "hatchet-migrate-clickhouse"), ("admin", "hatchet-admin"),
                ("loadtest", "hatchet-loadtest"), ("worker", "hatchet-loadtest/go"),
            ):
                run(["go", "build", "-o", str(work / binary), "./cmd/" + package],
                    "build-" + binary, check=True)
            run([str(work / "migrate")], "migrate", check=True)
            with (work / "token.log").open("w") as log:
                token = subprocess.run(
                    [str(work / "admin"), "token", "create", "--tenant-id", tenant,
                     "--name", namespace, "--expiresIn", "1h"],
                    cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=log, text=True, check=True,
                )
            matches = re.findall(r"eyJ[\w-]+\.[\w-]+\.[\w-]+", token.stdout)
            if len(matches) != 1:
                raise RuntimeError("Expected exactly one API token")
            env["HATCHET_CLIENT_TOKEN"] = matches[0]
            boot("api", "api")
            boot("engine", "engine", {"SERVER_SERVICES": "scheduler grpc-api",
                "SERVER_HEALTHCHECK_PORT": str(engine_health), "SERVER_PROMETHEUS_ADDRESS": f":{engine_metrics}"})
            boot("engine", "controller", {"SERVER_SERVICES": "controllers",
                "SERVER_HEALTHCHECK_PORT": str(controller_health), "SERVER_PROMETHEUS_ADDRESS": f":{controller_metrics}"})
            for _ in range(60):
                if any(p.poll() is not None for p in processes):
                    raise RuntimeError("A component exited during startup")
                try:
                    socket.create_connection(("127.0.0.1", api_port), timeout=1).close()
                    break
                except OSError:
                    time.sleep(1)
            else:
                raise RuntimeError("API startup timed out")
            since = datetime.datetime.now(datetime.timezone.utc).isoformat()
            boot("worker", "worker")
            result = run(
                [str(work / "loadtest"), "loadtest", "--externalWorker", "--select",
                 "default,batch,durable,dag,dag-shapes,dag-nested", "--events", "1",
                 "--duration", "3s", "--wait", "70s", "--registrationTimeout", "30s",
                 "--averageDurationThreshold", "10s", "--payloadSize", "1kb"],
                "loadtest", timeout=240,
            )
            if result.returncode:
                raise RuntimeError(f"Official loadtest failed with exit {result.returncode}")
            path = (f"/api/v1/stable/tenants/{tenant}/workflow-runs?limit=1000&only_tasks=false"
                    f"&since={urllib.parse.quote(since)}")
            for _ in range(60):
                rows = [row for row in api(path)["rows"]
                        if row["workflowName"].startswith(namespace)]
                if len(rows) == 51 and all(row["status"] == "COMPLETED" for row in rows):
                    break
                time.sleep(2)
            else:
                raise RuntimeError("Expected 51 completed workflow runs, including spawned children")
            run_ids = {row["metadata"]["id"] for row in rows}
            events = api(f"/api/v1/stable/tenants/{tenant}/events?limit=1000"
                         f"&since={urllib.parse.quote(since)}")["rows"]
            events = [event for event in events if event["key"].startswith(namespace)]
            if len(events) != 18:
                raise RuntimeError("Expected 18 user events")
            for event in events:
                triggered = event.get("triggeredRuns") or []
                if not triggered or any(run["workflowRunId"] not in run_ids for run in triggered):
                    raise RuntimeError("Event-to-run associations are incomplete")
                summary = event["workflowRunSummary"]
                if summary["succeeded"] != len(triggered) or any(
                    summary[key] for key in ("queued", "running", "failed", "cancelled")
                ):
                    raise RuntimeError("Event status counts do not match completed runs")
                if not event.get("payload"):
                    raise RuntimeError("Event payload was not returned")
            task = next(row for row in rows
                        if row["workflowName"].endswith("load-test-0"))
            task_id = task["taskExternalId"]
            detail = api(f"/api/v1/stable/workflow-runs/{task_id}")
            if not detail["run"].get("input") or not detail["run"].get("output"):
                raise RuntimeError("Task input or output was not returned")
            if not api(f"/api/v1/stable/tasks/{task_id}/task-events?limit=100&offset=0"):
                raise RuntimeError("Task events were not returned")
            if not api(f"/api/v1/stable/tenants/{tenant}/traces?run_external_id={task_id}")["rows"]:
                raise RuntimeError("Task traces were not returned")
            probe = work / "log-probe.go"
            probe.write_text('''package main
import("context";"os";"time";"github.com/hatchet-dev/hatchet/pkg/client")
func main(){ctx,cancel:=context.WithTimeout(context.Background(),15*time.Second);defer cancel()
c,err:=client.New();if err!=nil{panic(err)};level:="INFO";retry:=int32(0)
for range 2{if err=c.Event().PutLog(ctx,os.Getenv("PROBE_TASK_ID"),os.Getenv("PROBE_LOG_MESSAGE"),&level,&retry);err!=nil{panic(err)}}}
''')
            env.update(PROBE_TASK_ID=task_id, PROBE_LOG_MESSAGE=namespace + " 重复日志")
            run(["go", "run", str(probe)], "log-probe", timeout=90, check=True)
            logs = api(f"/api/v1/stable/tasks/{task_id}/logs?limit=100"
                       f"&search={urllib.parse.quote(env['PROBE_LOG_MESSAGE'])}")["rows"]
            if len(logs) != 2 or any(log["message"] != env["PROBE_LOG_MESSAGE"] for log in logs):
                raise RuntimeError("Two legitimate duplicate logs were not preserved")
            print("Official loadtest passed: 18 events, 51 completed workflow runs")
            print("API checks passed: event associations, status counts, payloads, task events, traces, duplicate logs")
            print("Test data retained in the configured CK database; all owned processes are stopping")
        except Exception:
            # Logs can contain SDK authentication data; never stream them automatically.
            destination = Path(tempfile.mkdtemp(prefix="hatchet-ck-e2e-failure-"))
            os.chmod(destination, 0o700)
            for stream in streams:
                stream.flush()
            for log in work.glob("*.log"):
                target = destination / log.name
                target.write_bytes(log.read_bytes())
                target.chmod(0o600)
            print(f"Private failure logs: {destination}")
            raise
        finally:
            for child in reversed(processes):
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            for child in processes:
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
            for stream in streams:
                stream.close()


if __name__ == "__main__":
    main()
