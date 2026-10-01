#!/usr/bin/env python3
"""Run the repository's official loadtest against the isolated PG, RabbitMQ, and TiDB resources."""
import argparse
import base64
import datetime
import json
import os
from pathlib import Path
import re
import signal
import shutil
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
    parser.add_argument("--binary-dir", type=Path, help="Reuse prebuilt binaries")
    parser.add_argument("--output", type=Path, help="Save private logs and JSON results")
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
    if not env.get("DATABASE_URL") or not env.get("SERVER_MSGQUEUE_RABBITMQ_URL"):
        raise RuntimeError("DATABASE_URL and SERVER_MSGQUEUE_RABBITMQ_URL are required")
    for port in (api_port, grpc_port, engine_health, controller_health, controller_metrics, engine_metrics):
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                raise RuntimeError(f"Port {port} is already occupied")
    namespace = "tidb_e2e_" + uuid.uuid4().hex[:12]
    database = namespace
    tenant = str(uuid.uuid4())
    pg_url = urllib.parse.urlsplit(env["DATABASE_URL"])
    rabbit = urllib.parse.urlsplit(env["SERVER_MSGQUEUE_RABBITMQ_URL"])
    auth = base64.b64encode((urllib.parse.unquote(rabbit.username) + ":" + urllib.parse.unquote(rabbit.password)).encode()).decode()
    rabbit_headers = {"Authorization": "Basic " + auth, "Content-Type": "application/json"}
    dbx = {}
    for line in Path('/Users/fatcat/workspace/docker/dbx/.env').read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            dbx[key] = value.strip().strip('"').strip("'")
    tidb_user = dbx.get('TIDB_USER', 'root')
    tidb_port = dbx.get('TIDB_PORT', '4000')
    tidb_password = dbx['TIDB_PASSWORD']
    env.update(
        DATABASE_URL=urllib.parse.urlunsplit(pg_url._replace(path='/' + database)),
        DATABASE_POSTGRES_DB_NAME=database,
        DATABASE_READ_REPLICA_ENABLED='false',
        READ_REPLICA_ENABLED='false',
        DATABASE_PGBOUNCER_URL='',
        SERVER_MSGQUEUE_RABBITMQ_URL=urllib.parse.urlunsplit(rabbit._replace(path='/' + database)),
        SERVER_MSGQUEUE_PUBSUB_RABBITMQ_URL=urllib.parse.urlunsplit(rabbit._replace(path='/' + database)),
        DATABASE_TIDB_DSN=f'{tidb_user}:{tidb_password}@tcp(127.0.0.1:{tidb_port})/{database}',
        DATABASE_OLAP_BACKEND="tidb",
        DEFAULT_TENANT_ID=tenant,
        SERVER_SECURITY_CHECK_ENABLED="false",
        SERVER_SAMPLING_ENABLED="false",
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
    processes = []
    owned_pg = owned_rabbit = owned_tidb = False

    def rabbit_request(path, method, body=None):
        request = urllib.request.Request('http://127.0.0.1:15672/api/' + path,
            headers=rabbit_headers, data=None if body is None else json.dumps(body).encode(), method=method)
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status

    def tidb_sql(statement):
        return subprocess.check_output(['/opt/homebrew/opt/mysql-client/bin/mysql', '--connect-timeout=3','-N', '-h', '127.0.0.1', '-P', tidb_port,
            '-u', tidb_user, '-e', statement], env={**env, 'MYSQL_PWD': tidb_password},
            stderr=subprocess.DEVNULL,text=True,timeout=90)
    with tempfile.TemporaryDirectory(prefix="hatchet-tidb-e2e-") as directory:
        work = Path(directory)
        os.chmod(work, 0o700)
        streams = []

        def run(args, name, **kwargs):
            child_env={**env,**kwargs.pop('env_override',{})}
            with (work / f"{name}.log").open("w") as log:
                return subprocess.run(args, cwd=ROOT, env=child_env, stdout=log, stderr=log, **kwargs)

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
            subprocess.run(['docker', 'exec', 'dbx-postgres', 'psql', '-U', 'root', '-d', 'postgres',
                '-v', 'ON_ERROR_STOP=1', '-c', 'CREATE DATABASE ' + database,
                '-c', 'ALTER DATABASE ' + database + " SET TIMEZONE='UTC'"],
                check=True, stdout=subprocess.DEVNULL)
            owned_pg = True
            rabbit_request('vhosts/' + database, 'PUT', {})
            owned_rabbit = True
            rabbit_request('permissions/' + database + '/' + urllib.parse.quote(rabbit.username, safe=''),
                'PUT', {'configure': '.*', 'write': '.*', 'read': '.*'})
            tidb_sql('CREATE DATABASE ' + database)
            owned_tidb = True
            for binary, package in (
                ("api", "hatchet-api"), ("engine", "hatchet-engine"),
                ("migrate", "hatchet-migrate"), ("migrate-tidb", "hatchet-migrate-tidb"), ("admin", "hatchet-admin"),
                ("loadtest", "hatchet-loadtest"), ("worker", "hatchet-loadtest/go"),
            ):
                source = None if args.binary_dir is None else args.binary_dir.resolve() / binary
                if source is not None and source.is_file():
                    os.symlink(source,work / binary)
                else:
                    run(["go", "build", "-o", str(work / binary), "./cmd/" + package],
                        "build-" + binary, check=True)
            run([str(work / "migrate")], "migrate", check=True)
            run([str(work / "migrate-tidb"), "--tiflash"], "migrate-tidb", check=True)
            run([str(work / "admin"), "seed"], "seed", check=True,env_override={'DATABASE_OLAP_BACKEND':'postgres'})
            with (work / "token.log").open("w") as log:
                token = subprocess.run(
                    [str(work / "admin"), "token", "create", "--tenant-id", tenant,
                     "--name", namespace, "--expiresIn", "1h"],
                    cwd=ROOT, env={**env,'DATABASE_OLAP_BACKEND':'postgres'}, stdout=subprocess.PIPE, stderr=log, text=True, check=True,
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
            for _ in range(90):
                if any(p.poll() is not None for p in processes):
                    raise RuntimeError("Component exited before OLAP readiness")
                ready=subprocess.run(['docker','exec','dbx-rabbitmq','rabbitmqctl','-q','list_queues','-p',database,'name','consumers','--timeout','5'],capture_output=True,text=True,timeout=10)
                if ready.returncode==0 and any(line.split()==['olap_queue_v2','1'] for line in ready.stdout.splitlines()): break
                time.sleep(1)
            else:
                raise RuntimeError("OLAP consumer startup timed out")
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
            core_query="SELECT (SELECT COUNT(*) FROM v1_task WHERE tenant_id='"+tenant+"'),(SELECT COUNT(*) FROM v1_dag WHERE tenant_id='"+tenant+"'),(SELECT COUNT(*) FROM v1_event WHERE tenant_id='"+tenant+"')"
            core_counts=[int(n) for n in subprocess.check_output(['docker','exec','dbx-postgres','psql','-U','root','-d',database,'-At','-v','ON_ERROR_STOP=1','-c',core_query],text=True).strip().split('|')]
            scope="tenant_id=UNHEX('"+tenant.replace('-','')+"')"
            tables=['v1_tasks_olap','v1_dags_olap','v1_events_olap','v1_task_events_olap','v1_log_line','v1_otel_trace_olap','v1_payloads_olap','v1_olap_pending_updates']
            counts={table:int(tidb_sql('SELECT COUNT(*) FROM '+database+'.'+table+' WHERE '+scope).strip()) for table in tables}
            if [counts[t] for t in tables[:3]]!=core_counts or counts['v1_olap_pending_updates']!=0:
                raise RuntimeError("Core task/DAG/event counts or final pending reconciliation differ")
            for table in tables[3:7]:
                if counts[table]==0:raise RuntimeError('Expected persisted rows in '+table)
            completed_tasks=int(tidb_sql('SELECT COUNT(*) FROM '+database+'.v1_tasks_olap t JOIN '+database+'.v1_runs_olap u ON u.tenant_id=t.tenant_id AND u.external_id=t.external_id AND u.inserted_at=t.inserted_at AND u.kind=\'task\' WHERE t.'+scope+" AND u.readable_status='COMPLETED'").strip())
            if completed_tasks!=core_counts[0]:raise RuntimeError('Final task status reconciliation differs')
            if args.output:
                args.output.mkdir(parents=True,exist_ok=True,mode=0o700)
                for stream in streams:
                    stream.flush()
                for log in work.glob("*.log"):
                    target=args.output / log.name
                    shutil.copy2(log,target)
                    target.chmod(0o600)
                (args.output / "result.json").write_text(json.dumps({"events":len(events),"completed_runs":len(rows),"completed_tasks":completed_tasks,"stored_counts":counts,"core_counts":core_counts,"duplicate_logs":len(logs),"scenarios":["default","batch","durable","dag","dag-shapes","dag-nested"],"security_check_enabled":False},indent=2)+"\n")
            print("Official loadtest passed: 18 events, 51 completed workflow runs")
            print("API checks passed: event associations, status counts, payloads, task events, traces, duplicate logs")
            print("Owned test data and processes are being cleaned")
        except Exception:
            # Logs can contain SDK authentication data; never stream them automatically.
            destination = Path(tempfile.mkdtemp(prefix="hatchet-tidb-e2e-failure-"))
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
            if owned_rabbit:
                rabbit_request('vhosts/' + database, 'DELETE')
            if owned_tidb:
                tidb_sql('DROP DATABASE ' + database)
            if owned_pg:
                subprocess.run(['docker', 'exec', 'dbx-postgres', 'psql', '-U', 'root', '-d', 'postgres',
                    '-v', 'ON_ERROR_STOP=1', '-c', 'DROP DATABASE ' + database + ' WITH (FORCE)'],
                    check=True, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
