#!/usr/bin/env python3
"""Compare console read visibility using isolated PG databases and RabbitMQ vhosts."""
import argparse
import base64
import concurrent.futures
import datetime
import http.server
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
BUCKETS = [.005, .01, .025, .05, .1, .2, .3, .5, .75, 1, 1.5, 2, 3, 5, 10, 30]


def loaded_env():
    raw = subprocess.check_output(['bash', '-c', 'set +x; set -a; source .env; env -0'], cwd=ROOT)
    return dict(x.decode().split('=', 1) for x in raw.split(b'\0') if b'=' in x)


def request(url, headers=None, body=None, method=None):
    req = urllib.request.Request(url, headers=headers or {}, data=None if body is None else json.dumps(body).encode(), method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as res:
            content = res.read()
            return res.status, json.loads(content) if content else None
    except urllib.error.HTTPError as exc:
        return exc.code, None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--samples', type=int, default=50)
    parser.add_argument('--warmup', type=int, default=5)
    parser.add_argument('--interval', type=float, default=1)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--scrape-hold', type=float, default=20)
    parser.add_argument('--backends', default='postgres,clickhouse', help='comma-separated: postgres,clickhouse,tidb,tidb-v1')
    parser.add_argument('--poll-ms', type=float, default=20, help='detail polling interval in milliseconds')
    parser.add_argument('--prometheus-targets', type=Path, default=Path('/Users/fatcat/workspace/docker/dbx/prometheus/hatchet-targets.json'))
    parser.add_argument('--no-prometheus', action='store_true', help='Do not edit external Prometheus configuration')
    parser.add_argument('--binary-dir', type=Path, help='Use prebuilt current binaries')
    parser.add_argument('--tidb-v1-binary-dir', type=Path, help='Preserved v1 API, engine and migration binaries')
    parser.add_argument('--max-tidb-memory-percent', type=float, default=90)
    args = parser.parse_args()
    backends = args.backends.split(',')
    if not backends or len(set(backends)) != len(backends) or any(value not in ('postgres', 'clickhouse', 'tidb', 'tidb-v1') for value in backends):
        parser.error('invalid backend list')
    if args.samples < 1 or args.warmup < 0 or args.interval < 0 or args.scrape_hold < 0 or args.poll_ms <= 0 or not 0 < args.max_tidb_memory_percent < 100:
        parser.error('invalid sample count or interval')
    if 'tidb-v1' in backends and args.tidb_v1_binary_dir is None:
        parser.error('tidb-v1 requires --tidb-v1-binary-dir')
    env_base = loaded_env()
    for port in [8081, 7071, 8741, 8742, 19101, 19102, 19103]:
        with socket.socket() as sock:
            if sock.connect_ex(('127.0.0.1', port)) == 0:
                raise RuntimeError(f'Port {port} is occupied')
    args.output.mkdir(parents=True, exist_ok=True)
    os.chmod(args.output, 0o700)
    run_id = uuid.uuid4().hex[:10]
    samples = {backend: [] for backend in backends}
    guard = threading.Lock()
    pg_url = urllib.parse.urlsplit(env_base['DATABASE_URL'])
    rabbit = urllib.parse.urlsplit(env_base['SERVER_MSGQUEUE_RABBITMQ_URL'])
    auth = base64.b64encode((urllib.parse.unquote(rabbit.username) + ':' + urllib.parse.unquote(rabbit.password)).encode()).decode()
    rabbit_headers = {'Authorization': 'Basic ' + auth, 'Content-Type': 'application/json'}
    owned_dbs, owned_tidb_dbs, owned_vhosts, children, streams = [], [], [], [], []
    monitor_stop, memory_pressure = threading.Event(), threading.Event()
    active_backend = None

    def monitor_memory():
        while not monitor_stop.wait(2):
            if active_backend not in ('tidb', 'tidb-v1'):
                continue
            try:
                content = subprocess.check_output(['docker', 'stats', '--no-stream', '--format', '{{json .}}', 'dbx-tidb', 'dbx-tikv', 'dbx-tiflash'], text=True, timeout=15)
                for line in content.splitlines():
                    item = json.loads(line)
                    if float(item['MemPerc'].rstrip('%')) >= args.max_tidb_memory_percent:
                        item.update(backend=active_backend, observed_at=time.time())
                        (args.output / 'memory-pressure.json').write_text(json.dumps(item, indent=2))
                        memory_pressure.set()
                        return
            except Exception:
                memory_pressure.set()
                return

    monitor = threading.Thread(target=monitor_memory, daemon=True)
    monitor.start()
    tidb_admin = None
    if any(b in backends for b in ('tidb', 'tidb-v1')):
        dbx = {}
        for line in Path('/Users/fatcat/workspace/docker/dbx/.env').read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                dbx[key] = value.strip().strip('"').strip("'")
        tidb_admin = {'user': dbx.get('TIDB_USER', 'root'), 'password': dbx['TIDB_PASSWORD'], 'port': dbx.get('TIDB_PORT', '4000')}

    def tidb_sql(statement):
        client = '/opt/homebrew/opt/mysql-client/bin/mysql'
        env = {**env_base, 'MYSQL_PWD': tidb_admin['password']}
        subprocess.run([client, '-h', '127.0.0.1', '-P', tidb_admin['port'], '-u', tidb_admin['user'], '-e', statement], env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != '/metrics':
                self.send_error(404)
                return
            lines = ['# HELP hatchet_olap_visibility_probe_duration_seconds Client-observed delay, including detail read latency and polling resolution.', '# TYPE hatchet_olap_visibility_probe_duration_seconds histogram']
            with guard:
                for backend, values in samples.items():
                    for phase in ['trigger_request', 'return_to_visible', 'trigger_to_visible']:
                        timings = [value[phase] for value in values]
                        labels = f'backend="{backend}",phase="{phase}"'
                        for bound in BUCKETS:
                            lines.append(f'hatchet_olap_visibility_probe_duration_seconds_bucket{{{labels},le="{bound}"}} {sum(t <= bound for t in timings)}')
                        lines.extend([f'hatchet_olap_visibility_probe_duration_seconds_bucket{{{labels},le="+Inf"}} {len(timings)}', f'hatchet_olap_visibility_probe_duration_seconds_count{{{labels}}} {len(timings)}', f'hatchet_olap_visibility_probe_duration_seconds_sum{{{labels}}} {sum(timings)}'])
            body = ('\n'.join(lines) + '\n').encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4')
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    exporter = http.server.ThreadingHTTPServer(('0.0.0.0', 19103), Handler)
    threading.Thread(target=exporter.serve_forever, daemon=True).start()
    added_targets = []
    if not args.no_prometheus:
        targets = json.loads(args.prometheus_targets.read_text())
        added_targets = [target for target in [
            {'targets': ['host.docker.internal:19101'], 'labels': {'component': 'latency-test-controller'}},
            {'targets': ['host.docker.internal:19103'], 'labels': {'component': 'latency-probe'}},
        ] if target not in targets]
        args.prometheus_targets.write_text(json.dumps(targets + added_targets))

    def interrupt(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)

    def stop():
        for child in reversed(children):
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        for child in children:
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
        children.clear()
        for stream in streams:
            stream.close()
        streams.clear()

    try:
        with tempfile.TemporaryDirectory(prefix='hatchet-latency-build-') as directory:
            work = Path(directory)

            def run(command, env, name, timeout=180):
                log_path = args.output / (name + '.log')
                with log_path.open('w') as log:
                    log_path.chmod(0o600)
                    proc = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=log, timeout=timeout)
                if proc.returncode:
                    raise RuntimeError(f'{name} failed; inspect private log {log_path}')

            def executable(binary, backend=None):
                root = args.tidb_v1_binary_dir if backend == 'tidb-v1' and binary in ('api', 'engine', 'migrate-tidb') else (args.binary_dir or work)
                return str(root / binary)

            def boot(binary, env, name, overrides=None, backend=None):
                log_path = args.output / (name + '.log')
                log = log_path.open('w')
                log_path.chmod(0o600)
                streams.append(log)
                children.append(subprocess.Popen([executable(binary, backend)], cwd=ROOT, env={**env, **(overrides or {})}, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True))

            packages = [('api', 'hatchet-api'), ('engine', 'hatchet-engine'), ('admin', 'hatchet-admin'), ('migrate', 'hatchet-migrate'), ('worker', 'hatchet-loadtest/go')]
            if 'clickhouse' in backends:
                packages.append(('migrate-ck', 'hatchet-migrate-clickhouse'))
            if any(b in backends for b in ('tidb', 'tidb-v1')):
                packages.append(('migrate-tidb', 'hatchet-migrate-tidb'))
            for binary, package in packages:
                if args.binary_dir is None:
                    run(['go', 'build', '-o', str(work / binary), './cmd/' + package], env_base, 'build-' + binary)
                elif not os.access(args.binary_dir / binary, os.X_OK):
                    raise RuntimeError(f'Missing executable {binary} in --binary-dir')
            for backend in samples:
                active_backend = backend
                if memory_pressure.is_set():
                    raise RuntimeError('Visibility probe stopped by the shared-container memory guard')
                database = 'hatchet_latency_' + run_id + '_' + {'postgres': 'pg', 'clickhouse': 'ck', 'tidb': 'tidb', 'tidb-v1': 'tidb_v1'}[backend]
                tenant = str(uuid.uuid4())
                vhost = database
                subprocess.run(['docker', 'exec', 'dbx-postgres', 'psql', '-U', 'root', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-c', 'CREATE DATABASE ' + database, '-c', 'ALTER DATABASE ' + database + " SET TIMEZONE='UTC'"], check=True, stdout=subprocess.DEVNULL)
                owned_dbs.append(database)
                if request('http://localhost:15672/api/vhosts/' + vhost, rabbit_headers, {}, 'PUT')[0] not in [201, 204]:
                    raise RuntimeError('RabbitMQ vhost creation failed')
                owned_vhosts.append(vhost)
                if request('http://localhost:15672/api/permissions/' + vhost + '/' + urllib.parse.quote(rabbit.username, safe=''), rabbit_headers, {'configure': '.*', 'write': '.*', 'read': '.*'}, 'PUT')[0] not in [201, 204]:
                    raise RuntimeError('RabbitMQ vhost permissions failed')
                env = dict(env_base)
                env.update(DATABASE_URL=urllib.parse.urlunsplit(pg_url._replace(path='/' + database)), DATABASE_POSTGRES_DB_NAME=database, DATABASE_OLAP_BACKEND='postgres', DEFAULT_TENANT_ID=tenant, DATABASE_READ_REPLICA_ENABLED='false', READ_REPLICA_ENABLED='false', DATABASE_PGBOUNCER_URL='', SERVER_MSGQUEUE_RABBITMQ_URL=urllib.parse.urlunsplit(rabbit._replace(path='/' + vhost)), SERVER_MSGQUEUE_PUBSUB_RABBITMQ_URL=urllib.parse.urlunsplit(rabbit._replace(path='/' + vhost)), SERVER_SECURITY_CHECK_ENABLED='false', SERVER_SAMPLING_ENABLED='false', SERVER_PORT='8081', SERVER_URL='http://localhost:8081', SERVER_GRPC_PORT='7071', SERVER_GRPC_BROADCAST_ADDRESS='127.0.0.1:7071', SERVER_INTERNAL_CLIENT_INTERNAL_GRPC_BROADCAST_ADDRESS='127.0.0.1:7071', SERVER_HEALTHCHECK_PORT='8741', HATCHET_CLIENT_HOST_PORT='127.0.0.1:7071', HATCHET_CLIENT_SERVER_URL='http://localhost:8081', HATCHET_CLIENT_TLS_STRATEGY='none', HATCHET_CLIENT_NAMESPACE='', HATCHET_LOADTEST_WORKFLOW_NAME='visibility-probe', HATCHET_LOADTEST_WORKER_NAME='visibility-probe-worker', HATCHET_LOADTEST_FAILURE_RATE='0', SERVER_PROMETHEUS_ADDRESS=':19102')
                run([executable('migrate')], env, backend + '-migrate')
                run([executable('admin'), 'seed'], env, backend + '-seed')
                if backend == 'clickhouse':
                    env.update(DATABASE_CLICKHOUSE_DATABASE=database, DATABASE_CLICKHOUSE_KEEPER_ROOT='/hatchet/latency/' + database)
                    run([executable('migrate-ck')], env, backend + '-migrate-ck')
                if backend in ('tidb', 'tidb-v1'):
                    tidb_sql('CREATE DATABASE ' + database)
                    owned_tidb_dbs.append(database)
                    env['DATABASE_TIDB_DSN'] = f"{tidb_admin['user']}:{tidb_admin['password']}@tcp(127.0.0.1:{tidb_admin['port']})/{database}"
                    run([executable('migrate-tidb', backend), '--tiflash'], env, backend + '-migrate-tidb')
                env['DATABASE_OLAP_BACKEND'] = 'tidb' if backend == 'tidb-v1' else backend
                env['SERVER_PROMETHEUS_ENABLED'] = 'true'
                token = subprocess.run([executable('admin'), 'token', 'create', '--tenant-id', tenant, '--name', 'latency-probe', '--expiresIn', '1h'], cwd=ROOT, env={**env,'DATABASE_OLAP_BACKEND':'postgres'}, capture_output=True, text=True, check=True)
                tokens = re.findall(r'eyJ[\w-]+\.[\w-]+\.[\w-]+', token.stdout)
                if len(tokens) != 1:
                    raise RuntimeError('Token creation did not return exactly one token')
                env['HATCHET_CLIENT_TOKEN'] = tokens[0]
                headers = {'Authorization': 'Bearer ' + tokens[0], 'Content-Type': 'application/json'}
                boot('api', env, backend + '-api', backend=backend)
                boot('engine', env, backend + '-engine', {'SERVER_SERVICES': 'scheduler grpc-api'}, backend=backend)
                boot('engine', env, backend + '-controller', {'SERVER_SERVICES': 'controllers', 'SERVER_HEALTHCHECK_PORT': '8742', 'SERVER_PROMETHEUS_ADDRESS': ':19101'}, backend=backend)
                for _ in range(60):
                    if any(child.poll() is not None for child in children):
                        raise RuntimeError('Component exited during startup')
                    with socket.socket() as sock:
                        if sock.connect_ex(('127.0.0.1', 8081)) == 0:
                            break
                    time.sleep(1)
                else:
                    raise RuntimeError('API startup timed out')
                boot('worker', env, backend + '-worker')
                for _ in range(90):
                    if any(child.poll() is not None for child in children):
                        raise RuntimeError('Component exited before OLAP consumer readiness')
                    ready = subprocess.run(['docker','exec','dbx-rabbitmq','rabbitmqctl','-q','list_queues','-p',vhost,'name','consumers','--timeout','5'],capture_output=True,text=True,timeout=10)
                    if ready.returncode == 0 and any(line.split()==['olap_queue_v2','1'] for line in ready.stdout.splitlines()):
                        break
                    time.sleep(1)
                else:
                    raise RuntimeError('OLAP consumer startup timed out')
                time.sleep(6)
                base = 'http://localhost:8081/api/v1/stable'
                print(backend + ': testing console trigger/detail visibility', flush=True)
                records = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    for index in range(args.warmup + args.samples):
                        if memory_pressure.is_set():
                            raise RuntimeError('Visibility probe stopped by the shared-container memory guard')
                        started = time.monotonic()
                        status, value = request(base + '/tenants/' + tenant + '/workflow-runs/trigger', headers, {'workflowName': 'visibility-probe', 'input': {'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'id': index, 'payload': 'a' * 1024}, 'additionalMetadata': {}, 'return_only_id': True})
                        returned = time.monotonic()
                        if status != 200:
                            raise RuntimeError(f'Trigger returned HTTP {status}')
                        run_id_value = value['run']['metadata']['id']
                        misses = 0
                        while time.monotonic() - returned < 30:
                            futures = [pool.submit(request, base + path + run_id_value, headers) for path in ['/tasks/', '/workflow-runs/']]
                            results = [future.result() for future in futures]
                            if any(status not in [200, 404] for status, _ in results):
                                raise RuntimeError('Unexpected detail response')
                            if all(status == 200 for status, _ in results):
                                visible = time.monotonic()
                                break
                            misses += 1
                            time.sleep(args.poll_ms / 1000)
                        else:
                            raise RuntimeError('Run was not readable within 30 seconds')
                        record = {'run_id': run_id_value, 'trigger_request': returned - started, 'return_to_visible': visible - returned, 'trigger_to_visible': visible - started, 'initial404s': misses, 'warmup': index < args.warmup}
                        records.append(record)
                        if not record['warmup']:
                            with guard:
                                samples[backend].append(record)
                            if len(samples[backend]) % 200 == 0:
                                (args.output / (backend + '-partial-samples.json')).write_text(json.dumps(records, indent=2))
                                print(f'{backend}: {len(samples[backend])}/{args.samples} formal visibility samples', flush=True)
                        time.sleep(max(0, args.interval - (time.monotonic() - started)))
                (args.output / (backend + '-samples.json')).write_text(json.dumps(records, indent=2))
                # Wait for terminal completion independently of the first-readable measurement.
                for _ in range(120):
                    rows = []
                    for offset in range(0, len(records), 1000):
                        status, data = request(base + '/tenants/' + tenant + '/workflow-runs?limit=1000&only_tasks=false&since=2026-01-01T00:00:00Z&offset=' + str(offset), headers)
                        if status != 200:
                            raise RuntimeError(f'Run list returned HTTP {status}')
                        rows.extend(data.get('rows', []))
                    if len(rows) == len(records) and all(row['status'] == 'COMPLETED' for row in rows):
                        break
                    time.sleep(1)
                else:
                    raise RuntimeError('Final completion reconciliation failed')
                for endpoint in ['controller', 'engine']:
                    port = 19101 if endpoint == 'controller' else 19102
                    content = urllib.request.urlopen(f'http://localhost:{port}/metrics').read()
                    (args.output / (backend + '-' + endpoint + '.prom')).write_bytes(content)
                (args.output / (backend + '-samples.json')).write_text(json.dumps(records, indent=2))
                summary = {}
                for phase in ['trigger_request', 'return_to_visible', 'trigger_to_visible']:
                    timings = sorted(value[phase] * 1000 for value in samples[backend])
                    summary[phase] = {name: timings[min(len(timings) - 1, math.ceil(q * len(timings)) - 1)] for name, q in [('p50_ms', .5), ('p95_ms', .95), ('p99_ms', .99)]}
                    summary[phase].update(mean_ms=sum(timings) / len(timings), max_ms=max(timings))
                summary.update(samples=args.samples, completed=len(rows), poll_ms=args.poll_ms, visibility_criterion='both_task_and_workflow_details', immediate404_fraction=sum(x['initial404s'] > 0 for x in samples[backend]) / args.samples)
                (args.output / (backend + '-summary.json')).write_text(json.dumps(summary, indent=2))
                print(backend + ': ' + json.dumps(summary), flush=True)
                time.sleep(args.scrape_hold)
                stop()
            time.sleep(args.scrape_hold)
    finally:
        monitor_stop.set()
        monitor.join(timeout=20)
        stop()
        exporter.shutdown()
        exporter.server_close()
        if not args.no_prometheus:
            targets = json.loads(args.prometheus_targets.read_text())
            args.prometheus_targets.write_text(json.dumps([target for target in targets if target not in added_targets]))
        for vhost in owned_vhosts:
            request('http://localhost:15672/api/vhosts/' + vhost, rabbit_headers, method='DELETE')
        for database in owned_dbs:
            subprocess.run(['docker', 'exec', 'dbx-postgres', 'psql', '-U', 'root', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1', '-c', 'DROP DATABASE ' + database + ' WITH (FORCE)'], check=True, stdout=subprocess.DEVNULL)
        for database in owned_tidb_dbs:
            tidb_sql('DROP DATABASE ' + database)
        print('Owned test processes, PG/TiDB databases and RabbitMQ vhosts cleaned; CK data retained', flush=True)


if __name__ == '__main__':
    main()
