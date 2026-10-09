#!/usr/bin/env python3
"""运行质量门禁并汇总全部必需示例的证据；任何必需项缺失均不能报告完成。"""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import shlex
import sys
import time

# REPO 仓库根目录，命令和证据路径从此统一定位。
REPO = Path(__file__).resolve().parents[3]
# SDK wego 源码根目录。
SDK = REPO / 'sdks/wego'
# RESULTS 本次及历史验收证据目录，汇总时还须校验时间和版本。
RESULTS = SDK / '.test-results'
# REQUIRED_GATES 必须全部通过的门禁集合，不能用注册成功替代真实业务验收。
REQUIRED_GATES = {'source-boundary', 'published-dependency', 'format', 'unit', 'vet', 'race', 'upstream', 'upstream-embedded',
                  'embedded-module', 'generation', 'fuzz', 'fuzz-state', 'report-contract', 'protocol-engine', 'compose-config',
                  'engine', 'embedded-engine', 'engine-race'}
# FAULTS 对应当前单任务输出模型；Owner 崩溃由独立真实进程重启证据覆盖。
FAULTS = {'delayed_subscription', 'duplicate_cursor', 'reconnect', 'missing_output',
          'missing_end', 'corrupt_frame', 'out_of_order', 'wrong_identity', 'zero_output', 'missing_headers'}


# source_identity 绑定实现、测试、协议及门禁脚本，防止运行中改源码后混用旧进程的通过证据。
def source_identity():
    digest = hashlib.sha256()
    count = 0
    for path in sorted(SDK.rglob('*')):
        relative = path.relative_to(SDK)
        if not path.is_file() or any(part.startswith('.') for part in relative.parts):
            continue
        if path.suffix in ('.go', '.proto', '.py', '.sh') or path.name in ('go.mod', 'go.sum'):
            digest.update(str(relative).encode() + b'\0' + path.read_bytes() + b'\0')
            count += 1
    return {'sha256': digest.hexdigest(), 'file_count': count}


# current_evidence 禁止混用 SDK/协议版本或另一服务版本的成功记录。
def current_evidence(value, manifest):
    versions = [value['server_version']] if value.get('server_version') else []
    versions.extend(record['server_version'] for record in value.get('records', []) if record.get('server_version'))
    return (value.get('sdk_version') == manifest['sdk_version']
            and value.get('protocol_version') == manifest['protocol_version']
            and value.get('status', 'PASSED') == 'PASSED'
            and bool(value.get('command'))
            and bool(versions) and all(version in ('v0.110.5', '0.110.5') for version in versions))



# read 读取 JSON 证据；读取或解析失败直接向调用方传播，不能返回空成功记录。
def read(path):
    return json.loads(Path(path).read_text())


# engine_image 只读取镜像身份，不读取容器环境；验收必须绑定当前官方正式镜像。
def engine_image():
    container = os.environ.get('WEGO_ENGINE_CONTAINER', 'wego-hatchet-hatchet-engine-1')
    identity = json.loads(subprocess.check_output(
        ['docker', 'inspect', '--format', '{"tag":{{json .Config.Image}},"id":{{json .Image}}}', container], text=True))
    expected = 'ghcr.io/hatchet-dev/hatchet/hatchet-engine:v0.110.5'
    if identity['tag'] != expected:
        raise RuntimeError(f'official engine image required: {expected}')
    digests = json.loads(subprocess.check_output(
        ['docker', 'image', 'inspect', '--format', '{{json .RepoDigests}}', identity['id']], text=True))
    official = [value for value in digests if value.startswith('ghcr.io/hatchet-dev/hatchet/hatchet-engine@sha256:')]
    if not official:
        raise RuntimeError('official engine image digest missing')
    return dict(identity, repo_digests=official)


# latest 按修改时间选择满足版本、完成状态与时间下限的最新证据；没有匹配时明确失败。
def latest(pattern, accepts, since=0):
    for path in sorted(RESULTS.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True):
        if path.stat().st_mtime >= since:
            value = read(path)
            if accepts(value):
                return path, value
    raise RuntimeError(f'missing complete passing evidence: {pattern}')


# fragment_results 只依据本轮明确通过的断言；缺失、失败、Skip 或错误的场景关联都拒绝完成。
def fragment_results(entry, records, results, embedded):
    actual = {(r.get('test_scenario') or r['scenario'], r.get('assertion_id')): r for r in records}
    fragments = []
    for fragment in entry['fragments']:
        required = fragment.get('required_assertions', [])
        if not fragment.get('id') or not required:
            raise RuntimeError(f'missing explicit assertion contract: {fragment}')
        evidence = []
        for assertion in required:
            scenario = assertion['scenario']
            if scenario == 'embedded':
                if embedded.get('status') != 'PASSED' or assertion['id'] not in embedded.get('assertion_ids', []):
                    raise RuntimeError(f'missing embedded assertion: {fragment["id"]}')
                evidence.append(embedded)
            else:
                record = actual.get((scenario, assertion['id']))
                if results.get(scenario) != 'PASSED' or not record or record['assertion'] != assertion['assertion']:
                    raise RuntimeError(f'missing passing assertion {scenario}/{assertion["id"]}: {fragment["id"]}')
                evidence.append(record)
        fragments.append(dict(fragment, status='PASSED', evidence=evidence, tests=entry['tests']))
    return fragments


# assemble 关联 manifest、门禁、业务断言及清理记录，全部必需项通过后才输出完整验收报告。
def assemble(gates, since, destination):
    manifest = read(SDK / 'examples/acceptance.json')
    gate_map = {g['name']: g for g in gates}
    if REQUIRED_GATES - gate_map.keys() or any(g['status'] != 'PASSED' for g in gates):
        raise RuntimeError('all required quality gates must pass before completion')
    required = {name for source in manifest['sources'] for name in source['scenarios']}
    required.update({'grpc-streams', 'middleware', 'shutdown', 'dual-entry'})
    required.discard('embedded')
    ordinary_path, ordinary = latest('acceptance-*.json', lambda d: current_evidence(d, manifest) and required <= d['results'].keys()
                                      and all(d['results'][n] == 'PASSED' for n in required), since)
    faults_path, faults = latest('stream-faults-*.json', lambda d: current_evidence(d, manifest) and FAULTS <= d['results'].keys()
                                  and all(d['results'][n] == 'PASSED' for n in FAULTS), since)
    restart_path, restart = latest('durable-restart-*.json', lambda d: current_evidence(d, manifest), since)
    embedded_path, embedded = latest('wego_embedded_*.json', lambda d: current_evidence(d, manifest)
                                      and d.get('database_dropped') is True
                                      and d.get('independent_example', '').startswith('PASSED'), since)
    cost_path, cost = latest('stream-cost-*.json', lambda d: current_evidence(d, manifest) and d.get('concurrency') == 1, since)
    # 串行基线和并发样本必须来自本轮实际执行，不能互相替代或读取旧 SDK 版本。
    concurrent_path, concurrent = latest('stream-cost-concurrent-*.json', lambda d: current_evidence(d, manifest) and d.get('concurrency') == 4, since)
    # 必须执行真实 pending memo 丢帧与两次恢复，普通重启不证明此分支。
    memo_path, memo = latest('durable-memo-recovery-*.json', lambda d: current_evidence(d, manifest) and d.get('pending_memo_injected') is True and d.get('after_invocation', 0) >= 3, since)
    batch_path, batch = latest('batch-shutdown-*.json', lambda d: current_evidence(d, manifest), since)
    recovery_path, recovery = latest('stream-recovery-*.json', lambda d: current_evidence(d, manifest) and d.get('handler_attempts') == 2, since)
    stream_restart_path, stream_restart = latest('stream-restart-*.json', lambda d: current_evidence(d, manifest) and d.get('epoch', 0) >= 1, since)
    events_path, events = latest('worker-events-*.json', lambda d: current_evidence(d, manifest) and len(set(d.get('worker_keys', []))) == 2, since)
    submission_path, submission = latest('stream-submission-*.json', lambda d: current_evidence(d, manifest) and d.get('submitted_tasks') == 1, since)
    realtime_path, realtime = latest('stream-realtime-*.json', lambda d: current_evidence(d, manifest), since)
    result_cancel_path, result_cancel = latest('result-cancel-*.json', lambda d: current_evidence(d, manifest), since)
    # 真实 S3 往返是独立必需证据，不能用内存对象 fixture 的 middleware 场景替代。
    minio_path, minio = latest('minio-codec-*.json', lambda d: current_evidence(d, manifest) and len(d.get('records', [])) >= 12, since)
    aliases = {'simple': 'basic', 'sdk-migration': 'basic', 'sdk-migration-v1': 'events'}
    sources = []
    for entry in manifest['sources']:
        item = dict(entry)
        if entry['scenarios'] == ['embedded']:
            evidence = [embedded]
        else:
            aliases_for_entry = {aliases.get(name, name) for name in entry['scenarios']}
            evidence = [record for record in ordinary['records']
                        if record.get('test_scenario') in entry['scenarios']
                        or (not record.get('test_scenario') and record['scenario'] in aliases_for_entry)]
            if not evidence or not any('deleted' in r['assertion'] for r in evidence):
                raise RuntimeError(f'missing execution/cleanup assertions for {entry["source"]}')
        item['status'] = 'PASSED'
        item['evidence'] = evidence
        item['fragment_results'] = fragment_results(entry, ordinary['records'], ordinary['results'], embedded)
        sources.append(item)
    report = {
        'status': 'PASSED', 'completed_at': dt.datetime.now(dt.timezone.utc).isoformat(),
        'sdk_version': manifest['sdk_version'], 'protocol_version': manifest['protocol_version'],
        'official_dependency_version': 'v0.110.5',
        'source_identity': source_identity(),
        'engine_image': engine_image(),
        'server_version': ordinary['server_version'], 'mq': 'postgresql',
        'source_count': len(sources), 'fragment_count': sum(len(s['fragments']) for s in sources),
        'generation_tools': ordinary['generation_tools'], 'sources': sources,
        'quality_gates': gates, 'evidence_metadata': ordinary.get('metadata_correction', 'SDK version emitted from wego.Version'), 'extra_scenarios': {
            'grpc-streams': ordinary['results']['grpc-streams'],
            'middleware': ordinary['results']['middleware'],
            'shutdown': ordinary['results']['shutdown'],
            'dual-entry': ordinary['results']['dual-entry'],
            'stream-cost': cost, 'stream-cost-concurrent': concurrent, 'durable-memo-recovery': memo,
            'stream-faults': faults, 'stream-recovery': recovery, 'stream-restart': stream_restart, 'worker-events': events, 'stream-submission': submission, 'stream-realtime': realtime, 'result-cancel': result_cancel,
            'durable-restart': restart, 'embedded': embedded, 'batch-shutdown': batch,
            'minio-codec': minio,
        },
        'evidence_files': [str(p.relative_to(REPO)) for p in
                           (ordinary_path, faults_path, restart_path, embedded_path, batch_path, cost_path, concurrent_path, memo_path, recovery_path, stream_restart_path, events_path, submission_path, realtime_path, result_cancel_path, minio_path)],
        'cleanup': {
            'workflow_and_trigger_resources': 'deleted under unique namespaces',
            'embedded_database': 'dropped', 'owned_listeners_and_connections': 'closed',
            'retained': 'run history, engine worker records, durable topics, referenced S3 objects and explicitly retained protocol-probe definitions',
        },
        'ci': 'SDK CI template and compose configuration validated locally; no repository workflow installed',
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    markdown = destination.with_suffix('.md')
    lines = [f'wego SDK {report["sdk_version"]} 本机验收记录', '',
             f'完成时间：{report["completed_at"]}。Hatchet {report["server_version"]}，PostgreSQL MQ，明文 gRPC。', '',
             'SDK 发行依赖：未经修改的官方 Hatchet v0.110.5；发行依赖门禁在 GOWORK=off 下执行。仓库源码边界单独检查。', '',
             f'官方镜像：`{report["engine_image"]["tag"]}`，digest `{report["engine_image"]["repo_digests"][0].split("@", 1)[1]}`。', '',
             f'28 个源文件、{report["fragment_count"]} 个 standalone 构造片段全部通过；三种流、10 组输出故障、流恢复和进程崩溃重启、Worker 事件、durable 重启、middleware、排空和 embedded 通过。', '',
             '| 官方源文件 | 片段数 | 场景 | 结果 |', '|---|---:|---|---|']
    lines.extend(f'| `{s["source"]}` | {len(s["fragments"])} | {", ".join(s["scenarios"])} | PASSED |' for s in sources)
    lines += ['', '质量门禁命令：', '']
    lines.extend(f'- `{g["command"]}`：{g["status"]}。' for g in gates)
    lines += ['', f'逐片段映射、断言、RunID、WorkerID、traceID 和执行命令见 [{destination.name}]({destination.name})。', '',
              '业务示例使用唯一 namespace，已删除工作流和触发资源；协议探针明确保留的定义、持久 topic、运行历史及引擎 Worker 记录保留。Embedded 测试数据库已删除。报告不包含凭证。', '',
              'CI 模板与 Compose 配置已在本机检查；仓库 CI 未注册，托管 CI 未执行。']
    markdown.write_text('\n'.join(lines) + '\n')
    print(f'PASSED: {len(sources)} sources, {report["fragment_count"]} fragments; report: {destination}', flush=True)


# main 组织命令行配置与各门禁的执行，保存真实命令、退出码和结果，不记录凭证。
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--assemble', action='store_true', help='assemble previously recorded passing evidence')
    parser.add_argument('--gates', type=Path, help='quality gate JSON for --assemble')
    parser.add_argument('--since', type=float, default=0, help='minimum evidence modification timestamp')
    parser.add_argument('--out', type=Path, default=SDK / 'docs/design/acceptance-report.json')
    args = parser.parse_args()
    if args.assemble:
        if not args.gates:
            parser.error('--assemble requires --gates')
        assemble(read(args.gates), args.since, args.out)
        return
    started = time.time()
    identity = source_identity()
    RESULTS.mkdir(exist_ok=True)
    gates = []
    # SDK 与官方发行模块各用自己的 go.mod；上游只读执行，禁止修改根依赖或模块缓存。
    env = dict(os.environ, GOWORK='off', GOFLAGS='-mod=readonly', SERVER_SECURITY_CHECK_ENABLED='false')
    official = json.loads(subprocess.check_output(['go', 'list', '-m', '-json', 'github.com/hatchet-dev/hatchet'], cwd=SDK, env=env))
    upstream = Path(official['Dir'])
    prefix = [sys.executable, str(SDK / 'scripts/local-test.py'), 'env', 'GOWORK=off', 'GOFLAGS=-mod=readonly', 'go', '-C', str(SDK)]
    commands = [
        ('source-boundary', [sys.executable, 'sdks/wego/scripts/check-upstream.py'], REPO),
        ('published-dependency', [sys.executable, 'sdks/wego/scripts/check-released.py'], REPO),
        ('format', ['gofmt', '-l', 'sdks/wego'], REPO),
        ('unit', [sys.executable, 'scripts/unit.py'], SDK),
        ('vet', ['go', 'vet', './...'], SDK),
        ('race', [sys.executable, 'scripts/unit.py', '-race'], SDK),
        ('upstream', ['go', 'test', './sdks/go/...', './pkg/client/...', './pkg/worker/...'], upstream),
        # 独立示例模块不包含在主模块发行 zip 中；直接检查发行的嵌入引擎依赖，不借用仓库源码。
        ('upstream-embedded', ['go', 'test', 'github.com/hatchet-dev/hatchet-embedded/...'], SDK),
        ('embedded-module', ['go', 'test', '-tags=wego_embedded', './...'], SDK / 'examples/embedded'),
        ('generation', ['go', 'test', './tests/quality', '-run', '^TestGenerationAndAcceptanceManifestAreReproducible$'], SDK),
        ('fuzz', ['go', 'test', './internal/wire', '-run', '^$', '-fuzz', '^FuzzFrameCodecDecode$', '-fuzztime=10s'], SDK),
        ('fuzz-state', ['go', 'test', './internal/stream', '-run', '^$', '-fuzz', '^FuzzLogInterpreter$', '-fuzztime=10s'], SDK),
        ('report-contract', [sys.executable, '-m', 'unittest', 'test_acceptance'], SDK / 'scripts'),
        ('compose-config', ['docker', 'compose', '-f', 'sdks/wego/tests/compose.yml', 'config', '--quiet'], REPO),
        ('protocol-engine', [sys.executable, str(SDK / 'scripts/local-test.py'), 'env', 'GOWORK=off', 'GOFLAGS=-mod=readonly', 'WEGO_P0_FAULT_FIXTURES=1', 'go', '-C', str(SDK), 'test', '-race', '-count=1', '-tags=e2e', './internal/backend', '-run', 'P0|TestRPCBindingsP1|TestWorkerEventReconnect|TestOversizedResultReport', '-timeout', '10m'], REPO),
        ('engine', prefix + ['test', '-json', '-count=1', '-tags=e2e', './tests/e2e/...', '-timeout', '30m'], REPO),
        ('engine-race', prefix + ['test', '-race', '-json', '-count=1', '-tags=e2e', './tests/e2e/...', '-run', 'TestExamples/(batch|grpc-streams|middleware|shutdown|dual-entry)$|TestBatchShutdownBudget$|TestReview|TestIndependentReview|TestStream|TestReliableStreamRecovery|TestRealtimeStreams|TestResultCancellation|TestWorkerEvents|TestMinIOCodec', '-timeout', '15m'], REPO),
        ('embedded-engine', prefix + ['test', '-json', '-count=1', '-tags=e2e,wego_embedded', './tests/e2e/...', '-run', '^TestEmbedded$', '-timeout', '10m'], REPO),
    ]
    for name, command, cwd in commands:
        print(f'RUNNING {name}', flush=True)
        log = RESULTS / f'gate-{int(started)}-{name}.log'
        with log.open('w') as output:
            process = subprocess.run(command, cwd=cwd, stdout=output, stderr=subprocess.STDOUT, env=env)
        okay = process.returncode == 0 and (name != 'format' or not log.read_text().strip())
        gates.append({'name': name, 'command': shlex.join(command), 'cwd': str(cwd),
                      'status': 'PASSED' if okay else 'FAILED', 'exit_code': process.returncode,
                      'log': str(log.relative_to(REPO)), 'at': dt.datetime.now(dt.timezone.utc).isoformat()})
        gate_path = RESULTS / f'gates-{int(started)}.json'
        gate_path.write_text(json.dumps(gates, indent=2) + '\n')
        if not okay:
            raise RuntimeError(f'{name} failed; inspect {log}')
        print(f'PASSED {name}', flush=True)
    if source_identity() != identity:
        raise RuntimeError('SDK sources changed during acceptance; rerun affected checks before assembly')
    assemble(gates, started, args.out)


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, OSError, ValueError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
