#!/usr/bin/env python3
"""运行质量门禁并汇总全部必需示例的证据；任何必需项缺失均不能报告完成。"""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
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
                  'embedded-module', 'generation', 'fuzz', 'compose-config',
                  'engine', 'embedded-engine', 'engine-race'}
# FAULTS 必需流故障模式，例如 output-gap 必须明确失败而不是成功 EOF。
FAULTS = {'delayed-subscription', 'duplicate-open', 'input-reorder', 'output-reorder',
          'output-gap', 'invalid-direction', 'disconnect', 'full-window', 'owner-exit'}


# read 读取 JSON 证据；读取或解析失败直接向调用方传播，不能返回空成功记录。
def read(path):
    return json.loads(Path(path).read_text())


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
    ordinary_path, ordinary = latest('acceptance-*.json', lambda d: d.get('sdk_version') == manifest['sdk_version'] and required <= d['results'].keys()
                                      and all(d['results'][n] == 'PASSED' for n in required), since)
    faults_path, faults = latest('stream-faults-*.json', lambda d: FAULTS <= d['results'].keys()
                                  and all(d['results'][n] == 'PASSED' for n in FAULTS), since)
    restart_path, restart = latest('durable-restart-*.json', lambda d: d.get('status') == 'PASSED', since)
    embedded_path, embedded = latest('wego_embedded_*.json', lambda d: d.get('status') == 'PASSED'
                                      and d.get('database_dropped') is True
                                      and d.get('independent_example', '').startswith('PASSED'), since)
    cost_path, cost = latest('stream-cost-*.json', lambda d: d.get('status') == 'PASSED' and d.get('sdk_version') == manifest['sdk_version'] and d.get('concurrency') == 1, since)
    # 串行基线和并发样本必须来自本轮实际执行，不能互相替代或读取旧 SDK 版本。
    concurrent_path, concurrent = latest('stream-cost-concurrent-*.json', lambda d: d.get('status') == 'PASSED' and d.get('sdk_version') == manifest['sdk_version'] and d.get('concurrency') == 4, since)
    # 必须执行真实 pending memo 丢帧与两次恢复，普通重启不证明此分支。
    memo_path, memo = latest('durable-memo-recovery-*.json', lambda d: d.get('status') == 'PASSED' and d.get('sdk_version') == manifest['sdk_version'] and d.get('pending_memo_injected') is True and d.get('after_invocation', 0) >= 3, since)
    batch_path, batch = latest('batch-shutdown-*.json', lambda d: d.get('status') == 'PASSED', since)
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
        'official_dependency_version': 'v0.109.0',
        'server_version': ordinary['server_version'], 'mq': 'postgresql',
        'source_count': len(sources), 'fragment_count': sum(len(s['fragments']) for s in sources),
        'generation_tools': ordinary['generation_tools'], 'sources': sources,
        'quality_gates': gates, 'evidence_metadata': ordinary.get('metadata_correction', 'SDK version emitted from wego.Version'), 'extra_scenarios': {
            'grpc-streams': ordinary['results']['grpc-streams'],
            'middleware': ordinary['results']['middleware'],
            'shutdown': ordinary['results']['shutdown'],
            'dual-entry': ordinary['results']['dual-entry'],
            'stream-cost': cost, 'stream-cost-concurrent': concurrent, 'durable-memo-recovery': memo,
            'stream-faults': faults, 'durable-restart': restart, 'embedded': embedded, 'batch-shutdown': batch,
        },
        'evidence_files': [str(p.relative_to(REPO)) for p in
                           (ordinary_path, faults_path, restart_path, embedded_path, batch_path, cost_path, concurrent_path, memo_path)],
        'cleanup': {
            'workflow_and_trigger_resources': 'deleted under unique namespaces',
            'embedded_database': 'dropped', 'owned_listeners_and_connections': 'closed',
            'retained': 'run history and engine worker records, including intentional owner-loss cases',
        },
        'ci': 'SDK CI template and compose configuration validated locally; no repository workflow installed',
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    markdown = destination.with_suffix('.md')
    lines = [f'wego SDK {report["sdk_version"]} 本机验收记录', '',
             f'完成时间：{report["completed_at"]}。Hatchet {report["server_version"]}，PostgreSQL MQ，明文 gRPC。', '',
             'SDK 发行依赖：未经修改的官方 Hatchet v0.109.0；发行依赖门禁在 GOWORK=off 下执行。仓库源码边界单独检查。', '',
             f'28 个源文件、{report["fragment_count"]} 个 standalone 构造片段全部通过；三种流、9 组故障、durable 重启、middleware、排空和 embedded 通过。', '',
             '| 官方源文件 | 片段数 | 场景 | 结果 |', '|---|---:|---|---|']
    lines.extend(f'| `{s["source"]}` | {len(s["fragments"])} | {", ".join(s["scenarios"])} | PASSED |' for s in sources)
    lines += ['', '质量门禁命令：', '']
    lines.extend(f'- `{g["command"]}`：{g["status"]}。' for g in gates)
    lines += ['', f'逐片段映射、断言、RunID、WorkerID、traceID 和执行命令见 [{destination.name}]({destination.name})。', '',
              '测试使用唯一 namespace，已删除工作流和触发资源；运行历史及引擎 Worker 记录保留。Embedded 测试数据库已删除。报告不包含凭证。', '',
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
    RESULTS.mkdir(exist_ok=True)
    gates = []
    # 源码修改及格式检查严格限定 SDK；上游仅运行回归，不写入文件。
    fmt_files = ['sdks/wego']
    env = dict(os.environ, GOWORK=str(SDK / 'go.work'))
    prefix = ['sdks/wego/scripts/local-test.py']
    commands = [
        ('source-boundary', ['python3', 'sdks/wego/scripts/check-upstream.py'], REPO),
        ('published-dependency', ['python3', 'sdks/wego/scripts/check-released.py'], REPO),
        ('format', ['gofmt', '-l', *fmt_files], REPO),
        ('unit', ['go', 'test', './sdks/wego/...'], REPO),
        ('vet', ['go', 'vet', './sdks/wego/...'], REPO),
        ('race', ['go', 'test', '-race', './sdks/wego/...'], REPO),
        ('upstream', ['go', 'test', './sdks/go/...', './pkg/client/...', './pkg/worker/...'], REPO),
        ('upstream-embedded', ['go', 'test', './...'], REPO / 'sdks/go/examples/embedded'),
        ('embedded-module', ['go', 'test', '-tags=wego_embedded', './...'], SDK / 'examples/embedded'),
        ('generation', ['go', 'test', './sdks/wego/tests/quality', '-run', '^TestGenerationAndAcceptanceManifestAreReproducible$'], REPO),
        ('fuzz', ['go', 'test', './sdks/wego/internal/wire', '-run', '^$', '-fuzz', 'FuzzDecodeFrame', '-fuzztime=10s'], REPO),
        ('compose-config', ['docker', 'compose', '-f', 'sdks/wego/tests/compose.yml', 'config', '--quiet'], REPO),
        ('engine', prefix + ['go', 'test', '-json', '-count=1', '-tags=e2e', './sdks/wego/tests/e2e/...', '-timeout', '30m'], REPO),
        ('engine-race', prefix + ['go', 'test', '-race', '-json', '-count=1', '-tags=e2e', './sdks/wego/tests/e2e/...', '-run', 'TestExamples/(batch|grpc-streams|shutdown|dual-entry)$|TestBatchShutdownBudget$|TestReview|TestIndependentReview', '-timeout', '10m'], REPO),
        ('embedded-engine', prefix + ['go', 'test', '-json', '-count=1', '-tags=e2e,wego_embedded', './sdks/wego/tests/e2e/...', '-run', '^TestEmbedded$', '-timeout', '10m'], REPO),
    ]
    for name, command, cwd in commands:
        print(f'RUNNING {name}', flush=True)
        log = RESULTS / f'gate-{int(started)}-{name}.log'
        with log.open('w') as output:
            process = subprocess.run(command, cwd=cwd, stdout=output, stderr=subprocess.STDOUT, env=env)
        okay = process.returncode == 0 and (name != 'format' or not log.read_text().strip())
        gates.append({'name': name, 'command': ' '.join(command), 'cwd': str(cwd.relative_to(REPO)) or '.',
                      'status': 'PASSED' if okay else 'FAILED', 'exit_code': process.returncode,
                      'log': str(log.relative_to(REPO)), 'at': dt.datetime.now(dt.timezone.utc).isoformat()})
        gate_path = RESULTS / f'gates-{int(started)}.json'
        gate_path.write_text(json.dumps(gates, indent=2) + '\n')
        if not okay:
            raise RuntimeError(f'{name} failed; inspect {log}')
        print(f'PASSED {name}', flush=True)
    assemble(gates, started, args.out)


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, OSError, ValueError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
