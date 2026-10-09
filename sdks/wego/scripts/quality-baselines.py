#!/usr/bin/env python3
"""收集覆盖率、复杂度和基准；明确区分生产代码、生成代码与真实引擎负载。"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

SDK = Path(__file__).resolve().parents[1]
RESULTS = SDK / ".test-results"
PREFIX = "github.com/hatchet-dev/hatchet/sdks/wego/"


def run(arguments, name, accepted=(0,)):
    """独立模块运行工具，完整诊断写文件，不把不明确退出当成门禁通过。"""
    result = subprocess.run(arguments, cwd=SDK, env={**os.environ, "GOWORK": "off"}, capture_output=True, text=True)
    (RESULTS / name).write_text(result.stdout + result.stderr)
    if result.returncode not in accepted:
        raise RuntimeError(f"{name}: command exited {result.returncode}; see {RESULTS / name}")
    return result.stdout


def coverage(path):
    """按语句加权，不平均各文件百分比；原始全量结果也保留，排除项可审计。"""
    packages, raw, production, excluded = {}, [0, 0], [0, 0], set()
    for line in path.read_text().splitlines()[1:]:
        location, statements, count = line.split()
        file = location.rsplit(":", 1)[0].removeprefix(PREFIX)
        total, hit = int(statements), int(statements) if int(count) > 0 else 0
        raw[0] += total
        raw[1] += hit
        if file.endswith(".pb.go") or file.startswith(("examples/", "tests/", "scripts/")):
            excluded.add(file)
            continue
        package = str(Path(file).parent)
        values = packages.setdefault(package, [0, 0])
        values[0] += total
        values[1] += hit
        production[0] += total
        production[1] += hit

    def summarize(values):
        total, hit = values
        return {"statements": total, "covered": hit, "percent": round(100 * hit / total, 3) if total else 100.0}

    return {"raw": summarize(raw), "production": summarize(production), "packages": {name: summarize(values) for name, values in sorted(packages.items())}, "excluded_files": sorted(excluded)}


def complexities(output):
    """只接受固定格式的 gocyclo 输出，文件移动不改变函数身份或隐藏复杂度增长。"""
    entries, locations = {}, set()
    for line in output.splitlines():
        match = re.fullmatch(r"(\d+) (\S+) (\S+) (.+):\d+:\d+", line)
        if not match:
            raise ValueError(f"unrecognized complexity output: {line}")
        score, package, function, file = match.groups()
        identity = f"{Path(file).parent}:{function}"
        if line.split()[-1] in locations:
            raise ValueError(f"duplicate function location: {identity}")
        locations.add(line.split()[-1])
        # 互斥 build tag 可以在不同文件定义同一函数，按所有变体的最高复杂度检查。
        # 实际构建仍负责检测同时启用的重复声明，不能仅扫描当前平台隐藏其他变体。
        entries[identity] = max(entries.get(identity, 0), int(score))
    return entries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collect", action="store_true", help="执行全量单元测试并收集覆盖率")
    parser.add_argument("--coverage", type=Path, default=RESULTS / "quality.cover")
    parser.add_argument("--complexity", action="store_true", help="固定 gocyclo v0.6.0，拒绝新高复杂度函数或基线增长")
    parser.add_argument("--benchmarks", action="store_true", help="运行分配基准并保存 CPU、heap profile")
    args = parser.parse_args()
    RESULTS.mkdir(exist_ok=True)
    policy = json.loads((SDK / "scripts/quality-baseline.json").read_text())
    report = {"coverage": None, "complexity": None, "failures": [], "manual_test_packages": [PREFIX + "tests/feature/client"]}
    if args.collect:
        run([sys.executable, "scripts/unit.py", "-coverprofile=" + str(args.coverage)], "quality-coverage.log")
    if args.coverage.exists():
        report["coverage"] = coverage(args.coverage)
        for name, minimum in policy["coverage_minimums"].items():
            actual = report["coverage"]["packages"].get(name)
            if actual is None or actual["percent"] < minimum:
                report["failures"].append(f"{name}: coverage below {minimum}%")
    else:
        raise ValueError("coverage profile missing; use --collect or --coverage")
    if args.complexity:
        output = run(["go", "run", "github.com/fzipp/gocyclo/cmd/gocyclo@v0.6.0", "-ignore", r"(_test\.go|\.pb\.go)", "internal", "server", "runtime", "client", "task"], "quality-complexity.log")
        scores = complexities(output)
        report["complexity"] = {name: value for name, value in scores.items() if value > 15}
        for name, value in report["complexity"].items():
            if value > policy["complexity_maximums"].get(name, 15):
                report["failures"].append(f"{name}: complexity {value} exceeds recorded maximum")
    if args.benchmarks:
        run(["go", "test", "./internal/wire", "./internal/stream", "-run", "^$", "-bench", "Benchmark(FrameCodec|ProtoJSONEnvelope|LogInterpreter|ClaimAcquire)$", "-benchmem", "-benchtime=150ms", "-count=3"], "quality-benchmarks.log")
        run(["go", "test", "./internal/wire", "-run", "^$", "-bench", "^BenchmarkFrameCodec$", "-benchtime=100ms", "-cpuprofile=" + str(RESULTS / "codec.cpu"), "-memprofile=" + str(RESULTS / "codec.heap"), "-o=" + str(RESULTS / "wire.test")], "quality-profiles.log")
        for name, sample in [("cpu", "cpu"), ("heap", "alloc_space")]:
            run(["go", "tool", "pprof", "-text", "-sample_index=" + sample, str(RESULTS / "wire.test"), str(RESULTS / ("codec." + name))], "quality-profile-" + name + ".log")
    (RESULTS / "quality-baselines.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    if report["failures"]:
        raise ValueError("; ".join(report["failures"]))
    print("覆盖率与复杂度门禁通过，原始数据和性能基线已保存到 .test-results。")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
