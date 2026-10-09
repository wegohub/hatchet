#!/usr/bin/env python3
"""运行独立 SDK 的单元测试；手动 feature client 需要用户单独启动 Worker。"""

import os
from pathlib import Path
import subprocess
import sys

SDK = Path(__file__).resolve().parents[1]
MANUAL = "github.com/hatchet-dev/hatchet/sdks/wego/tests/feature/client"


def unit_packages(packages):
    """只排除明确的手动演示入口，不排除 SDK、质量门禁或新增包。"""
    return [package for package in packages if package != MANUAL]


def main():
    env = {**os.environ, "GOWORK": "off"}
    packages = subprocess.check_output(["go", "list", "./..."], cwd=SDK, env=env, text=True).splitlines()
    if MANUAL in packages:
        print("单元测试范围排除手动 feature/client；真实 RPC 场景由 tests/e2e 独立验收。", file=sys.stderr)
    selected = unit_packages(packages)
    if not selected:
        raise RuntimeError("SDK unit package inventory is empty")
    return subprocess.call(["go", "test", *sys.argv[1:], *selected], cwd=SDK, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
