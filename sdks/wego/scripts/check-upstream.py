#!/usr/bin/env python3
"""检查 wego 不依赖对 Hatchet 源码的本地补丁；只读检查，不恢复或修改文件。"""
from pathlib import Path
import subprocess
import sys

# REPO 指向仓库根目录；与 SDK 独立模块的启动位置无关。
REPO = Path(__file__).resolve().parents[3]
# PROTECTED 覆盖 SDK 之外的整个仓库，包含根依赖、源码、示例和 CI 注册。
# 本门禁只报告差异，不通过自动恢复文件来掩盖边界违规或覆盖其他人的工作。
PROTECTED = ['.', ':(exclude)sdks/wego']
# result 同时覆盖已暂存、未暂存及新增文件；例如新增 context_bridge.go 也必须失败。
result = subprocess.run(['git', 'status', '--porcelain', '--untracked-files=all', '--', *PROTECTED],
                        cwd=REPO, capture_output=True, text=True, check=True)
if result.stdout.strip():
    print('Hatchet source boundary failed:\n' + result.stdout, file=sys.stderr)
    raise SystemExit(1)
print('PASSED: Hatchet sources and root dependencies are unchanged')
