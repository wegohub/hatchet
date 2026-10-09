#!/usr/bin/env python3
"""关闭工作区后验证官方发行依赖，防止本地源码掩盖接口缺失或生命周期问题。"""
import json
import os
from pathlib import Path
import subprocess
import sys

# SDK 是独立模块根目录；发布验证不加载仓库根模块和开发用 go.work。
SDK = Path(__file__).resolve().parents[1]
# ENV 只关闭本进程及子进程的工作区，不修改应用或用户的全局 Go 配置。
ENV = dict(os.environ, GOWORK='off')


# main 先验证模块来源，再执行单元测试和可选 embedded 适配层的编译。
def main():
    dependency = json.loads(subprocess.check_output(
        ['go', 'list', '-m', '-json', 'github.com/hatchet-dev/hatchet'], cwd=SDK, env=ENV))
    # 例如本地 replace 会绕过发行版 API 检查，必须明确拒绝而不是继续报告通过。
    if dependency['Version'] != 'v0.110.5' or dependency.get('Replace'):
        raise RuntimeError('expected unmodified official Hatchet v0.110.5 dependency')
    subprocess.run([sys.executable, 'scripts/unit.py'], cwd=SDK, env=ENV, check=True)
    subprocess.run(['go', 'test', '-tags=wego_embedded', './internal/backend', '-run', '^$'],
                   cwd=SDK, env=ENV, check=True)


if __name__ == '__main__':
    main()
