#!/usr/bin/env python3
"""在本机运行验收，令牌只传入子进程环境，不打印或写入报告。"""
import os
from pathlib import Path
import subprocess
import sys
import shlex

# env 是调用方环境的副本；子进程改用此视图，不修改父进程环境。
env = dict(os.environ)
# 独立 SDK 工作区只在子进程启用，不修改 Hatchet 根模块。
env["GOWORK"] = str(Path(__file__).resolve().parents[1] / "go.work")
# 本机 MinIO 与 codec 密钥只从私有部署文件注入子进程；显式环境配置始终优先。
# 例如 MINIO_ROOT_USER 映射到标准 AWS_ACCESS_KEY_ID，报告不会包含它的值。
compose_env = Path(env.get("WEGO_COMPOSE_ENV", str(Path(__file__).resolve().parents[1] / "deployment/compose/.env")))
if compose_env.exists():
    values = {}
    for line in compose_env.read_text().splitlines():
        key, separator, value = line.partition("=")
        if separator and not key.strip().startswith("#"):
            values[key.strip()] = value.strip().strip('"').strip("'")
    for source, target in [
        ("MINIO_ROOT_USER", "AWS_ACCESS_KEY_ID"),
        ("MINIO_ROOT_PASSWORD", "AWS_SECRET_ACCESS_KEY"),
        ("WEGO_CODEC_AES_KEY", "WEGO_CODEC_AES_KEY"),
    ]:
        if values.get(source):
            env.setdefault(target, values[source])
    env.setdefault("WEGO_S3_ENDPOINT", "http://127.0.0.1:" + values.get("MINIO_API_PORT", "9000"))
    env.setdefault("WEGO_S3_BUCKET", "wego-codec")
    env.setdefault("AWS_REGION", "us-east-1")
# 优先使用环境令牌，未提供时从明确的本机配置文件读取。
if not env.get('HATCHET_CLIENT_TOKEN'):
    # token_file 指向本地令牌来源，例如 WEGO_TOKEN_FILE 设置的 .env。
    token_file = Path(env.get('WEGO_TOKEN_FILE', 'examples/go/simple/.env'))
    # 兼容纯 JWT 和 KEY=VALUE 两种文件格式；只提取令牌，不回显文件内容。
    for line in token_file.read_text().splitlines():
        # candidate 是去除首尾空白后的行；JWT 至少应符合三段格式。
        candidate = line.strip()
        if candidate.startswith('eyJ') and candidate.count('.') == 2 and ' ' not in candidate:
            env['HATCHET_CLIENT_TOKEN'] = candidate
            break
        # key、separator、value 分离环境赋值，例如 HATCHET_CLIENT_TOKEN=...。
        key, separator, value = line.partition('=')
        if separator and key.strip() == 'HATCHET_CLIENT_TOKEN':
            env['HATCHET_CLIENT_TOKEN'] = value.strip().strip('"').strip("'")
            break
if not env.get('HATCHET_CLIENT_TOKEN'):
    raise SystemExit('HATCHET_CLIENT_TOKEN or WEGO_TOKEN_FILE is required')
# args 是用户传入的执行命令；为空时运行普通真实引擎验收。
args = sys.argv[1:] or ['go', 'test', '-tags=e2e', './sdks/wego/tests/e2e/...', '-timeout', '30m']
env["WEGO_TEST_COMMAND"] = shlex.join(args)
# 将子进程的真实退出码交给调用者，业务失败不能被包装为脚本成功。
raise SystemExit(subprocess.call(args, env=env))
