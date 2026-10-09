#!/usr/bin/env python3
"""仅对 p0 专用 namespace 注入保留期故障；不属于 SDK 运行逻辑。

示例：forget-producer 删除单个测试 producer 水位，下一次 seq>0 应明确失败。
配置和令牌仅用于确定本机数据库和授权租户，不打印、不保存凭证。
"""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import subprocess
import uuid
from urllib.parse import unquote, urlparse

# args 限制操作与地址；禁止将任意 SQL 或非测试 namespace 传入本机数据库。
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("action", choices=["forget-producer", "delete-origin"])
parser.add_argument("namespace")
parser.add_argument("topic")
parser.add_argument("--producer")
args = parser.parse_args()
if os.environ.get("WEGO_P0_FAULT_FIXTURES") != "1":
    raise SystemExit("explicit WEGO_P0_FAULT_FIXTURES=1 required")
if not re.fullmatch(r"p0-[a-z0-9-]{1,80}", args.namespace):
    raise SystemExit("only isolated p0 namespace allowed")
if not re.fullmatch(r"rpc\.[0-9a-f-]{36}", args.topic):
    raise SystemExit("only isolated RPC task topic allowed")

# tenant 只接受环境令牌里的标准 UUID，不允许 SQL 片段混入身份。
token = os.environ["HATCHET_CLIENT_TOKEN"]
claims = json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "==="))
tenant = str(uuid.UUID(claims.get("sub") or claims["tenant_id"]))
# config 从部署的本机配置提取数据库地址，脚本不回显文件。
config = {}
for line in (Path(__file__).resolve().parents[1] / "deployment" / "compose" / ".env").read_text().splitlines():
    key, separator, value = line.partition("=")
    if separator and not key.lstrip().startswith("#"):
        config[key.strip()] = value.strip().strip('"').strip("'")
url = urlparse(config["DATABASE_URL"])
where = f"tenant_id='{tenant}'::uuid AND namespace='{args.namespace}' AND topic='{args.topic}'"
if args.action == "forget-producer":
    if not args.producer or not re.fullmatch(r"[0-9a-f-]{36}:[0-9]+", args.producer):
        raise SystemExit("test producer must be task UUID and epoch")
    sql = f"DELETE FROM v1_stream_producer_cursor WHERE {where} AND producer_id='{args.producer}';"
else:
    sql = f"DELETE FROM v1_stream_message WHERE {where} AND id=(SELECT MIN(id) FROM v1_stream_message WHERE {where});"
# Unix socket 使用容器本地认证；密码不进入命令行或输出。
command = ["docker", "exec", "-i", os.environ.get("WEGO_POSTGRES_CONTAINER", "dbx-postgres"),
           "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", unquote(url.username or ""), "-d", url.path.lstrip("/")]
result = subprocess.run(command, input=sql, text=True, capture_output=True)
if result.returncode:
    raise SystemExit("local stream fault fixture failed; check deployment database configuration")
print("isolated stream retention fault applied")
