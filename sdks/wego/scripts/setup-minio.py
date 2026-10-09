#!/usr/bin/env python3
"""补齐本机 MinIO 和示例 codec 的私有配置，不回显或覆盖已有凭证。"""
from pathlib import Path
import os
import secrets

# path 默认指向此 SDK 的私有部署文件，允许显式环境覆盖。
path = Path(os.environ.get("WEGO_COMPOSE_ENV", str(Path(__file__).resolve().parents[1] / "deployment/compose/.env")))
if not path.exists():
    raise SystemExit("请先复制 deployment/compose/.env.example 为 .env 并配置 DATABASE_URL")
# existing 保留原有数据库及 Hatchet 地址；只补齐 MinIO/codec 所需字段。
lines = path.read_text().splitlines()
existing = {line.partition("=")[0].strip(): line.partition("=")[2].strip() for line in lines if "=" in line}
# generated 在缺失或模板占位时使用；不会在日志、命令参数或验收报告中输出。
generated = {
    "MINIO_ROOT_USER": "wego-local",
    "MINIO_ROOT_PASSWORD": secrets.token_urlsafe(32),
    "MINIO_API_PORT": "9000",
    "MINIO_CONSOLE_PORT": "9001",
    "WEGO_CODEC_AES_KEY": secrets.token_hex(32),
}
for key, value in generated.items():
    if existing.get(key) and not existing[key].startswith("YOUR_"):
        continue
    lines = [line for line in lines if line.partition("=")[0].strip() != key]
    lines.append(key + "=" + value)
# 以私有权限写临时文件，再原子替换；写入途中也不能短暂暴露凭证。
temporary = path.with_name(".env.minio-tmp")
descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
try:
    with os.fdopen(descriptor, "w") as output:
        output.write("\n".join(lines) + "\n")
    os.replace(temporary, path)
finally:
    temporary.unlink(missing_ok=True)
print("MinIO 与 codec 私有配置已就绪；凭证未输出。")
