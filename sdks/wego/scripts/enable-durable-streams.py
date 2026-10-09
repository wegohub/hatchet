#!/usr/bin/env python3
"""使用租户 token 验证身份，再在自托管数据库中开启该租户的 Durable Streams。

直接运行会隐藏输入 token；自动化可使用 HATCHET_CLIENT_TOKEN、--token-file
或 --token-stdin。数据库连接读取 DATABASE_URL 或部署目录的私有 .env。
"""

import argparse
import base64
import binascii
import getpass
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
from urllib import error, request
from urllib.parse import parse_qsl, unquote, urlsplit


# ENABLE_SQL 只更新 durable_streams；例如 audit_logs=true 的租户仍保留该授权。
# 从 Tenant 表选择身份，防止连接错库后为不存在的租户创建孤立 entitlement。
# 已开启时不更新时间；最终读回和 COMMIT 成功后才向调用方报告成功。
ENABLE_SQL = """
BEGIN;
INSERT INTO tenant_entitlement (tenant_id, durable_streams)
SELECT id, TRUE FROM "Tenant" WHERE id = :'tenant_id'::uuid
ON CONFLICT (tenant_id) DO UPDATE
SET durable_streams = TRUE, updated_at = NOW()
WHERE NOT tenant_entitlement.durable_streams;
SELECT t.id FROM "Tenant" t
JOIN tenant_entitlement e ON e.tenant_id = t.id
WHERE t.id = :'tenant_id'::uuid AND e.durable_streams;
COMMIT;
"""


class ScriptError(Exception):
    """可直接展示的诊断；不携带 token、数据库 URI 或底层错误响应。"""


class NoRedirect(request.HTTPRedirectHandler):
    """禁止认证请求跳转，避免将 Bearer token 转发到其他地址。"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """即使目标在同一主机也拒绝跳转；API 地址必须指向实际服务。"""
        return None


def read_env(path):
    """读取简单 KEY=VALUE 配置，不执行 shell 展开或配置文件中的命令。"""
    if not path.exists():
        return {}
    try:
        values = {}
        for line in path.read_text().splitlines():
            key, separator, value = line.partition("=")
            if separator and not key.strip().startswith("#"):
                values[key.strip()] = value.strip().strip('"').strip("'")
        return values
    except (OSError, UnicodeError):
        raise ScriptError("无法读取部署配置文件，请检查路径和权限。") from None


def read_token(args):
    """显式文件/标准输入优先于环境，均不把凭证放入子进程参数。"""
    try:
        if args.token_file:
            text = Path(args.token_file).read_text()
            for line in text.splitlines():
                key, separator, value = line.partition("=")
                if separator and key.strip() == "HATCHET_CLIENT_TOKEN":
                    return value.strip().strip('"').strip("'")
            return text.strip()
        if args.token_stdin:
            return sys.stdin.read(65537).strip()
        if os.environ.get("HATCHET_CLIENT_TOKEN"):
            return os.environ["HATCHET_CLIENT_TOKEN"].strip()
        if not sys.stdin.isatty():
            raise ScriptError("请设置 HATCHET_CLIENT_TOKEN，或使用 --token-file / --token-stdin。")
        return getpass.getpass("租户 token（输入不显示）：").strip()
    except (OSError, UnicodeError, EOFError):
        raise ScriptError("无法读取租户 token，请检查输入来源。") from None


def token_tenant(token):
    """只解析 sub 以定位认证请求；JWT 签名、有效期和撤销状态交给 API 验证。"""
    try:
        if not token or len(token) > 65536 or any(c.isspace() for c in token):
            raise ValueError()
        parts = token.split(".")
        if len(parts) != 3 or not all(parts):
            raise ValueError()
        claims = json.loads(base64.b64decode(
            parts[1] + "=" * (-len(parts[1]) % 4), altchars=b"-_", validate=True,
        ))
        return str(uuid.UUID(claims["sub"]))
    except (ValueError, KeyError, TypeError, AttributeError, binascii.Error):
        raise ScriptError("token 格式无效，必须提供 Hatchet 租户 JWT。") from None


def authenticate(token, tenant, api_url):
    """认证完成且响应租户匹配后才允许写库；不采用未验证 JWT 中的地址。"""
    try:
        address = urlsplit(api_url)
        if (address.scheme not in ("http", "https") or not address.hostname
                or address.username is not None or address.password is not None
                or address.query or address.fragment or any(c.isspace() for c in api_url)):
            raise ValueError()
        address.port
    except ValueError:
        raise ScriptError("API 地址无效，请使用不含凭证、查询参数的 http(s) 地址。") from None
    call = request.Request(
        api_url.rstrip("/") + "/api/v1/tenants/" + tenant,
        headers={"Authorization": "Bearer " + token, "Accept": "application/json"},
    )
    try:
        with request.build_opener(NoRedirect()).open(call, timeout=10) as response:
            # 租户详情应很小；例如错误代理返回大页面时不能无限读取。
            data = response.read(65537)
            if len(data) > 65536:
                raise ScriptError("API 租户响应超过大小限制。")
            actual = str(uuid.UUID(json.loads(data)["metadata"]["id"]))
            if actual != tenant:
                raise ScriptError("API 返回的租户与 token 不匹配，未修改数据库。")
    except error.HTTPError as failure:
        failure.close()
        raise ScriptError("API 认证失败（HTTP %d），未修改数据库。" % failure.code) from None
    except (error.URLError, OSError):
        raise ScriptError("无法连接 Hatchet API，请检查地址、TLS 和服务状态。") from None
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ScriptError("API 返回的租户详情无效，未修改数据库。") from None


def connection_service(database_url):
    """将 URI 转为 libpq service 配置；例如密码中的 @ 解码后保持普通字段值。"""
    try:
        address = urlsplit(database_url)
        if (address.scheme not in ("postgres", "postgresql") or not address.hostname
                or address.path in ("", "/") or any(c.isspace() for c in database_url)):
            raise ValueError()
        settings = {"host": unquote(address.hostname), "port": str(address.port or 5432),
                    "dbname": unquote(address.path[1:])}
        if address.username is not None:
            settings["user"] = unquote(address.username)
        if address.password is not None:
            settings["password"] = unquote(address.password)
        settings.update(parse_qsl(address.query, keep_blank_values=True, strict_parsing=True))
        # 用户指定的 TLS/options 仍生效；连接及 SQL 等待追加脚本自身预算。
        settings.update(connect_timeout="5", application_name="wego-enable-durable-streams")
        settings["options"] = settings.get("options", "") + " -c statement_timeout=15000 -c lock_timeout=5000"
        if any(not re.fullmatch(r"[a-z_]+", key) or any(c in value for c in "\r\n\x00")
               for key, value in settings.items()):
            raise ValueError()
    except (ValueError, AttributeError):
        raise ScriptError("DATABASE_URL 无效，请配置 PostgreSQL 数据库连接。") from None
    return "[wego-enable-durable-streams]\n" + "\n".join(key + "=" + value for key, value in settings.items()) + "\n"


def enable_tenant(tenant, database_url, container):
    """事务内只开启目标租户；连接凭证仅存在于私有临时文件和标准输入。"""
    try:
        tenant = str(uuid.UUID(tenant))
    except (ValueError, AttributeError, TypeError):
        raise ScriptError("租户 ID 必须是 UUID。") from None
    service = connection_service(database_url)
    arguments = ["psql", "-X", "-w", "-qAt", "--dbname=service=wego-enable-durable-streams",
                 "-v", "ON_ERROR_STOP=1", "-v", "tenant_id=" + tenant]
    # 子进程不需要租户 token；连接配置只放私有文件/标准输入，不进入命令行。
    # -w 禁止数据库密码交互，避免错误配置使 psql 消费待执行 SQL 作为密码。
    environment = {key: value for key, value in os.environ.items()
                   if key not in ("HATCHET_CLIENT_TOKEN", "DATABASE_URL")}
    sql = ENABLE_SQL
    service_path = None
    if container or urlsplit(database_url).hostname == "host.docker.internal" or not shutil.which("psql"):
        if not shutil.which("docker"):
            raise ScriptError("需要本机 psql，或可运行 PostgreSQL 容器的 Docker。")
        container = container or os.environ.get("WEGO_POSTGRES_CONTAINER", "dbx-postgres")
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", container):
            raise ScriptError("PostgreSQL 容器名称无效。")
        # 固定 shell 只解码 stdin 首行，不解释配置为命令；umask 和 trap 保护并清理凭证。
        launcher = ('umask 077; PGSERVICEFILE=$(mktemp) || exit 1; export PGSERVICEFILE; '
                    'trap \'rm -f "$PGSERVICEFILE"\' EXIT HUP INT TERM; '
                    'IFS= read -r encoded || exit 1; '
                    'printf %s "$encoded" | base64 -d > "$PGSERVICEFILE" || exit 1; "$@"')
        arguments = ["docker", "exec", "-i", container, "sh", "-c", launcher, "sh"] + arguments
        sql = base64.b64encode(service.encode()).decode() + "\n" + sql
    else:
        try:
            # NamedTemporaryFile 使用 0600 权限；psql 启动前关闭文件，完成后在 finally 删除。
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as output:
                service_path = Path(output.name)
                output.write(service)
            environment["PGSERVICEFILE"] = str(service_path)
        except OSError:
            if service_path:
                service_path.unlink(missing_ok=True)
            raise ScriptError("无法创建私有数据库连接文件。") from None
    try:
        result = subprocess.run(arguments, input=sql, text=True, capture_output=True,
                                env=environment, timeout=30)
    except subprocess.TimeoutExpired:
        raise ScriptError("数据库操作超时；可重跑确认，重复执行不会改变其他授权。") from None
    except OSError:
        raise ScriptError("无法启动数据库客户端，请检查 psql 或 Docker。") from None
    finally:
        if service_path:
            service_path.unlink(missing_ok=True)
    if result.returncode:
        # libpq 错误可能包含连接信息，不能原样输出到控制台或报告。
        raise ScriptError("数据库操作失败，请检查数据库连接、账号权限及 Hatchet 迁移。")
    if result.stdout.strip() != tenant:
        raise ScriptError("数据库中未找到对应租户，未创建授权；请检查是否连接了同一实例。")


def main(argv=None):
    """解析部署参数，依次完成输入、API 认证、事务更新及成功确认。"""
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--token-file", help="token 文件，支持纯 JWT 或 HATCHET_CLIENT_TOKEN=...")
    source.add_argument("--token-stdin", action="store_true", help="从标准输入读取 token")
    parser.add_argument("--api-url", help="Hatchet API 地址，默认读取配置或 http://localhost:8080")
    parser.add_argument("--env-file", type=Path, default=Path(os.environ.get(
        "WEGO_COMPOSE_ENV", str(Path(__file__).resolve().parents[1] / "deployment/compose/.env"))),
        help="私有部署 .env 路径；环境 DATABASE_URL 优先")
    parser.add_argument("--db-container", help="使用此容器中的 psql；默认自动检测本机 psql")
    args = parser.parse_args(argv)
    try:
        config = read_env(args.env_file)
        database_url = os.environ.get("DATABASE_URL") or config.get("DATABASE_URL")
        if not database_url:
            raise ScriptError("请在环境或部署 .env 中配置 DATABASE_URL。")
        api_url = (args.api_url or os.environ.get("HATCHET_CLIENT_SERVER_URL")
                   or "http://localhost:" + config.get("HATCHET_HTTP_PORT", "8080"))
        token = read_token(args)
        tenant = token_tenant(token)
        authenticate(token, tenant, api_url)
        enable_tenant(tenant, database_url, args.db_container)
        print("租户 %s 的 Durable Streams 已开启（重复执行安全）。" % tenant)
        return 0
    except ScriptError as failure:
        print(str(failure), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("操作已中断；可重跑确认开启状态。", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
