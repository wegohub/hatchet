"""部署脚本回归：认证失败不写库、凭证不泄漏、目标 UUID 和超时受约束。"""

import base64
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib import error


# script 导入带连字符的工具文件；导入本身不读取凭证或修改数据库。
spec = importlib.util.spec_from_file_location(
    "enable_durable_streams", Path(__file__).with_name("enable-durable-streams.py"),
)
script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(script)


class EnableDurableStreamsTest(unittest.TestCase):
    """无需数据库的协议及隐私回归；数据库 SQL 另用本机隔离 schema 验证。"""

    def setUp(self):
        """建立合成 UUID 和未签名测试 token；不能将其当成实际认证成功凭据。"""
        self.tenant = "11111111-1111-4111-8111-111111111111"
        self.other = "22222222-2222-4222-8222-222222222222"
        claims = base64.urlsafe_b64encode(json.dumps({"sub": self.tenant}).encode()).decode().rstrip("=")
        self.token = "eyJhbGciOiJIUzI1NiJ9." + claims + ".fake-signature"
        self.database = "postgresql://account:private-password@host.docker.internal:5432/testdb"

    def test_token_subject_is_not_authentication(self):
        """能解析 sub 的伪造/过期 token 仍须通过 API；拒绝后不能调用数据库。"""
        self.assertEqual(script.token_tenant(self.token), self.tenant)
        for status in (401, 403):
            with self.subTest(status=status), patch.dict(os.environ, {
                "HATCHET_CLIENT_TOKEN": self.token, "DATABASE_URL": self.database,
            }), patch.object(script.request, "build_opener") as opener, \
                    patch.object(script, "enable_tenant") as database, \
                    patch("sys.stderr", new_callable=io.StringIO) as diagnostic:
                opener.return_value.open.side_effect = error.HTTPError(
                    "http://localhost", status, self.token, {}, None,
                )
                self.assertEqual(script.main(["--env-file", "/missing/config", "--api-url", "http://localhost:8080"]), 1)
                database.assert_not_called()
                self.assertNotIn(self.token, diagnostic.getvalue())
                self.assertNotIn("private-password", diagnostic.getvalue())

    def test_authentication_must_match_tenant(self):
        """租户详情返回另一 UUID 时拒绝；Authorization 使用原 token。"""
        with patch.object(script.request, "build_opener") as opener:
            response = opener.return_value.open.return_value.__enter__.return_value
            response.read.return_value = json.dumps({"metadata": {"id": self.other}}).encode()
            with self.assertRaisesRegex(script.ScriptError, "不匹配"):
                script.authenticate(self.token, self.tenant, "http://localhost:8080")
            call = opener.return_value.open.call_args.args[0]
            self.assertEqual(call.get_header("Authorization"), "Bearer " + self.token)
            self.assertTrue(call.full_url.endswith("/api/v1/tenants/" + self.tenant))

    def test_redirect_is_not_followed(self):
        """跨站和同站跳转都不转发认证头；HTTP 302 只返回无凭证诊断。"""
        self.assertIsNone(script.NoRedirect().redirect_request(None, None, 302, "", {}, "https://other.invalid"))
        with patch.object(script.request, "build_opener") as opener:
            opener.return_value.open.side_effect = error.HTTPError("http://localhost", 302, self.token, {}, None)
            with self.assertRaisesRegex(script.ScriptError, "HTTP 302") as caught:
                script.authenticate(self.token, self.tenant, "http://localhost:8080")
            self.assertNotIn(self.token, str(caught.exception))

    def test_malformed_token_and_api_address(self):
        """非法 sub、空 token 及无效地址在产生网络请求之前失败。"""
        for token in ("", "abc", self.token + "\n", "a.e30.b", "a.@@@.b"):
            with self.subTest(token=token), self.assertRaises(script.ScriptError):
                script.token_tenant(token)
        for url in ("file:///tmp/config", "http://user:secret@localhost", "http://localhost:bad", "http://localhost?query", "http://localhost/\n"):
            with self.subTest(url=url), self.assertRaises(script.ScriptError):
                script.authenticate(self.token, self.tenant, url)

    def test_token_file_and_noninteractive_input(self):
        """纯 JWT 与 .env 格式都可读；没有隐藏输入终端时要求明确指定来源。"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "token"
            for contents in (self.token, 'HATCHET_CLIENT_TOKEN="' + self.token + '"\n'):
                path.write_text(contents)
                args = type("Args", (), dict(token_file=str(path), token_stdin=False))()
                self.assertEqual(script.read_token(args), self.token)
        with patch.dict(os.environ, {}, clear=True), patch("sys.stdin", io.StringIO(self.token)):
            args = type("Args", (), dict(token_file=None, token_stdin=False))()
            with self.assertRaises(script.ScriptError):
                script.read_token(args)
            args.token_stdin = True
            self.assertEqual(script.read_token(args), self.token)

    def test_credentials_are_not_process_arguments(self):
        """本机 psql 用私有文件，容器用 stdin；两种路径都不传入租户 token。"""
        for container in (None, "postgres-fixture"):
            with self.subTest(container=container), patch.object(script.shutil, "which", return_value="tool"), \
                    patch.object(script.subprocess, "run") as run, \
                    patch.dict(os.environ, {"HATCHET_CLIENT_TOKEN": self.token, "DATABASE_URL": self.database}):
                run.return_value = subprocess.CompletedProcess([], 0, self.tenant + "\n", "")
                url = self.database.replace("host.docker.internal", "localhost")
                script.enable_tenant(self.tenant, url, container)
                command = run.call_args.args[0]
                options = run.call_args.kwargs
                self.assertNotIn("private-password", " ".join(command))
                self.assertNotIn(self.token, " ".join(command))
                self.assertNotIn("HATCHET_CLIENT_TOKEN", options["env"])
                self.assertEqual(options["timeout"], 30)
                self.assertIn("tenant_id=" + self.tenant, command)
                if container:
                    encoded, sql = options["input"].split("\n", 1)
                    self.assertEqual(base64.b64decode(encoded).decode(), script.connection_service(url))
                    self.assertEqual(sql, script.ENABLE_SQL)
                else:
                    self.assertFalse(Path(options["env"]["PGSERVICEFILE"]).exists())

    def test_connection_fields_cannot_inject_configuration(self):
        """URL 编码控制字符不能注入另一 service；正常密码、TLS、options 保留。"""
        for url in (self.database + "?options=%0A[other]", self.database + "?%0Ahost=other",
                    self.database.replace("private-password", "secret%00value")):
            with self.subTest(url=url), self.assertRaises(script.ScriptError):
                script.connection_service(url)
        service = script.connection_service(self.database + "?sslmode=require")
        self.assertIn("sslmode=require\n", service)
        self.assertIn("password=private-password\n", service)

    def test_missing_tenant_and_database_errors(self):
        """空读回、带密码底层错误及超时均明确失败，不能伪报开启成功。"""
        with patch.object(script.shutil, "which", return_value="tool"), patch.object(script.subprocess, "run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, "", "")
            with self.assertRaisesRegex(script.ScriptError, "未找到"):
                script.enable_tenant(self.tenant, self.database, "postgres-fixture")
            run.return_value = subprocess.CompletedProcess([], 1, "", self.database + self.token)
            with self.assertRaises(script.ScriptError) as caught:
                script.enable_tenant(self.tenant, self.database, "postgres-fixture")
            self.assertNotIn("private-password", str(caught.exception))
            self.assertNotIn(self.token, str(caught.exception))
            run.side_effect = subprocess.TimeoutExpired("psql", 30)
            with self.assertRaisesRegex(script.ScriptError, "超时"):
                script.enable_tenant(self.tenant, self.database, "postgres-fixture")


if __name__ == "__main__":
    unittest.main()
