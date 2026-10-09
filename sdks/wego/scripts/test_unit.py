"""手动演示与必需 SDK 测试的范围边界必须显式且精确。"""

import unittest
import unit


class UnitScopeTests(unittest.TestCase):
    """新增测试包不得因路径前缀类似而被忽略。"""

    def test_only_explicit_manual_client_is_excluded(self):
        """完整 SDK、质量门禁、其他 feature 包及新包继续进入单元测试。"""
        packages = [unit.MANUAL, unit.MANUAL + "extra", unit.MANUAL + "/nested", "sdk/internal/client", "sdk/tests/quality", "sdk/new-package"]
        self.assertEqual(unit.unit_packages(packages), packages[1:])


if __name__ == "__main__":
    unittest.main()
