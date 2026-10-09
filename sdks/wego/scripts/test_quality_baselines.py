"""覆盖率统计不隐藏零覆盖包，复杂度门禁不依赖源码行号。"""

import importlib.util
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("quality_baselines", Path(__file__).with_name("quality-baselines.py"))
QUALITY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(QUALITY)


class QualityBaselineTests(unittest.TestCase):
    """用可人工核算的语句数验证报告及排除项，避免统计口径制造虚高覆盖率。"""

    def test_weighted_coverage_keeps_raw_and_zero_packages(self):
        """两个生产包为 2/6，不按文件百分比平均；生成及示例代码仍计入原始分母。"""
        with tempfile.TemporaryDirectory() as directory:
            profile = Path(directory) / "profile"
            prefix = QUALITY.PREFIX
            profile.write_text("mode: set\n" + "\n".join([
                f"{prefix}internal/wire/log.go:1.1,2.1 2 1",
                f"{prefix}internal/wire/log.go:3.1,4.1 1 0",
                f"{prefix}internal/client/run.go:1.1,2.1 3 0",
                f"{prefix}internal/wire/stream.pb.go:1.1,2.1 10 0",
                f"{prefix}examples/simple/main.go:1.1,2.1 4 0",
            ]))
            result = QUALITY.coverage(profile)
        self.assertEqual(result["raw"]["statements"], 20)
        self.assertEqual(result["production"]["statements"], 6)
        self.assertEqual(result["production"]["covered"], 2)
        self.assertEqual(result["packages"]["internal/client"]["percent"], 0)
        self.assertEqual(len(result["excluded_files"]), 2)

    def test_complexity_identity_ignores_line_numbers(self):
        """文件拆分或注释增减不能改变同一函数的复杂度身份。"""
        first = QUALITY.complexities("18 backend (*Backend).submit internal/backend/submit.go:10:1")
        second = QUALITY.complexities("18 backend (*Backend).submit internal/backend/submit.go:100:1")
        self.assertEqual(first, second)

    def test_complexity_rejects_duplicate_or_malformed_output(self):
        """工具异常与重复身份不能变成缺省的 15 或被最后一条静默覆盖。"""
        with self.assertRaises(ValueError):
            QUALITY.complexities("unexpected tool output")
        with self.assertRaises(ValueError):
            QUALITY.complexities("18 backend Call internal/backend/one.go:1:1\n19 backend Call internal/backend/one.go:1:1")

    def test_build_variants_use_highest_complexity(self):
        """互斥 tag 的两个实现取最大值，不能用简单的关闭实现掩盖 embedded 热路径。"""
        scores = QUALITY.complexities("1 backend Call internal/backend/disabled.go:1:1\n19 backend Call internal/backend/enabled.go:1:1")
        self.assertEqual(scores, {"internal/backend:Call": 19})


if __name__ == "__main__":
    unittest.main()
