"""验收组装回归：逐片段缺失、失败和错误归组都必须阻止完成。"""
import unittest
import acceptance


class FragmentEvidenceTest(unittest.TestCase):
    # setUp 为同一片段建立两个独立断言，整体场景通过不能代替第二个断言。
    def setUp(self):
        self.entry = {'tests': ['TestExample/task'], 'fragments': [
            {'id': 'source::handler::task', 'required_assertions': [
                {'scenario': 'task', 'id': 'a', 'assertion': 'first'},
                {'scenario': 'task', 'id': 'b', 'assertion': 'second'}]}]}
        self.records = [{'scenario': 'task', 'assertion_id': 'a', 'assertion': 'first'},
                        {'scenario': 'task', 'assertion_id': 'b', 'assertion': 'second'}]

    # test_explicit_evidence 只有两项实际断言都存在时才允许 PASSED。
    def test_explicit_evidence(self):
        result = acceptance.fragment_results(self.entry, self.records, {'task': 'PASSED'}, {})
        self.assertEqual(len(result[0]['evidence']), 2)

    # test_missing_assertion 单一必需断言缺失时，即使任务整体通过也必须失败。
    def test_missing_assertion(self):
        with self.assertRaises(RuntimeError):
            acceptance.fragment_results(self.entry, self.records[:1], {'task': 'PASSED'}, {})

    # test_wrong_scenario 相同文字在另一场景执行，不能用来证明当前片段。
    def test_wrong_scenario(self):
        self.records[1]['test_scenario'] = 'other'
        with self.assertRaises(RuntimeError):
            acceptance.fragment_results(self.entry, self.records, {'task': 'PASSED'}, {})

    # test_failed_or_skipped Skip、缺失和失败均不是通过。
    def test_failed_or_skipped(self):
        for state in ['FAILED', 'SKIPPED', 'NOT_RUN', '']:
            with self.subTest(state=state), self.assertRaises(RuntimeError):
                acceptance.fragment_results(self.entry, self.records, {'task': state}, {})

    # test_embedded_requires_record 嵌入进程整体通过也必须携带对应断言身份。
    def test_embedded_requires_record(self):
        self.entry['fragments'][0]['required_assertions'] = [{'scenario': 'embedded', 'id': 'embedded'}]
        with self.assertRaises(RuntimeError):
            acceptance.fragment_results(self.entry, [], {}, {'status': 'PASSED'})


if __name__ == '__main__':
    unittest.main()
