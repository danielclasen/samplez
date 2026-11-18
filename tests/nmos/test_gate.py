"""gate.py against results it has to pass and results it has to fail.

usage: python3 -m unittest tests/nmos/test_gate.py
"""

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import importlib.util

spec = importlib.util.spec_from_file_location("gate", Path(__file__).with_name("gate.py"))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)

FRAME = [{"name": n, "state": "Not Applicable"} for n in ["__init__", "set_up_tests", "tear_down_tests"]]


class Gate(unittest.TestCase):
    def verdict(self, results, minimum=2, expected=()):
        with tempfile.TemporaryDirectory() as d:
            doc = {"minimum": {run: minimum for run in gate.RUNS}, "expected": list(expected)}
            (Path(d) / "expected.json").write_text(json.dumps(doc))
            for run in gate.RUNS:
                if run in results:
                    (Path(d) / f"{run}.json").write_text(json.dumps({"results": results[run]}))
            out = io.StringIO()
            with redirect_stdout(out):
                code = gate.main(Path(d) / "expected.json", d)
            return code, out.getvalue()

    def passing(self, n=2):
        return FRAME + [{"name": f"test_{i:02}", "state": "Pass"} for i in range(n)]

    def test_full_runs_pass(self):
        code, out = self.verdict({run: self.passing() for run in gate.RUNS})
        self.assertEqual(code, 0, out)

    def test_a_run_that_loses_tests_fails(self):
        results = {run: self.passing() for run in gate.RUNS}
        results["is-05-01"] = self.passing(1)
        code, out = self.verdict(results)
        self.assertEqual(code, 1)
        self.assertIn("is-05-01: 1 tests ran, at least 2 expected", out)

    def test_a_run_without_results_fails(self):
        results = {run: self.passing() for run in gate.RUNS}
        results["bcp-007-03-01"] = []
        code, out = self.verdict(results)
        self.assertEqual(code, 1)
        self.assertIn("bcp-007-03-01: 0 tests ran", out)
        del results["bcp-007-03-01"]
        self.assertEqual(self.verdict(results)[0], 1)

    def test_only_a_not_applicable_frame_is_skipped(self):
        results = {run: self.passing() for run in gate.RUNS}
        results["is-04-01-unicast"][1] = {"name": "set_up_tests", "state": "Fail", "detail": "x"}
        code, out = self.verdict(results)
        self.assertEqual(code, 1)
        self.assertIn("is-04-01-unicast set_up_tests: Fail", out)

    def test_a_listed_result_passes_and_a_stale_entry_fails(self):
        results = {run: self.passing() for run in gate.RUNS}
        results["is-05-01"].append({"name": "test_09", "state": "Manual"})
        listed = {"run": "is-05-01", "test": "test_09", "result": "Manual", "reason": "r"}
        self.assertEqual(self.verdict(results, expected=[listed])[0], 0)
        self.assertEqual(self.verdict(results)[0], 1)
        stale = {"run": "is-05-01", "test": "test_10", "result": "Manual", "reason": "r"}
        self.assertEqual(self.verdict(results, expected=[listed, stale])[0], 1)


if __name__ == "__main__":
    unittest.main()
