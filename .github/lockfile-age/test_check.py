"""check.py against lockfiles it has to pass and lockfiles it has to fail.

usage: python3 -B .github/lockfile-age/test_check.py
"""

import importlib.util
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

spec = importlib.util.spec_from_file_location("check", Path(__file__).with_name("check.py"))
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)

NOW = datetime(2026, 11, 18, tzinfo=timezone.utc)
REGISTRY = 'source = "registry+https://github.com/rust-lang/crates.io-index"'


def lock(*packages):
    out = "version = 4\n"
    for name, version, source in packages:
        out += f'\n[[package]]\nname = "{name}"\nversion = "{version}"\n'
        if source:
            out += source + "\n"
    return out


AGES = {("old", "1.0.0"): 30, ("old", "1.1.0"): 3, ("young", "0.1.0"): 13.9, ("edge", "2.0.0"): 14}


def lookup(name, version):
    return NOW - timedelta(days=AGES[(name, version)])


class Check(unittest.TestCase):
    def run_check(self, base, head, allow=""):
        return check.check(base, head, check.allowed(allow), NOW, lookup)

    def test_a_young_added_crate_fails_an_old_one_passes(self):
        base = lock(("old", "1.0.0", REGISTRY))
        problems, _, n = self.run_check(base, lock(("old", "1.0.0", REGISTRY), ("young", "0.1.0", REGISTRY)))
        self.assertEqual(n, 1)
        self.assertEqual(len(problems), 1)
        self.assertIn("young 0.1.0", problems[0])
        problems, _, _ = self.run_check(base, lock(("old", "1.0.0", REGISTRY), ("edge", "2.0.0", REGISTRY)))
        self.assertEqual(problems, [])

    def test_a_changed_version_is_checked_an_unchanged_one_is_not(self):
        base = lock(("old", "1.0.0", REGISTRY))
        problems, _, _ = self.run_check(base, lock(("old", "1.1.0", REGISTRY)))
        self.assertEqual(len(problems), 1)
        # Only the added one is looked up: an unknown pair would raise.
        problems, _, n = self.run_check(base, base)
        self.assertEqual((problems, n), ([], 0))

    def test_git_and_path_packages_are_not_checked(self):
        problems, _, n = self.run_check(
            lock(),
            lock(("mxl", "0.1.0", 'source = "git+https://example.invalid/mxl#abc"'), ("local", "0.7.0", None)),
        )
        self.assertEqual((problems, n), ([], 0))

    def test_the_allow_file_admits_a_listed_release_only(self):
        head = lock(("young", "0.1.0", REGISTRY), ("old", "1.1.0", REGISTRY))
        problems, notes, _ = self.run_check(lock(), head, "# c\nyoung 0.1.0 a fix we need\n")
        self.assertEqual(len(problems), 1)
        self.assertIn("old 1.1.0", problems[0])
        self.assertTrue(any("allowed: a fix we need" in n for n in notes))
        with self.assertRaises(ValueError):
            check.allowed("young 0.1.0\n")

    def test_the_repository_lockfile_parses(self):
        root = Path(__file__).resolve().parents[2]
        packages = check.registry_packages((root / "Cargo.lock").read_text())
        self.assertIn("tokio", {n for n, _ in packages})
        check.allowed((Path(__file__).with_name("allow.txt")).read_text())


if __name__ == "__main__":
    unittest.main()
