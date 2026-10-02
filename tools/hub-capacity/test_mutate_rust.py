"""Reject false mutation confirmations and bind compilation to the mutated source owner."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("capacity_mutations", Path(__file__).with_name("mutate-rust.py"))
MUTATIONS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MUTATIONS)


class MutationTests(unittest.TestCase):
    def fixture(self, root):
        source = root / "crates/owner/src/lib.rs"
        source.parent.mkdir(parents=True)
        source.write_text("original expression\n")
        (root / "Cargo.toml").write_text('[workspace]\nmembers = ["crates/owner"]\n')
        (source.parent.parent / "Cargo.toml").write_text('[package]\nname = "source-owner"\nversion = "0.1.0"\n')
        return source

    def test_nested_source_selects_its_package_for_both_commands(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self.fixture(root)
            with patch.object(MUTATIONS, "ROOT", root):
                owner, compilation, regression = MUTATIONS.mutation_commands(str(source.relative_to(root)), "must_reject")
                self.assertEqual(owner, "source-owner")
                self.assertEqual(compilation[compilation.index("-p") + 1], owner)
                self.assertEqual(regression[regression.index("-p") + 1], owner)
                self.assertIn("--no-run", compilation)
                self.assertEqual(regression[regression.index("-E") + 1], "test(must_reject)")
                self.assertEqual(regression[regression.index("--no-tests") + 1], "fail")
                with self.assertRaisesRegex(ValueError, "repository"):
                    MUTATIONS.mutation_commands("../outside.rs", "must_reject")

    def run_rejected_case(self, results, message):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self.fixture(root)
            original = source.read_bytes()
            output = root / "report"
            case = ("expression", str(source.relative_to(root)), "original", "mutated", "must_reject")
            with patch.object(MUTATIONS, "ROOT", root), patch.object(MUTATIONS, "CASES", [case]), \
                    patch.object(sys, "argv", ["mutate-rust.py", "--output", str(output)]), \
                    patch.object(MUTATIONS, "execute", side_effect=results):
                with self.assertRaisesRegex(ValueError, message):
                    MUTATIONS.main()
            self.assertEqual(source.read_bytes(), original)
            report = json.loads((output / "report.json").read_text())
            self.assertEqual(len(report), 1)
            self.assertFalse(report[0].get("detected", False))
            self.assertEqual(report[0]["package"], "source-owner")

    def test_compiler_failure_cannot_confirm_a_mutation(self):
        self.run_rejected_case([(0, "", {}), (1, "compiler error", {})], "did not compile")

    def test_unrelated_failure_cannot_confirm_the_selected_regression(self):
        self.run_rejected_case([(0, "", {}), (0, "", {}), (100, "FAIL unrelated_test", {})], "selected regression")


if __name__ == "__main__":
    unittest.main()
