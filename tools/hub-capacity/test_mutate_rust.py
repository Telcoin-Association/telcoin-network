"""Reject false mutation confirmations and bind compilation to the mutated source owner."""

import importlib.util
import hashlib
import json
import re
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("capacity_mutations", Path(__file__).with_name("mutate-rust.py"))
MUTATIONS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MUTATIONS)


class MutationTests(unittest.TestCase):

    def test_legacy_case_ids_and_public_admission_registry_are_preserved(self):
        legacy_ids = [case[0] for case in MUTATIONS.CASES[:58]]
        self.assertEqual(len(MUTATIONS.CASES), 70)
        self.assertEqual(len(MUTATIONS.PUBLIC_ADMISSION_CASES), 12)
        self.assertEqual(
            hashlib.sha256(json.dumps(legacy_ids, separators=(",", ":")).encode()).hexdigest(),
            "4d72d20bbd56e32d2821fbb03cd4bd85e7a12cab3d72707bc323ecd84298b327",
        )
        self.assertEqual(MUTATIONS.CASES[58:], MUTATIONS.PUBLIC_ADMISSION_CASES)

    def test_all_registered_rewrites_have_one_current_source_anchor(self):
        for name, relative, before, after, regression in MUTATIONS.CASES:
            with self.subTest(mutation=name):
                source = (MUTATIONS.ROOT / relative).read_text()
                self.assertEqual(source.count(before), 1, name)
                self.assertNotEqual(before, after, name)
        manager_tests = (MUTATIONS.ROOT / "crates/network-libp2p/src/tests/peer_manager.rs").read_text()
        identity_tests = (MUTATIONS.ROOT / "crates/network-libp2p/src/peers/all_peers.rs").read_text()
        for name, relative, before, after, regression in MUTATIONS.PUBLIC_ADMISSION_CASES:
            with self.subTest(regression=regression):
                self.assertRegex(
                    manager_tests + identity_tests,
                    rf"#\[(?:tokio::)?test\]\s+(?:async\s+)?fn\s+{re.escape(regression)}\s*\(",
                )

    def test_public_classification_mutant_keeps_library_identity_reference(self):
        case = next(case for case in MUTATIONS.CASES if case[0] == "public_provisional_classification")
        _, relative, before, after, _ = case
        source = (MUTATIONS.ROOT / relative).read_text()
        mutated = source.replace(before, after, 1)
        self.assertNotIn(before, mutated)
        self.assertEqual(mutated.count("self.peers.peer_has_confirmed_identity(peer_id)"), 1)

    def fixture(self, root):
        source = root / "crates/owner/src/lib.rs"
        source.parent.mkdir(parents=True)
        source.write_text("original expression\n")
        (root / "Cargo.toml").write_text('[workspace]\nmembers = ["crates/owner"]\n')
        (source.parent.parent / "Cargo.toml").write_text('[package]\nname = "source-owner"\nversion = "0.1.0"\n')
        return source

    def standalone_fixture(self, root, relative, *, lockfile=True, package_name="libp2p-kad"):
        (root / "Cargo.toml").write_text('[workspace]\nexclude = ["patches/kad"]\n')
        package = root / "patches/kad"
        source = package / relative
        source.parent.mkdir(parents=True)
        source.write_text("original expression\n")
        (package / "Cargo.toml").write_text(f'[package]\nname = "{package_name}"\nversion = "0.49.0"\n')
        (root / "Cargo.lock").write_text("# Synthetic root lockfile\nversion = 3\n")
        if lockfile:
            (package / "Cargo.lock").write_text("# Synthetic standalone lockfile\nversion = 3\n")
        return source

    def test_nested_source_selects_its_package_for_both_commands(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self.fixture(root)
            with patch.object(MUTATIONS, "ROOT", root):
                owner, compilation, regression = MUTATIONS.mutation_commands(str(source.relative_to(root)), "must_reject")
                self.assertEqual(owner, "source-owner")
                self.assertEqual(compilation, ["cargo", "+1.94", "test", "--locked", "-p", owner, "--no-run"])
                self.assertEqual(regression, ["cargo", "+1.94", "nextest", "run", "--locked", "-p", owner,
                                              "-E", "test(must_reject)", "--no-tests", "fail", "--test-threads", "1"])
                with self.assertRaisesRegex(ValueError, "repository"):
                    MUTATIONS.mutation_commands("../outside.rs", "must_reject")

    def test_example_regression_selects_the_example_in_both_commands(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            source = root / "crates/owner/examples/probe.rs"
            source.parent.mkdir()
            source.write_text("original expression\n")
            with patch.object(MUTATIONS, "ROOT", root):
                owner, compilation, regression = MUTATIONS.mutation_commands(str(source.relative_to(root)), "must_reject")
            self.assertEqual(owner, "source-owner")
            for command in (compilation, regression):
                self.assertEqual(command[command.index("--example") + 1], "probe")
                self.assertIn("--locked", command)
            self.assertIn("--no-run", compilation)
            self.assertEqual(regression[regression.index("--no-tests") + 1], "fail")

    def test_excluded_dependency_library_selects_only_the_library(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self.standalone_fixture(root, "src/behaviour.rs")
            with patch.object(MUTATIONS, "ROOT", root):
                owner, compilation, regression = MUTATIONS.mutation_commands(str(source.relative_to(root)), "must_reject")
            self.assertEqual(owner, "libp2p-kad")
            self.assertEqual(compilation, ["cargo", "+1.94", "test", "--locked", "--manifest-path",
                                          "patches/kad/Cargo.toml", "--no-run", "--lib"])
            self.assertEqual(regression, ["cargo", "+1.94", "nextest", "run", "--locked", "--manifest-path",
                                         "patches/kad/Cargo.toml", "-E", "test(must_reject)",
                                         "--no-tests", "fail", "--test-threads", "1", "--lib"])

    def test_excluded_library_without_lockfile_uses_locked_root_package(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self.standalone_fixture(root, "src/lib.rs", lockfile=False,
                                             package_name="libp2p-connection-limits")
            with patch.object(MUTATIONS, "ROOT", root):
                owner, compilation, regression = MUTATIONS.mutation_commands(str(source.relative_to(root)), "must_reject")
            self.assertEqual(owner, "libp2p-connection-limits")
            self.assertEqual(compilation, ["cargo", "+1.94", "test", "--locked", "-p", owner, "--no-run", "--lib"])
            self.assertEqual(regression, ["cargo", "+1.94", "nextest", "run", "--locked", "-p", owner,
                                         "-E", "test(must_reject)", "--no-tests", "fail", "--test-threads", "1", "--lib"])

    def test_excluded_example_without_lockfile_uses_root_package_example(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self.standalone_fixture(root, "examples/probe.rs", lockfile=False)
            with patch.object(MUTATIONS, "ROOT", root):
                owner, compilation, regression = MUTATIONS.mutation_commands(str(source.relative_to(root)), "must_reject")
            self.assertEqual(compilation, ["cargo", "+1.94", "test", "--locked", "-p", owner, "--no-run", "--example", "probe"])
            self.assertEqual(regression, ["cargo", "+1.94", "nextest", "run", "--locked", "-p", owner,
                                         "-E", "test(must_reject)", "--no-tests", "fail", "--test-threads", "1", "--example", "probe"])

    def test_workspace_package_with_local_lockfile_keeps_root_selection(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self.fixture(root)
            (source.parent.parent / "Cargo.lock").write_text("# Synthetic package lockfile\nversion = 3\n")
            with patch.object(MUTATIONS, "ROOT", root):
                owner, compilation, regression = MUTATIONS.mutation_commands(str(source.relative_to(root)), "must_reject")
            self.assertEqual(compilation, ["cargo", "+1.94", "test", "--locked", "-p", owner, "--no-run"])
            self.assertEqual(regression, ["cargo", "+1.94", "nextest", "run", "--locked", "-p", owner,
                                         "-E", "test(must_reject)", "--no-tests", "fail", "--test-threads", "1"])

    def test_excluded_dependency_example_selects_the_example_without_library(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self.standalone_fixture(root, "examples/probe.rs")
            with patch.object(MUTATIONS, "ROOT", root):
                owner, compilation, regression = MUTATIONS.mutation_commands(str(source.relative_to(root)), "must_reject")
            self.assertEqual(owner, "libp2p-kad")
            self.assertEqual(compilation, ["cargo", "+1.94", "test", "--locked", "--manifest-path",
                                          "patches/kad/Cargo.toml", "--no-run", "--example", "probe"])
            self.assertEqual(regression, ["cargo", "+1.94", "nextest", "run", "--locked", "--manifest-path",
                                         "patches/kad/Cargo.toml", "-E", "test(must_reject)",
                                         "--no-tests", "fail", "--test-threads", "1", "--example", "probe"])

    def test_excluded_dependency_integration_source_does_not_select_library(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self.standalone_fixture(root, "tests/probe.rs")
            with patch.object(MUTATIONS, "ROOT", root):
                owner, compilation, regression = MUTATIONS.mutation_commands(str(source.relative_to(root)), "must_reject")
            self.assertEqual(owner, "libp2p-kad")
            for command in (compilation, regression):
                self.assertEqual(command[command.index("--manifest-path") + 1], "patches/kad/Cargo.toml")
                self.assertIn("--locked", command)
                self.assertNotIn("--lib", command)
                self.assertNotIn("--example", command)
                self.assertNotIn("-p", command)
            self.assertEqual(regression[regression.index("--no-tests") + 1], "fail")

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
