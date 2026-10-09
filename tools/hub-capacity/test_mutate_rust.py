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
        self.assertEqual(len(MUTATIONS.CASES), 77)
        self.assertEqual(len(MUTATIONS.PUBLIC_ADMISSION_CASES), 12)
        self.assertEqual(len(MUTATIONS.INGRESS_CASES), 5)
        self.assertEqual(len(MUTATIONS.INTEGRATION_CASES), 2)
        self.assertEqual(
            hashlib.sha256(json.dumps(legacy_ids, separators=(",", ":")).encode()).hexdigest(),
            "4d72d20bbd56e32d2821fbb03cd4bd85e7a12cab3d72707bc323ecd84298b327",
        )
        self.assertEqual(
            hashlib.sha256(json.dumps(MUTATIONS.CASES[:70], separators=(",", ":")).encode()).hexdigest(),
            "1f5cc43664eea3e08b939411bcbe6ea432da2534d8e59f94c4a7c526469c8704",
        )
        self.assertEqual(MUTATIONS.CASES[58:70], MUTATIONS.PUBLIC_ADMISSION_CASES)
        self.assertEqual(MUTATIONS.CASES[70:75], MUTATIONS.INGRESS_CASES)
        self.assertEqual(
            hashlib.sha256(json.dumps(MUTATIONS.CASES[:75], separators=(",", ":")).encode()).hexdigest(),
            "407e42d59f38b7e8a1ecd824565551e6737297b2863aba41975391825c8026b8",
        )
        self.assertEqual(MUTATIONS.CASES[75:], MUTATIONS.INTEGRATION_CASES)

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
        ingress_tests = (MUTATIONS.ROOT / "crates/consensus/worker/src/network/ingress.rs").read_text()
        for name, relative, before, after, regression in MUTATIONS.INGRESS_CASES:
            with self.subTest(regression=regression):
                self.assertRegex(
                    ingress_tests,
                    rf"#\[(?:tokio::)?test(?:\([^\]]*\))?\]\s+(?:async\s+)?fn\s+{re.escape(regression)}\s*\(",
                )

    def test_public_classification_mutant_keeps_library_identity_reference(self):
        case = next(case for case in MUTATIONS.CASES if case[0] == "public_provisional_classification")
        _, relative, before, after, _ = case
        source = (MUTATIONS.ROOT / relative).read_text()
        mutated = source.replace(before, after, 1)
        self.assertNotIn(before, mutated)
        self.assertEqual(mutated.count("self.peers.peer_has_confirmed_identity(peer_id)"), 1)

    def test_unpolled_expiry_mutant_preserves_library_cleanup_reference(self):
        case = next(case for case in MUTATIONS.CASES if case[0] == "worker_ingress_unpolled_expiry_owner")
        _, relative, before, after, _ = case
        source = (MUTATIONS.ROOT / relative).read_text().split("#[cfg(test)]", 1)[0]
        mutated = source.replace(before, after, 1)
        self.assertEqual(mutated.count("self.0.close();"), 1)
        self.assertIn("std::mem::ManuallyDrop::new(ExpiryOwner(self.clone()))", mutated)
        self.assertIn("let _owner = owner;", mutated)

    def test_integration_controls_leave_the_selected_regression_unchanged(self):
        owners = {
            "dao_committee_overlap_scoring": "tn-network-libp2p",
            "own_batch_cache_retention": "tn-node",
        }
        for name, relative, before, after, regression in MUTATIONS.INTEGRATION_CASES:
            with self.subTest(mutation=name):
                source = (MUTATIONS.ROOT / relative).read_text()
                production, tests = source.split("#[cfg(test)]", 1)
                self.assertEqual(production.count(before), 1)
                self.assertRegex(
                    tests,
                    rf"#\[(?:test|tokio::test)\]\s+(?:async\s+)?fn\s+{re.escape(regression)}\s*\(",
                )
                mutated = source.replace(before, after, 1)
                self.assertEqual(mutated.split("#[cfg(test)]", 1)[1], tests)
                owner, compilation, selected = MUTATIONS.mutation_commands(relative, regression)
                self.assertEqual(owner, owners[name])
                self.assertIn("--no-run", compilation)
                self.assertEqual(selected[selected.index("-E") + 1], f"test({regression})")
                self.assertEqual(selected[selected.index("--no-tests") + 1], "fail")

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
