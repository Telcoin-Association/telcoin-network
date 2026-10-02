"""Exercise binary reuse against real, isolated Git histories."""

import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("capacity_runner", Path(__file__).with_name("docker-run.py"))
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class SourceProvenanceTests(unittest.TestCase):
    """Only committed harness changes may reuse a binary from an ancestor."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="capacity-source-")
        self.addCleanup(self.directory.cleanup)
        self.repository = Path(self.directory.name)
        self.harness = self.repository / "tools/hub-capacity"
        self.harness.mkdir(parents=True)
        self.environment = os.environ | {
            "GIT_AUTHOR_NAME": "Qualification fixture", "GIT_COMMITTER_NAME": "Qualification fixture",
            "GIT_AUTHOR_EMAIL": "fixture@example.invalid", "GIT_COMMITTER_EMAIL": "fixture@example.invalid"}
        self.git("init", "--quiet")
        self.head = None
        self.binary_revision = self.commit("Cargo.toml", "[workspace]\n")

    def git(self, *arguments):
        return subprocess.run(["git", *arguments], cwd=self.repository, env=self.environment,
                              check=True, capture_output=True, text=True).stdout.strip()

    def commit(self, name, contents):
        path = self.repository / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
        self.git("add", "--", name)
        arguments = ["commit-tree", self.git("write-tree"), "-m", "qualification fixture"]
        if self.head:
            arguments += ["-p", self.head]
        self.head = self.git(*arguments)
        self.git("update-ref", "HEAD", self.head)
        return self.head

    def verify(self):
        with patch.object(RUNNER, "ROOT", self.harness):
            return RUNNER.verify_source_provenance(self.binary_revision)

    def test_same_revision(self):
        result = self.verify()
        self.assertEqual(result["binary_revision"], result["qualification_revision"])
        self.assertEqual(result["qualification_only_changes"], [])

    def test_committed_harness_and_documentation_changes(self):
        self.commit("tools/hub-capacity/prepare.py", "# fixture\n")
        self.commit("docs/src/network/hub-capacity.md", "Fixture documentation.\n")
        result = self.verify()
        self.assertEqual(result["binary_revision"], self.binary_revision)
        self.assertEqual(result["qualification_revision"], self.head)
        self.assertNotEqual(self.binary_revision, self.head)
        self.assertEqual(len(result["qualification_only_changes"]), 2)

    def test_changed_build_inputs_rejected(self):
        for name in ("Cargo.toml", "crates/network-libp2p/src/lib.rs", ".github/workflows/pr.yaml",
                     "tn-contracts", "tools/hub-capacity/profile-v1.json", "tools/hub-capacity/nested/control.py"):
            with self.subTest(name=name):
                self.commit(name, "changed input\n")
                with self.assertRaisesRegex(ValueError, "different source inputs") as failure:
                    self.verify()
                self.assertIn(name, str(failure.exception))

    def test_dirty_input_outside_harness_rejected(self):
        (self.repository / "Cargo.toml").write_text("uncommitted input\n")
        with self.assertRaisesRegex(ValueError, "clean committed worktree"):
            self.verify()

    def test_unrelated_history_rejected(self):
        alternative = self.git("commit-tree", self.git("write-tree"), "-m", "unrelated history")
        self.git("update-ref", "HEAD", alternative)
        with self.assertRaisesRegex(ValueError, "must be an ancestor"):
            self.verify()


if __name__ == "__main__":
    unittest.main()
