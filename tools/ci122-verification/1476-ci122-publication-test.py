"""In-memory and temporary-file tests, without Git, network or real archives."""
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("publication", Path(__file__).with_name("1476-ci122-publication.py"))
publication = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publication)
HELPER = "a" * 40
COMMIT = "b" * 40
TREE = "c" * 40


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bundle = self.root / "bundle"
        self.bundle.mkdir()
        for name in publication.FILES:
            if name != "SHA256SUMS":
                (self.bundle / name).write_bytes(b"bounded fixture\n")
        (self.bundle / "report.json").write_text(json.dumps({"candidate": {"passed": True, "failures": []}}))
        checksums = "".join(publication.file_digest(self.bundle / name)["sha256"] + "  " + name + "\n"
                            for name in sorted(publication.FILES) if name != "SHA256SUMS")
        (self.bundle / "SHA256SUMS").write_text(checksums)
        self.files = {name: publication.file_digest(self.bundle / name) for name in publication.FILES}
        self.proof = {"qualification_head": publication.HEAD, "capacity_run_id": publication.RUN,
                      "helper_sha": HELPER, "helper_run_id": 1, "helper_run_attempt": 1, "helper_job": "verify",
                      "candidate_passed": True, "report_equal": True, "independently_rescored": True,
                      "quic_authenticated": True, "binaries_authenticated": True,
                      "mutation_controls_authenticated": True, "mutation_case_count": 77, "public_files": self.files}

    def validate(self):
        return publication.verify_bundle(self.bundle, self.proof, HELPER, 1, 1)

    def test_exact_seven_file_bundle(self):
        self.assertEqual(self.validate(), self.files)

    def test_fail_closed_provenance(self):
        for key, value in (("candidate_passed", False), ("report_equal", False), ("mutation_case_count", 76),
                           ("helper_sha", "d" * 40), ("helper_run_id", 2), ("qualification_head", "e" * 40),
                           ("quic_authenticated", False), ("binaries_authenticated", False)):
            with self.subTest(key=key), patch.dict(self.proof, {key: value}):
                with self.assertRaises(ValueError):
                    self.validate()

    def test_extra_file_and_symlink_rejected(self):
        extra = self.bundle / "private-key"
        extra.write_bytes(b"fixture")
        with self.assertRaises(ValueError):
            self.validate()
        extra.unlink()
        readme = self.bundle / "README.md"
        readme.unlink()
        readme.symlink_to(self.bundle / "plan.json")
        with self.assertRaises(ValueError):
            self.validate()

    def test_changed_bytes_and_checksum_rejected(self):
        (self.bundle / "README.md").write_bytes(b"changed\n")
        with self.assertRaises(ValueError):
            self.validate()
        self.proof["public_files"]["README.md"] = publication.file_digest(self.bundle / "README.md")
        with self.assertRaises(ValueError):
            self.validate()

    def test_public_verification_immutable_bytes(self):
        urls = []
        def opener(url, timeout):
            urls.append(url)
            self.assertEqual(timeout, 60)
            return io.BytesIO((self.bundle / url.rsplit("/", 1)[1]).read_bytes())
        actual = publication.verify_public(COMMIT, self.files, opener)
        self.assertEqual(set(actual), set(publication.FILES))
        self.assertTrue(all("/" + COMMIT + "/" in url for url in urls))

    def test_public_extra_and_truncated_bytes_rejected(self):
        for suffix in (b"extra", b""):
            def opener(url, timeout):
                raw = (self.bundle / url.rsplit("/", 1)[1]).read_bytes()
                return io.BytesIO(raw + suffix if suffix else raw[:-1])
            with self.assertRaises(ValueError):
                publication.verify_public(COMMIT, self.files, opener)

    def test_disk_allocation_guard(self):
        total = sum(value["bytes"] for value in self.files.values())
        free = publication.RESERVE + 2 * total + publication.SLACK
        with patch.object(publication.shutil, "disk_usage", return_value=type("Usage", (), {"free": free})()):
            self.assertEqual(publication.disk_floor(self.root, total), free)
        with patch.object(publication.shutil, "disk_usage", return_value=type("Usage", (), {"free": free - 1})()):
            with self.assertRaises(ValueError):
                publication.disk_floor(self.root, total)

    def mock_commands(self, calls, existing=False):
        blobs = {name: str(index + 1) * 40 for index, name in enumerate(sorted(publication.FILES))}
        def command(args, cwd, env, payload=None, **kwargs):
            calls.append((args, payload, env))
            operation = args[3]
            self.assertEqual(args[:3], ["git", "-c", "core.autocrlf=false"])
            self.assertNotIn("fixture-token", " ".join(args))
            if operation == "hash-object":
                self.assertIn("--no-filters", args)
                return blobs[Path(args[-1]).name] + "\n"
            if operation == "write-tree":
                return TREE + "\n"
            if operation == "commit-tree":
                self.assertNotIn("-p", args)
                self.assertIn(b"Signed-off-by: Onyeka Obi <softwareengineerasaservant@isurvivable.cv>", payload)
                return COMMIT + "\n"
            if operation == "cat-file":
                return "tree " + TREE + "\nauthor fixture\n\nmessage\n"
            if operation == "ls-tree":
                return "".join("100644 blob " + blobs[name] + "\t" + name + "\n" for name in sorted(publication.FILES))
            if operation == "ls-remote":
                has_push = any(call[0][3] == "push" for call in calls)
                return COMMIT + "\trefs/heads/" + publication.BRANCH + "\n" if existing or has_push else ""
            return ""
        return command

    def test_orphan_normal_push_recipe_without_execution(self):
        calls = []
        with patch.object(publication, "command", self.mock_commands(calls)), \
             patch.object(publication, "disk_floor", return_value=publication.RESERVE + publication.SLACK), \
             patch.object(publication, "verify_public", return_value=self.files), \
             patch.dict(publication.os.environ, {"GH_TOKEN": "fixture-token"}), patch("sys.stdout", new=io.StringIO()):
            publication.publish(self.bundle, self.files, self.root)
        push = [call[0] for call in calls if call[0][3] == "push"]
        self.assertEqual(push, [["git", "-c", "core.autocrlf=false", "push", "https://github.com/" + publication.REPOSITORY + ".git", COMMIT + ":refs/heads/" + publication.BRANCH]])
        self.assertEqual(sum(call[0][3] == "update-index" for call in calls), 7)

    def test_existing_remote_branch_blocks_push(self):
        calls = []
        with patch.object(publication, "command", self.mock_commands(calls, existing=True)), \
             patch.object(publication, "disk_floor", return_value=publication.RESERVE + publication.SLACK), \
             patch.dict(publication.os.environ, {"GH_TOKEN": "fixture-token"}):
            with self.assertRaises(ValueError):
                publication.publish(self.bundle, self.files, self.root)
        self.assertFalse(any(call[0][3] == "push" for call in calls))


if __name__ == "__main__":
    unittest.main(verbosity=2)
