EXPECTED_OFFICIAL_CODEJOB_SHA256 = '0eff490af7988c6b6f072b2eb9139220a8e51a8fb63c8a99990ab7cea0d3c756'
EXPECTED_OFFICIAL_CODEJOB_ID = 114023377041
"""Validate remote proof gates with fixtures, without running reader mains."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("verification", Path(__file__).with_name("1476-ci122-full-verify.py"))
verification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verification)


class VerificationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.reader = self.root / "reader.py"
        self.reader.write_text("# fixture source\n")
        self.manifest = {"ready": True, "qualification_head": verification.HEAD, "qualification_tree": verification.TREE,
                         "capacity_run_id": verification.RUN, "quic_run_id": verification.QUIC_RUN,
                         "roles": {"fixture": {"name": "reader.py", "sha256": verification.sha256(self.reader)}}}
        self.manifest_path = self.root / "manifest.json"

    def check_manifest(self):
        verification.write_json(self.manifest_path, self.manifest)
        return verification.checked_manifest(self.root, self.manifest_path, verification.sha256(self.manifest_path))

    def test_pinned_dependency_bytes(self):
        _, paths = self.check_manifest()
        self.assertEqual(paths["fixture"], self.reader)
        self.reader.write_text("changed\n")
        with self.assertRaises(ValueError):
            verification.checked_manifest(self.root, self.manifest_path, verification.sha256(self.manifest_path))

    def test_pending_bindings_and_wrong_head_rejected(self):
        for key, value in (("ready", False), ("qualification_head", "a" * 40)):
            self.manifest[key] = value
            with self.assertRaises(ValueError):
                self.check_manifest()
            self.manifest_path.unlink()
            self.manifest[key] = True if key == "ready" else verification.HEAD

    def test_dependency_escape_and_symlink_rejected(self):
        self.manifest["roles"]["fixture"]["name"] = "../reader.py"
        with self.assertRaises(ValueError):
            self.check_manifest()
        self.manifest_path.unlink()
        self.manifest["roles"]["fixture"]["name"] = "reader.py"
        self.reader.unlink()
        self.reader.symlink_to(self.root / "manifest.json")
        with self.assertRaises(ValueError):
            self.check_manifest()

    def test_duplicate_json_key_rejected(self):
        self.manifest_path.write_text('{"ready":true,"ready":false}')
        with self.assertRaises(ValueError):
            verification.load_json(self.manifest_path)

    def official_fixtures(self):
        run = {"id": verification.RUN, "head_sha": verification.HEAD, "run_attempt": 1, "event": "workflow_dispatch",
               "status": "completed", "conclusion": "success", "repository": {"id": 780459444}}
        jobs = {"jobs": [{"id": index + 1, "name": name, "run_id": verification.RUN, "head_sha": verification.HEAD,
                          "status": "completed", "conclusion": "success"} for index, name in enumerate(sorted(verification.JOB_NAMES))]}
        return run, jobs

    def test_all_official_jobs_required(self):
        run, jobs = self.official_fixtures()
        self.assertEqual(set(verification.official_capacity(run, jobs)), verification.JOB_NAMES)
        jobs["jobs"].pop()
        with self.assertRaises(ValueError):
            verification.official_capacity(run, jobs)

    def actual_code_job(self):
        path = Path(__file__).with_name("1476-ci122-official-codejob-fixture.json")
        self.assertIsInstance(EXPECTED_OFFICIAL_CODEJOB_SHA256, str)
        self.assertEqual(verification.sha256(path), EXPECTED_OFFICIAL_CODEJOB_SHA256)
        return verification.load_json(path)

    def test_actual_official_reusable_workflow_name(self):
        run, jobs = self.official_fixtures()
        actual = self.actual_code_job()
        self.assertEqual(actual["name"], "Manual hub capacity qualification / Manual hub capacity code checks")
        jobs["jobs"] = [job for job in jobs["jobs"] if job["name"] != actual["name"]] + [actual]
        self.assertEqual(verification.official_capacity(run, jobs)[actual["name"]], EXPECTED_OFFICIAL_CODEJOB_ID)

    def test_old_unprefixed_names_reject_actual_job(self):
        run, jobs = self.official_fixtures()
        actual = self.actual_code_job()
        jobs["jobs"] = [job for job in jobs["jobs"] if job["name"] != actual["name"]] + [actual]
        original_names = {name.removeprefix("Manual hub capacity qualification / ") for name in verification.JOB_NAMES}
        with patch.object(verification, "JOB_NAMES", original_names), self.assertRaises(ValueError):
            verification.official_capacity(run, jobs)

    def test_arbitrary_prefix_does_not_match(self):
        run, jobs = self.official_fixtures()
        jobs["jobs"][0]["name"] = "Other workflow / " + jobs["jobs"][0]["name"].split(" / ", 1)[1]
        with self.assertRaises(ValueError):
            verification.official_capacity(run, jobs)

    def test_failed_stale_duplicate_official_jobs_rejected(self):
        for change in ("failed", "stale", "duplicate"):
            run, jobs = self.official_fixtures()
            if change == "failed":
                jobs["jobs"][0]["conclusion"] = "failure"
            elif change == "stale":
                run["run_attempt"] = 2
            else:
                jobs["jobs"].append(jobs["jobs"][0])
            with self.subTest(change=change), self.assertRaises(ValueError):
                verification.official_capacity(run, jobs)

    def result_fixtures(self):
        actual = {name: "f" * 64 for name in ("telcoin-network", "node-record-api", "hub-capacity-peer")}
        quic = {"verified": True, "final_result": "PASS", "head_sha": verification.HEAD, "run_id": verification.QUIC_RUN}
        binaries = {"outcome": "pass", "head_sha": verification.HEAD, "run_id": verification.RUN, "archive": {"actual_binary_sha256": actual.copy()}}
        mutations = {"head": verification.HEAD, "cases": 77, "verified_logs": 231, "exact_members": 233, "canonical_shard_digests_consistent": True}
        scored = {"report_equal": True, "candidate_passed": True}
        return quic, binaries, mutations, actual, scored

    def test_authenticated_results_and_actual_binary_map(self):
        verification.reader_results(*self.result_fixtures())

    def test_non_pass_incomplete_mutations_and_invented_map_rejected(self):
        for change in ("non_pass", "mutations", "map"):
            result = self.result_fixtures()
            if change == "non_pass":
                result[4]["candidate_passed"] = False
            elif change == "mutations":
                result[2]["cases"] = 74
            else:
                result[3]["telcoin-network"] = "e" * 64
            with self.subTest(change=change), self.assertRaises(ValueError):
                verification.reader_results(*result)

    def test_receipt_exclusive_write(self):
        verification.write_json(self.manifest_path, {"bounded": True})
        with self.assertRaises(FileExistsError):
            verification.write_json(self.manifest_path, {"replacement": True})


if __name__ == "__main__":
    unittest.main(verbosity=2)
