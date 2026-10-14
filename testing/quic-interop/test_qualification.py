"""Adversarial checks of the release evidence contract, without privileged networking."""

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("quic_qualification", Path(__file__).with_name("qualification.py"))
QUALIFICATION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(QUALIFICATION)


class QualificationTests(unittest.TestCase):
    """Missing evidence or failed measurements must never become a passing gate."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.path = self.root / "report.json"
        artifacts = []
        for kind in ("node-binary", "node-config", "host-collector", "traffic",
                     "application-telemetry", "firewall"):
            file = self.root / kind
            file.write_text(kind)
            artifacts.append({"kind": kind, "path": kind,
                              "sha256": hashlib.sha256(file.read_bytes()).hexdigest()})
        metrics = {name: 1 for name in QUALIFICATION.METRICS}
        self.report = {
            "version": 1, "candidate": "a" * 40,
            "environment": {"representative_nic": True, "firewall_disabled": True,
                            "host": "test", "kernel": "test", "interface": "enp1s0",
                            "driver": "test", "offloads": "recorded", "uplink_bps": 1000,
                            "representativeness": "test fixture, not deployment evidence"},
            "measurement_started_at": "2026-09-30T02:00:00+00:00",
            "acceptance": {"selected_at": "2026-09-30T01:00:00+00:00",
                           "ingress_min_bps": 100, "ingress_max_bps": 500, "metrics": metrics},
            "real_sources": 2, "retry_completed_sources": 2,
            "swarm_roles": ["primary", "worker-0"],
            "cases": [{"role": role, "received_bps": 200, "generator_capacity_bps": 300,
                       "metrics": dict(metrics)} for role in ("primary", "worker-0", "process")],
            "artifacts": artifacts,
        }

    def validate(self, commit="a" * 40):
        self.path.write_text(json.dumps(self.report))
        return QUALIFICATION.validate(self.path, commit)

    def test_complete_report_passes(self):
        self.assertEqual(self.validate()["status"], "passed")

    def test_wrong_candidate_fails(self):
        with self.assertRaises(ValueError):
            self.validate("b" * 40)

    def test_missing_measurement_fails(self):
        self.report["cases"][0]["metrics"]["timer_progress"] = None
        with self.assertRaises(ValueError):
            self.validate()

    def test_failed_tail_bound_fails(self):
        self.report["cases"][0]["metrics"]["established_p99_ms"] = 2
        with self.assertRaises(ValueError):
            self.validate()

    def test_missing_worker_fails(self):
        self.report["cases"] = [case for case in self.report["cases"] if case["role"] != "worker-0"]
        with self.assertRaises(ValueError):
            self.validate()

    def test_saturation_and_late_thresholds_fail(self):
        self.report["acceptance"]["ingress_max_bps"] = 1000
        with self.assertRaises(ValueError):
            self.validate()
        self.report["acceptance"]["ingress_max_bps"] = 500
        self.report["acceptance"]["selected_at"] = "2026-09-30T03:00:00+00:00"
        with self.assertRaises(ValueError):
            self.validate()

    def test_changed_artifact_fails(self):
        (self.root / "node-binary").write_text("different candidate")
        with self.assertRaises(ValueError):
            self.validate()

    def test_loopback_or_zero_progress_fails(self):
        self.report["environment"]["interface"] = "lo"
        with self.assertRaises(ValueError):
            self.validate()
        self.report["environment"]["interface"] = "enp1s0"
        self.report["acceptance"]["metrics"]["consensus_progress"] = 0
        with self.assertRaises(ValueError):
            self.validate()


if __name__ == "__main__":
    unittest.main()
