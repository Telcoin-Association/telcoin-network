"""Synthetic fixtures test rejection behavior, never population qualification."""

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("qualify", ROOT / "qualify.py")
QUALIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(QUALIFY)


def declaration():
    """Build a complete synthetic declaration with deterministic identities."""
    phase = {"revision": "a" * 40, "build_command": "synthetic fixture",
             "binary_sha256": {"telcoin-network": "b" * 64}, "profile": {}}
    observers = [f"synthetic-dao-{index}" for index in range(8)]
    phase["profile"] = {"dao_observers": observers}
    candidate_profile = {**QUALIFY.read_json(ROOT / "profile-v1.json"), "dao_observers": observers,
                         "bootstrap_peers": {key: {"synthetic": True} for key in observers}}
    return {
        "version": 1, "baseline": phase,
        "candidate": {**phase, "profile": candidate_profile},
        "envelope": {"cpus_per_hub": 4, "ram_bytes_per_hub": 8 * 1024**3,
                     "link_mbps": 25, "rtt_ms": 50, "loss_percent": 0.1,
                     "public_peers": 64, "shared_nat_peers": 16, "dao_observers": 8, "committee_peers": 4,
                     "workers_per_hub": 2, "duration_seconds": 600,
                     "hardware": "synthetic", "network_setup": "synthetic"},
        "hubs": ["hub-0"], "threshold_owner": "synthetic fixture",
        "adapter_command": "synthetic fixture",
        "thresholds": {"max_rss_bytes": 4 * 1024**3, "max_cpu_cores": 3,
                       "max_queue_occupancy": 100, "max_progress_stall_seconds": 15,
                       "scenarios": {scenario: {"minimum_attempts": 10,
                           "minimum_success_rate": 0.99, "max_p99_ms": 1000,
                           **({"max_cancelled_fraction": 0.35} if scenario == "committee_progress" else {})}
                           for scenario in QUALIFY.SCENARIOS}},
    }


def evidence(plan, phase="candidate"):
    """Build telemetry fixtures solely for validating the scorer."""
    swarm = {"connections": 86, "connection_limit": 86, "streams_per_connection_limit": 16,
             "ordinary_peers": 64, "dao_connected": 8,
             "receive_credit_per_connection_bytes": 1_073_741_824 // 258,
             "queue_occupancy": 0, "rejections": {"capacity": 0}}
    hub = {"rss_bytes": 1024**3, "cpu_seconds": 0, "progress": 1,
           "dao_connected": 8, "source_rows": 64,
           "swarms": {name: copy.deepcopy(swarm) for name in ("primary", "worker-0", "worker-1")},
           "tasks": {service: 0 for service in QUALIFY.SERVICES}}
    for network, allocation in hub["swarms"].items():
        allocation["tasks"] = dict.fromkeys(QUALIFY.TASK_LIMITS[network], 0)
        allocation["task_limits"] = QUALIFY.TASK_LIMITS[network].copy()
    samples = []
    for second in range(0, 601, 5):
        observation = copy.deepcopy(hub)
        observation["cpu_seconds"] = second
        observation["progress"] = 1 + second
        samples.append({"elapsed_seconds": second, "hubs": {"hub-0": observation}})
    return {"phase": phase, "plan_sha256": QUALIFY.digest(plan),
            "revision": plan[phase]["revision"],
            "profile_sha256": QUALIFY.digest(plan[phase]["profile"]),
            "binary_sha256": plan[phase]["binary_sha256"], "envelope": plan["envelope"],
            "artifacts": [{"path": "synthetic.json", "sha256": hashlib.sha256(b"synthetic").hexdigest()}],
            "samples": samples, "operations": {scenario: [
                {"id": str(index), "success": True, "latency_ms": 10,
                 "elapsed_seconds": index * 60, "hops": 2, "rejection_reason": None}
                for index in range(10)] for scenario in QUALIFY.SCENARIOS}}


class QualificationTests(unittest.TestCase):
    def setUp(self):
        self.plan = declaration()
        self.run = evidence(self.plan)

    def test_cancelled_votes_have_a_separate_finite_budget(self):
        operations = self.run["operations"]["committee_progress"]
        operations.extend({"id": f"cancel-{index}", "success": False, "cancelled": True,
                           "latency_ms": 100, "elapsed_seconds": 100,
                           "rejection_reason": "proposal cancelled"} for index in range(4))
        QUALIFY.validate_evidence(self.plan, self.run, "candidate")
        self.assertTrue(QUALIFY.score(self.plan, self.run)["passed"])
        operations.extend({**operations[-1], "id": f"extra-{index}"} for index in range(2))
        self.assertFalse(QUALIFY.score(self.plan, self.run)["passed"])
        operations[-1]["cancelled"] = "false"
        with self.assertRaisesRegex(ValueError, "classified as cancelled"):
            QUALIFY.validate_evidence(self.plan, self.run, "candidate")

    def test_matching_chain_deployment(self):
        for phase in ("baseline", "candidate"):
            self.plan[phase]["profile"]["libp2p_config"] = {"chain_id": 4476}
        QUALIFY.validate_plan(self.plan)

    def test_chain_deployment_rejects_capacity_overrides(self):
        for phase in ("baseline", "candidate"):
            self.plan[phase]["profile"]["libp2p_config"] = {"chain_id": 4476, "max_connections": 1000}
        with self.assertRaisesRegex(ValueError, "only chain_id"):
            QUALIFY.validate_plan(self.plan)

    def test_mismatched_chain_deployment(self):
        self.plan["baseline"]["profile"]["libp2p_config"] = {"chain_id": 4477}
        self.plan["candidate"]["profile"]["libp2p_config"] = {"chain_id": 4476}
        with self.assertRaisesRegex(ValueError, "same chain settings"):
            QUALIFY.validate_plan(self.plan)

    def test_complete_fixture_and_cli(self):
        QUALIFY.validate_plan(self.plan)
        QUALIFY.validate_evidence(self.plan, self.run, "candidate")
        self.assertTrue(QUALIFY.score(self.plan, self.run)["passed"])
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "synthetic.json").write_bytes(b"synthetic")
            paths = {name: Path(directory) / (name + ".json")
                     for name in ("declaration", "plan", "baseline", "candidate", "report")}
            paths["declaration"].write_text(json.dumps(self.plan))
            paths["baseline"].write_text(json.dumps(evidence(self.plan, "baseline")))
            paths["candidate"].write_text(json.dumps(self.run))
            for arguments in (
                ["freeze", str(paths["declaration"]), "--output", str(paths["plan"])],
                ["score", str(paths["plan"]), str(paths["baseline"]), str(paths["candidate"]),
                 "--output", str(paths["report"])],
            ):
                result = subprocess.run([sys.executable, "-I", str(ROOT / "qualify.py"), *arguments],
                                        capture_output=True, text=True, check=False)
                self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(json.loads(paths["report"].read_text())["candidate"]["passed"])

    def test_missing_and_altered_raw_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(OSError):
                QUALIFY.verify_artifacts(self.run, Path(directory))
            (Path(directory) / "synthetic.json").write_bytes(b"altered")
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                QUALIFY.verify_artifacts(self.run, Path(directory))

    def test_missing_worker_dao_failure_and_direct_gossip(self):
        del self.run["samples"][0]["hubs"]["hub-0"]["swarms"]["worker-1"]
        with self.assertRaisesRegex(ValueError, "all configured workers"):
            QUALIFY.validate_evidence(self.plan, self.run, "candidate")
        self.run = evidence(self.plan)
        self.run["samples"][1]["hubs"]["hub-0"]["dao_connected"] = 7
        self.assertFalse(QUALIFY.score(self.plan, self.run)["passed"])
        self.run = evidence(self.plan)
        self.run["samples"][1]["hubs"]["hub-0"]["swarms"]["worker-1"]["dao_connected"] = 7
        self.assertFalse(QUALIFY.score(self.plan, self.run)["passed"])
        self.run = evidence(self.plan)
        for sample in self.run["samples"]:
            sample["hubs"]["hub-0"]["swarms"]["worker-0"]["ordinary_peers"] = 63
        self.assertFalse(QUALIFY.score(self.plan, self.run)["passed"])
        self.run = evidence(self.plan)
        self.run["operations"]["gossip_two_hops"][0]["hops"] = 1
        with self.assertRaisesRegex(ValueError, "direct hub"):
            QUALIFY.validate_evidence(self.plan, self.run, "candidate")

    def test_memory_cpu_task_and_queue_limits(self):
        for field, value in (("rss_bytes", 5 * 1024**3), ("cpu_seconds", 100),
                             ("source_rows", 259)):
            run = evidence(self.plan)
            run["samples"][1]["hubs"]["hub-0"][field] = value
            self.assertFalse(QUALIFY.score(self.plan, run)["passed"], field)
        run = evidence(self.plan)
        run["samples"][0]["hubs"]["hub-0"]["tasks"]["batch_stream"] = 11
        self.assertFalse(QUALIFY.score(self.plan, run)["passed"])
        run = evidence(self.plan)
        hub = run["samples"][0]["hubs"]["hub-0"]
        hub["tasks"]["batch_stream"] = 6
        hub["swarms"]["worker-0"]["tasks"]["batch_stream"] = 6
        QUALIFY.validate_evidence(self.plan, run, "candidate")
        self.assertFalse(QUALIFY.score(self.plan, run)["passed"])
        hub["swarms"]["worker-0"]["tasks"]["batch_stream"] = 0
        with self.assertRaisesRegex(ValueError, "measured primary and worker totals"):
            QUALIFY.validate_evidence(self.plan, run, "candidate")
        run = evidence(self.plan)
        run["samples"][0]["hubs"]["hub-0"]["swarms"]["primary"]["queue_occupancy"] = 101
        self.assertFalse(QUALIFY.score(self.plan, run)["passed"])

    def test_stale_plan_missing_operations_and_stalled_application(self):
        self.plan["thresholds"]["max_rss_bytes"] += 1
        with self.assertRaisesRegex(ValueError, "predeclared"):
            QUALIFY.validate_evidence(self.plan, self.run, "candidate")
        self.run = evidence(self.plan)
        del self.run["operations"]["concurrent_sync"]
        with self.assertRaisesRegex(ValueError, "missing workload"):
            QUALIFY.validate_evidence(self.plan, self.run, "candidate")
        self.run = evidence(self.plan)
        self.run["samples"][-1]["hubs"]["hub-0"]["progress"] = 1
        self.assertFalse(QUALIFY.score(self.plan, self.run)["passed"])
        self.run = evidence(self.plan)
        for sample in self.run["samples"][:10]:
            sample["hubs"]["hub-0"]["progress"] = 1
        self.assertFalse(QUALIFY.score(self.plan, self.run)["passed"])

    def test_sparse_nan_duplicate_keys_and_baseline_substitution(self):
        del self.run["samples"][1]
        with self.assertRaisesRegex(ValueError, "five seconds"):
            QUALIFY.validate_evidence(self.plan, self.run, "candidate")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.json"
            for value in ('{"cpu":NaN}', '{"cpu":1,"cpu":2}'):
                path.write_text(value)
                with self.assertRaises(ValueError):
                    QUALIFY.read_json(path)
        with self.assertRaisesRegex(ValueError, "phase"):
            QUALIFY.validate_evidence(self.plan, evidence(self.plan, "baseline"), "candidate")
        self.plan["candidate"]["profile"]["public_peer_limit"] = 64.0
        with self.assertRaisesRegex(ValueError, "candidate configuration"):
            QUALIFY.validate_plan(self.plan)


if __name__ == "__main__":
    unittest.main()
