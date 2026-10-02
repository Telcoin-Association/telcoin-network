"""Synthetic peer agents validate scheduling and failure retention, never live capacity."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("hub_workload", ROOT / "workload.py")
WORKLOAD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORKLOAD)


class WorkloadTests(unittest.TestCase):
    def test_total_driver_capacity_preserves_refused_attempts(self):
        active = peak = 0
        lock = threading.Lock()

        def execute(_agent, scenario, operation_id, origin, _timeout):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.1)
            with lock:
                active -= 1
            return {"scenario": scenario, "id": operation_id, "success": False,
                    "rejection_reason": "synthetic refusal", "latency_ms": 100,
                    "elapsed_seconds": time.monotonic() - origin}

        with tempfile.TemporaryDirectory() as directory:
            topology = Path(directory) / "topology.json"
            topology.write_text('{"synthetic":true}')
            manifest = {"topology_artifact": str(topology), "scenarios": {
                scenario: {"concurrency": 16, "burst_size": 16, "agents": [
                    {"identity": "synthetic", "argv": ["synthetic"]}
                ]} for scenario in WORKLOAD.QUALIFY.SCENARIOS}}
            plan = {"envelope": {"duration_seconds": 0.03, "public_peers": 1,
                                 "shared_nat_peers": 1, "dao_observers": 1},
                    "thresholds": {"scenarios": {scenario: {"minimum_attempts": 16}
                                                  for scenario in WORKLOAD.QUALIFY.SCENARIOS}}}
            output = Path(directory) / "operations.jsonl"
            with patch.object(WORKLOAD, "MAX_ACTIVE_COMMANDS", 4), patch.object(WORKLOAD, "execute", execute):
                WORKLOAD.run(plan, manifest, output, time.monotonic())
            operations = [json.loads(line) for line in output.read_text().splitlines()]
            self.assertLessEqual(peak, 4)
            self.assertGreater(peak, 1)
            self.assertEqual(len(operations), 129)
            self.assertEqual(len({(entry["scenario"], entry["id"]) for entry in operations}), 129)
            self.assertTrue(any(entry["rejection_reason"] == "driver_capacity" for entry in operations))

    def test_agent_nonce_route_and_refusals(self):
        script = (
            "import json, os, time; published = time.time_ns() // 1000; print(json.dumps({'operation_id': os.environ['HUB_CAPACITY_OPERATION_ID'],"
            "'scenario': os.environ['HUB_CAPACITY_SCENARIO'], 'success': True, 'identity': 'synthetic',"
            "'route': ['sender', 'relay', 'receiver'], 'trace': {"
            "'receipt': {'message_id': 'synthetic', 'propagation_source': 'relay', 'received_unix_us': published + 1000},"
            "'publication': {'record': {'fields': {'event': 'gossip_publish', 'message_id': 'synthetic', 'source': 'sender', 'unix_us': str(published)}}}}}))"
        )
        agent = {"identity": "synthetic", "argv": [sys.executable, "-B", "-I", "-c", script]}
        result = WORKLOAD.execute(agent, "gossip_two_hops", "nonce", time.monotonic(), 2)
        self.assertTrue(result["success"])
        self.assertEqual(result["hops"], 2)
        self.assertGreater(result["latency_ms"], 0)
        self.assertEqual(result["latency_ms"], 1)
        agent["argv"][-1] = script.replace("'message_id': 'synthetic', 'source': 'sender'", "'message_id': 'other', 'source': 'sender'")
        self.assertFalse(WORKLOAD.execute(agent, "gossip_two_hops", "nonce", time.monotonic(), 2)["success"])
        agent["argv"][-1] = script.replace("['sender', 'relay', 'receiver']", "['sender', 'receiver']").replace("'propagation_source': 'relay'", "'propagation_source': 'receiver'")
        self.assertFalse(WORKLOAD.execute(agent, "gossip_two_hops", "nonce", time.monotonic(), 2)["success"])
        agent["argv"][-1] = script.replace("os.environ['HUB_CAPACITY_OPERATION_ID']", "'wrong'")
        self.assertIn("acknowledgement", WORKLOAD.execute(agent, "record_lookup", "nonce", time.monotonic(), 2)["rejection_reason"])
        agent["argv"][-1] = script.replace("'identity': 'synthetic'", "'identity': 'different-peer'")
        wrong_peer = WORKLOAD.execute(agent, "record_lookup", "nonce", time.monotonic(), 2)
        self.assertFalse(wrong_peer["success"])
        self.assertIn("declared peer", wrong_peer["rejection_reason"])
        agent["argv"][-1] = "import time; time.sleep(2)"
        self.assertEqual(WORKLOAD.execute(agent, "record_lookup", "nonce", time.monotonic(), 0.01)["rejection_reason"], "timeout")

    def test_committee_batch_preserves_failure_and_cancellation(self):
        script = (
            "import json, os, time; now = time.time_ns() // 1000; print(json.dumps({"
            "'operation_id': os.environ['HUB_CAPACITY_OPERATION_ID'], 'scenario': os.environ['HUB_CAPACITY_SCENARIO'],"
            "'identity': 'synthetic', 'success': True, 'trace': {'observations': ["
            "{'source': 'synthetic', 'record': {'fields': {'event': 'committee_request', 'unix_us': str(now),"
            "'latency_us': '1000', 'completed': completed, 'success': success}}}"
            "for completed, success in [(True, True), (True, False), (False, False)]]}}))"
        )
        agent = {"identity": "synthetic", "argv": [sys.executable, "-B", "-I", "-c", script]}
        result = WORKLOAD.execute(agent, "committee_progress", "nonce", time.monotonic(), 2)
        self.assertTrue(result["success"], result["rejection_reason"])
        measured = result["committee_observations"]
        self.assertEqual([entry["success"] for entry in measured], [True, False, False])
        self.assertEqual([entry["cancelled"] for entry in measured], [False, False, True])
        self.assertEqual([entry["latency_ms"] for entry in measured], [1, 1, 1])
        agent["argv"][-1] = script.replace("'source': 'synthetic'", "'source': 'wrong-hub'")
        self.assertFalse(WORKLOAD.execute(agent, "committee_progress", "nonce", time.monotonic(), 2)["success"])

    def test_concurrent_scenarios_have_complete_attempt_populations(self):
        script = "import json, os; print(json.dumps({'operation_id': os.environ['HUB_CAPACITY_OPERATION_ID'], 'scenario': os.environ['HUB_CAPACITY_SCENARIO'], 'success': False, 'rejection_reason': 'synthetic refusal'}))"
        with tempfile.TemporaryDirectory() as directory:
            topology = Path(directory) / "topology.json"
            topology.write_text('{"synthetic":true}')
            manifest = {"topology_artifact": str(topology), "scenarios": {
                scenario: {"concurrency": 1, "agents": [
                    {"identity": "synthetic", "argv": [sys.executable, "-B", "-I", "-c", script]}
                ]} for scenario in WORKLOAD.QUALIFY.SCENARIOS}}
            plan = {"envelope": {"duration_seconds": 0.1, "public_peers": 1,
                                 "shared_nat_peers": 1, "dao_observers": 1},
                    "thresholds": {"scenarios": {scenario: {"minimum_attempts": 2}
                                                  for scenario in WORKLOAD.QUALIFY.SCENARIOS}}}
            output = Path(directory) / "operations.jsonl"
            WORKLOAD.run(plan, manifest, output, time.monotonic())
            operations = [json.loads(line) for line in output.read_text().splitlines()]
            self.assertEqual(len(operations), 17)
            self.assertEqual(sum(entry["scenario"] == "committee_progress" for entry in operations), 3)
            self.assertEqual({entry["scenario"] for entry in operations}, WORKLOAD.QUALIFY.SCENARIOS)
            self.assertTrue(all(not entry["success"] and entry["rejection_reason"] for entry in operations))
            plan["envelope"]["public_peers"] = 64
            with self.assertRaisesRegex(ValueError, "insufficient distinct"):
                WORKLOAD.validate_manifest(plan, manifest)


if __name__ == "__main__":
    unittest.main()
