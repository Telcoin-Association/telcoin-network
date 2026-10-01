"""Synthetic peer agents validate scheduling and failure retention, never live capacity."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("hub_workload", ROOT / "workload.py")
WORKLOAD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORKLOAD)


class WorkloadTests(unittest.TestCase):
    def test_agent_nonce_route_and_refusals(self):
        script = (
            "import json, os; print(json.dumps({'operation_id': os.environ['HUB_CAPACITY_OPERATION_ID'],"
            "'scenario': os.environ['HUB_CAPACITY_SCENARIO'], 'success': True,"
            "'route': ['sender', 'relay', 'receiver'], 'trace': 'synthetic trace'}))"
        )
        agent = {"identity": "synthetic", "argv": [sys.executable, "-B", "-I", "-c", script]}
        result = WORKLOAD.execute(agent, "gossip_two_hops", "nonce", time.monotonic(), 2)
        self.assertTrue(result["success"])
        self.assertEqual(result["hops"], 2)
        self.assertGreater(result["latency_ms"], 0)
        agent["argv"][-1] = script.replace("'relay', ", "")
        self.assertFalse(WORKLOAD.execute(agent, "gossip_two_hops", "nonce", time.monotonic(), 2)["success"])
        agent["argv"][-1] = script.replace("os.environ['HUB_CAPACITY_OPERATION_ID']", "'wrong'")
        self.assertIn("acknowledgement", WORKLOAD.execute(agent, "record_lookup", "nonce", time.monotonic(), 2)["rejection_reason"])
        agent["argv"][-1] = "import time; time.sleep(2)"
        self.assertEqual(WORKLOAD.execute(agent, "record_lookup", "nonce", time.monotonic(), 0.01)["rejection_reason"], "timeout")

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
            self.assertEqual(len(operations), 16)
            self.assertEqual({entry["scenario"] for entry in operations}, WORKLOAD.QUALIFY.SCENARIOS)
            self.assertTrue(all(not entry["success"] and entry["rejection_reason"] for entry in operations))
            plan["envelope"]["public_peers"] = 64
            with self.assertRaisesRegex(ValueError, "insufficient distinct"):
                WORKLOAD.validate_manifest(plan, manifest)


if __name__ == "__main__":
    unittest.main()
