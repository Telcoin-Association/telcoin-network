"""Execute the real CI aggregation script against transport lane outcomes."""

import json
import os
from pathlib import Path
import subprocess
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]


def gate_script():
    source = (ROOT / ".github/workflows/pr.yaml").read_text()
    return textwrap.dedent(source.split("  ci-success:\n", 1)[1].split("        run: |\n", 1)[1])


def execute(event, run_lanes, transport, script=None, capacity=None, connection_limits="success"):
    lanes = ("adiri-test", "archive-mode-gate", "clippy", "fmt", "test")
    results = {name: {"result": "success" if run_lanes else "skipped"} for name in lanes}
    results.update({"ci-scope": {"result": "success", "outputs": {"run_lanes": str(run_lanes).lower()}},
                    "attest": {"result": "success" if event == "pull_request" else "skipped"},
                    "transport-evidence": {"result": transport}})
    results.update({name: {"result": "success"} for name in (
        "connection-limits-patch", "hub-capacity", "hub-capacity-mutations", "hub-capacity-qualification")})
    results["connection-limits-patch"] = {"result": connection_limits}
    if capacity:
        results.update({name: {"result": outcome} for name, outcome in capacity.items()})
    return subprocess.run(["bash", "-c", gate_script() if script is None else script],
                          env={"PATH": os.environ["PATH"], "EVENT_NAME": event, "RESULTS": json.dumps(results)},
                          capture_output=True)


class TransportGate(unittest.TestCase):
    def test_capacity_lanes_remain_required(self):
        for event, run_lanes in (("pull_request", False), ("merge_group", True)):
            for lane in ("connection-limits-patch", "hub-capacity", "hub-capacity-mutations", "hub-capacity-qualification"):
                for outcome in ("skipped", "cancelled", "failure"):
                    with self.subTest(event=event, lane=lane, outcome=outcome):
                        self.assertNotEqual(execute(event, run_lanes, "success",
                            capacity={lane: outcome}).returncode, 0)

    def test_maintainer_keeps_transport_required(self):
        self.assertEqual(execute("pull_request", False, "success").returncode, 0)
        for outcome in ("skipped", "cancelled", "failure"):
            with self.subTest(outcome=outcome):
                self.assertNotEqual(execute("pull_request", False, outcome).returncode, 0)

    def test_merge_queue_requires_success(self):
        self.assertEqual(execute("merge_group", True, "success").returncode, 0)
        for outcome in ("skipped", "cancelled", "failure"):
            with self.subTest(outcome=outcome):
                self.assertNotEqual(execute("merge_group", True, outcome).returncode, 0)


class ConnectionLimitsGate(unittest.TestCase):
    def test_maintainer_keeps_patch_lane_required(self):
        for outcome in ("skipped", "cancelled", "failure"):
            with self.subTest(outcome=outcome):
                self.assertNotEqual(execute("pull_request", False, "success", connection_limits=outcome).returncode, 0)

    def test_merge_queue_requires_patch_lane_success(self):
        for outcome in ("skipped", "cancelled", "failure"):
            with self.subTest(outcome=outcome):
                self.assertNotEqual(execute("merge_group", True, "success", connection_limits=outcome).returncode, 0)


if __name__ == "__main__":
    unittest.main()
