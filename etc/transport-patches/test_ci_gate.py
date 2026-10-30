"""Execute the real CI aggregation script against lane outcomes: compile, transport and QUIC profile pin lanes."""

import json
import os
from pathlib import Path
import subprocess
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]

# ci-scope gates only the two test lanes; the compile lanes run for every author on both events.
ALWAYS_RUN = ("archive-mode-gate", "clippy", "fmt")
GATED = ("adiri-test", "test")
EVERY_PAIR = (("pull_request", False), ("pull_request", True), ("merge_group", True))


def gate_script():
    source = (ROOT / ".github/workflows/pr.yaml").read_text()
    return textwrap.dedent(source.split("  ci-success:\n", 1)[1].split("        run: |\n", 1)[1])


def execute(event, run_tests, transport, script=None, connection_limits="success", pins="success",
            quinn_proto="success", fmt=None, clippy=None, archive_mode_gate=None, test=None, adiri_test=None):
    """Run the gate on the fixture; a lane keyword left as None keeps the fixture's result for that lane."""
    results = {name: {"result": "success"} for name in ALWAYS_RUN}
    results.update({name: {"result": "success" if run_tests else "skipped"} for name in GATED})
    overrides = {"fmt": fmt, "clippy": clippy, "archive-mode-gate": archive_mode_gate, "test": test,
                 "adiri-test": adiri_test}
    results.update({name: {"result": outcome} for name, outcome in overrides.items() if outcome is not None})
    results.update({"ci-scope": {"result": "success", "outputs": {"run_tests": str(run_tests).lower()}},
                    "attest": {"result": "success" if event == "pull_request" else "skipped"},
                    "transport-evidence": {"result": transport},
                    "connection-limits-patch": {"result": connection_limits},
                    "quinn-proto-patch": {"result": quinn_proto},
                    "quic-profile-pins": {"result": pins}})
    return subprocess.run(["bash", "-c", gate_script() if script is None else script],
                          env={"PATH": os.environ["PATH"], "EVENT_NAME": event, "RESULTS": json.dumps(results)},
                          capture_output=True)


def lane_keyword(name):
    return name.replace("-", "_")


class TransportGate(unittest.TestCase):
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


class QuinnProtoGate(unittest.TestCase):
    def test_every_author_and_event_requires_success(self):
        for event, run_tests in EVERY_PAIR:
            self.assertEqual(execute(event, run_tests, "success").returncode, 0)
            for outcome in ("skipped", "cancelled", "failure"):
                with self.subTest(event=event, run_tests=run_tests, outcome=outcome):
                    self.assertNotEqual(execute(event, run_tests, "success", quinn_proto=outcome).returncode, 0)


class QuicProfilePinsGate(unittest.TestCase):
    def test_every_author_and_event_requires_success(self):
        for event, run_tests in EVERY_PAIR:
            self.assertEqual(execute(event, run_tests, "success").returncode, 0)
            for outcome in ("skipped", "cancelled", "failure"):
                with self.subTest(event=event, run_tests=run_tests, outcome=outcome):
                    self.assertNotEqual(execute(event, run_tests, "success", pins=outcome).returncode, 0)


class CompileGate(unittest.TestCase):
    def test_compile_lanes_are_required_for_every_author_and_event(self):
        for event, run_tests in EVERY_PAIR:
            self.assertEqual(execute(event, run_tests, "success").returncode, 0)
            for lane in ALWAYS_RUN:
                for outcome in ("skipped", "cancelled", "failure"):
                    with self.subTest(event=event, run_tests=run_tests, lane=lane, outcome=outcome):
                        result = execute(event, run_tests, "success", **{lane_keyword(lane): outcome})
                        self.assertNotEqual(result.returncode, 0)

    def test_test_lanes_skip_only_when_ci_scope_says_so(self):
        self.assertEqual(execute("pull_request", False, "success", test="skipped", adiri_test="skipped").returncode, 0)
        for lane in GATED:
            with self.subTest(lane=lane):
                result = execute("pull_request", True, "success", **{lane_keyword(lane): "skipped"})
                self.assertNotEqual(result.returncode, 0)

    def test_merge_queue_skips_nothing(self):
        # run_tests=False cannot come out of ci-scope on merge_group; it is here to show the
        # queue's refusal does not depend on what ci-scope said. The fixture marks both test
        # lanes skipped when run_tests is False, which the queue refuses on its own, so each
        # subtest sets them to success and skips only the lane under test.
        for run_tests in (True, False):
            self.assertEqual(
                execute("merge_group", run_tests, "success", test="success", adiri_test="success").returncode, 0
            )
            for lane in ALWAYS_RUN + GATED:
                with self.subTest(run_tests=run_tests, lane=lane):
                    lanes = {"test": "success", "adiri_test": "success", lane_keyword(lane): "skipped"}
                    result = execute("merge_group", run_tests, "success", **lanes)
                    self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
