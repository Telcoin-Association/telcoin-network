"""Tests that the harness builds the right commands without touching any host."""

import contextlib
import io
import itertools
import json
from pathlib import Path
import tempfile
import unittest

import harness


def inventory_fixture():
    hosts = [{"name": f"node-{index}", "ssh": f"tn@10.0.0.{index}", "datadir": f"/data/node-{index}",
              "address": "0x" + str(index) * 40, "metrics_url": f"http://10.0.0.{index}:9101/metrics",
              "rpc_url": f"http://10.0.0.{index}:8545", "multiaddr": f"/ip4/10.0.0.{index}/udp/49590/quic-v1"}
             for index in range(2)]
    return {
        "hosts": hosts, "workers_per_node": 1, "cpus_per_node": 4, "ram_bytes_per_node": 1000,
        "binary": {"baseline": "/opt/base/telcoin-network", "candidate": "/opt/candidate/telcoin-network"},
        "revision": {"baseline": "a" * 40, "candidate": "b" * 40},
        "build_command": {"baseline": "cargo build --release", "candidate": "cargo build --release"},
        "network_config": "network-config.yaml",
        "process_budget": {"swarm_count": 2, "max_established_connections": 16, "max_established_connections_per_peer": 3,
                           "max_inbound_streams": 240, "max_receive_credit_bytes": 24000},
        "generator": "pressure --target {target} --connections 64", "hostile_targets": ["node-1"],
        "catch_up_node": "node-1", "catch_up_pause_secs": 30, "reconnect_gap_secs": 5, "samples": 3, "interval": 1,
        "node_args": ["--metrics", "0.0.0.0:9101"], "genesis_dir": "/data/genesis", "genesis_args": ["--chain-id", "2017"],
        "artifacts": ["network-config.yaml"], "decisions": "thresholds.proposed.json, pending maintainer decision",
    }


class Finished:
    def __init__(self, stdout, returncode):
        self.stdout, self.returncode = stdout, returncode

    def communicate(self):
        return self.stdout, None


class HarnessTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        (self.root / "network-config.yaml").write_text("peer_limits: {}\n")
        self.calls, self.sleeps, self.writes = [], [], {}
        self.result = {"failed_scrapes": 0, "failures": []}
        self.harness = self.make(inventory_fixture())

    def tearDown(self):
        self.directory.cleanup()

    def make(self, inventory, code=0, capture_code=0, clock=None, probe=lambda url: True):
        runner = lambda argv: (self.calls.append(argv),
                               Finished('{"opened": 64}', capture_code if str(harness.CAPTURE) in argv else code))[1]
        return harness.Harness(inventory, self.root / "inventory.json", runner=runner,
                               sleep=self.sleeps.append, write=lambda target, text: self.writes.__setitem__(Path(target).name, text),
                               read=lambda target: json.dumps(self.result), clock=clock or itertools.count().__next__, probe=probe)

    def test_only_the_candidate_sets_process_budget(self):
        self.assertEqual(self.harness.network_config("baseline"), "peer_limits: {}\n")
        self.assertTrue(self.harness.network_config("candidate").endswith(
            "process_budget:\n  swarm_count: 2\n  max_established_connections: 16\n  max_established_connections_per_peer: 3\n"
            "  max_inbound_streams: 240\n  max_receive_credit_bytes: 24000\n"))
        wrong = inventory_fixture()
        wrong["process_budget"]["swarm_count"] = 3
        with self.assertRaises(ValueError):
            self.make(wrong).network_config("candidate")
        (self.root / "network-config.yaml").write_text("process_budget:\n  swarm_count: 2\n")
        with self.assertRaises(ValueError):
            self.harness.network_config("baseline")

    def test_hostile_run_stages_config_starts_nodes_and_records_the_generator(self):
        self.assertEqual(self.harness.run("candidate", "hostile", self.root / "results"), 0)
        commands = [" ".join(argv) for argv in self.calls]
        self.assertTrue(any(command.startswith("scp ") and command.endswith("tn@10.0.0.1:/data/node-1/network-config") for command in commands))
        self.assertEqual(sum("/opt/candidate/telcoin-network node" in command for command in commands), 2)
        self.assertIn("pressure --target /ip4/10.0.0.1/udp/49590/quic-v1 --connections 64", commands)
        capture = next(argv for argv in self.calls if str(harness.CAPTURE) in argv)
        self.assertEqual(capture[capture.index("--phase") + 1], "hostile")
        self.assertIn("process_budget:", self.writes["hostile.network-config.yaml"])
        manifest = json.loads(self.writes["hostile.capture.json"])
        self.assertEqual((manifest["revision"], manifest["topology"]["validators"]), ("b" * 40, 2))
        self.assertEqual(json.loads(self.writes["phase.json"])["generator"], [{"opened": 64}])

    def test_catch_up_starts_the_lagging_node_late(self):
        self.harness.run("baseline", "catch-up", self.root / "results")
        starts = [argv[1] for argv in self.calls if argv[0] == "ssh" and "nohup" in argv[2]]
        self.assertEqual(starts, ["tn@10.0.0.0", "tn@10.0.0.1"])
        self.assertEqual(self.sleeps, [30])
        self.assertEqual(json.loads(self.writes["phase.json"])["catch_up_node"], "node-1")
        self.assertNotIn("process_budget", self.writes["catch-up.network-config.yaml"])

    def test_failed_command_stops_the_harness_and_inventory_is_checked(self):
        with self.assertRaises(RuntimeError):
            self.make(inventory_fixture(), code=255).setup()
        for key, value in (("catch_up_node", "node-9"), ("hostile_targets", ["node-9"]), ("hostile_targets", []),
                           ("generator", "pressure"), ("ready_timeout_secs", 0), ("stop_timeout_secs", "60")):
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                self.make({**inventory_fixture(), key: value})

    def test_without_a_generator_the_hostile_and_mixed_phases_stay_pending(self):
        inventory = {key: value for key, value in inventory_fixture().items() if key not in ("generator", "hostile_targets")}
        unarmed = self.make(inventory)
        self.assertEqual(unarmed.phases(), ("steady", "catch-up", "reconnect"))
        for phase in ("hostile", "mixed"):
            with self.subTest(phase=phase), self.assertRaises(ValueError):
                unarmed.run("candidate", phase, self.root / "results")
        self.assertEqual(self.calls, [])
        self.assertEqual(self.writes, {})
        self.assertEqual(unarmed.run("candidate", "steady", self.root / "results"), 0)
        self.assertIn("generator none", json.loads(self.writes["steady.capture.json"])["workload"])

    def test_plan_prints_commands_and_runs_nothing(self):
        path = self.root / "inventory.json"
        path.write_text(json.dumps(inventory_fixture()))
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            self.assertEqual(harness.main([str(path), "plan", "--output", str(self.root / "results")]), 0)
        text = printed.getvalue()
        self.assertIn("keytool generate validator", text)
        self.assertIn("sleep 30", text)
        self.assertFalse((self.root / "results").exists())

    def test_plan_without_a_generator_skips_the_generator_phases(self):
        path = self.root / "inventory.json"
        path.write_text(json.dumps({key: value for key, value in inventory_fixture().items() if key != "generator"}))
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            self.assertEqual(harness.main([str(path), "plan", "--output", str(self.root / "results")]), 0)
        text = printed.getvalue()
        self.assertIn("steady", text)
        self.assertNotIn("hostile", text)
        self.assertNotIn("pressure", text)

    def test_reconnect_waits_for_metrics_and_records_expected_down_windows(self):
        probes = []
        armed = self.make(inventory_fixture(), probe=lambda url: probes.append(url) is None)
        self.assertEqual(armed.run("baseline", "reconnect", self.root / "results"), 0)
        windows = json.loads(self.writes["phase.json"])["expected_down"]
        self.assertEqual([window["node"] for window in windows], ["node-0", "node-1"])
        self.assertTrue(all(window["start_unix_seconds"] < window["end_unix_seconds"] for window in windows))
        self.assertEqual(probes, ["http://10.0.0.0:9101/metrics", "http://10.0.0.1:9101/metrics"] * 2)
        self.assertEqual(self.sleeps, [5, 5])
        with self.assertRaisesRegex(RuntimeError, "not ready"):
            self.make(inventory_fixture(), probe=lambda url: False).await_metrics(inventory_fixture()["hosts"][0])

    def test_run_fails_only_for_scrape_failures_outside_expected_down_windows(self):
        self.result = {"failed_scrapes": 1, "failures": [
            {"node": "node-1", "sample": 1, "started_unix_seconds": 7, "finished_unix_seconds": 7, "error": "refused"}]}
        flaky = self.make(inventory_fixture(), capture_code=1, clock=lambda: 7)
        self.assertEqual(flaky.run("baseline", "steady", self.root / "results"), 1)
        self.assertEqual(flaky.run("baseline", "reconnect", self.root / "results"), 0)
        self.assertEqual(json.loads(self.writes["phase.json"])["capture_exit"], 1)

    def test_stop_escalates_to_sigkill_and_begin_refuses_a_live_pid(self):
        host = inventory_fixture()["hosts"][0]
        self.make({**inventory_fixture(), "stop_timeout_secs": 5}).stop(host)
        self.harness.begin(host, "/opt/base/telcoin-network")
        stop, begin = (argv[2] for argv in self.calls[-2:])
        self.assertIn("seq 5", stop)
        self.assertLess(stop.index("kill -9"), stop.index("survived SIGKILL"))
        self.assertLess(stop.index("survived SIGKILL"), stop.index("rm -f"))
        self.assertLess(begin.index("node already running"), begin.index("nohup"))
        self.assertIn(">> /data/node-0/node.log", begin)
        with self.assertRaises(RuntimeError):
            self.make(inventory_fixture(), code=1).stop(host)


if __name__ == "__main__":
    unittest.main()
