"""Deterministic evidence-record regressions from fixture inputs; no node, no host."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("record", Path(__file__).with_name("record.py"))
record = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(record)

SS = (
    "State  Recv-Q Send-Q Local Address:Port Peer Address:Port Process\n"
    'UNCONN 0      0      0.0.0.0:9000      0.0.0.0:*     users:(("telcoin",pid=4242,fd=12)) ino:5001 sk:1 <->\n'
    "\t skmem:(r0,rb8388608,t0,tb212992,f0,w0,o0,bl0,d0)\n"
    'UNCONN 0      0      0.0.0.0:53        0.0.0.0:*     users:(("other",pid=77,fd=3)) ino:5002 sk:2 <->\n'
    "\t skmem:(r0,rb212992,t0,tb212992,f0,w0,o0,bl0,d0)\n"
)


def threshold(value):
    return {"value": value, "owner": "network-ops", "date": "2026-10-23"}


def manifest():
    return {
        "schema_version": 1,
        "run_id": "run-01",
        "build": {"node_commit": "abc123", "lockfile_sha256": "lock", "carried_patch_revision": "patch-7",
                  "features": ["default"], "rust_toolchain": "1.90",
                  "configuration": {"name": "plain", "receive_buffer_bytes_requested": None,
                                    "receive_buffer_bytes_effective": None, "inbound_accept_task": "inline"}},
        "deployment": {"roles": ["primary", "worker-0"], "worker_count": 1, "honest_peers": 4,
                       "shared_addresses": [], "admission_policy_state": "default", "link_speed_bps": 10_000_000_000,
                       "mtu": 1500, "topology_artifact": "topology.json", "firewall_disabled_evidence": "nft.txt",
                       "node_cpu_affinity": "0-7"},
        "workload": {"honest_workload": "committee traffic plus one fresh dial per second"},
        "method": {"sampling": "1 s", "trial_count": 3, "baseline_run_id": "run-00",
                   "instrumentation_overhead": "collector-off control run-00b"},
        "predeclared_acceptance": {
            "maximum_honest_p99_latency_ms": threshold(100),
            "maximum_honest_connection_failure_fraction": threshold(0.05),
            "maximum_socket_drops": threshold(0),
            "minimum_commit_rate_per_second": threshold(1.0),
            "maximum_process_cpu_cores": threshold(None),
            "legacy_threshold": 7,
        },
        "open_limitations": ["manifest note"],
    }


def metadata(ss_status="ok"):
    return {
        "identity": {"pid": 4242},
        "interface": "enp1s0",
        "requested_interval_seconds": 1,
        "host_before": {
            "uname": ["Linux", "host"],
            "executable_sha256": {"status": "ok", "value": "binhash"},
            "proc": {"sys/net/core/rmem_default": {"status": "ok", "value": "212992\n"},
                     "sys/net/core/rmem_max": {"status": "ok", "value": "8388608\n"}},
            "commands": [{"argv": ["ss", "-u", "-a", "-n", "-m", "-e", "-p"], "status": ss_status, "stdout": SS}],
        },
    }


def collector_summary(udp_status="ok"):
    return {"socket_drop_deltas": {
        "udp": {"status": udp_status, "value": {"5001": {"status": "ok", "value": {"drops": 3}}}},
        "udp6": {"status": "ok", "value": {}},
    }}


def measurement():
    return {
        "interval_seconds": 1, "scrape_count": 3, "gaps": [], "definitions": {"received_total_pps": {}},
        "bucket_bounds": {}, "unavailable": [],
        "summary": {
            "received": {"run_rate_pps": 210.0, "total": {"max": 210.0}, "by_stage": {}},
            "established_latency": {"outbound_request_response": {"p99": {"value": 0.05}}},
            "honest_dials": {"success_fraction": 0.98},
            "consensus": {"subdags_committed": {"rate_per_second": 2.0}},
            "process": {"cpu_cores": {"p99": 1.5}},
        },
    }


def generator(swarm):
    return {"swarm": swarm, "requested_rate_pps": 1000, "offered_rate_pps": 990.0, "offered_rate_bps": 9_700_000,
            "payload_bytes": 1200, "source_population": {"kind": "range", "count": 16},
            "send_errors": {"ENOBUFS": 2}, "achieved_fraction": 0.99}


def generators():
    return [{"path": "initial.json", "summary": generator("initial")},
            {"path": "peerlist.json", "summary": generator("peerlist")}]


def results(entries):
    return {entry["name"]: entry["result"] for entry in entries}


class ReceiveBufferTests(unittest.TestCase):
    def test_rb_parse_restricted_to_pid(self):
        rows = record.parse_receive_buffers(SS, 4242)
        self.assertEqual([entry["rb"] for entry in rows], [8388608])
        self.assertTrue(rows[0]["row"].startswith("UNCONN 0      0      0.0.0.0:9000"))
        self.assertEqual([entry["rb"] for entry in record.parse_receive_buffers(SS)], [8388608, 212992])
        self.assertEqual(record.parse_receive_buffers(SS, 1), [])
        self.assertEqual(record.parse_receive_buffers("", 4242), [])

    def test_sysctl_and_drops(self):
        host = metadata()["host_before"]
        self.assertEqual(record.sysctl_int(host, "rmem_max"), 8388608)
        self.assertIsNone(record.sysctl_int({}, "rmem_max"))
        self.assertEqual(record.socket_drop_total(collector_summary()), (3, None))
        self.assertEqual(record.socket_drop_total(collector_summary("unavailable"))[0], None)
        self.assertEqual(record.socket_drop_total({})[0], None)


class ThresholdTests(unittest.TestCase):
    def test_pass_fail_unknown_per_kind(self):
        observed = record.observations(measurement(), collector_summary())
        self.assertAlmostEqual(observed["maximum_honest_p99_latency_ms"], 50.0)
        self.assertAlmostEqual(observed["maximum_honest_connection_failure_fraction"], 0.02)
        self.assertEqual(observed["maximum_socket_drops"], 3)
        self.assertEqual(observed["minimum_commit_rate_per_second"], 2.0)
        self.assertEqual(observed["maximum_process_cpu_cores"], 1.5)
        entries = record.evaluate_thresholds(manifest()["predeclared_acceptance"], observed)
        self.assertEqual(results(entries), {
            "maximum_honest_p99_latency_ms": "pass",
            "maximum_honest_connection_failure_fraction": "pass",
            "maximum_socket_drops": "fail",
            "minimum_commit_rate_per_second": "pass",
            "maximum_process_cpu_cores": "unknown",
            "legacy_threshold": "unknown",
        })
        by_name = {entry["name"]: entry for entry in entries}
        self.assertEqual(by_name["maximum_process_cpu_cores"]["reason"], "threshold value not declared")
        self.assertEqual(by_name["legacy_threshold"]["reason"], "no evaluator for this threshold name")
        self.assertEqual(by_name["maximum_socket_drops"]["owner"], "network-ops")
        self.assertEqual(by_name["legacy_threshold"]["value"], 7)

    def test_missing_observation_is_unknown(self):
        observed = record.observations(None, None)
        self.assertEqual(set(observed.values()), {None})
        entries = record.evaluate_thresholds(manifest()["predeclared_acceptance"], observed)
        self.assertEqual(set(results(entries).values()), {"unknown"})
        self.assertEqual(entries[0]["reason"], "observation unavailable")

    def test_minimum_direction(self):
        entry = record.threshold_entry(5)
        self.assertEqual(record.evaluate_threshold("minimum_commit_rate_per_second", entry, 4.9)["result"], "fail")
        self.assertEqual(record.evaluate_threshold("minimum_commit_rate_per_second", entry, 5)["result"], "pass")
        self.assertEqual(record.evaluate_threshold("maximum_socket_drops", entry, 5)["result"], "pass")
        self.assertEqual(record.evaluate_threshold("maximum_socket_drops", entry, 6)["result"], "fail")


class BuildRecordTests(unittest.TestCase):
    def build(self, meta=None, summary=None, measured=None, gens=None):
        limitations, artifacts = [], []
        document = record.build_record(manifest(), metadata() if meta is None else meta,
                                       collector_summary() if summary is None else summary,
                                       measurement() if measured is None else measured,
                                       generators() if gens is None else gens, "plain", limitations, artifacts)
        return document, limitations

    def test_every_group(self):
        document, limitations = self.build()
        self.assertEqual(limitations, [])
        self.assertEqual(set(document), {"build", "topology", "host_and_traffic", "method", "thresholds"})
        build = document["build"]
        self.assertEqual((build["node_revision"], build["carried_patch_revision"], build["binary_sha256"]),
                         ("abc123", "patch-7", "binhash"))
        self.assertEqual(build["configuration"]["name"], "plain")
        self.assertEqual(build["configuration"]["receive_buffer_bytes_effective"], 8388608)
        self.assertEqual(build["configuration"]["host_sysctls"], {"rmem_default": 212992, "rmem_max": 8388608})
        self.assertEqual(build["configuration"]["inbound_accept_task"], "inline")
        topology = document["topology"]
        self.assertEqual((topology["roles"], topology["swarm_count"], topology["worker_count"], topology["honest_peers"]),
                         (["primary", "worker-0"], 2, 1, 4))
        self.assertEqual(topology["admission_policy_state"], "default")
        traffic = document["host_and_traffic"]
        self.assertEqual(traffic["host"]["interface"], "enp1s0")
        self.assertEqual(traffic["nic"], {"link_speed_bps": 10_000_000_000, "mtu": 1500})
        self.assertEqual(traffic["firewall_state"], "nft.txt")
        self.assertEqual([entry["swarm"] for entry in traffic["offered"]], ["initial", "peerlist"])
        self.assertEqual(traffic["received"]["total_rate_pps"], 210.0)
        self.assertEqual(traffic["received"]["socket_drops"], 3)
        self.assertEqual(traffic["generator_limits"][0]["send_errors"], {"ENOBUFS": 2})
        method = document["method"]
        self.assertEqual(method["trial_count"], 3)
        self.assertEqual(method["baseline_run_id"], "run-00")
        self.assertEqual(method["sampling"]["measurement_scrapes"], 3)
        self.assertEqual(method["result_distributions"]["honest_dials"], {"success_fraction": 0.98})
        self.assertEqual(results(document["thresholds"])["maximum_socket_drops"], "fail")

    def test_missing_inputs_become_limitations(self):
        document, limitations = self.build(meta={}, summary={}, measured=None, gens=[])
        self.assertIsNone(document["build"]["configuration"]["receive_buffer_bytes_effective"])
        self.assertIsNone(document["host_and_traffic"]["received"]["total_rate_pps"])
        self.assertIsNone(document["host_and_traffic"]["received"]["socket_drops"])
        self.assertEqual(document["topology"]["swarm_count"], 0)
        joined = "\n".join(limitations)
        for fragment in ("effective receive buffer", "socket drops", "measurement:", "generators:"):
            self.assertIn(fragment, joined)
        self.assertEqual(set(results(document["thresholds"]).values()), {"unknown"})

    def test_ss_without_node_rows_is_a_limitation(self):
        meta = metadata()
        meta["identity"]["pid"] = 1
        _document, limitations = self.build(meta=meta)
        self.assertTrue(any("no skmem row" in entry for entry in limitations))


class CapacityTests(unittest.TestCase):
    @staticmethod
    def entry(run_id, rate, outcome, name="plain"):
        return {"run_id": run_id, "build": {"configuration": {"name": name}},
                "host_and_traffic": {"received": {"total_rate_pps": rate}},
                "thresholds": [{"result": "pass"}, {"result": outcome}]}

    def test_highest_passing_rate(self):
        figure = record.capacity([self.entry("r1", 1000.0, "pass"), self.entry("r2", 1500.0, "pass"),
                                  self.entry("r3", 2000.0, "fail")])
        self.assertEqual((figure["capacity_received_rate_pps"], figure["run_id"], figure["configuration"]),
                         (1500.0, "r2", "plain"))
        self.assertIsNone(figure["reason"])
        self.assertEqual([row["result"] for row in figure["records"]], ["pass", "pass", "fail"])
        self.assertEqual(figure["notes"], [record.ADVISORY_NOTE])

    def test_unknown_blocks_the_figure(self):
        figure = record.capacity([self.entry("r1", 1000.0, "pass"), self.entry("r4", 2500.0, "unknown")])
        self.assertIsNone(figure["capacity_received_rate_pps"])
        self.assertIn("unknown", figure["reason"])

    def test_nothing_passes(self):
        figure = record.capacity([self.entry("r3", 2000.0, "fail")])
        self.assertIsNone(figure["capacity_received_rate_pps"])
        self.assertIn("no record passes", figure["reason"])

    def test_mixed_configurations_refused(self):
        figure = record.capacity([self.entry("r1", 1000.0, "pass"), self.entry("r2", 1500.0, "pass", "accept_task")])
        self.assertIsNone(figure["capacity_received_rate_pps"])
        self.assertIsNone(figure["configuration"])
        self.assertIn("configurations", figure["reason"])


class CommandLineTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.manifest = self.root / "run.json"
        self.manifest.write_text(json.dumps(manifest()))
        collector = self.root / "collector"
        collector.mkdir()
        (collector / "metadata.json").write_text(json.dumps(metadata()))
        (collector / "summary.json").write_text(json.dumps(collector_summary()))
        self.collector = collector
        self.measurement = self.root / "measurement.json"
        self.measurement.write_text(json.dumps(measurement()))
        self.generators = []
        for swarm in ("initial", "peerlist"):
            path = self.root / f"{swarm}.json"
            path.write_text(json.dumps(generator(swarm)))
            self.generators += ["--generator", str(path)]

    def tearDown(self):
        self.directory.cleanup()

    def test_build_then_capacity(self):
        output = self.root / "out" / "evidence-record.json"
        code = record.main(["build", "--manifest", str(self.manifest), "--collector", str(self.collector),
                            "--measurement", str(self.measurement), "--configuration", "plain",
                            "--output", str(output)] + self.generators)
        self.assertEqual(code, 0)
        document = json.loads(output.read_text())
        self.assertEqual(document["run_id"], "run-01")
        self.assertEqual(document["schema_version"], record.SCHEMA_VERSION)
        self.assertEqual(document["open_limitations"], ["manifest note"])
        self.assertEqual(sorted(entry["label"] for entry in document["artifacts"]),
                         sorted(["manifest", "collector metadata", "collector summary", "measurement",
                                 f"generator {self.root / 'initial.json'}", f"generator {self.root / 'peerlist.json'}"]))
        self.assertTrue(all(len(entry["sha256"]) == 64 for entry in document["artifacts"]))
        self.assertEqual(document["build"]["configuration"]["receive_buffer_bytes_effective"], 8388608)
        capacity = self.root / "capacity.json"
        code = record.main(["capacity", "--records", str(output), "--output", str(capacity)])
        self.assertEqual(code, 0)
        figure = json.loads(capacity.read_text())
        self.assertIsNone(figure["capacity_received_rate_pps"])
        self.assertEqual(figure["records"][0]["result"], "fail")
        self.assertEqual(figure["configuration"], "plain")

    def test_missing_inputs_exit_3(self):
        output = self.root / "partial.json"
        code = record.main(["build", "--manifest", str(self.manifest), "--configuration", "receive_buffer",
                            "--output", str(output)])
        self.assertEqual(code, 3)
        document = json.loads(output.read_text())
        self.assertIn("collector metadata: not supplied", document["open_limitations"])
        self.assertIn("measurement: not supplied", document["open_limitations"])
        self.assertEqual(document["build"]["configuration"]["name"], "receive_buffer")

    def test_unreadable_manifest_exits_2(self):
        self.assertEqual(record.main(["build", "--manifest", str(self.root / "absent.json"), "--configuration",
                                      "plain", "--output", str(self.root / "x.json")]), 2)


if __name__ == "__main__":
    unittest.main()
