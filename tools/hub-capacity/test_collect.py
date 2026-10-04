"""Collector rejection tests use synthetic proc and metric fixtures, never capacity evidence."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("hub_collect", ROOT / "collect.py")
COLLECT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COLLECT)


def telemetry():
    lines = []
    for network in COLLECT.SWARMS:
        for name, value in {
            "established_connections": 7, "established_connection_limit": 86,
            "ordinary_peers_connected": 7, "dao_observers_connected": 8,
            "inbound_streams_per_connection_limit": 16,
            "receive_credit_per_connection_bytes": 4161790,
            "command_queue_occupancy": 2, "inbound_requests_pending": 3,
            "record_queries_pending": 1, "outbound_requests_pending": 4,
            "px_disconnects_pending": 0,
        }.items():
            lines.append(f'tn_network_{name}{{network="{network}"}} {value}')
        for service in COLLECT.CLASSES[network]:
            lines.append(f'tn_network_serve_tasks_active{{network="{network}",class="{service}"}} 1')
            lines.append(f'tn_network_serve_tasks_limit{{network="{network}",class="{service}"}} 5')
        lines.append(f'tn_network_serve_rejections_total{{network="{network}",class="batch_stream",reason="global_limit"}} 2')
    for name in ("source_address_rows", "source_peer_rows", "source_prefix_rows", "source_connections"):
        lines.append(f'tn_network_{name} 21')
    lines.extend(["progress 8", "dao 8", "tn_network_source_accounting_enabled 1"])
    return "\n".join(lines)


class CollectorTests(unittest.TestCase):
    def test_production_log_retention_preserves_large_raw_input_and_scoped_guard(self):
        with tempfile.TemporaryDirectory(prefix="capacity-log-retention-") as directory:
            root = Path(directory)
            source = root / "validator.jsonl"
            with source.open("wb") as stream:
                stream.write(b"raw diagnostic header\n")
                stream.seek(65 * 1024**2 - 20)
                stream.write(b"raw diagnostic tail\n")
            output = root / "retained"
            output.mkdir()
            artifacts = COLLECT.retain_protocol_logs([source], output)
            retained = output / artifacts[0]["path"]
            self.assertEqual(artifacts[0]["sha256"], COLLECT.file_hash(source))
            self.assertEqual(retained.stat().st_size, source.stat().st_size)
            with source.open("rb") as incoming, retained.open("rb") as copied:
                for chunk in iter(lambda: incoming.read(1024 * 1024), b""):
                    self.assertEqual(copied.read(len(chunk)), chunk)
                self.assertEqual(copied.read(1), b"")
            COLLECT.QUALIFY.verify_artifacts({"artifacts": artifacts}, output)
            with self.assertRaisesRegex(ValueError, "exceeds"):
                COLLECT.retain_file(source, output / "topology.json")
            self.assertFalse((output / "topology.json").exists())
            for name in ("workload.log", "protocol-00.jsonl.extra", "unrelated-protocol-00.jsonl"):
                retained.rename(output / name)
                with self.subTest(name=name), self.assertRaisesRegex(ValueError, "exceeds"):
                    COLLECT.QUALIFY.verify_artifacts({"artifacts": [
                        {"path": name, "sha256": artifacts[0]["sha256"]},
                    ]}, output)
                (output / name).rename(retained)

    def test_production_log_budget_exhaustion_removes_partial_copy(self):
        with tempfile.TemporaryDirectory(prefix="capacity-log-exhaustion-") as directory:
            root = Path(directory)
            source = root / "validator.jsonl"
            with source.open("wb") as stream:
                stream.truncate(1024**2 + 1)
            output = root / "retained"
            output.mkdir()
            with mock.patch.object(COLLECT.QUALIFY, "MAX_PROTOCOL_LOG_BYTES", 1024**2):
                with self.assertRaisesRegex(ValueError, "exceeds"):
                    COLLECT.retain_protocol_logs([source], output)
                self.assertEqual(list(output.iterdir()), [])
                # Even a supplied matching hash cannot make an oversized log pass scoring.
                source.rename(output / "protocol-00.jsonl")
                artifact = {"path": "protocol-00.jsonl", "sha256": COLLECT.file_hash(output / "protocol-00.jsonl")}
                with self.assertRaisesRegex(ValueError, "exceeds"):
                    COLLECT.QUALIFY.verify_artifacts({"artifacts": [artifact]}, output)

    def test_production_log_512mib_limit_rejects_next_byte_without_partial_copy(self):
        self.assertEqual(COLLECT.QUALIFY.MAX_PROTOCOL_LOG_BYTES, 512 * 1024**2)
        with tempfile.TemporaryDirectory(prefix="capacity-log-boundary-") as directory:
            root = Path(directory)
            source = root / "validator.jsonl"
            with source.open("wb") as stream:
                stream.truncate(COLLECT.QUALIFY.MAX_PROTOCOL_LOG_BYTES + 1)
            self.assertEqual(source.stat().st_size, 536870913)
            output = root / "retained"
            output.mkdir()
            with self.assertRaisesRegex(ValueError, "exceeds 512 MiB"):
                COLLECT.retain_protocol_logs([source], output)
            self.assertEqual(list(output.iterdir()), [])
            source.rename(output / "protocol-00.jsonl")
            with self.assertRaisesRegex(ValueError, "exceeds 512 MiB"):
                COLLECT.QUALIFY.verify_artifacts({"artifacts": [
                    {"path": "protocol-00.jsonl", "sha256": "0" * 64},
                ]}, output)

    def test_rejected_empty_copy_and_existing_destination_remain_distinct(self):
        with tempfile.TemporaryDirectory(prefix="capacity-log-copy-") as directory:
            root = Path(directory)
            source, destination = root / "source", root / "retained"
            source.write_bytes(b"")
            with self.assertRaisesRegex(ValueError, "empty"):
                COLLECT.retain_file(source, destination)
            self.assertFalse(destination.exists())
            destination.write_bytes(b"existing evidence")
            source.write_bytes(b"replacement")
            with self.assertRaises(FileExistsError):
                COLLECT.retain_file(source, destination)
            self.assertEqual(destination.read_bytes(), b"existing evidence")

    def test_operation_finishing_during_metrics_fetch_is_captured(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = root / "profile.json"
            topology = root / "topology.json"
            protocol_log = root / "validator.jsonl"
            profile.write_text("{}")
            topology.write_text("{}")
            protocol_log.write_bytes(b"synthetic production log\n")
            phase = {"revision": "a" * 40, "profile": {},
                     "binary_sha256": {"telcoin-network": "b" * 64}}
            plan = {"hubs": ["hub"], "baseline": phase, "adapter_command": "synthetic-workload",
                    "envelope": {"duration_seconds": 4, "committee_peers": 1}}
            frozen = {"plan": plan, "plan_sha256": COLLECT.QUALIFY.digest(plan)}
            bindings = {"hubs": {"hub": {"revision": phase["revision"], "profile_path": str(profile),
                        "pid": 42, "metrics_url": "http://synthetic.invalid/metrics",
                        "progress": {"name": "synthetic_progress"}}},
                        "workload": ["synthetic-workload"], "topology_artifact": str(topology),
                        "protocol_logs": [str(protocol_log)]}
            output = root / "evidence"
            clock = {"time": 0.0, "reads": 0, "completed": False}

            def metrics_read(_maximum):
                clock["reads"] += 1
                if clock["reads"] == 3:
                    clock["time"] += 0.5
                    clock["completed"] = True
                    operation = {"scenario": "record_lookup", "id": "closing-operation",
                                 "success": False, "latency_ms": 500,
                                 "elapsed_seconds": clock["time"], "rejection_reason": "fixture"}
                    (output / "operations.jsonl").write_text(json.dumps(operation) + "\n")
                return b""

            def sleep(seconds):
                clock["time"] += seconds

            child = mock.Mock()
            child.poll.side_effect = lambda: 0 if clock["completed"] else None
            child.wait.return_value = 0
            response = mock.MagicMock()
            response.__enter__.return_value = response
            response.read.side_effect = metrics_read
            # The synthetic deployment bypasses hardware validation. Exercise the real
            # collection loop and retained operation timestamps, without qualifying capacity.
            with mock.patch.object(COLLECT.QUALIFY, "validate_plan"), \
                 mock.patch.object(COLLECT.QUALIFY, "validate_evidence"), \
                 mock.patch.object(COLLECT, "validate_process"), \
                 mock.patch.object(COLLECT, "file_hash", return_value="b" * 64), \
                 mock.patch.object(COLLECT, "process_sample", return_value=(
                     {"rss_bytes": 1, "cpu_seconds": 0}, 1, "synthetic proc stat")), \
                 mock.patch.object(COLLECT, "observations", return_value={}), \
                 mock.patch.object(COLLECT.subprocess, "Popen", return_value=child), \
                 mock.patch.object(COLLECT.urllib.request, "urlopen", return_value=response), \
                 mock.patch.object(COLLECT.time, "monotonic", side_effect=lambda: clock["time"]), \
                 mock.patch.object(COLLECT.time, "sleep", side_effect=sleep):
                evidence = COLLECT.collect(frozen, bindings, "baseline", output)
            operation = evidence["operations"]["record_lookup"][0]
            self.assertEqual(operation["elapsed_seconds"], 4.5)
            self.assertFalse(operation["success"])
            self.assertGreaterEqual(evidence["samples"][-1]["elapsed_seconds"], operation["elapsed_seconds"])
            self.assertEqual(clock["reads"], 4)
            self.assertEqual((output / "protocol-00.jsonl").read_bytes(), protocol_log.read_bytes())
            self.assertIn({"path": "protocol-00.jsonl", "sha256": "b" * 64}, evidence["artifacts"])

    def test_full_mapping_and_worker_omission(self):
        binding = {"progress": {"name": "progress"}, "dao_connected": {"name": "dao"}}
        parsed = COLLECT.parse_metrics(telemetry())
        result = COLLECT.observations(parsed, binding, "candidate")
        self.assertEqual(result["tasks"]["batch_stream"], 2)
        self.assertEqual(result["tasks"]["epoch_stream"], 1)
        self.assertEqual(result["swarms"]["worker-1"]["queue_occupancy"], 10)
        self.assertEqual(result["source_rows"], 21)
        self.assertEqual(result["dao_connected"], 8)
        self.assertEqual(sum(result["swarms"]["worker-0"]["rejections"].values()), 2)
        incomplete = "\n".join(line for line in telemetry().splitlines() if 'network="worker-1"' not in line)
        with self.assertRaisesRegex(ValueError, "missing metric"):
            COLLECT.observations(COLLECT.parse_metrics(incomplete), binding, "candidate")

    def test_capacity_sampling_preserves_required_validation(self):
        binding = {"progress": {"name": "progress"}}
        expected = COLLECT.observations(COLLECT.parse_metrics(telemetry()), binding, "candidate")
        raw = telemetry() + '\nreth_unrelated_histogram_bucket{le="+Inf"} 9000'
        selected = COLLECT.capacity_metrics(raw, "progress")
        self.assertEqual(COLLECT.observations(selected, binding, "candidate"), expected)
        self.assertNotIn(("reth_unrelated_histogram_bucket", (("le", "+Inf"),)), selected)
        for invalid in ("progress NaN", "tn_network_source_address_rows 3",
                        "tn_network_connections{broken} 1"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                COLLECT.capacity_metrics(raw + "\n" + invalid, "progress")

    def test_duplicate_nonfinite_and_malformed_metrics(self):
        for text in ('metric 1\nmetric 2', 'metric NaN', 'metric inf', 'metric{a="x",a="y"} 1',
                     'metric{broken} 1'):
            with self.subTest(text=text), self.assertRaises(ValueError):
                COLLECT.parse_metrics(text)
        escaped = COLLECT.parse_metrics('metric{a="quote\\\" and slash\\\\"} 2')
        self.assertEqual(COLLECT.select(escaped, "metric", {"a": 'quote" and slash\\'}), 2)
        with self.assertRaisesRegex(ValueError, "integer"):
            COLLECT.select(COLLECT.parse_metrics('metric 1.5'), "metric")

    def test_process_cpu_rss_and_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            process = Path(directory) / "42"
            process.mkdir()
            fields = ["0"] * 22
            fields[0], fields[11], fields[12], fields[19], fields[21] = "S", "200", "50", "1000", "256"
            (process / "stat").write_text("42 (comm with ) spaces) " + " ".join(fields))
            sample, identity, raw = COLLECT.process_sample(42, Path(directory), ticks=100, page_size=4096)
            self.assertEqual(sample, {"rss_bytes": 1048576, "cpu_seconds": 2.5})
            self.assertEqual(identity, 1000)
            self.assertIn("comm with ) spaces", raw)
            fields[19] = "1001"
            (process / "stat").write_text("42 (replacement) " + " ".join(fields))
            self.assertNotEqual(COLLECT.process_sample(42, Path(directory), 100, 4096)[1], identity)

    def test_raw_artifacts_and_failed_operations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = COLLECT.RawLog(root)
            log.append({"metric": "raw evidence"})
            artifacts = log.artifacts()
            self.assertEqual(artifacts[0]["sha256"], COLLECT.file_hash(root / artifacts[0]["path"]))
            operations = root / "operations.jsonl"
            failed = {"scenario": "record_lookup", "id": "1", "success": False,
                      "latency_ms": 100, "elapsed_seconds": 1, "rejection_reason": "timeout"}
            operations.write_text(json.dumps(failed) + "\n")
            result = COLLECT.read_operations(operations)
            self.assertFalse(result["record_lookup"][0]["success"])
            self.assertEqual(result["record_lookup"][0]["rejection_reason"], "timeout")
            self.assertEqual(set(result), COLLECT.QUALIFY.SCENARIOS)


if __name__ == "__main__":
    unittest.main()
