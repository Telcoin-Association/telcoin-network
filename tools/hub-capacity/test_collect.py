"""Collector rejection tests use synthetic proc and metric fixtures, never capacity evidence."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


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
