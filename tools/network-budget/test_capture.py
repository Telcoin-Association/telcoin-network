"""Regression tests for bounded, honest calibration evidence."""

import argparse
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import capture


class CaptureTests(unittest.TestCase):
    """Reject ambiguous samples and preserve scrape failure evidence."""

    def test_bounded_observations(self):
        text = '\n'.join([
            'tn_network_established_connections{network="primary"} 3',
            'tn_network_established_connections{network="worker-0"} 4',
            'reth_process_resident_memory_bytes 1024',
            'unrelated_metric{peer="arbitrary"} 99',
        ])
        values = capture.observations(text, 1)
        self.assertEqual([item["value"] for item in values], [3, 4, 1024])
        self.assertEqual(values[1]["labels"], {"network": "worker-0"})

    def test_process_metrics_use_the_exporter_prefix(self):
        # The exporter renders process metrics with the `reth` prefix; bare names are not exported.
        values = capture.observations('process_resident_memory_bytes 1\nreth_process_cpu_seconds_total 2', 1)
        self.assertEqual([item["metric"] for item in values], ["reth_process_cpu_seconds_total"])

    def test_class_metrics_in_summary_and_bucket_form(self):
        text = '\n'.join([
            'tn_network_inbound_requests_pending{network="primary",class="vote"} 2',
            'tn_network_inbound_requests_shed_total{network="worker-0",class="batch",reason="queue_full"} 5',
            'tn_network_inbound_request_service_seconds{network="primary",class="vote",quantile="0.99"} 0.25',
            'tn_network_inbound_request_service_seconds_bucket{network="primary",class="epoch_record",le="+Inf"} 7',
            'tn_network_inbound_request_service_seconds_bucket{network="primary",class="epoch_record",le="0.5"} 6',
            'tn_network_inbound_request_service_seconds_sum{network="primary",class="vote"} 1.5',
            'tn_network_inbound_request_service_seconds_count{network="primary",class="vote"} 7',
        ])
        values = capture.observations(text, 1)
        self.assertEqual(len(values), 7)
        self.assertEqual(values[1]["labels"], {"network": "worker-0", "class": "batch", "reason": "queue_full"})
        missing = capture.missing_metrics(values)
        self.assertNotIn(capture.SERVICE, missing)
        self.assertIn("reth_process_resident_memory_bytes", missing)
        self.assertIn(capture.SERVICE, capture.missing_metrics(values[:2]))

    def test_block_number_is_a_quantity_or_missing(self):
        replies = [io.BytesIO(b'{"jsonrpc":"2.0","id":1,"result":"0x2a"}'), io.BytesIO(b'{"result":null}'),
                   io.BytesIO(b'[]'), OSError("refused")]
        with patch.object(capture, "urlopen", side_effect=replies):
            found = [capture.block_number("http://localhost:8545") for _ in replies]
        self.assertEqual(found[0], {"number": 42})
        self.assertEqual([sorted(item) for item in found[1:]], [["missing"]] * 3)

    def test_rejects_unbounded_and_ambiguous_samples(self):
        invalid = [
            'tn_network_established_connections{network="worker-1"} 1',
            'tn_network_established_connections{network="primary",peer="a"} 1',
            'tn_network_established_connections{network="primary",network="primary"} 1',
            'tn_network_established_connections{network="primary"} NaN',
            'tn_network_established_connections{network="primary"} -1',
            'reth_process_resident_memory_bytes 1\nreth_process_resident_memory_bytes 2',
            'reth_process_resident_memory_bytes{network="primary"} 1',
            'tn_network_inbound_requests_pending{network="primary",class="peer-a"} 1',
            'tn_network_inbound_requests_pending{network="primary"} 1',
            'tn_network_inbound_requests_shed_total{network="primary",class="vote",reason="other"} 1',
            'tn_network_inbound_request_service_seconds{network="primary",class="vote",quantile="1.5"} 1',
            'tn_network_inbound_request_service_seconds_bucket{network="primary",class="vote",le="-Inf"} 1',
            'tn_network_inbound_request_service_seconds_bucket{network="primary",class="vote",le="nan"} 1',
            'unrelated_metric 10',
        ]
        for text in invalid:
            with self.subTest(text=text), self.assertRaises(ValueError):
                capture.observations(text, 1)

    def test_capture_pins_artifacts_and_records_missing_and_failed_scrapes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "configuration.json"
            artifact.write_text("{}")
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "revision": "0" * 40, "build_command": "fixture only",
                "topology": {"validators": 1, "workers_per_node": 1, "cpus_per_node": 8, "ram_bytes_per_node": 32 * 1024**3},
                "nodes": [{"name": "node-0", "metrics_url": "http://localhost/metrics"}],
                "artifacts": [artifact.name], "workload": "synthetic collector fixture",
                "decisions": "production thresholds pending",
            }))
            args = argparse.Namespace(manifest=manifest, output=root / "output", phase="baseline", samples=2, interval=0.001)
            with patch.object(capture, "urlopen", side_effect=[io.BytesIO(b"reth_process_resident_memory_bytes 1024\n"), OSError("unavailable")]), patch.object(capture.time, "sleep"):
                self.assertEqual(capture.capture(args), 1)
            records = [json.loads(line) for line in (args.output / "observations.jsonl").read_text().splitlines()]
            self.assertEqual(records[0]["missing_networks"], ["primary", "worker-0"])
            self.assertIn("tn_network_established_connections", records[0]["missing_metrics"])
            self.assertEqual(records[1]["error"], "unavailable")
            self.assertNotIn("observations", records[1])
            self.assertEqual([record["block"] for record in records], [{"missing": "no rpc_url"}] * 2)
            result = json.loads((args.output / "result.json").read_text())
            self.assertEqual(result, {"failed_scrapes": 1, "acceptance": "pending"})
            provenance = json.loads((args.output / "manifest.json").read_text())
            self.assertEqual(provenance["artifacts"], [capture.file_record(artifact)])
            with self.assertRaises(FileExistsError):
                capture.capture(args)

    def test_capture_polls_blocks_and_validates_rpc_url(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "configuration.json").write_text("{}")
            node = {"name": "node-0", "metrics_url": "http://localhost/metrics", "rpc_url": "http://localhost:8545"}

            def manifest(nodes):
                path = root / "manifest.json"
                path.write_text(json.dumps({
                    "revision": "0" * 40, "build_command": "fixture only",
                    "topology": {"validators": 1, "workers_per_node": 1, "cpus_per_node": 8, "ram_bytes_per_node": 1024},
                    "nodes": nodes, "artifacts": ["configuration.json"], "workload": "synthetic collector fixture",
                    "decisions": "production thresholds pending",
                }))
                return path

            args = argparse.Namespace(manifest=manifest([node]), output=root / "output", phase="steady", samples=1, interval=0.001)
            replies = [io.BytesIO(b"reth_process_resident_memory_bytes 1024\n"), io.BytesIO(b'{"result":"0x10"}')]
            with patch.object(capture, "urlopen", side_effect=replies):
                self.assertEqual(capture.capture(args), 0)
            self.assertEqual(json.loads((args.output / "observations.jsonl").read_text())["block"], {"number": 16})
            for bad in ({**node, "rpc_url": "file:///etc/hosts"}, {**node, "peer": "extra"}):
                refused = argparse.Namespace(manifest=manifest([bad]), output=root / "refused", phase="steady", samples=1, interval=1)
                with self.subTest(node=bad), self.assertRaises(ValueError):
                    capture.capture(refused)


if __name__ == "__main__":
    unittest.main()
