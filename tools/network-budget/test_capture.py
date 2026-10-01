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
            'process_resident_memory_bytes 1024',
            'unrelated_metric{peer="arbitrary"} 99',
        ])
        values = capture.observations(text, 1)
        self.assertEqual([item["value"] for item in values], [3, 4, 1024])
        self.assertEqual(values[1]["labels"], {"network": "worker-0"})

    def test_rejects_unbounded_and_ambiguous_samples(self):
        invalid = [
            'tn_network_established_connections{network="worker-1"} 1',
            'tn_network_established_connections{network="primary",peer="a"} 1',
            'tn_network_established_connections{network="primary",network="primary"} 1',
            'tn_network_established_connections{network="primary"} NaN',
            'tn_network_established_connections{network="primary"} -1',
            'process_resident_memory_bytes 1\nprocess_resident_memory_bytes 2',
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
            with patch.object(capture, "urlopen", side_effect=[io.BytesIO(b"process_resident_memory_bytes 1024\n"), OSError("unavailable")]), patch.object(capture.time, "sleep"):
                self.assertEqual(capture.capture(args), 1)
            records = [json.loads(line) for line in (args.output / "observations.jsonl").read_text().splitlines()]
            self.assertEqual(records[0]["missing_networks"], ["primary", "worker-0"])
            self.assertIn("tn_network_established_connections", records[0]["missing_metrics"])
            self.assertEqual(records[1]["error"], "unavailable")
            self.assertNotIn("observations", records[1])
            result = json.loads((args.output / "result.json").read_text())
            self.assertEqual(result, {"failed_scrapes": 1, "acceptance": "pending"})
            provenance = json.loads((args.output / "manifest.json").read_text())
            self.assertEqual(provenance["artifacts"], [capture.file_record(artifact)])
            with self.assertRaises(FileExistsError):
                capture.capture(args)


if __name__ == "__main__":
    unittest.main()
