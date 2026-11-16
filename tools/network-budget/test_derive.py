"""Tests that derivation refuses missing inputs and reproduces the node allocation."""

import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

import derive
import evaluate
from test_evaluate import everywhere, item, record, write_run

TRANSPORT = {"peak_connections_per_peer": 2, "peak_inbound_streams_per_connection": 10,
             "peak_receive_credit_bytes_per_connection": 1000, "source": "fixture trace"}


class DeriveTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.transport = self.root / "transport.json"
        self.transport.write_text(json.dumps(TRANSPORT))

    def tearDown(self):
        self.directory.cleanup()

    def honest(self, phases=derive.HONEST_PHASES):
        for phase in phases:
            write_run(self.root, "baseline", phase, everywhere(0, 0, [
                item(derive.ESTABLISHED, 3), {"metric": derive.ESTABLISHED, "labels": {"network": "worker-0"}, "value": 5}],
                complete_swarms=False))

    def test_allocation_matches_the_node_rule(self):
        self.assertEqual(derive.allocate({"swarm_count": 3, "max_established_connections": 10,
                                          "max_established_connections_per_peer": 9, "max_inbound_streams": 19,
                                          "max_receive_credit_bytes": 90}),
                         {"connections": 3, "connections_per_peer": 3, "streams_per_connection": 2, "receive_credit_per_connection": 10})
        with self.assertRaises(ValueError):
            derive.allocate({"swarm_count": 3, "max_established_connections": 2, "max_established_connections_per_peer": 1,
                             "max_inbound_streams": 9, "max_receive_credit_bytes": 9})

    def test_derivation_scales_peaks_and_stays_proposed(self):
        self.honest()
        result = derive.derive(self.root, self.transport, 1.5)
        # The public peak scales to 8, and the smallest per-swarm total whose public share holds 8 is 12.
        self.assertEqual(result["process_budget"], {"swarm_count": 2, "max_established_connections": 24,
                                                    "max_established_connections_per_peer": 3,
                                                    "max_inbound_streams": 360, "max_receive_credit_bytes": 36000})
        self.assertEqual(result["allocation"], {"connections": 12, "connections_per_peer": 3,
                                                "streams_per_connection": 15, "receive_credit_per_connection": 1500})
        self.assertEqual(result["public_share_per_swarm"], 8)
        self.assertEqual((result["status"], result["acceptance"]), ("proposed", "pending maintainer decision"))
        self.assertEqual(len(result["inputs"]["baseline"]), 3)
        output = self.root / "derived.json"
        self.assertEqual(derive.main([str(self.root), str(self.transport), "--headroom", "1.5", "--output", str(output)]), 0)
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            derive.main([str(self.root), str(self.transport), "--headroom", "1.5", "--output", str(output)])

    def test_derived_total_keeps_the_privileged_reserve_above_the_public_peak(self):
        for peak in (1, 8, 16, 50, 78, 128):
            total = derive.total_for_public(peak)
            self.assertGreaterEqual(derive.public_share(total), peak)
            self.assertLess(derive.public_share(total - 1), peak)
        self.assertEqual((derive.public_share(128), derive.public_share(72), derive.public_share(12)), (78, 44, 8))

    def test_refuses_missing_or_invalid_inputs(self):
        with self.assertRaisesRegex(ValueError, "reconnect"):
            derive.derive(self.root, self.transport, 1.5)
        for phase in derive.HONEST_PHASES:
            write_run(self.root, "baseline", phase, everywhere(0, 0, [item(derive.ESTABLISHED, 3)], complete_swarms=False))
        with self.assertRaisesRegex(ValueError, "worker-0"):
            derive.derive(self.root, self.transport, 1.5)
        # derive.py and evaluate.py name the same gap when a node has no samples.
        partial = self.root / "partial"
        for phase in derive.HONEST_PHASES:
            write_run(partial, "baseline", phase, [record(0, "node-0", 0, [item(derive.ESTABLISHED, 3)])])
        with self.assertRaisesRegex(ValueError, "no samples for node-1"):
            derive.derive(partial, self.transport, 1.5)
        self.assertIn("no samples for node-1", evaluate.gap(evaluate.load_run(partial, "baseline", derive.HONEST_PHASES[0]), "baseline"))
        for headroom in (0.5, float("inf")):
            with self.subTest(headroom=headroom), self.assertRaises(ValueError):
                derive.derive(self.root, self.transport, headroom)
        for broken in ({**TRANSPORT, "source": ""}, {**TRANSPORT, "peak_connections_per_peer": 0},
                       {key: value for key, value in TRANSPORT.items() if key != "source"}):
            self.transport.write_text(json.dumps(broken))
            with self.subTest(transport=broken), self.assertRaises(ValueError):
                derive.derive(self.root, self.transport, 1.5)


if __name__ == "__main__":
    unittest.main()
