"""Check real-traffic selection and rejection contracts without claiming live capacity."""

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


TRAFFIC, WORKLOAD = load("traffic"), load("workload")


class TrafficTests(unittest.TestCase):
    def test_bulk_rejects_missing_worker_or_empty_transfer(self):
        digests = ["0x" + f"{number:064x}" for number in range(1, 5)]
        valid = {"completed": True, "transfers": [
            {"swarm": "primary", "completed": True, "bytes": 10},
            *[{"swarm": role, "completed": True, "bytes": 131072, "batch_digests": digests.copy()}
              for role in ("worker-0", "worker-1")]]}
        WORKLOAD.validate_bulk_trace(valid)
        variants = []
        missing = copy.deepcopy(valid)
        missing["transfers"][2]["swarm"] = "worker-0"
        variants.append(missing)
        empty = copy.deepcopy(valid)
        empty["transfers"][1]["bytes"] = 0
        variants.append(empty)
        tiny = copy.deepcopy(valid)
        tiny["transfers"][1]["bytes"] = 1
        variants.append(tiny)
        wrong = copy.deepcopy(valid)
        wrong["transfers"][2]["batch_digests"][0] = "0x" + "f" * 64
        variants.append(wrong)
        for invalid in variants:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                WORKLOAD.validate_bulk_trace(invalid)

    def test_selects_completed_epoch_with_real_transactions(self):
        blocks = [{"nonce": hex((epoch << 32) | number), "sha3Uncles": "0x" + f"{number:064x}",
                   "transactions": ["fixture"] if populated else []}
                  for number, epoch, populated in [(1, 0, False), (2, 1, True), (3, 1, True),
                      (4, 1, True), (5, 1, True), (6, 2, True)]]
        def rpc(_url, method, parameters):
            if method == "eth_blockNumber":
                return hex(len(blocks))
            return blocks[int(parameters[0], 16) - 1]
        with tempfile.TemporaryDirectory() as temporary, patch.object(TRAFFIC, "wait_chain"), patch.object(TRAFFIC, "rpc", side_effect=rpc):
            root = Path(temporary)
            TRAFFIC.targets("http://10.147.0.10:8545", root / "targets.json", root / "observations.json")
            selected = json.loads((root / "targets.json").read_text())
            self.assertEqual(selected["sync_epoch"], 1)
            self.assertEqual(selected["batch_digests"], [block["sha3Uncles"] for block in blocks[1:5]])
            self.assertEqual(len(json.loads((root / "observations.json").read_text())["blocks"]), 6)
            blocks[-1]["nonce"] = hex((1 << 32) | 6)
            with self.assertRaisesRegex(ValueError, "no completed epoch"):
                TRAFFIC.targets("http://10.147.0.10:8545", root / "invalid.json", root / "invalid-observations.json")


if __name__ == "__main__":
    unittest.main()
