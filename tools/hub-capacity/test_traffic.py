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
    def test_seed_fence_waits_for_the_exact_canonical_transaction(self):
        expected = "0x" + "a" * 64
        receipt = {"transactionHash": expected, "blockNumber": "0x2", "blockHash": "0x" + "b" * 64}
        block = {"number": receipt["blockNumber"], "hash": receipt["blockHash"], "transactions": [expected]}
        attempts = []
        with patch.object(TRAFFIC, "rpc", side_effect=[None, receipt, block]) as rpc, \
                patch.object(TRAFFIC.time, "monotonic", return_value=0), patch.object(TRAFFIC.time, "sleep"):
            result = TRAFFIC.wait_canonical_inclusion("http://10.147.0.10:8545", expected, attempts)
        self.assertEqual(result, {"transaction_hash": expected, "block_number": "0x2", "block_hash": receipt["blockHash"]})
        self.assertEqual(attempts[0]["receipt"], None)
        self.assertEqual(attempts[1]["receipt"], receipt)
        self.assertEqual(attempts[1]["canonical_block"], block)
        self.assertEqual([call.args[1] for call in rpc.call_args_list],
                         ["eth_getTransactionReceipt", "eth_getTransactionReceipt", "eth_getBlockByNumber"])

    def test_seed_fence_rejects_wrong_identity_block_membership_and_deadline(self):
        expected = "0x" + "a" * 64
        receipt = {"transactionHash": expected, "blockNumber": "0x2", "blockHash": "0x" + "b" * 64}
        block = {"number": receipt["blockNumber"], "hash": receipt["blockHash"], "transactions": [expected]}
        for replies in ([{**receipt, "transactionHash": "wrong"}],
                        [receipt, {**block, "hash": "wrong"}],
                        [receipt, {**block, "number": "0x3"}],
                        [receipt, {**block, "transactions": []}]):
            attempts = []
            with self.subTest(replies=replies), patch.object(TRAFFIC, "rpc", side_effect=replies), \
                    patch.object(TRAFFIC.time, "monotonic", return_value=0), self.assertRaises(ValueError):
                TRAFFIC.wait_canonical_inclusion("http://10.147.0.10:8545", expected, attempts)
            self.assertEqual(len(attempts), 1)
            self.assertIn("receipt", attempts[0])
        attempts = []
        with patch.object(TRAFFIC, "rpc", return_value=None) as rpc, \
                patch.object(TRAFFIC.time, "monotonic", side_effect=[0, 0, 0, 0.11, 0.11]), \
                patch.object(TRAFFIC.time, "sleep"), self.assertRaises(TimeoutError):
            TRAFFIC.wait_canonical_inclusion("http://10.147.0.10:8545", expected, attempts, timeout=0.1)
        rpc.assert_called_once_with("http://10.147.0.10:8545", "eth_getTransactionReceipt", [expected], timeout=0.1)
        self.assertEqual(attempts[0]["receipt"], None)

    def test_initial_feed_fences_four_small_groups_before_submitting_more(self):
        received, fences = [], []
        hashes = ["0x" + f"{nonce:064x}" for nonce in range(512)]

        def rpc(_url, method, parameters):
            self.assertEqual(method, "eth_sendRawTransaction")
            nonce = int(parameters[0].split("-")[1])
            received.append(nonce)
            return hashes[nonce]

        def fence(_url, expected_hash, attempts):
            nonce = hashes.index(expected_hash)
            self.assertEqual(received[-1], nonce)
            fences.append(nonce)
            attempts.append({"receipt": {"transactionHash": expected_hash}})
            return {"transaction_hash": expected_hash, "block_number": hex(len(fences))}

        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(TRAFFIC, "wait_chain"), patch.object(TRAFFIC, "rpc", side_effect=rpc), \
                patch.object(TRAFFIC, "wait_canonical_inclusion", side_effect=fence, create=True), \
                patch.object(TRAFFIC.time, "monotonic", return_value=0), patch.object(TRAFFIC.time, "sleep"):
            fixture, output = Path(temporary) / "fixture.json", Path(temporary) / "warmup.jsonl"
            fixture.write_text(json.dumps({"chain_id": TRAFFIC.CHAIN, "count": 512,
                "transaction_hashes": hashes, "transactions": [f"signed-{nonce}" for nonce in range(512)]}))
            TRAFFIC.feed(fixture, "http://10.147.0.10:8545", output, False, None)
            rows = [json.loads(line) for line in output.read_text().splitlines()]
        self.assertEqual(received, list(range(128)))
        self.assertEqual(fences, [3, 7, 11, 15])
        self.assertEqual([row["nonce"] for row in rows if "canonical_inclusion" in row], fences)
        self.assertTrue(all(row["success"] for row in rows))

    def test_selects_four_distinct_batches_across_completed_epochs(self):
        blocks = [{"nonce": hex(epoch << 32), "sha3Uncles": "0x" + f"{number:064x}",
                   "transactions": ["fixture"] if number < 5 else []}
                  for number, epoch in enumerate((0, 1, 1, 2, 3), start=1)]

        def rpc(_url, method, parameters):
            if method == "eth_blockNumber":
                return hex(len(blocks))
            return blocks[int(parameters[0], 16) - 1]

        with tempfile.TemporaryDirectory() as temporary, patch.object(TRAFFIC, "wait_chain"), \
                patch.object(TRAFFIC, "rpc", side_effect=rpc):
            output = Path(temporary) / "targets.json"
            TRAFFIC.targets("http://10.147.0.10:8545", output, Path(temporary) / "observations.json")
            selected = json.loads(output.read_text())
        self.assertEqual(selected["sync_epoch"], 0)
        self.assertEqual(selected["batch_digests"], [block["sha3Uncles"] for block in blocks[:4]])
        self.assertEqual(selected["batch_epochs"],
                         {block["sha3Uncles"]: int(block["nonce"], 16) >> 32 for block in blocks[:4]})

    def test_feed_retries_reset_without_dropping_nonces(self):
        transactions = [f"signed-{nonce}" for nonce in range(512)]
        received = []

        def rpc(_url, method, parameters):
            if method == "eth_getTransactionByHash":
                return None
            self.assertEqual(method, "eth_sendRawTransaction")
            received.append(parameters[0])
            if len(received) == 1:
                raise ConnectionResetError("connection reset after submission")
            return "0x" + "a" * 64

        with tempfile.TemporaryDirectory() as directory, patch.object(TRAFFIC, "wait_chain"), \
                patch.object(TRAFFIC.time, "sleep"), patch.object(TRAFFIC, "rpc", rpc):
            fixture, output = Path(directory) / "transactions.json", Path(directory) / "stream.jsonl"
            fixture.write_text(json.dumps({"chain_id": TRAFFIC.CHAIN, "count": 512,
                                          "transaction_hashes": ["0x" + "a" * 64] * 512,
                                          "transactions": transactions}))
            TRAFFIC.feed(fixture, "http://10.147.0.10:8545", output, True, None)
            rows = [json.loads(line) for line in output.read_text().splitlines()]
        self.assertEqual([row["nonce"] for row in rows], list(range(128, 512)))
        self.assertEqual(received, [transactions[128], *transactions[128:]])
        self.assertTrue(all(row["success"] for row in rows))
        self.assertEqual(rows[0]["attempts"], [
            {"attempt": 1, "success": False, "error": "ConnectionResetError"},
            {"attempt": 1, "method": "eth_getTransactionByHash", "success": False,
             "transaction_hash": None},
            {"attempt": 2, "success": True}])

    def test_reset_after_acceptance_verifies_hash_without_resubmitting(self):
        expected_hash = "0x" + "b" * 64
        attempts = []
        with patch.object(TRAFFIC, "rpc", side_effect=[
                ConnectionResetError("response lost"), {"hash": expected_hash}]) as rpc:
            result = TRAFFIC.submit("http://10.147.0.10:8545", "signed", expected_hash, attempts)
        self.assertEqual(result, expected_hash)
        self.assertEqual([call.args[1:] for call in rpc.call_args_list], [
            ("eth_sendRawTransaction", ["signed"]),
            ("eth_getTransactionByHash", [expected_hash])])
        self.assertFalse(attempts[0]["success"])
        self.assertEqual(attempts[1]["transaction_hash"], expected_hash)
        self.assertTrue(attempts[1]["success"])

    def test_unrelated_transaction_cannot_confirm_uncertain_delivery(self):
        attempts = []
        with patch.object(TRAFFIC.time, "sleep"), patch.object(TRAFFIC, "rpc", side_effect=[
                ConnectionResetError("response lost"), {"hash": "0x" + "b" * 64},
                ValueError("transaction rejected")]) as rpc:
            with self.assertRaises(ValueError):
                TRAFFIC.submit("http://10.147.0.10:8545", "signed", "0x" + "a" * 64, attempts)
        self.assertEqual(rpc.call_count, 3)
        self.assertFalse(any(attempt["success"] for attempt in attempts))

    def test_feed_preserves_terminal_transport_failure(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(TRAFFIC, "wait_chain"), \
                patch.object(TRAFFIC.time, "sleep"), \
                patch.object(TRAFFIC, "rpc", side_effect=ConnectionResetError("connection reset")) as rpc:
            fixture, output = Path(directory) / "transactions.json", Path(directory) / "stream.jsonl"
            fixture.write_text(json.dumps({"chain_id": TRAFFIC.CHAIN, "count": 512,
                                          "transaction_hashes": ["0x" + "a" * 64] * 512,
                                          "transactions": ["signed"] * 512}))
            with self.assertRaises(ConnectionResetError):
                TRAFFIC.feed(fixture, "http://10.147.0.10:8545", output, True, None)
            rows = [json.loads(line) for line in output.read_text().splitlines()]
        self.assertEqual(rpc.call_count, 6)
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]["success"])
        self.assertEqual(len(rows[0]["attempts"]), 6)
        self.assertEqual(sum("method" not in attempt for attempt in rows[0]["attempts"]), 3)

    def test_feed_does_not_retry_protocol_rejection(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(TRAFFIC, "wait_chain"), \
                patch.object(TRAFFIC.time, "sleep"), \
                patch.object(TRAFFIC, "rpc", side_effect=ValueError("transaction rejected")) as rpc:
            fixture, output = Path(directory) / "transactions.json", Path(directory) / "stream.jsonl"
            fixture.write_text(json.dumps({"chain_id": TRAFFIC.CHAIN, "count": 512,
                                          "transaction_hashes": ["0x" + "a" * 64] * 512,
                                          "transactions": ["signed"] * 512}))
            with self.assertRaises(ValueError):
                TRAFFIC.feed(fixture, "http://10.147.0.10:8545", output, True, None)
            rows = [json.loads(line) for line in output.read_text().splitlines()]
        self.assertEqual(rpc.call_count, 1)
        self.assertFalse(rows[0]["success"])
        self.assertEqual(rows[0]["attempts"], [{"attempt": 1, "success": False, "error": "ValueError"}])

    def test_failed_target_selection_retains_canonical_observations(self):
        block = {"nonce": "0x0", "sha3Uncles": "0x" + "a" * 64, "transactions": ["fixture"]}
        with tempfile.TemporaryDirectory(prefix="capacity-target-observations-") as directory:
            output, observations = Path(directory) / "targets.json", Path(directory) / "canonical.json"
            with patch.object(TRAFFIC, "rpc", side_effect=[hex(TRAFFIC.CHAIN), "0x1", block]):
                with self.assertRaisesRegex(ValueError, "no completed epoch"):
                    TRAFFIC.targets("http://10.147.0.10:8545", output, observations)
            self.assertEqual(json.loads(observations.read_text())["blocks"], [block])
            self.assertFalse(output.exists())

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
