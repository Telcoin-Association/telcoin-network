#!/usr/bin/env python3
"""Create offline chain-4476 fixtures and drive real isolated transaction and batch traffic."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request


CHAIN = 4476
ADDRESS = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
# Public Anvil test key, funded only by the isolated chain-4476 genesis.
KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
SUBMISSION_ATTEMPTS = 3
SUBMISSION_RETRY_SECONDS = 0.1
SEED_GROUP_SIZE = 4
SEED_GROUPS = 4
SEED_INCLUSION_TIMEOUT = 60
SEED_INCLUSION_POLL_SECONDS = 0.25


def rpc(url, method, parameters, timeout=10):
    """Read a bounded real RPC response, preserving protocol failures."""
    request = urllib.request.Request(url, json.dumps({"jsonrpc": "2.0", "id": 1,
        "method": method, "params": parameters}).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read(2 * 1024**2 + 1)
    if len(raw) > 2 * 1024**2:
        raise ValueError("transaction RPC response exceeds 2 MiB")
    result = json.loads(raw)
    if result.get("error") or "result" not in result:
        raise ValueError(f"transaction RPC rejected {method}: {result.get('error')}")
    return result["result"]


def submit(url, raw, expected_hash, attempts):
    """Retry uncertain transport delivery of the same signed transaction, retaining every attempt."""
    for number in range(1, SUBMISSION_ATTEMPTS + 1):
        try:
            result = rpc(url, "eth_sendRawTransaction", [raw])
            if result != expected_hash:
                raise ValueError("transaction RPC did not acknowledge the exact signed transaction")
        except (OSError, ValueError) as error:
            attempts.append({"attempt": number, "success": False, "error": type(error).__name__})
            transport = isinstance(error, (ConnectionError, TimeoutError)) or (
                isinstance(error, urllib.error.URLError)
                and isinstance(error.reason, (ConnectionError, TimeoutError)))
            if not transport:
                raise
            probe = {"attempt": number, "method": "eth_getTransactionByHash", "success": False}
            try:
                known = rpc(url, "eth_getTransactionByHash", [expected_hash])
                probe["transaction_hash"] = known.get("hash") if isinstance(known, dict) else None
                probe["success"] = probe["transaction_hash"] == expected_hash
            except (OSError, ValueError) as probe_error:
                probe["error"] = type(probe_error).__name__
            attempts.append(probe)
            if probe["success"]:
                return expected_hash
            if number == SUBMISSION_ATTEMPTS:
                raise
            time.sleep(SUBMISSION_RETRY_SECONDS)
        else:
            attempts.append({"attempt": number, "success": True})
            return result


def create(cast, output):
    """Sign all declared inputs offline before freezing the run, without contacting any RPC."""
    def sign(nonce):
        data = "0x" + hashlib.shake_256(f"tn-capacity-4476-{nonce}".encode()).hexdigest(32768)
        argv = [cast, "mktx", ADDRESS, data, "--private-key", KEY, "--chain", str(CHAIN),
                "--nonce", str(nonce), "--gas-limit", "2000000", "--gas-price", "10000000000",
                "--legacy", "--rpc-url", "http://127.0.0.1:1"]
        raw = subprocess.run(argv, check=True, capture_output=True, timeout=10).stdout.decode().strip()
        if not raw.startswith("0x") or len(raw) > 68000:
            raise ValueError("offline transaction exceeds its 34 KiB wire bound")
        transaction_hash = subprocess.run([cast, "keccak", raw], check=True,
            capture_output=True, timeout=10).stdout.decode().strip()
        if not transaction_hash.startswith("0x") or len(transaction_hash) != 66:
            raise ValueError("offline transaction hash is not a Keccak-256 digest")
        return raw, transaction_hash
    version = subprocess.run([cast, "--version"], check=True, capture_output=True, timeout=10).stdout.decode().strip()
    with ThreadPoolExecutor(max_workers=8) as pool:
        signed = list(pool.map(sign, range(512)))
    transactions, transaction_hashes = map(list, zip(*signed))
    output.write_text(json.dumps({"chain_id": CHAIN, "sender": ADDRESS, "cast_version": version,
        "count": 512, "calldata_bytes": 32768, "initial_count": 128, "stream_count": 384,
        "stream_duration_seconds": 600, "initial_interval_seconds": 0.5,
        "initial_fenced_groups": SEED_GROUPS, "initial_group_size": SEED_GROUP_SIZE,
        "initial_inclusion_timeout_seconds": SEED_INCLUSION_TIMEOUT,
        "initial_inclusion_poll_seconds": SEED_INCLUSION_POLL_SECONDS,
        "submission_attempts": SUBMISSION_ATTEMPTS,
        "submission_retry_seconds": SUBMISSION_RETRY_SECONDS,
        "batch_selection": "first four distinct executed nonempty batches from completed epochs; retain each source epoch",
        "transaction_hashes": transaction_hashes,
        "transactions": transactions}, separators=(",", ":")) + "\n")


def wait_chain(url):
    """Wait only for the owned isolated chain and reject any other chain identity."""
    if url not in ("http://10.147.0.10:8545", "http://10.147.0.11:8545"):
        raise ValueError("transaction traffic requires an owned private qualification hub")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            chain = int(rpc(url, "eth_chainId", []), 16)
        except (OSError, ValueError):
            time.sleep(0.25)
        else:
            if chain != CHAIN:
                raise ValueError("transaction fixture is restricted to chain 4476")
            return
    raise TimeoutError("isolated transaction RPC did not become ready")


def wait_canonical_inclusion(url, expected_hash, attempts, timeout=SEED_INCLUSION_TIMEOUT):
    """Bound warmup fences by a real receipt and its matching canonical block."""
    deadline = time.monotonic() + timeout

    def read(method, parameters):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("seed transaction did not reach canonical inclusion before its deadline")
        return rpc(url, method, parameters, timeout=min(10, remaining))

    while time.monotonic() < deadline:
        attempt = {"unix_us": time.time_ns() // 1000}
        attempts.append(attempt)
        receipt = read("eth_getTransactionReceipt", [expected_hash])
        attempt["receipt"] = receipt
        if receipt is not None:
            if receipt["transactionHash"] != expected_hash:
                raise ValueError("seed receipt does not identify the exact signed transaction")
            block = read("eth_getBlockByNumber", [receipt["blockNumber"], False])
            attempt["canonical_block"] = block
            if block is None or block["number"] != receipt["blockNumber"] or block["hash"] != receipt["blockHash"] or expected_hash not in block["transactions"]:
                raise ValueError("seed receipt does not match an observed canonical block")
            return {"transaction_hash": expected_hash, "block_number": receipt["blockNumber"],
                    "block_hash": receipt["blockHash"]}
        time.sleep(min(SEED_INCLUSION_POLL_SECONDS, max(0, deadline - time.monotonic())))
    raise TimeoutError("seed transaction did not reach canonical inclusion before its deadline")


def feed(fixture, url, output, stream, pid_file):
    """Submit real transactions at the declared cadence and retain each acknowledgement."""
    wait_chain(url)
    if fixture.stat().st_size > 40 * 1024**2:
        raise ValueError("signed transaction fixture exceeds 40 MiB")
    inputs = json.loads(fixture.read_text())
    if inputs["chain_id"] != CHAIN or inputs["count"] != 512 or len(inputs["transactions"]) != 512 \
            or len(inputs["transaction_hashes"]) != 512:
        raise ValueError("transaction fixture does not match the declared workload")
    start, end, interval = (128, 512, 600 / 384) if stream else (0, 128, 0.5)
    if pid_file:
        pid_file.write_text(str(os.getpid()))
    origin = time.monotonic()
    with output.open("x") as log:
        for nonce in range(start, end):
            time.sleep(max(0, origin + (nonce - start) * interval - time.monotonic()))
            raw = inputs["transactions"][nonce]
            row = {"nonce": nonce, "raw_sha256": hashlib.sha256(raw.encode()).hexdigest(),
                   "success": False, "attempts": []}
            try:
                row["transaction_hash"] = submit(url, raw, inputs["transaction_hashes"][nonce], row["attempts"])
                row["success"] = True
                if not stream and nonce < SEED_GROUP_SIZE * SEED_GROUPS and (nonce + 1) % SEED_GROUP_SIZE == 0:
                    before = time.monotonic()
                    row["inclusion_attempts"] = []
                    row["canonical_inclusion"] = wait_canonical_inclusion(url, row["transaction_hash"], row["inclusion_attempts"])
                    origin += time.monotonic() - before
            finally:
                row["unix_us"] = time.time_ns() // 1000
                log.write(json.dumps(row) + "\n")
                log.flush()


def targets(url, output, observations):
    """Select the first four distinct executed batches from completed epochs, retaining provenance."""
    wait_chain(url)
    height = int(rpc(url, "eth_blockNumber", []), 16)
    if height > 2048:
        raise ValueError("warmup block observations exceed the declared 2048-block bound")
    blocks = [rpc(url, "eth_getBlockByNumber", [hex(number), False]) for number in range(1, height + 1)]
    observations.write_text(json.dumps({"rpc_url": url, "height": height, "blocks": blocks}, separators=(",", ":")) + "\n")
    if not blocks or any(block is None for block in blocks):
        raise ValueError("warmup has no complete canonical block observations")
    current_epoch = int(blocks[-1]["nonce"], 16) >> 32
    by_digest = {}
    for block in blocks:
        epoch = int(block["nonce"], 16) >> 32
        if epoch < current_epoch and block["transactions"]:
            by_digest.setdefault(block["sha3Uncles"], epoch)
    selected = list(by_digest.items())[:4]
    if len(selected) != 4:
        raise ValueError("warmup has no completed epochs with four executed nonempty batches")
    output.write_text(json.dumps({"sync_epoch": selected[0][1],
        "batch_digests": [digest for digest, _epoch in selected],
        "batch_epochs": dict(selected)}, separators=(",", ":")) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("create", "initial", "stream", "targets"))
    parser.add_argument("--cast")
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--url", default="http://10.147.0.10:8545")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--observations", type=Path)
    parser.add_argument("--pid-file", type=Path)
    args = parser.parse_args()
    signal.signal(signal.SIGINT, lambda *_: sys.exit(130))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    if args.mode == "create":
        create(args.cast, args.output)
    elif args.mode == "targets":
        targets(args.url, args.output, args.observations)
    else:
        feed(args.fixture, args.url, args.output, args.mode == "stream", args.pid_file)


if __name__ == "__main__":
    main()
