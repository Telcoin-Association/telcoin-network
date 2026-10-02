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
import urllib.request


CHAIN = 4476
ADDRESS = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
# Public Anvil test key, funded only by the isolated chain-4476 genesis.
KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"


def rpc(url, method, parameters):
    """Read a bounded real RPC response, preserving protocol failures."""
    request = urllib.request.Request(url, json.dumps({"jsonrpc": "2.0", "id": 1,
        "method": method, "params": parameters}).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=10) as response:
        raw = response.read(2 * 1024**2 + 1)
    if len(raw) > 2 * 1024**2:
        raise ValueError("transaction RPC response exceeds 2 MiB")
    result = json.loads(raw)
    if result.get("error") or "result" not in result:
        raise ValueError(f"transaction RPC rejected {method}: {result.get('error')}")
    return result["result"]


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
        return raw
    version = subprocess.run([cast, "--version"], check=True, capture_output=True, timeout=10).stdout.decode().strip()
    with ThreadPoolExecutor(max_workers=8) as pool:
        transactions = list(pool.map(sign, range(512)))
    output.write_text(json.dumps({"chain_id": CHAIN, "sender": ADDRESS, "cast_version": version,
        "count": 512, "calldata_bytes": 32768, "initial_count": 128, "stream_count": 384,
        "stream_duration_seconds": 600, "initial_interval_seconds": 0.05,
        "batch_selection": "first completed epoch with four distinct executed nonempty batches",
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


def feed(fixture, url, output, stream, pid_file):
    """Submit real transactions at the declared cadence and retain each acknowledgement."""
    wait_chain(url)
    if fixture.stat().st_size > 40 * 1024**2:
        raise ValueError("signed transaction fixture exceeds 40 MiB")
    inputs = json.loads(fixture.read_text())
    if inputs["chain_id"] != CHAIN or inputs["count"] != 512 or len(inputs["transactions"]) != 512:
        raise ValueError("transaction fixture does not match the declared workload")
    start, end, interval = (128, 512, 600 / 384) if stream else (0, 128, 0.5)
    if pid_file:
        pid_file.write_text(str(os.getpid()))
    origin = time.monotonic()
    with output.open("x") as log:
        for nonce in range(start, end):
            time.sleep(max(0, origin + (nonce - start) * interval - time.monotonic()))
            raw = inputs["transactions"][nonce]
            result = rpc(url, "eth_sendRawTransaction", [raw])
            log.write(json.dumps({"nonce": nonce, "unix_us": time.time_ns() // 1000,
                "raw_sha256": hashlib.sha256(raw.encode()).hexdigest(), "transaction_hash": result}) + "\n")
            log.flush()


def targets(url, output, observations):
    """Select four real batch digests from the earliest completed epoch containing fixture traffic."""
    wait_chain(url)
    height = int(rpc(url, "eth_blockNumber", []), 16)
    if height > 2048:
        raise ValueError("warmup block observations exceed the declared 2048-block bound")
    blocks = [rpc(url, "eth_getBlockByNumber", [hex(number), False]) for number in range(1, height + 1)]
    observations.write_text(json.dumps({"rpc_url": url, "height": height, "blocks": blocks}, separators=(",", ":")) + "\n")
    if not blocks or any(block is None for block in blocks):
        raise ValueError("warmup has no complete canonical block observations")
    current_epoch = int(blocks[-1]["nonce"], 16) >> 32
    by_epoch = {}
    for block in blocks:
        epoch = int(block["nonce"], 16) >> 32
        if epoch < current_epoch and block["transactions"]:
            by_epoch.setdefault(epoch, set()).add(block["sha3Uncles"])
    selected = next(((epoch, sorted(digests)[:4]) for epoch, digests in sorted(by_epoch.items())
                     if len(digests) >= 4), None)
    if selected is None:
        raise ValueError("warmup has no completed epoch with four executed nonempty batches")
    output.write_text(json.dumps({"sync_epoch": selected[0], "batch_digests": selected[1]}, separators=(",", ":")) + "\n")


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
