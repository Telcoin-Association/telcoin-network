#!/usr/bin/env python3
"""Run the isolated QUIC TLS profile and retain raw samples and provenance."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import platform
import resource
import statistics
import subprocess
import time
import tomllib

ROOT = Path(__file__).resolve().parent
SCENARIOS = (
    "default", "reverse-server", "reverse-client", "x25519", "p256", "p384",
    "hybrid", "reconnect", "incompatible", "wrong-peer",
    "kx-x25519", "kx-p256", "kx-p384", "kx-hybrid",
)
PINNED = ("libp2p-tls", "rustls", "aws-lc-rs", "aws-lc-sys", "rustls-webpki")


def sha256(path):
    """Fingerprint an artifact without including local path names in the report."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def command(*args):
    """Capture a provenance command and fail if it is unavailable."""
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def distribution(values):
    """Report microseconds, retaining the raw nanosecond samples separately."""
    ordered = sorted(value / 1000 for value in values)
    return {
        "median_us": statistics.median(ordered),
        "p95_us": ordered[math.ceil(len(ordered) * 0.95) - 1],
    }


def validate_versions():
    """Do not mistake an independently resolved provider for the node's provider."""
    node = tomllib.loads((ROOT.parent.parent / "Cargo.lock").read_text())
    fixture = tomllib.loads((ROOT / "Cargo.lock").read_text())
    versions = {}
    for name in PINNED:
        expected = {p["version"] for p in node["package"] if p["name"] == name}
        actual = {p["version"] for p in fixture["package"] if p["name"] == name}
        if actual != expected or len(actual) != 1:
            raise ValueError(f"node/profile dependency drift: {name}: {expected} != {actual}")
        versions[name] = next(iter(actual))
    return versions


def run(binary, output, samples):
    """Measure each scenario in its own process, including a fresh reconnect cache."""
    output.mkdir(parents=True, exist_ok=False)
    report = {
        "schema": 1,
        "versions": validate_versions(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "rustc": command("rustc", "+1.94", "-Vv"),
        "source_revision": command("git", "rev-parse", "HEAD"),
        "source_status": command("git", "status", "--porcelain", "--untracked-files=no"),
        "binary_sha256": sha256(binary),
        "node_lock_sha256": sha256(ROOT.parent.parent / "Cargo.lock"),
        "source_sha256": {
            path: sha256(ROOT / path)
            for path in ("Cargo.toml", "Cargo.lock", "prepare.py", "profile.rs", "src/main.rs", "run.py")
        },
        "samples_per_scenario": samples,
        "timing_scope": "Instrumented in-memory QUIC TLS, Ed25519 identity, P-256 certificate. No UDP or packet protection.",
        "cpu_scope": "Child process user+system CPU, both peers, setup, sampling and JSON output included.",
        "scenarios": {},
    }
    for scenario in SCENARIOS:
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        start = time.monotonic()
        raw = output / f"{scenario}.jsonl"
        with raw.open("w") as stream:
            subprocess.run([str(binary), scenario, str(samples)], stdout=stream, check=True)
        elapsed = time.monotonic() - start
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        rows = [json.loads(line) for line in raw.read_text().splitlines()]
        if len(rows) != samples or any(row["scenario"] != scenario for row in rows):
            raise ValueError(f"incomplete samples for {scenario}")
        results = [row["result"] for row in rows]
        summary = {
            "raw_sha256": sha256(raw),
            "process_wall_seconds": elapsed,
            "process_cpu_seconds": after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
        }
        if scenario in ("incompatible", "wrong-peer"):
            if not all("rejected" in row for row in results):
                raise ValueError(f"negative scenario accepted: {scenario}")
            summary["rejections"] = samples
            summary["reason"] = results[0]["rejected"]
        elif scenario.startswith("kx-"):
            warm = results[1:]
            summary.update({
                "group": warm[0]["group"],
                "warm_samples": len(warm),
                "timings": {
                    field: distribution(row[field] for row in warm)
                    for field in ("client_start_ns", "server_exchange_ns", "client_complete_ns")
                },
            })
        else:
            # Keep the first sample in raw evidence, but separate cold setup effects.
            warm = results[1:]
            expected_kind = "Resumed" if scenario == "reconnect" else "Full"
            if results[0]["kind"] != "Full" or any(row["kind"] != expected_kind for row in warm):
                raise ValueError(f"unexpected handshake kind for {scenario}")
            expected_counts = {"parse": 1, "certificate_signature": 1, "extension_signature": 1} if expected_kind == "Resumed" else {
                "parse": 3, "certificate_signature": 3, "extension_signature": 3, "transcript_signature": 1,
            }
            for row in warm:
                counts = {name: value[0] for name, value in row["server_phases_count_ns"].items()}
                if counts != expected_counts:
                    raise ValueError(f"unexpected verification work for {scenario}: {counts}")
            summary.update({
                "group": warm[0]["group"],
                "kind": expected_kind,
                "warm_samples": len(warm),
                "timings": {
                    field: distribution(row[field] for row in warm)
                    for field in ("wall_ns", "server_first_flight_ns", "server_read_ns", "server_peer_id_ns")
                },
                "verification_counts_per_handshake": expected_counts,
                "verification_timings": {
                    phase: distribution(row["server_phases_count_ns"][phase][1] for row in warm)
                    for phase in expected_counts
                },
            })
        report["scenarios"][scenario] = summary
        print(f"{scenario}: {samples} verified samples", flush=True)
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(output / "report.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/tn-quic-handshake-profile")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=1000)
    args = parser.parse_args()
    if not 2 <= args.samples <= 100_000:
        parser.error("samples must be 2..100000")
    run(args.binary.resolve(), args.output.resolve(), args.samples)
