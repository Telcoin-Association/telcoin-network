"""Run focused checks and diff-scoped mutations on an isolated GitHub runner."""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path.cwd()
OUT = ROOT / "validation-1377"
OUT.mkdir(exist_ok=True)
PHASE = sys.argv[1]
EXPECTED = os.environ["PR_HEAD"]
if re.fullmatch(r"[0-9a-f]{40}", EXPECTED) is None:
    raise ValueError("PR_HEAD must be a complete commit ID")
PIN = "1.94"
NIGHTLY = (ROOT / "rust-nightly").read_text().strip()
FILES = [
    "crates/tn-reth/src/peer_batch.rs",
    "crates/batch-validator/src/validator.rs",
    "crates/batch-builder/src/batch.rs",
    "crates/batch-builder/src/test_utils.rs",
    "crates/batch-builder/tests/it/main.rs",
    "crates/batch-builder/tests/it/peer_batch_residuals.rs",
    "crates/batch-builder/README.md",
    "docs/peer-batch-deferral.md",
]
ORIGINAL = {name: (ROOT / name).read_bytes() for name in FILES}
for name, content in ORIGINAL.items():
    expected_content = subprocess.check_output(["git", "show", f"{EXPECTED}:{name}"])
    if content != expected_content:
        raise ValueError(f"Validation source differs from PR_HEAD: {name}")
REPORT = {
    "pr_head": EXPECTED,
    "runner_head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    "phase": PHASE,
    "source_sha256": {name: hashlib.sha256(data).hexdigest() for name, data in ORIGINAL.items()},
    "checks": [],
}


def run(label, command, *, mutant=False):
    print(f"Running {label}: {' '.join(command)}", flush=True)
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    (OUT / f"{label}.log").write_text(result.stdout)
    passed = result.returncode == 0
    if mutant:
        passed = result.returncode != 0 and "test result: FAILED." in result.stdout
    REPORT["checks"].append({
        "label": label,
        "command": command,
        "exit_code": result.returncode,
        "expected_test_failure": mutant,
        "passed": passed,
    })
    (OUT / f"{PHASE}.json").write_text(json.dumps(REPORT, indent=2) + "\n")
    print("\n".join(result.stdout.splitlines()[-70:]), flush=True)
    if not passed:
        raise RuntimeError(f"Validation failed: {label}")


def test_command(package, target, pattern, feature=False):
    command = ["cargo", f"+{PIN}", "test", "--locked", "-p", package]
    if feature:
        command += ["--features", "tn-reth/adiri"]
    command += target + [pattern, "--", "--nocapture"]
    return command


def mutate(label, name, before, after, command):
    original = ORIGINAL[name].decode()
    if original.count(before) != 1:
        raise ValueError(f"Mutation must target one exact source span: {label}")
    try:
        (ROOT / name).write_text(original.replace(before, after, 1))
        run(label, command, mutant=True)
    finally:
        (ROOT / name).write_bytes(ORIGINAL[name])


try:
    if PHASE == "clippy":
        packages = [
            "e2e-tests", "exex-indexer", "exex-lifecycle", "state-sync", "telcoin-network",
            "telcoin-network-cli", "tn-batch-builder", "tn-batch-validator", "tn-engine",
            "tn-executor", "tn-exex", "tn-network-libp2p", "tn-node", "tn-primary", "tn-reth",
            "tn-rpc", "tn-storage", "tn-test-utils", "tn-test-utils-committee", "tn-types",
            "tn-worker",
        ]
        command = ["cargo", f"+{NIGHTLY}", "clippy", "--locked"]
        for package in packages:
            command += ["-p", package]
        command += ["--all-targets", "--no-deps"]
        run("clippy-default", command + ["--", "-D", "warnings"])
        run("clippy-all-features", command + ["--all-features", "--", "-D", "warnings"])
    elif PHASE == "tests":
        cases = [
            ("peer-window", "tn-reth", ["--lib"], "peer_batch::"),
            ("builder-capacity", "tn-batch-builder", ["--lib"], "peer_batch_capacity_loss_"),
            ("race-execution", "tn-batch-builder", ["--test", "it"], "peer_batch_residuals::"),
        ]
        for feature in [False, True]:
            suffix = "adiri" if feature else "default"
            for label, package, target, pattern in cases:
                run(f"{label}-{suffix}", test_command(package, target, pattern, feature))
        mutate(
            "mutant-drop-counter", "crates/tn-reth/src/peer_batch.rs",
            "self.metrics.insertions_dropped_total.increment(dropped);",
            "self.metrics.insertions_dropped_total.increment(dropped.saturating_sub(dropped));",
            test_command("tn-reth", ["--lib"], "telemetry_distinguishes_capacity_loss_"),
        )
        mutate(
            "mutant-window-release", "crates/tn-reth/src/peer_batch.rs",
            "self.metrics.retained_hashes.decrement(PeerBatchMetrics::retained_value(self.seen.len()));",
            "self.metrics.retained_hashes.decrement(0.0);",
            test_command("tn-reth", ["--lib"], "telemetry_tracks_window_lifetimes_"),
        )
        mutate(
            "mutant-capacity", "crates/tn-reth/src/peer_batch.rs",
            "} else if self.seen.len() >= self.cap {", "} else if self.cap == 0 {",
            test_command("tn-batch-builder", ["--lib"], "peer_batch_capacity_loss_"),
        )
        mutate(
            "mutant-validator-registration", "crates/batch-validator/src/validator.rs",
            ".for_each(|pool| pool.peer_batch_txs().register_metrics(worker_id));",
            ".for_each(|pool| { let _ = pool.peer_batch_txs(); });",
            test_command("tn-batch-builder", ["--test", "it"], "peer_batch_residuals::"),
        )
        for label, package, target, pattern in cases:
            run(f"{label}-restored", test_command(package, target, pattern))
    else:
        raise ValueError(f"Unknown validation phase: {PHASE}")
finally:
    for name, content in ORIGINAL.items():
        (ROOT / name).write_bytes(content)
    REPORT["sources_restored"] = all((ROOT / name).read_bytes() == content for name, content in ORIGINAL.items())
    (OUT / f"{PHASE}.json").write_text(json.dumps(REPORT, indent=2) + "\n")
