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
BASE = "08a507835011675a32348ac60d0488bbba891552"
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
    "crates/types/src/worker/batch_slots.rs",
    "crates/types/src/worker/mod.rs",
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


def compiler_errors(output):
    errors = []
    for line in output.splitlines():
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(entry, dict):
            continue
        message = entry.get("message", {})
        if entry.get("reason") == "compiler-message" and isinstance(message, dict) and message.get("level") == "error":
            errors.append({
                "code": (message.get("code") or {}).get("code"),
                "message": message["message"],
                "locations": [
                    [span["file_name"], span["line_start"], span["column_start"]]
                    for span in message.get("spans", []) if span.get("is_primary")
                ],
            })
    return sorted(errors, key=lambda error: json.dumps(error, sort_keys=True))


def run(label, command, *, mutant=False, required=True):
    print(f"Running {label}: {' '.join(command)}", flush=True)
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    (OUT / f"{label}.log").write_text(result.stdout)
    passed = result.returncode == 0
    if mutant:
        passed = result.returncode != 0 and "test result: FAILED." in result.stdout
    summaries = re.findall(r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed;", result.stdout)
    tests_passed = sum(int(count) for count, _ in summaries)
    tests_failed = sum(int(count) for _, count in summaries)
    if len(command) > 2 and command[2] == "test":
        passed = passed and tests_passed + tests_failed > 0
    check = {
        "label": label,
        "command": command,
        "exit_code": result.returncode,
        "expected_test_failure": mutant,
        "passed": passed,
        "tests_passed": tests_passed,
        "tests_failed": tests_failed,
        "diagnostics": compiler_errors(result.stdout),
    }
    REPORT["checks"].append(check)
    (OUT / f"{PHASE}.json").write_text(json.dumps(REPORT, indent=2) + "\n")
    print("\n".join(result.stdout.splitlines()[-70:]), flush=True)
    if required and not passed:
        raise RuntimeError(f"Validation failed: {label}")
    return check


def test_command(package, target, pattern, feature=False):
    command = ["cargo", f"+{PIN}", "test", "--locked", "-p", package]
    if feature:
        command += ["--features", "adiri" if package == "tn-types" else "tn-reth/adiri"]
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
            "tn-worker", "tn-config", "tn-metrics", "tn-network-types", "tn-worker-gateway",
        ]
        command = ["cargo", f"+{NIGHTLY}", "clippy", "--locked", "--keep-going", "--message-format=json"]
        for package in packages:
            command += ["-p", package]
        command += ["--all-targets", "--no-deps"]
        modes = {
            "default": ["--", "-D", "warnings"],
            "all-features": ["--all-features", "--", "-D", "warnings"],
        }
        broad = {
            mode: run(f"clippy-{mode}", command + flags, required=False)
            for mode, flags in modes.items()
        }
        focused = [
            "cargo", f"+{NIGHTLY}", "clippy", "--locked", "--keep-going", "--message-format=json",
            "-p", "tn-reth", "-p", "tn-batch-validator", "-p", "tn-batch-builder", "-p", "tn-types",
            "--all-targets", "--no-deps",
        ]
        changed = {
            mode: run(f"clippy-changed-{mode}", focused + flags, required=False)
            for mode, flags in modes.items()
        }
        if not all(check["passed"] for check in broad.values()):
            subprocess.run(["git", "fetch", "--no-tags", "--depth=1", "origin", BASE], check=True)
            changed_paths = subprocess.check_output(
                ["git", "diff", "--name-only", BASE, EXPECTED], text=True,
            ).splitlines()
            if set(changed_paths) != set(FILES):
                raise ValueError("The baseline substitution must cover the complete PR diff")
            baseline_paths = set(subprocess.check_output(
                ["git", "ls-tree", "-r", "--name-only", BASE, "--", *FILES], text=True,
            ).splitlines())
            REPORT["baseline_commit"] = BASE
            try:
                for name in FILES:
                    if name in baseline_paths:
                        (ROOT / name).write_bytes(subprocess.check_output(["git", "show", f"{BASE}:{name}"]))
                    else:
                        (ROOT / name).unlink()
                baseline = {
                    mode: run(f"clippy-baseline-{mode}", command + flags, required=False)
                    for mode, flags in modes.items()
                }
                REPORT["baseline_comparisons"] = [
                    {
                        "mode": mode,
                        "head_exit": broad[mode]["exit_code"],
                        "base_exit": baseline[mode]["exit_code"],
                        "same_diagnostics": broad[mode]["diagnostics"] == baseline[mode]["diagnostics"],
                        "nonempty_diagnostics": bool(broad[mode]["diagnostics"]),
                    }
                    for mode in modes
                ]
            finally:
                for name, content in ORIGINAL.items():
                    (ROOT / name).write_bytes(content)
        if not all(check["passed"] for check in [*broad.values(), *changed.values()]):
            raise RuntimeError("Clippy failed; reports preserve the focused results and baseline comparison")
    elif PHASE == "slot-core":
        command = test_command("tn-types", ["--lib"], "worker::batch_slots::tests::")
        run("slot-core-default", command)
        run("slot-core-adiri", test_command("tn-types", ["--lib"], "worker::batch_slots::tests::", True))
        source = "crates/types/src/worker/batch_slots.rs"
        mutations = [
            ("slot-resolved-sequence",
             "() if position.sequence < slot.sequence => Ok(false),",
             "() if position.sequence < slot.sequence => Ok(true),"),
            ("slot-repeated-timeout",
             "slot.timeout_voters.contains(&author)",
             "(slot.timeout_voters.contains(&author) && position.view.0 == u64::MAX)"),
            ("slot-rotation",
             "slot.view = next_view;",
             "slot.view = next_view.min(position.view);"),
            ("slot-producer",
             "() if self.producer(*position)? != record.authority() =>",
             "() if self.producer(*position)? != record.authority() && position.bucket.0 == u32::MAX =>"),
            ("slot-body-signature",
             "if bls_verify_secure(&self.signature, &self.authority, &bytes) {",
             "if bls_verify_secure(&self.signature, &self.authority, &bytes) || self.epoch == committee.epoch() {"),
            ("slot-execution-fence",
             "slot.opening = SlotOpening::Pending(output);",
             "slot.opening = SlotOpening::Ready(BatchSlotParent::new(output, B256::ZERO));"),
            ("slot-independent-buckets",
             "            Ok(BatchSlotTransition::Selected)\n",
             "            self.buckets.iter_mut().for_each(|other| other.sequence = sequence);\n"
             "            Ok(BatchSlotTransition::Selected)\n"),
            ("slot-conflicting-vote",
             "() if next_order == old_order && self != next => Err(BatchSlotError::ConflictingVote),",
             "() if next_order == old_order && self != next => Ok(()),"),
            ("slot-reservation-rewind",
             "() if next_order < old_order => Err(BatchSlotError::StalePosition),",
             "() if next_order < old_order => Ok(()),"),
        ]
        for label, before, after in mutations:
            mutate(f"mutant-{label}", source, before, after, command)
        run("slot-core-restored", command)
    elif PHASE == "tests":
        cases = [
            ("peer-window", "tn-reth", ["--lib"], "peer_batch::"),
            ("builder-capacity", "tn-batch-builder", ["--lib"], "peer_batch_capacity_loss_"),
            ("race-execution", "tn-batch-builder", ["--test", "it"], "peer_batch_residuals::"),
            ("slot-core", "tn-types", ["--lib"], "worker::batch_slots::tests::"),
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
