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
    "crates/types/src/worker/batch_slot_votes.rs",
    "crates/types/Cargo.toml",
    "crates/config/src/consensus.rs",
    "crates/storage/src/lib.rs",
    "Cargo.lock",
    "crates/types/src/worker/batch_slot_control.rs",
    "crates/types/src/worker/sealed_batch.rs",
    "crates/storage/tests/batch_slot_votes.rs",
    "crates/tn-reth/src/env/slot_admission.rs",
    "crates/tn-reth/src/env/mod.rs",
    "crates/tn-reth/src/evm/mod.rs",
    "crates/tn-reth/src/txn_pool.rs",
    "crates/consensus/worker/src/network/handler.rs",
    "crates/consensus/worker/src/network/primary.rs",
    "crates/consensus/worker/src/worker.rs",
    "crates/config/src/keys.rs",
    "crates/types/src/error.rs",
    "crates/batch-builder/src/lib.rs",
    "crates/consensus/worker/src/network/error.rs",
    "crates/engine/src/error.rs",
    "crates/engine/src/payload_builder.rs",
    "crates/tn-reth/src/payload.rs",
    "crates/node/src/manager/node.rs",
    "crates/node/src/manager/node/run_epoch.rs",
    "crates/node/src/manager/node/batch_slots.rs",
    "crates/types/src/forks.rs",
    "crates/engine/Cargo.toml",
    "crates/engine/tests/it/main.rs",
    "crates/engine/tests/it/native_slots.rs",
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
    if package == "tn-storage":
        command += ["--features", "test-utils"]
    if feature:
        features = {"tn-types": "adiri", "tn-storage": "tn-types/adiri", "tn-engine": "adiri,tn-reth/adiri"}
        command += ["--features", features.get(package, "tn-reth/adiri")]
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
        command += ["--workspace", "--all-targets", "--no-deps"]
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
        run("slot-runtime-compile", ["cargo", f"+{PIN}", "check", "--locked", "-p", "tn-node", "--all-targets", "--features", "tn-types/test-utils"])
        command = test_command("tn-types", ["--lib"], "worker::batch_slot")
        run("slot-core-default", command)
        run("slot-core-adiri", test_command("tn-types", ["--lib"], "worker::batch_slot", True))
        storage_command = test_command("tn-storage", ["--test", "batch_slot_votes"], "")
        run("slot-storage-default", storage_command)
        run("slot-storage-adiri", test_command("tn-storage", ["--test", "batch_slot_votes"], "", True))
        execution_command = test_command("tn-engine", ["--test", "it"], "native_slots")
        run("slot-execution-default", execution_command)
        run("slot-execution-adiri", test_command("tn-engine", ["--test", "it"], "native_slots", True))
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
             "() if self != next => Err(BatchSlotError::ConflictingVote),",
             "() if self != next => Ok(()),"),
            ("slot-late-vote",
             "position.view > authorization.position.view",
             "position.view != authorization.position.view"),
            ("slot-unopened-view",
             "() if position.view > authorization.position.view => Err(BatchSlotError::FutureView),",
             "() if position.view > authorization.position.view && position.view.0 == u64::MAX => Err(BatchSlotError::FutureView),"),
            ("slot-worker-capacity",
             "let count = u64::from(self.producer_count.get());",
             "let count = u64::from(self.bucket_count.get());"),
        ]
        for label, before, after in mutations:
            mutate(f"mutant-{label}", source, before, after, command)
        store_source = "crates/types/src/worker/batch_slot_votes.rs"
        storage_mutations = [
            ("slot-vote-durability",
             "self.database\n                .persist::<BatchSlotVotes>()\n                .await\n                .map_err(BatchSlotVoteStoreError::Database)",
             "Ok(())"),
            ("slot-lost-reservation",
             ".insert::<BatchSlotVotes>(vote.key(), vote)",
             ".remove::<BatchSlotVotes>(vote.key())"),
            ("slot-lost-history",
             "|authorization| slots.vote_for_authorization(record, &authorization)",
             "|_authorization| slots.vote(record)"),
            ("slot-history-durability",
             "self.database\n            .persist::<BatchSlotAuthorizations>()\n            .await\n            .map_err(BatchSlotVoteStoreError::Database)",
             "Ok(())"),
            ("slot-epoch-rewind",
             "stored.is_some_and(|epoch| epoch > self.epoch)",
             "stored.is_some_and(|epoch| epoch > self.epoch && epoch == u32::MAX)"),
            ("slot-epoch-marker-durability",
             "self.database\n                            .persist::<BatchSlotStoreEpoch>()\n                            .await\n                            .map_err(BatchSlotVoteStoreError::Database)",
             "Ok(())"),
        ]
        for label, before, after in storage_mutations:
            mutate(f"mutant-{label}", store_source, before, after, storage_command)
        mutate("mutant-slot-unpublished-retry", "crates/types/src/worker/batch_slot_control.rs",
               "self.previous.vote(record)?;", "record.authenticate(&self.previous)?;", storage_command)
        execution_source = "crates/engine/src/payload_builder.rs"
        mutate("mutant-slot-losing-execution", execution_source,
               "if transition == BatchSlotTransition::Selected {",
               "if transition == BatchSlotTransition::Selected || matches!(record.message(), BatchSlotMessage::Proposal { .. }) {", execution_command)
        mutate("mutant-slot-control-anchor", execution_source,
               "!output.close_epoch() && !slot_state_changed",
               "!output.close_epoch() && (!slot_state_changed || batches.is_empty())", execution_command)
        publication_point = "    let slot_state_changed = slot_output.as_ref().is_some_and(BatchSlotOutput::changed);"
        premature_publication = publication_point + "\n" + """    if let Some(mut premature) = slot_output.take() {
        premature.finalize(canonical_header.hash()).map_err(TnEngineError::BatchSlot)?;
        reth_env.batch_slots().commit_blocking(premature).map_err(TnEngineError::BatchSlotPublication)?;
    }"""
        mutate("mutant-slot-premature-publication", execution_source,
               publication_point, premature_publication, execution_command)
        mutate("mutant-slot-admission-nonce", "crates/tn-reth/src/evm/mod.rs",
               "        caller.bump_nonce();", "        // Mutation: omit the admission nonce advance.", execution_command)
        mutate("mutant-slot-selected-epoch-boundary", "crates/tn-reth/src/payload.rs",
               "output.close_epoch() && is_final",
               "output.close_epoch() && is_final && output.close_epoch_for_last_batch(self.batch_index).is_some_and(|last| last)", execution_command)
        mutate("mutant-slot-envelope", source,
               "if canonical == *envelope {", "if canonical.epoch == envelope.epoch {", storage_command)
        mutate("mutant-slot-publication-anchor", "crates/types/src/worker/batch_slot_control.rs",
               "output.candidate.position(bucket).map(|_| ())", "output.previous.position(bucket).map(|_| ())", storage_command)
        mutate("mutant-slot-wire-budget", source,
               "prototype.encode().map(|bytes| bytes.len().saturating_add(4))",
               "prototype.encode().map(|bytes| bytes.len())", storage_command)
        run("slot-core-restored", command)
        run("slot-storage-restored", storage_command)
        run("slot-execution-restored", execution_command)
    elif PHASE == "tests":
        cases = [
            ("peer-window", "tn-reth", ["--lib"], "peer_batch::"),
            ("builder-capacity", "tn-batch-builder", ["--lib"], "peer_batch_capacity_loss_"),
            ("race-execution", "tn-batch-builder", ["--test", "it"], "peer_batch_residuals::"),
            ("slot-core", "tn-types", ["--lib"], "worker::batch_slot"),
            ("slot-storage", "tn-storage", ["--test", "batch_slot_votes"], ""),
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
