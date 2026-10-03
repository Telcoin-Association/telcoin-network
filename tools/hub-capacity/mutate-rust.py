#!/usr/bin/env python3
"""Confirm new load-bearing Rust regressions with compiling, reverted production mutations.

The required Hub lane compiles and tests the full workspace before this script.
These cases change expressions without changing public signatures or types.
Each control, mutant compilation and selected regression uses the source's owning
package, avoiding a rebuild of unrelated workspace binaries for every expression.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tomllib


ROOT = Path(__file__).resolve().parents[2]
CASES = [
    ("closed_connection_io", "crates/network-libp2p/src/consensus.rs",
     "ReqResOutboundFailure::Io(e) => match e.kind() {\n                        ErrorKind::NotConnected\n                        | ErrorKind::ConnectionReset",
     "ReqResOutboundFailure::Io(e) => match e.kind() {\n                        ErrorKind::ConnectionReset",
     "disconnected_request_io_does_not_score_peer"),
    ("cancelled_permit_occupancy", "crates/network-libp2p/src/capacity.rs",
     "active.decrement(1.0);", "active.decrement(0.0);", "cancellation_releases_reserved_occupancy"),
    ("reserved_permit_occupancy", "crates/network-libp2p/src/capacity.rs",
     "metrics.active.increment(1.0);", "metrics.active.increment(0.0);", "cancellation_releases_reserved_occupancy"),
    ("independent_worker_metrics", "crates/network-libp2p/src/capacity.rs",
     "let network = network_label(network);", 'let network = network_label(network).replace("worker-1", "worker-0");',
     "workers_have_independent_series"),
    ("application_query_cap", "crates/network-libp2p/src/consensus.rs",
     "\n                    >= 100\n", "\n                    >= 101\n", "application_record_queries_are_bounded_and_complete"),
    ("application_record_key", "crates/network-libp2p/src/consensus.rs",
     "let query_id =\n                        self.swarm.behaviour_mut().kademlia.get_record(node_record_key(&key));",
     "let query_id =\n                        self.swarm.behaviour_mut().kademlia.get_record(libp2p::kad::RecordKey::new(&encode(&key)));",
     "application_record_queries_are_bounded_and_complete"),
    ("application_record_completion", "crates/network-libp2p/src/consensus.rs",
     "if is_last_step || application_ready {", "if is_last_step {",
     "application_record_queries_are_bounded_and_complete"),
    ("authority_record_readiness", "crates/network-libp2p/src/consensus.rs",
     "let discovered = matches_requested_key", "let discovered = false",
     "authority_record_is_usable_before_lookup_finishes"),
    ("rotation_public_ceiling", "crates/network-libp2p/src/peers/manager.rs",
     ".max(public_excess)", ".min(public_excess)", "public_peer_limit_prunes_after_committee_rotation"),
    ("established_public_boundary", "crates/network-libp2p/src/peers/manager.rs",
     "> limit.get()", ">= limit.get()",
     "public_peer_limit_preserves_protected_headroom"),
    ("dao_retention_reservation", "crates/network-libp2p/src/peers/manager.rs",
     ".filter(|key| self.dao_observers.contains(key))", ".filter(|_| false)",
     "dao_metrics_track_identity_and_disconnection"),
    ("independent_serve_classes", "crates/config/src/network_serve.rs",
     "usize::from(self.prefetch.get())", "usize::from(self.worker_shed.get())", "operator_limits_are_finite_and_independent"),
    ("mesh_degree_order", "crates/config/src/gossip_mesh.rs",
     "self.low > self.target", "self.low > self.high", "rejects_invalid_mesh_relationships"),
    ("remote_periodic_replication", "crates/network-libp2p/src/kad.rs",
     "(self.retention.is_none() || record.publisher == Some(self.local_peer_id))", "true",
     "test_kad_record_jobs_publish_own_record_only"),
    ("duplicate_rotation_disconnect", "crates/network-libp2p/src/peers/manager.rs",
     "PeerAction::Disconnect | PeerAction::DisconnectWithPX => self.temporarily_ban(peer_id),",
     "PeerAction::Disconnect | PeerAction::DisconnectWithPX => self.apply_peer_action(peer_id, action),",
      "public_peer_limit_prunes_after_committee_rotation"),
    ("delayed_vote_penalty", "crates/consensus/primary/src/error/network.rs",
     "HeaderError::AlreadyVotedForLaterRound { .. } | HeaderError::TooOld { .. } => None,",
     "HeaderError::AlreadyVotedForLaterRound { .. } | HeaderError::TooOld { .. } => Some(Penalty::Fatal),",
     "delayed_vote_errors_do_not_penalize_committee"),
    ("cached_vote_equivocation", "crates/consensus/primary/src/network/handler.rs",
     "// A second digest in the same slot is equivocation.\n                            HeaderError::AlreadyVoted(header.digest(), header.round())",
     "// A second digest in the same slot is equivocation.\n                            HeaderError::AlreadyVotedForLaterRound { theirs: header.round(), ours: last_round }",
      "test_vote_different_digest_same_round_rejected"),
    ("local_nonce_gap_retention", "crates/batch-builder/src/lib.rs",
     "pool.reserve_local_seal(batch.digest(), batch.batch().transactions())",
     "pool.reserve_local_seal(batch.digest(), &[])",
     "locally_sealed_nonce_gap_recovers_without_resubmission"),
    ("shared_local_recovery_capacity", "crates/tn-reth/src/env/mod.rs",
     "self.inner.local_recovery.clone(),",
     "Arc::new(Mutex::new(crate::txn_pool::LocalSealRecovery::default())),",
     "local_recovery_capacity_is_shared_across_worker_pools"),
    ("canonical_before_ack_cleanup", "crates/tn-reth/src/txn_pool.rs",
     "seal.reservations == 0 && seal.transactions.is_empty()",
     "seal.reservations == 0 && !seal.transactions.is_empty()",
     "local_recovery_canonical_before_ack_and_duplicate_cleanup"),
    ("cancelled_local_replay_ownership", "crates/tn-reth/src/txn_pool.rs",
     "transaction.execution = self.resume;",
     "transaction.execution = LocalExecutionPhase::Replaying;",
     "local_recovery_replay_cancellation_keeps_accepted_bytes"),
    ("local_cache_iterator_cleanup", "crates/consensus/worker/src/worker.rs",
     "resolved.push(hash);",
     'self.store.remove::<OurNodeBatchesCache>(&hash).expect("mutation removal under iterator");\n                    resolved.push(hash);',
     "local_cache_resolved_cleanup_drops_iterator_before_write"),
    ("local_cache_capacity_before_quorum", "crates/consensus/worker/src/worker.rs",
     "if !already_cached\n            && (",
     "if std::hint::black_box(false) && !already_cached\n            && (",
     "local_cache_exhaustion_refuses_before_quorum"),
    ("observer_known_handoff", "crates/tn-reth/src/txn_pool.rs",
     "let observer_handoff = lease.resume == LocalExecutionPhase::ObserverRetryReady;",
     "let observer_handoff = false;",
     "observer_retry_handoff_survives_prune_and_known_race"),
    ("observer_ack_role", "crates/consensus/worker/src/worker.rs",
     "self.local_recovery.observer_accepted(digest);",
     "let _ = digest;",
     "observer_ack_failed_forward_restores_same_digest_reservation"),
    ("cache_retry_ownership", "crates/consensus/worker/src/worker.rs",
     ".map(|()| !already_cached)", ".map(|()| true)",
     "local_cache_same_digest_retry_preserves_accepted_bytes"),
    ("seal_lock_serialization", "crates/types/src/worker/sealed_batch.rs",
     "lease.guard = Some(lock.lock_owned().await);", "lease.guard = None;",
     "local_batch_seal_locks_bound_serialize_and_release"),
    ("seal_lock_capacity", "crates/types/src/worker/sealed_batch.rs",
     "(entries.len() < 1024).then(|| {", "(entries.len() <= 1024).then(|| {",
     "local_batch_seal_locks_bound_serialize_and_release"),
    ("seal_lock_prompt_cleanup", "crates/types/src/worker/sealed_batch.rs",
     "if std::sync::Arc::strong_count(&self.lock) == 1 {", "if std::hint::black_box(false) && std::sync::Arc::strong_count(&self.lock) == 1 {",
     "local_batch_seal_locks_bound_serialize_and_release"),
    ("cache_retry_immutable_contents", "crates/consensus/worker/src/worker.rs",
     "is_none_or(|cached| cached == batch)", "is_none_or(|_| true)",
     "local_cache_same_digest_retry_preserves_accepted_bytes"),
]


def execute(argv, directory, label):
    """Retain finite compiler/test logs, without accepting a compiler failure as a killed mutant."""
    result = subprocess.run(argv, cwd=ROOT, capture_output=True, timeout=1800)
    raw = result.stdout + result.stderr
    if len(raw) > 64 * 1024**2:
        raise ValueError("mutation command log exceeds 64 MiB")
    path = directory / (label + ".log")
    path.write_bytes(raw)
    return result.returncode, re.sub(r"\x1b\[[0-9;]*m", "", raw.decode(errors="replace")), {
        "argv": argv, "exit_code": result.returncode,
        "log": path.name, "sha256": hashlib.sha256(raw).hexdigest()}


def mutation_commands(relative, regression):
    """Compile the expression's owning crate and run the same mandatory regression."""
    source = (ROOT / relative).resolve()
    repository = ROOT.resolve()
    if not source.is_relative_to(repository):
        raise ValueError("mutation source must belong to the repository")
    manifests = [directory / "Cargo.toml" for directory in source.parents
                 if directory.is_relative_to(repository) and (directory / "Cargo.toml").is_file()]
    if not manifests:
        raise ValueError("mutation source must belong to a Cargo package")
    package = tomllib.loads(manifests[0].read_text()).get("package", {}).get("name")
    if not package:
        raise ValueError("mutation source must have an owning Cargo package")
    compile_argv = ["cargo", "+1.94", "test", "--locked", "-p", package, "--no-run"]
    test_argv = ["cargo", "+1.94", "nextest", "run", "--locked", "-p", package,
                 "-E", f"test({regression})", "--no-tests", "fail", "--test-threads", "1"]
    return package, compile_argv, test_argv


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    reports = []
    try:
        for name, relative, before, after, regression in CASES:
            path = ROOT / relative
            original = path.read_bytes()
            text = original.decode()
            if text.count(before) != 1:
                raise ValueError(f"{name}: mutation must select exactly one production expression")
            package, compile_argv, test_argv = mutation_commands(relative, regression)
            control_code, _, control = execute(test_argv, args.output, name + "-control")
            if control_code:
                raise ValueError(f"{name}: original regression must pass")
            report = {"mutation": name, "path": relative, "package": package, "regression": regression,
                      "source_sha256": hashlib.sha256(original).hexdigest(), "control": control}
            try:
                path.write_text(text.replace(before, after))
                compiler, _, compilation = execute(compile_argv, args.output, name + "-compile")
                report["compilation"] = compilation
                if compiler:
                    raise ValueError(f"{name}: mutant did not compile, no mutation confirmation")
                code, output, test = execute(test_argv, args.output, name + "-test")
                report["test"] = test
                report["detected"] = code == 100 and re.search(r"FAIL[^\n]*" + re.escape(regression), output) is not None
                if not report["detected"]:
                    raise ValueError(f"{name}: selected regression did not fail on the compiling mutant")
            finally:
                path.write_bytes(original)
                report["restored_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
                reports.append(report)
            print(json.dumps({"mutation": name, "detected": True}, sort_keys=True), flush=True)
    finally:
        with (args.output / "report.json").open("x") as output:
            json.dump(reports, output, allow_nan=False, sort_keys=True, indent=2)
            output.write("\n")


if __name__ == "__main__":
    main()
