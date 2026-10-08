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
MAX_LOG_BYTES = 64 * 1024**2
CASES = [
    ("catchup_demotion_verified_target", "crates/consensus/primary/src/network/handler.rs",
     "self.consensus_bus\n                                .publish_consensus_num_hash_if_newer(epoch, number, hash);",
     "let _ = std::hint::black_box(false) && self.consensus_bus\n                                .publish_consensus_num_hash_if_newer(epoch, number, hash);",
     "test_consensus_result_demotion_preserves_verified_catchup_target"),
    ("catchup_demotion_check_order", "crates/consensus/primary/src/network/handler.rs",
     "if self.behind_consensus(epoch, round, Some(number)).await {",
     "if { self.consensus_bus.publish_consensus_num_hash_if_newer(epoch, number, hash); self.behind_consensus(epoch, round, Some(number)).await } {",
     "test_consensus_result_demotion_preserves_verified_catchup_target"),
    ("catchup_tracker_initial_replay", "crates/state-sync/src/consensus.rs",
     "if initial_target.1 > 0 {", "if std::hint::black_box(false) && initial_target.1 > 0 {",
     "track_recent_consensus_replays_preexisting_verified_target_once"),
    ("catchup_tracker_intervening_update", "crates/state-sync/src/consensus.rs",
     "let _ = consensus_bus.consensus_request_queue().send(initial_target).await;",
     "let _ = consensus_bus.consensus_request_queue().send(initial_target).await; let _ = rx_gossip_update.borrow_and_update();",
     "track_recent_consensus_preserves_update_during_startup_backpressure"),
    ("catchup_tracker_default_target", "crates/state-sync/src/consensus.rs",
     "if initial_target.1 > 0 {", "if std::hint::black_box(true) || initial_target.1 > 0 {",
     "track_recent_consensus_does_not_request_default_target"),
    ("catchup_tracker_queue_shutdown", "crates/state-sync/src/consensus.rs",
     "_ = &rx_shutdown => {},",
     "_ = async { let _ = &rx_shutdown; std::future::pending::<()>().await } => {},",
     "track_recent_consensus_shutdown_cancels_full_queue"),
    ("queued_manager_dial_dedup", "crates/network-libp2p/src/peers/behavior.rs",
     "else if self.dial_attempt_already_registered(&peer_id) {",
     "else if std::hint::black_box(false) && self.dial_attempt_already_registered(&peer_id) {",
     "queued_duplicate_dial_preserves_first_reply"),
    ("queued_kademlia_dial_dedup", "crates/network-libp2p/src/peers/behavior.rs",
     "else if self.dial_attempt_already_registered(&peer_id) {",
     "else if std::hint::black_box(false) && self.dial_attempt_already_registered(&peer_id) {",
     "queued_dial_defers_to_kademlia_pending_connection"),
    ("redundant_dial_failure_preserves_reply", "crates/network-libp2p/src/peers/behavior.rs",
     "PeerCondition::NotDialing | PeerCondition::DisconnectedAndNotDialing",
     "PeerCondition::Disconnected",
     "redundant_condition_failure_preserves_pending_dial_completion"),
    ("vote_request_identity_exhaustion", "crates/consensus/primary/src/network/mod.rs",
     "|next| next.checked_add(1)", "|next| Some(next.wrapping_add(1))",
     "request_identity_exhaustion_preserves_the_final_watermark"),
    ("vote_request_identity_reuse", "crates/consensus/primary/src/network/mod.rs",
     ".map(|request_id| Self { generation, request_id })",
     ".map(|request_id| Self { generation, request_id: std::hint::black_box(1_u64).min(request_id) })",
     "vote_observation_identity_survives_cloned_and_recreated_handles"),
    ("connection_publication_late_response", "patches/libp2p-kad/src/behaviour.rs",
     "if !matches!(context, PutRecordContext::Connection(_)) || accepted {",
     "if std::hint::black_box(true) || !matches!(context, PutRecordContext::Connection(_)) || accepted {",
     "finished_query_rejects_a_late_response_on_its_still_live_target"),
    ("connection_publication_handler", "patches/libp2p-kad/src/behaviour.rs",
     "handler: NotifyHandler::One(*connection),", "handler: NotifyHandler::Any,",
     "new_connection_publication_uses_its_handler_and_ignores_old_handler_reply"),
    ("connection_publication_capacity", "patches/libp2p-kad/src/behaviour.rs",
     "} else if self.active_connection_publications() >= MAX_ACTIVE_CONNECTION_PUBLICATIONS {",
     "} else if std::hint::black_box(false) && self.active_connection_publications() >= MAX_ACTIVE_CONNECTION_PUBLICATIONS {",
     "saturated_publications_remain_pending_and_deliver_after_a_slot_frees"),
    ("connection_publication_same_identity", "crates/network-libp2p/src/consensus.rs",
     "self.publish_our_data_to_peer(peer_id, connection_id);",
     "if !self.connected_peers.contains(&peer_id) { self.publish_our_data_to_peer(peer_id, connection_id); }",
     "restarted_identity_receives_record_with_old_connection_alive"),
    ("required_connection_reservation", "patches/libp2p-connection-limits/src/lib.rs",
     "current.saturating_add(missing_after)", "current.saturating_add(missing_after).min(current)",
     "required_identities_recover_without_increasing_the_total_cap"),
    ("filtered_expired_identity_delivery", "patches/libp2p-kad/src/behaviour.rs",
     "if !record.is_expired(now) || matches!(self.record_filtering, StoreInserts::FilterBoth) {",
     "if !record.is_expired(now) {",
     "filtered_expired_record_reaches_application_without_storage"),
    ("expired_identity_storage_separation", "crates/network-libp2p/src/consensus.rs",
     "if record.is_expired(Instant::now()) {",
     "if std::hint::black_box(false) && record.is_expired(Instant::now()) {",
     "expired_kad_live_self_identity_is_confirmed_without_storage"),
    ("expired_identity_source_binding", "crates/network-libp2p/src/consensus.rs",
     "let live_self_owned = record.publisher == Some(source)",
     "let live_self_owned = true",
     "expired_kad_relay_cannot_replace_pinned_or_own_record"),
    ("expired_identity_physical_source", "crates/network-libp2p/src/consensus.rs",
     "&& self.swarm.is_connected(&source)\n                            && self.swarm.behaviour().peer_manager.is_connected(&source)",
     "&& true\n                            && self.swarm.behaviour().peer_manager.is_connected(&source)",
     "expired_kad_requires_physical_source_connection"),
    ("expired_identity_pending_ban", "crates/network-libp2p/src/consensus.rs",
     "&& self.swarm.behaviour().peer_manager.is_connected(&source)",
     "&& true",
     "expired_kad_pending_disconnect_cannot_promote_identity"),
    ("expired_identity_configured_cache", "crates/network-libp2p/src/peers/manager.rs",
     "if self.can_confirm_expired_public_identity(&source, &bls_key) {",
     "if { let _eligible = self.can_confirm_expired_public_identity(&source, &bls_key); true } {",
     "expired_kad_configured_key_claim_preserves_binding_and_cache"),
    ("expired_identity_existing_public_binding", "crates/network-libp2p/src/peers/manager.rs",
     "!self.peers.has_confirmed_identity(bls_key)",
     "{ let _occupied = self.peers.has_confirmed_identity(bls_key); true }",
     "expired_kad_existing_public_identity_cannot_be_rotated"),
    ("expired_identity_address_metadata", "crates/network-libp2p/src/peers/manager.rs",
     "self.peers.upsert_peer(bls_key, info.pubkey, retained_addresses);",
     "self.peers.upsert_peer(bls_key, info.pubkey, { drop(retained_addresses); info.multiaddrs });",
     "expired_kad_live_self_identity_is_confirmed_without_storage"),
    ("physical_query_pool_state", "crates/network-libp2p/src/consensus.rs",
     'self.swarm.is_connected(&peer_id), "IsPeerConnected"',
     'self.swarm.behaviour().peer_manager.connected_peers().contains(&peer_id), "IsPeerConnected"',
     "physical_peer_query_waits_for_last_connection_close"),
    ("reconnect_physical_close", "bin/node-record-api/examples/hub-capacity-peer.rs",
     "disconnected(handle, target, peer).await?;",
     "let _disconnected = disconnected; connected(handle, target, false).await?;",
     "reconnect_waits_for_physical_close_before_redial"),
    ("reconnect_identity_close", "bin/node-record-api/examples/hub-capacity-peer.rs",
     "!peers.contains(&target) && !physically_connected",
     "{ let _logical_close = !peers.contains(&target); !physically_connected }",
     "reconnect_waits_for_physical_close_before_redial"),
    ("trusted_live_connection_state", "crates/network-libp2p/src/peers/all_peers.rs",
     ".for_each(|record| peer.retain_connection_state(record));",
     ".for_each(|_| { let _retain_connection_state: fn(&mut Peer, &Peer) = Peer::retain_connection_state; });",
     "test_add_trusted_peer_preserves_connected_authenticated_state"),
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
     "let mut excess_peer_count = aggregate_excess.max(public_excess);",
     "let mut excess_peer_count = aggregate_excess.min(public_excess);", "public_peer_limit_prunes_after_committee_rotation"),
    ("established_public_boundary", "crates/network-libp2p/src/peers/manager.rs",
     "self.confirmed_ordinary_peer_count() > limit.get()", "self.confirmed_ordinary_peer_count() >= limit.get()",
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


# Append admission guards without removing or reordering the original58 case identities.
PUBLIC_ADMISSION_CASES = [
    # Keep the library helper referenced while classifying provisional peers as ordinary.
    ("public_provisional_classification", "crates/network-libp2p/src/peers/manager.rs", "self.peers.peer_has_confirmed_identity(peer_id) && !self.peer_is_important(peer_id)", "(self.peers.peer_has_confirmed_identity(peer_id) || std::hint::black_box(true)) && !self.peer_is_important(peer_id)", "delayed_protected_identity_preserves_public_admission"),
    ("public_aggregate_admission_bound", "crates/network-libp2p/src/peers/manager.rs", "|| self.peers.connected_peer_ids().count() > self.config.target_num_peers", "|| (std::hint::black_box(false) && self.peers.connected_peer_ids().count() > self.config.target_num_peers)", "provisional_public_admission_is_bounded_by_aggregate_target"),
    ("public_identity_promotion", "crates/network-libp2p/src/peers/manager.rs", "self.peers.upsert_peer(bls_key, info.pubkey, info.multiaddrs);\n            self.enforce_public_peer_limits();", "self.peers.upsert_peer(bls_key, info.pubkey, info.multiaddrs);", "public_identity_promotion_prunes_immediately"),
    ("public_expired_identity_promotion", "crates/network-libp2p/src/peers/manager.rs", "self.peers.upsert_peer(bls_key, info.pubkey, retained_addresses);\n            self.enforce_public_peer_limits();", "self.peers.upsert_peer(bls_key, info.pubkey, retained_addresses);", "public_identity_promotion_prunes_immediately"),
    ("public_cached_identity_promotion", "crates/network-libp2p/src/peers/manager.rs", "self.apply_unban_actions(unban_actions);\n        self.enforce_public_peer_limits();", "self.apply_unban_actions(unban_actions);", "public_identity_promotion_prunes_immediately"),
    ("public_protected_arrival_enforcement", "crates/network-libp2p/src/peers/behavior.rs", "self.enforce_public_peer_limits();\n        self.push_event(PeerEvent::PeerConnected", "self.push_event(PeerEvent::PeerConnected", "protected_arrival_prunes_immediately_and_retains_physical_leases"),
    ("public_ordinary_prune_eligibility", "crates/network-libp2p/src/peers/manager.rs", "(aggregate_excess > 0 || ordinary)", "std::hint::black_box(true)", "ordinary_pruning_skips_provisional_peers_without_aggregate_excess"),
    ("public_independent_deficit_accounting", "crates/network-libp2p/src/peers/manager.rs", "public_excess.saturating_sub(usize::from(ordinary))", "public_excess.saturating_sub(1)", "aggregate_pruning_does_not_consume_ordinary_deficit"),
    ("public_legacy_enforcement_gate", "crates/network-libp2p/src/peers/manager.rs", "pub(super) fn enforce_public_peer_limits(&mut self) {\n        if self.public_peer_limit.is_some() {", "pub(super) fn enforce_public_peer_limits(&mut self) {\n        if std::hint::black_box(true) {", "legacy_population_admission_preserves_existing_excess_window"),
    ("public_confirmation_backing_record", "crates/network-libp2p/src/peers/all_peers.rs", "self.peers.get(&PeerIdentity::Confirmed(*bls_key)).is_some_and(|peer| {\n                peer.bls_public_key() == Some(*bls_key) && peer.peer_id() == Some(*peer_id)\n            })", "std::hint::black_box(bls_key) == bls_key", "confirmed_admission_identity_requires_matching_stored_record"),
    ("public_confirmation_transport_binding", "crates/network-libp2p/src/peers/all_peers.rs", "peer.bls_public_key() == Some(*bls_key) && peer.peer_id() == Some(*peer_id)", "peer.bls_public_key() == Some(*bls_key)", "confirmed_admission_identity_requires_matching_stored_record"),
    ("public_conservative_population_gauge", "crates/network-libp2p/src/peers/manager.rs", "self.peers.connected_peer_ids().filter(|peer| !self.peer_is_important(peer)).count()", "self.confirmed_ordinary_peer_count()", "public_population_gauge_preserves_provisional_qualification_gate"),
]
CASES += PUBLIC_ADMISSION_CASES

def execute(argv, directory, label):
    """Retain finite compiler/test logs, without accepting a compiler failure as a killed mutant."""
    result = subprocess.run(argv, cwd=ROOT, capture_output=True, timeout=1800)
    raw = result.stdout + result.stderr
    if len(raw) > MAX_LOG_BYTES:
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
    excluded = tomllib.loads((repository / "Cargo.toml").read_text()).get("workspace", {}).get("exclude", [])
    excluded_package = manifests[0].parent.relative_to(repository).as_posix() in excluded
    # An excluded patch without its own lock uses the root's locked dependency graph.
    standalone = excluded_package and (manifests[0].parent / "Cargo.lock").is_file()
    selection = (["--manifest-path", manifests[0].relative_to(repository).as_posix()]
                 if standalone else ["-p", package])
    compile_argv = ["cargo", "+1.94", "test", "--locked", *selection, "--no-run"]
    test_argv = ["cargo", "+1.94", "nextest", "run", "--locked", *selection,
                 "-E", f"test({regression})", "--no-tests", "fail", "--test-threads", "1"]
    target = []
    if source.parent == manifests[0].parent / "examples":
        target = ["--example", source.stem]
    elif excluded_package and source.is_relative_to(manifests[0].parent / "src"):
        target = ["--lib"]
    compile_argv += target
    test_argv += target
    return package, compile_argv, test_argv


def select_cases(shard_index=0, shard_count=1):
    """Partition the unchanged ordered registry without omitting or duplicating a control."""
    if not 1 <= shard_count <= len(CASES) or not 0 <= shard_index < shard_count:
        raise ValueError("invalid mutation shard index/count")
    names = [case[0] for case in CASES]
    if len(set(names)) != len(names):
        raise ValueError("duplicate mutation registry identity")
    return [case for index, case in enumerate(CASES) if index % shard_count == shard_index]


def provenance(source, run_id, run_attempt):
    """Bind receipts to the clean checked-out source and the caller's current CI execution."""
    if not re.fullmatch(r"[0-9a-f]{40}", source or "") or any(
            not re.fullmatch(r"[1-9][0-9]{0,19}", value or "") for value in (run_id, run_attempt)):
        raise ValueError("invalid mutation source/run provenance")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip()
    tree = subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], cwd=ROOT).decode().strip()
    if head != source or subprocess.run(["git", "diff", "--quiet", "HEAD", "--"], cwd=ROOT).returncode:
        raise ValueError("mutation checkout must match the clean expected source")
    return {"source": head, "tree": tree, "run_id": run_id, "run_attempt": run_attempt,
            "registry_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "case_set_sha256": hashlib.sha256(json.dumps(CASES, separators=(",", ":")).encode()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--source")
    parser.add_argument("--run-id")
    parser.add_argument("--run-attempt")
    args = parser.parse_args()
    selected = select_cases(args.shard_index, args.shard_count)
    supplied = (args.source, args.run_id, args.run_attempt)
    if (args.shard_count > 1 or any(supplied)) and not all(supplied):
        raise ValueError("sharded mutation execution requires source/run provenance")
    identity = provenance(*supplied) if all(supplied) else None
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {"version": 1, "provenance": identity, "shard_index": args.shard_index,
                "shard_count": args.shard_count, "cases": [case[0] for case in selected],
                "complete": False}
    (args.output / "manifest.json").write_text(json.dumps(manifest, sort_keys=True) + "\n")
    reports = []
    try:
        for name, relative, before, after, regression in selected:
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
                report["mutated_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
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
                if report["restored_sha256"] != report["source_sha256"]:
                    raise ValueError(f"{name}: mutation source was not restored")
            print(json.dumps({"mutation": name, "detected": True}, sort_keys=True), flush=True)
        if identity is not None and provenance(*supplied) != identity:
            raise ValueError("mutation source provenance changed during execution")
        manifest["complete"] = True
        (args.output / "manifest.json").write_text(json.dumps(manifest, sort_keys=True) + "\n")
    finally:
        with (args.output / "report.json").open("x") as output:
            json.dump(reports, output, allow_nan=False, sort_keys=True, indent=2)
            output.write("\n")


if __name__ == "__main__":
    main()
