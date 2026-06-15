# Code Review: branch `separate-validator-operator-trust` vs `origin/main`

Date: 2026-06-10
Scope: Full branch diff `origin/main...HEAD` (merge base `34911812`), 93 files, ~4,075 insertions / ~1,526 deletions. Themes: operator-vs-committee trust separation in the libp2p peer layer, digest newtypes (`ConsensusHeaderDigest`/`EpochDigest`/`ConsensusNumHash`), immutable Arc-backed `Header`/`ConsensusOutput`, `ConsensusChainReader/Writer` traits, archive durability (directory fsync), duplicate-batch handling in committed sub-DAGs, state-sync simplification.

## Summary

The trust-separation core is largely sound (every penalty path passes the `TrustBasis` exemption; committee windows and operator allowlist survive epoch rotation), but review found one Critical interaction between the new duplicate-batch handling and epoch closing, a High ban-evasion path via network-key rotation in `upsert_peer`, a High wrong-pack write in storage, and a cluster of Medium durability/liveness issues.

| # | Title | Severity | Category | Status |
|---|-------|----------|----------|--------|
| 1 | Epoch-closing system call silently skipped when final output contains a duplicate batch digest | Critical | Consensus Safety | Confirmed |
| 2 | Duplicate batch digests desynchronize `batch_digests` from `batches`, misaligning digest→block mapping | High | Consensus Safety | Confirmed |
| 3 | `upsert_peer` wipes ban/score/operator state on network-key rotation (ban evasion) | High | Security | Confirmed |
| 4 | Epoch-mismatched ConsensusOutput can be appended into the wrong pack file | High | State corruption | Confirmed |
| 5 | `collect_epoch_records` u32 underflow at epoch 0 disables epoch-record collector (pre-existing) | High | Liveness | Confirmed |
| 6 | `save_consensus_output` fallback silently drops un-persisted outputs and returns Ok | Medium | Error Handling | Confirmed |
| 7 | Epoch-level directory entries never fsynced in `base_path` | Medium | Durability | Confirmed |
| 8 | `stream_import` accepts trailing batch records, producing a pack that fails `files_consistent` on next open | Medium | State corruption | Confirmed |
| 9 | Torn write to a single LatestConsensus slot file prevents node startup (pre-existing) | Medium | Error Handling | Confirmed |
| 10 | LayeredDatabase background write errors logged-and-dropped while memory layer cleared (pre-existing) | Medium | Durability | Confirmed |
| 11 | Backward header walk spins forever requesting consensus number 0 | Medium | Liveness | Confirmed |
| 12 | Non-atomic check-then-set on `last_published_consensus_num_hash` can regress watch value | Medium | Concurrency | Confirmed |
| 13 | Public RPC JSON representation of digests silently changed from 0x-hex to base58 | Medium | Serde/API compatibility | Confirmed |
| 14 | `ConsensusOutput::deserialize` accepts unvalidated `batches`/`batch_digests`/`address` fields | Medium | Serde correctness | Confirmed |
| 15 | `Digest` string deserialization silently zero-pads short bs58 inputs, reachable from RPC params | Low | Serde correctness | Confirmed |
| 16 | Committee member discovered mid-epoch left in `temporarily_banned` cache | Low | Consensus Safety (liveness) | Confirmed |
| 17 | Unidentified committee validator can be Fatal-penalized before BLS identity resolution | Low | Consensus Safety (liveness) | Confirmed |
| 18 | Stale `bls_by_peer_id` entries and unbounded known-peer growth from kad records | Low | Resource exhaustion | Confirmed |
| 19 | `add_trusted_peer` unconditionally decrements banned-peer total | Low | Error Handling | Confirmed |
| 20 | `CommittedSubDag::leader()` panics on deserialized empty-headers subdag (pre-existing) | Low | Bugs | Confirmed |
| 21 | `fsync_directory` failures silently ignored on all create paths | Low | Error Handling | Confirmed |
| 22 | `DataFile::rename` reports failure after the rename already succeeded | Low | Error Handling | Confirmed |
| 23 | Pack-download retry loop re-downloads up to 100 times on concurrent-import TOCTOU | Low | Liveness | Confirmed |
| 24 | `request_missing_packs` no longer repairs incomplete packs behind the latest processed epoch | Low | Epoch-boundary correctness | Confirmed |
| 25 | Gossip update landing in the inter-epoch gap is never forwarded to the backfill queue | Low | Concurrency | Confirmed |
| 26 | Duplicate-batch warn log dumps the full output digest list per duplicate occurrence | Low | Diagnostics | Confirmed |
| 27 | CI verification fully skipped for two additional GitHub accounts | Low | CI policy | Confirmed |
| 28 | Bidirectional `From<B256>` on every digest newtype permits cross-domain digest laundering | Informational | Type confusion | Spot-checked (informational) |
| 29 | `Header::validate` digest-integrity check downgraded to `debug_assert_eq!` | Informational | Bugs | Spot-checked (informational) |

## Findings

### 1. Epoch-closing system call silently skipped when final output contains a duplicate batch digest
- **Severity (initial)**: Critical
- **Category**: Consensus Safety
- **Location**: `crates/types/src/primary/output.rs:240-244` (interacting with `crates/consensus/executor/src/subscriber.rs:450-456`)
- **Claim**: `close_epoch_for_last_batch(index)` returns `Some(true)` only when `(index + 1) >= batch_digests.len()`. The subscriber keeps duplicate occurrences in `batch_digests` (pushed at subscriber.rs:450-456) but drops them from `batches` (subscriber.rs:477-493), so the maximum flattened `batch_index` reached by the engine loop is `batch_digests.len() - dup_count - 1` and the condition is never satisfied. If the epoch-closing `ConsensusOutput` contains a duplicated batch digest, no `TNPayload` receives `close_epoch: Some(randomness)`, so `apply_consensus_block_rewards` and `apply_closing_epoch_contract_call` are never invoked. All validators skip identically; the on-chain epoch is never concluded. A single Byzantine validator can trigger this by including an already-referenced batch digest in its header during the epoch-boundary commit window.
- **Key Question**: With `close_epoch == true` and `batch_digests` of length N containing one duplicate while flattened `batches` has N-1 entries, does any iteration of the payload loop satisfy `(index + 1) >= N`, and is there any other code path that applies the closing system calls for a non-empty final output?
- **Relevant Files**: crates/types/src/primary/output.rs, crates/consensus/executor/src/subscriber.rs, crates/engine/src/payload_builder.rs, crates/tn-reth/src/payload.rs, crates/tn-reth/src/evm/block.rs, crates/node/src/manager/node/epoch.rs
- **Source**: tn-review

### 2. Duplicate batch digests desynchronize `batch_digests` from `batches`, misaligning digest→block mapping
- **Severity (initial)**: High
- **Category**: Consensus Safety
- **Location**: `crates/consensus/executor/src/subscriber.rs:477-493` (root cause), manifesting at `crates/engine/src/payload_builder.rs:117-120`
- **Claim**: In `Subscriber::fetch_batches`, `batch_digests` is built by pushing every payload digest of every header including duplicates, but duplicate occurrences are dropped from `cert_batches`/`batches`. The resulting `ConsensusOutput` has `batch_digests.len() > flatten_batches().len()`. The engine's `execute_consensus_output` loop indexes `output.get_batch_digest(batch_index)` by the flattened-batches enumeration index, so every batch positioned after a duplicate's second occurrence executes with the wrong `batch_digest` (used as the block's ommers-hash field) and wrong `mix_hash`/prev_randao. The guard at payload_builder.rs:51-55 is only a `debug_assert_eq!`. Behavior is identical on all honest validators (no divergence), but canonical blocks permanently record incorrect batch-digest bindings and duplicated randomness.
- **Key Question**: In a sub-dag where one batch digest appears in two headers, does `output.batch_digests().len()` exceed `output.flatten_batches().len()`, and does the block built for the post-duplicate batch receive the duplicate's digest as its `batch_digest`/`mix_hash` instead of its own?
- **Relevant Files**: crates/consensus/executor/src/subscriber.rs, crates/engine/src/payload_builder.rs, crates/types/src/primary/output.rs, crates/tn-reth/src/payload.rs
- **Source**: tn-review

### 3. `upsert_peer` wipes ban/score/operator state on network-key rotation (ban evasion)
- **Severity (initial)**: High
- **Category**: Security
- **Location**: `crates/network-libp2p/src/peers/all_peers.rs:155-174`
- **Claim**: `upsert_peer` resolves the existing record via `identity_for(&peer_id)` where `peer_id` derives from the *new* network key. When a peer stored under `Confirmed(bls)` republishes a `NodeRecord` with the same BLS key but a rotated network key, `identity_for(new_peer_id)` returns `Unidentified(new_peer_id)` (not yet in `bls_by_peer_id`), `self.peers.remove(&current)` returns `None`, a fresh `Peer` (score 0, not banned, `operator_allowlisted=false`) is created, and `self.peers.insert(PeerIdentity::Confirmed(bls), peer)` overwrites the existing banned/penalized record. The stale `bls_by_peer_id[old_peer_id]` entry is left orphaned. Reachable without the banned peer connecting, via `process_kad_put_request`/`process_kad_query_result` → `add_known_peer` → `upsert_peer`. A peer banned under its BLS identity sheds the ban by republishing a record with a rotated network key. The doc comment at line 162 states accumulated state should be preserved.
- **Key Question**: When `peers` already contains `Confirmed(bls)` with network key A and `upsert_peer(bls, networkKeyB, ...)` is called, is the old record's score/ban/operator state preserved, or is it overwritten by a fresh `Peer`?
- **Relevant Files**: crates/network-libp2p/src/peers/all_peers.rs, crates/network-libp2p/src/consensus.rs, crates/network-libp2p/src/peers/peer.rs
- **Source**: tn-review

### 4. Epoch-mismatched ConsensusOutput can be appended into the wrong pack file
- **Severity (initial)**: High
- **Category**: State corruption
- **Location**: `crates/storage/src/consensus.rs:494-498`, `crates/storage/src/consensus_pack.rs:882-896`
- **Claim**: `ConsensusChain::save_consensus_output` branch 1 writes to `current_pack` without checking `consensus.sub_dag().leader_epoch() == pack.epoch()`. In `Inner::save_consensus_output`, `consensus_idx = consensus_number.saturating_sub(self.epoch_meta.start_consensus_number)` maps any output with `number < start_consensus_number` to index 0; on a freshly-created empty epoch pack, the guards `0 < 0` and `0 != 0` both pass, so a prior-epoch header and its batches are appended at index 0 of the new epoch's pack, and the legitimate first output of the epoch is then silently dropped as "already saved". Symmetrically, the first output of epoch E+1 arriving while `current_pack` is still the sealed epoch-E pack hits `consensus_idx == len` and is appended into the sealed pack. The corrupted pack persists across restarts and `trunc_and_heal` cannot repair it (the record is CRC-valid and indexed). Reachability: after a restart with stale `LatestConsensus` slot files (fsynced only on epoch change), and `handle_consensus_header` (subscriber.rs:139-145) only rejects headers with epoch *greater* than the committee epoch, not lesser.
- **Key Question**: Can `save_consensus_output` be invoked with an output whose leader_epoch differs from `current_pack.epoch()` while `consensus.number() > latest_consensus.number()` — specifically after a restart with stale slot files and a freshly-opened empty epoch pack, or at the sealed/next-pack boundary before `new_epoch` runs?
- **Relevant Files**: crates/storage/src/consensus.rs, crates/storage/src/consensus_pack.rs, crates/consensus/executor/src/subscriber.rs, crates/node/src/manager/node/epoch.rs
- **Source**: tn-review

### 5. `collect_epoch_records` u32 underflow at epoch 0 disables epoch-record collector (pre-existing)
- **Severity (initial)**: High
- **Category**: Liveness
- **Location**: `crates/state-sync/src/epoch.rs:96`, `crates/state-sync/src/epoch.rs:113`, `crates/state-sync/src/epoch.rs:136`
- **Claim**: When a fresh node requests epoch 0's record/cert from peers and validation of a peer response fails, the function executes `return epoch - 1` with `epoch == 0`. With release-profile arithmetic (no `overflow-checks`), this wraps to `u32::MAX`, which becomes `last_epoch` in `spawn_epoch_record_collector` (epoch.rs:190-201); `requested_epoch >= last_epoch` is then effectively never true again, silently disabling epoch-record collection for the process lifetime. A node that cannot collect epoch records cannot sync past epoch boundaries. In debug builds this panics. The code is byte-identical on `origin/main` (pre-existing, file modified by this branch).
- **Key Question**: Does `request_epoch_cert(Some(0), None)` accept the first peer response such that an invalid record reaches the `return epoch - 1` statements at lines 113/136 with `epoch == 0`, with no saturating arithmetic or earlier guard?
- **Relevant Files**: crates/state-sync/src/epoch.rs, crates/consensus/primary/src/network/mod.rs, crates/node/src/manager/node/epoch.rs, Cargo.toml
- **Source**: tn-review

### 6. `save_consensus_output` fallback silently drops un-persisted outputs and returns Ok
- **Severity (initial)**: Medium
- **Category**: Error Handling
- **Location**: `crates/storage/src/consensus.rs:499-515` (also `consensus.rs:296`, `consensus.rs:599-605`)
- **Claim**: When `current_pack` is `None` and the output is not found in the static pack (or `get_static` fails, or `contains_consensus_header_number` errors — swallowed by `unwrap_or_default()` at line 502), the function emits only a `warn!` and returns `Ok(())`. Callers (`state_sync::save_consensus` → executor handlers) treat `Ok` as durably persisted, then broadcast/sign the output. `current_pack` can be `None` outside the intended replay scenario because `ConsensusChain::new` (line 296) swallows `open_append_exists` errors with `.ok()` — a damaged pack at startup yields `None`, and live committed outputs would be acknowledged but never persisted. `persist_current()` (lines 599-605) is also a silent no-op when `current_pack` is `None`.
- **Key Question**: Is there a production sequence where `current_pack` is `None` while live (non-replay) outputs flow through `save_consensus_output`, and does caller behavior depend on `Ok` implying durability?
- **Relevant Files**: crates/storage/src/consensus.rs, crates/state-sync/src/lib.rs, crates/consensus/executor/src/subscriber.rs
- **Source**: tn-review

### 7. Epoch-level directory entries never fsynced in `base_path`
- **Severity (initial)**: Medium
- **Category**: Durability
- **Location**: `crates/storage/src/consensus.rs:431-446` (rename), `crates/storage/src/consensus_pack.rs:548` and `:769` (create_dir_all), `crates/storage/src/epoch_records.rs:482`
- **Claim**: The branch added `fsync_directory` for file creates/renames inside the archive layer, but nothing ever fsyncs `base_path` itself. (a) `ConsensusChain::stream_import` does `remove_dir_all(&base_dir)` then `rename(&path_base_dir, &base_dir)` with no `fsync_directory(&self.base_path)` — after power failure the delete can be durable while the rename dirent is not, leaving the epoch absent despite import success. (b) `open_append`/`stream_import`/`epoch_records::open_append` create the `epoch-N` directory via `create_dir_all` with no parent fsync, so a whole epoch's pack can vanish after power loss. Both recoverable by re-streaming from peers, but they defeat the durability guarantee `persist()`/`stream_import` claim.
- **Key Question**: After power failure between `stream_import`'s rename (or `open_append`'s `create_dir_all`) and journal commit, does node recovery detect the missing epoch dir and re-stream, and is loss of the node's own current-epoch pack (its `read_last_committed` crash-recovery source) tolerable?
- **Relevant Files**: crates/storage/src/consensus.rs, crates/storage/src/consensus_pack.rs, crates/storage/src/epoch_records.rs, crates/storage/src/archive/data_file.rs
- **Source**: tn-review

### 8. `stream_import` accepts trailing batch records, producing a pack that fails `files_consistent` on next open
- **Severity (initial)**: Medium
- **Category**: State corruption
- **Location**: `crates/storage/src/consensus_pack.rs:812-878` (no post-loop check on `batches`)
- **Claim**: Extra/Missing-batch validation only runs when a `Consensus` record is encountered. `Batch` records streamed after the last `Consensus` record are appended to the data file and indexes, and the loop ends with no `ExtraBatches` check on the residual set. The import passes the final-header check in `ConsensusChain::stream_import` and is renamed into place — but `files_consistent()` on the next `open_static` requires the last `consensus_idx` record to be the last record in the file, so the imported pack is immediately judged `CorruptPack` and unreadable until re-streamed. A malicious peer can repeatedly cause accept-then-corrupt cycles (peer-triggerable resync churn).
- **Key Question**: Can a peer append batch records after the final consensus record in a streamed pack and have the import accepted, and is the resulting `open_static` `CorruptPack` → re-stream loop the only consequence?
- **Relevant Files**: crates/storage/src/consensus_pack.rs, crates/storage/src/consensus.rs, crates/consensus/primary/src/network/mod.rs
- **Source**: tn-review

### 9. Torn write to a single LatestConsensus slot file prevents node startup (pre-existing)
- **Severity (initial)**: Medium
- **Category**: Error Handling
- **Location**: `crates/storage/src/consensus.rs:128-131` (propagation), `consensus.rs:93-115` (read_slot)
- **Claim**: `LatestConsensus::new` propagates `read_slot` errors with `?`: a CRC failure or short read in either slot file (e.g., torn 16-byte in-place `write_all` during power failure — the update path rewrites in place with no temp-file/rename) makes `ConsensusChain::new` fail, so the node cannot start even though the other slot is intact and packs carry ground truth. The dual-slot + CRC scheme is designed to tolerate exactly one corrupt slot, but error handling treats one bad slot as fatal. Pre-existing code in an in-scope changed file.
- **Key Question**: Does a crash mid-`write_all` of one slot file produce a CRC-failing file that hard-fails `ConsensusChain::new` (and node startup) rather than falling back to the surviving slot or pack-derived recovery?
- **Relevant Files**: crates/storage/src/consensus.rs
- **Source**: tn-review

### 10. LayeredDatabase background write errors logged-and-dropped while memory layer cleared (pre-existing)
- **Severity (initial)**: Medium
- **Category**: Durability
- **Location**: `crates/storage/src/layered_db.rs:113-161`
- **Claim**: In `db_run`, a failed `ins.insert(&db)` (or `insert_txn`, `remove`, `clear_table`, txn `commit`) is only logged; the insert is still pushed to `committed_inserts` and the memory-overlay entry is cleared even though it was never persisted. Subsequent reads return `None` and no error reaches the writer (the `DbTxMut` API returns `Ok` on enqueue). For `TableHint::Epoch` tables a paired-table write can land in one table but not another with no detection on open. Behavior unchanged from main (the branch only added table names to log lines); reported for completeness.
- **Key Question**: Is any consensus-critical invariant (e.g., Certificates vs CertificateDigestByRound consistency on restart) broken if one background insert fails while its sibling succeeds, and is log-only handling acceptable for MDBX map-full errors?
- **Relevant Files**: crates/storage/src/layered_db.rs, crates/storage/src/lib.rs, crates/storage/src/composite_db.rs
- **Source**: tn-review

### 11. Backward header walk spins forever requesting consensus number 0
- **Severity (initial)**: Medium
- **Category**: Liveness
- **Location**: `crates/state-sync/src/consensus.rs:42-69` (loop at `:278-301`)
- **Claim**: The old `get_consensus_header` terminated the backward walk via `if number <= consensus_chain.latest_consensus_number() { return None; }` (true at 0). The new pack-presence check (`consensus_header_by_number(number)`) can never be `Some` for number 0: packs reject numbers below `start_consensus_number` (epoch 0 starts at 1), header 0 is never written, and the cache never holds 0. The only `Done` for 0 is a successful `network.request_consensus(0, hash)`, which no peer can serve (handler returns `UnknownConsensusHeaderDigest`). The first backfill task always gets `end_number = last_number.unwrap_or_default() = 0` (consensus.rs:335), so once it walks past header 1 it enters `ConsensusHeaderResult::Retry` forever — up to 3 peer requests every 1-5 s plus a warn log for the process lifetime, permanently holding one of the 6 backfill task slots. Reachable by any Observer/CvvInactive node syncing while epoch 0 is in progress, or any fresh node whose walk outruns pack downloads.
- **Key Question**: Is `ConsensusHeaderResult::Done` unreachable for `number == 0` (no pack, cache, or peer source), and does the first backfill task always use `end_number = 0` so `number < end_number` never breaks the loop?
- **Relevant Files**: crates/state-sync/src/consensus.rs, crates/storage/src/consensus_pack.rs, crates/storage/src/consensus.rs, crates/consensus/primary/src/network/handler.rs, crates/types/src/task_manager.rs
- **Source**: tn-review

### 12. Non-atomic check-then-set on `last_published_consensus_num_hash` can regress watch value
- **Severity (initial)**: Medium
- **Category**: Concurrency
- **Location**: `crates/consensus/primary/src/network/handler.rs:290-292` (stale check at `:254-259`)
- **Claim**: `process_gossip` is spawned as an independent task per gossip message, so multiple `PrimaryGossip::Consensus` handlers run concurrently. The handler reads `published_consensus_num_hash()` early, awaits (`get_committee`, `behind_consensus`), then calls `last_published_consensus_num_hash().send_replace((epoch, number, hash))` unconditionally. A concurrent handler that reached quorum for a higher number can be overwritten by a stale lower number, violating the watch's monotonic contract. The branch added the atomic `publish_consensus_num_hash_if_newer` helper (consensus_bus.rs:404-420) and converted state-sync call sites but left this site racy. Same pattern: `requested_missing_epoch` still uses non-atomic `borrow` + `send_replace(current.max(previous_epoch))` at node/src/manager/node/epoch.rs:478-479 and 496-497 despite `set_request_missing_epoch_if_newer` existing. Effect is transient and influences only sync/catch-up decisions.
- **Key Question**: Can two concurrent `PrimaryGossip::Consensus` tasks with different numbers interleave between the early read and `send_replace`, and is any consumer (e.g., `behind_consensus` epoch_behind check) sensitive to a temporarily regressed value?
- **Relevant Files**: crates/consensus/primary/src/network/handler.rs, crates/consensus/primary/src/network/mod.rs, crates/consensus/primary/src/consensus_bus.rs, crates/node/src/manager/node/epoch.rs, crates/state-sync/src/epoch.rs
- **Source**: tn-review

### 13. Public RPC JSON representation of digests silently changed from 0x-hex to base58
- **Severity (initial)**: Medium
- **Category**: Serde/API compatibility
- **Location**: `crates/types/src/crypto/mod.rs:44` (bs58 human-readable serialization), `crates/execution/tn-rpc/src/rpc_ext.rs:24,43,87`, `crates/types/src/primary/block.rs:20`, `crates/types/src/primary/epoch.rs:30`
- **Claim**: `ConsensusHeader.parent_hash`, `EpochRecord.parent_hash`, `EpochVote.epoch_hash`, and `EpochCertificate.epoch_hash` changed from `B256` (0x-hex JSON) to digest newtypes wrapping `Digest<32>` (bs58 JSON). These types are returned by public JSON-RPC endpoints `tn_latestConsensusHeader`/`tn_epoch`, and `tn_epochRecordByHash` now takes an `EpochDigest` parameter (was `BlockHash`/hex). Existing RPC clients sending/expecting 0x-hex break. Binary (bcs/bincode) encoding is unchanged. No test pins the JSON-RPC wire format. (Found independently by two review tracks.)
- **Key Question**: Is the hex→bs58 JSON representation change for the public `tn` namespace intentional and coordinated with external consumers, or should the digest newtypes serialize as 0x-hex in human-readable form?
- **Relevant Files**: crates/types/src/crypto/mod.rs, crates/types/src/primary/block.rs, crates/types/src/primary/epoch.rs, crates/execution/tn-rpc/src/rpc_ext.rs, crates/execution/tn-rpc/src/lib.rs
- **Source**: tn-review

### 14. `ConsensusOutput::deserialize` accepts unvalidated `batches`/`batch_digests`/`address` fields
- **Severity (initial)**: Medium
- **Category**: Serde correctness
- **Location**: `crates/types/src/primary/output.rs:94-104`
- **Claim**: This branch newly adds `Serialize`/`Deserialize` to `ConsensusOutput`. The deserializer recomputes only `consensus_header_hash_cache` from `(parent_hash, sub_dag, number)`. The serialized form also carries `batches: Vec<CertifiedBatch>` (including per-batch beneficiary `address`) and `batch_digests`, which are NOT covered by the consensus header digest and are not cross-checked against `sub_dag` header payloads on deserialize. A hand-crafted serialized `ConsensusOutput` can carry a valid consensus header hash alongside arbitrary transactions/beneficiaries. The doc comment "This value is included in [Self] digest" on `batch_digests` (output.rs:53) is false — `digest_from_parts` hashes only parent_hash, sub_dag digest, and number. No production path currently deserializes `ConsensusOutput`, so this is latent.
- **Key Question**: Will any future or in-flight sync/network path deserialize `ConsensusOutput` from untrusted bytes, and should `deserialize` validate `batch_digests`/`batches` against `sub_dag` header payloads?
- **Relevant Files**: crates/types/src/primary/output.rs, crates/types/src/primary/block.rs, crates/storage/src/consensus_pack.rs, crates/consensus/executor/src/subscriber.rs
- **Source**: tn-review

### 15. `Digest` string deserialization silently zero-pads short bs58 inputs, reachable from RPC params
- **Severity (initial)**: Low
- **Category**: Serde correctness
- **Location**: `crates/types/src/crypto/mod.rs:83-92`
- **Claim**: `DigestVisitor::visit_str` calls `bs58::decode(v).onto(&mut bytes)` and ignores the returned decoded length, so a bs58 string decoding to fewer than 32 bytes is silently accepted as a digest with a zeroed tail rather than rejected as `invalid_length`. The visitor is pre-existing, but the branch newly routes JSON-RPC inputs through it (`tn_epochRecordByHash` takes `EpochDigest`; on main the param was `BlockHash` whose hex deserializer enforces exact length). Effect is wrong lookups/NotFound, not consensus divergence.
- **Key Question**: Should `visit_str` check that the decoded length equals `DIGEST_LEN` before calling `visit_bytes`?
- **Relevant Files**: crates/types/src/crypto/mod.rs, crates/execution/tn-rpc/src/rpc_ext.rs
- **Source**: tn-review

### 16. Committee member discovered mid-epoch left in `temporarily_banned` cache
- **Severity (initial)**: Low
- **Category**: Consensus Safety (liveness)
- **Location**: `crates/network-libp2p/src/peers/manager.rs:683-693`
- **Claim**: `add_known_peer` calls `apply_membership_if_committee(bls_key)` (which unbans at the `AllPeers` level and resets score) but does NOT call `forgive_temporarily_banned`. `PeerManager::peer_banned` checks `temporarily_banned.contains(peer_id) || self.peers.peer_banned(peer_id)`, so a committee member discovered mid-epoch while still in `temporarily_banned` remains connection-blocked until the cache entry expires (default 600 s). `update_committees` does forgive, but only runs at epoch start.
- **Key Question**: Can a committee member be in `temporarily_banned` when its record is first learned via `add_known_peer`, and does anything lift the temporary ban before the next epoch boundary?
- **Relevant Files**: crates/network-libp2p/src/peers/manager.rs, crates/network-libp2p/src/peers/cache.rs
- **Source**: tn-review

### 17. Unidentified committee validator can be Fatal-penalized before BLS identity resolution
- **Severity (initial)**: Low
- **Category**: Consensus Safety (liveness)
- **Location**: `crates/network-libp2p/src/peers/all_peers.rs:750-770` (exemption), `crates/network-libp2p/src/consensus.rs:1058-1079` (verify_gossip)
- **Claim**: The trust exemption returns `Validator` only for `PeerIdentity::Confirmed(bls)` in a committee set; `Unidentified` peers are not exempt. A committee validator opening an inbound connection before this node learns its BLS key is keyed `Unidentified`; in `verify_gossip` such a source has `peer_to_bls == None`, and for a restricted-publisher topic the acceptance predicate fails, yielding `GossipAcceptance::Reject` and `Penalty::Fatal` (consensus.rs:819-823), banning the peer. The ban lifts once the record is discovered (`apply_membership_if_committee`), but there is a window where a legitimate validator is banned/gossipsub-blacklisted. Bounded by kad discovery/prefetch; late-gossip acceptance documented as future work.
- **Key Question**: Can an inbound committee peer reach a `Penalty::Fatal` path while still `Unidentified`, and how reliably does `find_authorities` prefetch resolve identity first?
- **Relevant Files**: crates/network-libp2p/src/peers/all_peers.rs, crates/network-libp2p/src/consensus.rs
- **Source**: tn-review

### 18. Stale `bls_by_peer_id` entries and unbounded known-peer growth from kad records
- **Severity (initial)**: Low
- **Category**: Resource exhaustion
- **Location**: `crates/network-libp2p/src/peers/all_peers.rs:155-174` (orphan creation), `:950-969` (prune_disconnected_peers)
- **Claim**: (a) On network-key rotation, the old `bls_by_peer_id[old_peer_id]` entry is never removed — `evict(Confirmed(bls))` removes only the entry derived from the peer's current network key — so the index accumulates orphans. (b) Peers learned only via kad (`add_known_peer` → `upsert_peer`) are inserted with `ConnectionStatus::Unknown` and never pruned by `prune_disconnected_peers`/`prune_banned_peers` (which filter on `Disconnected`/`Banned`); `peers`, `bls_by_peer_id`, and the manager's `known_peers` grow with every distinct BLS-keyed record. Bounded in practice by the kad store record cap, not by the peer maps.
- **Key Question**: Is there any pruning path for `Confirmed` peers that are known-but-never-connected, and is the count of distinct attacker-introduced BLS keys bounded independently of kad store limits?
- **Relevant Files**: crates/network-libp2p/src/peers/all_peers.rs, crates/network-libp2p/src/peers/manager.rs, crates/network-libp2p/src/kad.rs
- **Source**: tn-review

### 19. `add_trusted_peer` unconditionally decrements banned-peer total
- **Severity (initial)**: Low
- **Category**: Error Handling
- **Location**: `crates/network-libp2p/src/peers/all_peers.rs:147`
- **Claim**: `add_trusted_peer` calls `banned_peers.remove_banned_peer(trusted_peer.known_ip_addresses())`. `new_trusted` constructs the peer with empty multiaddrs, but `BannedPeers::remove_banned_peer` always executes `self.total = self.total.saturating_sub(1)` regardless of whether the peer was banned (banned.rs:43). Adding a never-banned trusted peer decrements the global banned total, which feeds `prune_banned_peers`' excess calculation. The count can drift below the true number of banned peers if trusted peers are added after bans accumulate.
- **Key Question**: Is `add_trusted_peer` ever invoked after bans accumulate, and does an under-counted total cause real banned peers to escape pruning or be pruned prematurely?
- **Relevant Files**: crates/network-libp2p/src/peers/all_peers.rs, crates/network-libp2p/src/peers/banned.rs, crates/network-libp2p/src/peers/peer.rs
- **Source**: tn-review

### 20. `CommittedSubDag::leader()` panics on deserialized empty-headers subdag (pre-existing)
- **Severity (initial)**: Low
- **Category**: Bugs
- **Location**: `crates/types/src/primary/output.rs:423`
- **Claim**: `leader()` uses `.expect("sub dag MUST have a leader")` and `CommittedSubDag::deserialize` (output.rs:331-339) accepts `headers: []` from the wire (inside peer-fetched `ConsensusHeader`s). `Default` guarantees one header but deserialization has no such guard. Identical `expect` existed on main; exposure is gated because peer-fetched headers are digest-verified against trusted hashes before `leader_epoch()` is called — though `leader_epoch()` at state-sync/src/lib.rs:326 runs before the chain-digest check at :344 (the header source is the hash-verified cache). Defense-in-depth gap, not introduced by this branch.
- **Key Question**: Should `CommittedSubDag::deserialize` reject empty `headers` to make the `leader()` invariant unforgeable at the serde boundary?
- **Relevant Files**: crates/types/src/primary/output.rs, crates/state-sync/src/lib.rs, crates/consensus/primary/src/network/mod.rs
- **Source**: tn-review

### 21. `fsync_directory` failures silently ignored on all create paths
- **Severity (initial)**: Low
- **Category**: Error Handling
- **Location**: `crates/storage/src/archive/data_file.rs:59-64`, `crates/storage/src/archive/digest_index/index.rs:296-301,338-340`, `crates/storage/src/archive/digest_index/odx_header.rs:54-58`, `crates/storage/src/archive/position_index/index.rs:127-133,143-145`
- **Claim**: Every newly-added `fsync_directory` call on the create paths discards its result with `let _ =`, so an fsync failure (EIO, or platforms where opening a directory fails) silently voids the durability guarantee the call was added to provide. Inconsistently, the rename path in `DataFile::rename` propagates the same error.
- **Key Question**: Should create-path fsync failures at minimum be logged, and is the inconsistency with the rename path intentional?
- **Relevant Files**: crates/storage/src/archive/data_file.rs, crates/storage/src/archive/digest_index/index.rs, crates/storage/src/archive/digest_index/odx_header.rs, crates/storage/src/archive/position_index/index.rs
- **Source**: tn-review

### 22. `DataFile::rename` reports failure after the rename already succeeded
- **Severity (initial)**: Low
- **Category**: Error Handling
- **Location**: `crates/storage/src/archive/data_file.rs:190-206`
- **Claim**: After a successful `fs::rename`, `self.data_file_path` is updated, then `fsync_directory(new_parent)` is called and its error is returned as `RenameError::RenameIO`. The caller then believes the rename failed although it succeeded on disk and the in-memory path was updated — a retry hits the `self.data_file_path == path` early-return Ok, masking the earlier failure. On Windows (where `File::open` on a directory fails) every rename would error despite succeeding.
- **Key Question**: Do any callers of `Pack::rename`/`DataFile::rename` take corrective action on `RenameError` that misbehaves when the rename actually succeeded?
- **Relevant Files**: crates/storage/src/archive/data_file.rs, crates/storage/src/certificate_pack.rs
- **Source**: tn-review

### 23. Pack-download retry loop re-downloads up to 100 times on concurrent-import TOCTOU
- **Severity (initial)**: Low
- **Category**: Liveness
- **Location**: `crates/state-sync/src/consensus.rs:183-250`
- **Claim**: On main, after `request_epoch_pack` returned Ok the loop broke unconditionally. On this branch the `break` moved inside the `Ok(Some(final_header))` arm; the `Ok(None)`/`Err` arms of the final-header lookup fall through to retry, re-running the whole pack request. The dedup checks (`already_streaming_epoch`, `is_epoch_complete`) are evaluated only once before the loop. Because `stream_import` returns `Ok(())` immediately when another worker is importing the same epoch, a second fetch worker loops (request → header-not-found → sleep → re-request) until the first worker completes or `attempts > 100` triggers requeue. Self-healing churn with misleading "STUCK" logs; no permanent stall identified.
- **Key Question**: Can the `Ok(None)`/`Err` arms occur for reasons other than a concurrent import or a dummy epoch-0 record, and is the 100-attempt cycle guaranteed to terminate once the epoch's record/pack are locally complete?
- **Relevant Files**: crates/state-sync/src/consensus.rs, crates/storage/src/consensus.rs, crates/storage/src/epoch_records.rs
- **Source**: tn-review

### 24. `request_missing_packs` no longer repairs incomplete packs behind the latest processed epoch
- **Severity (initial)**: Low
- **Category**: Epoch-boundary correctness
- **Location**: `crates/state-sync/src/consensus.rs:425-432`
- **Claim**: The old implementation walked backward from the latest epoch record through trailing incomplete packs and re-requested from the first missing one. The new implementation starts at `consensus_chain.latest_consensus_epoch()` and iterates forward only. Any epoch with an incomplete/corrupt pack *before* the latest processed epoch (e.g., slot files ahead of damaged pack data after a crash) is never re-requested by this safety net; the node permanently fails to serve those historical packs to syncing peers, and `is_epoch_complete` errors are only logged. This function was added "in response to an early testnet freeze" as a wonky-state recovery net.
- **Key Question**: Is there state (crash between slot persist and pack persist, or out-of-order concurrent imports plus restart) where an epoch < `latest_consensus_epoch()` has an incomplete pack the node itself still needs, with nothing else re-requesting it?
- **Relevant Files**: crates/state-sync/src/consensus.rs, crates/storage/src/consensus.rs
- **Source**: tn-review

### 25. Gossip update landing in the inter-epoch gap is never forwarded to the backfill queue
- **Severity (initial)**: Low
- **Category**: Concurrency
- **Location**: `crates/state-sync/src/consensus.rs:127-150` (subscribe at `:132`)
- **Claim**: `spawn_track_recent_consensus` is epoch-scoped (shut down at every epoch boundary) and re-subscribes to the app-scoped `last_published_consensus_num_hash` watch on each epoch start. `watch::Sender::subscribe()` marks the current value as seen, so a value published in the gap between the old task's death and the new task's first `changed()` is never forwarded to `consensus_request_queue` — no backfill task is spawned for it. Recovery depends on the next quorum gossip publication; on a live network that arrives within seconds, so impact is a transient stall. Loop structure pre-dates the branch; branch touched this function.
- **Key Question**: Is there a scenario (very slow production near an epoch boundary) where the missed watch value is the last publication for an extended period, stalling an observer's catch-up?
- **Relevant Files**: crates/state-sync/src/consensus.rs, crates/consensus/primary/src/consensus_bus.rs, crates/node/src/manager/node/epoch.rs
- **Source**: tn-review

### 26. Duplicate-batch warn log dumps the full output digest list per duplicate occurrence
- **Severity (initial)**: Low
- **Category**: Diagnostics
- **Location**: `crates/consensus/executor/src/subscriber.rs:488`
- **Claim**: The warn on the duplicate path logs `?batch_digests` — the entire `VecDeque` of all batch digests in the output (up to ~300 per the SAFETY comment) — once per duplicate occurrence. A Byzantine validator persistently including duplicates makes every committed sub-dag emit large warn lines on every validator, inflating log volume.
- **Key Question**: Is the full list needed in this warn, or should it log only the duplicate digest and certificate digest?
- **Relevant Files**: crates/consensus/executor/src/subscriber.rs
- **Source**: tn-review

### 27. CI verification fully skipped for two additional GitHub accounts
- **Severity (initial)**: Low
- **Category**: CI policy
- **Location**: `.github/workflows/pr.yaml:30`
- **Claim**: The maintainer skip-list `MAINTAINERS=("grantkee" "sstanfield" "MavenRain" "Huwonk")` gained two accounts. For PRs authored by any listed account, the `fmt`, `clippy`, and `test` jobs are all skipped, and the `ci-success` branch-protection gate still passes because its `if: always()` step only fails on `result == "failure"` — skipped jobs count as success. PRs from these accounts merge with zero automated checks.
- **Key Question**: Is adding MavenRain and Huwonk to the CI-bypass allowlist an authorized policy decision, and is it acceptable that `ci-success` reports green when every check job was skipped?
- **Relevant Files**: .github/workflows/pr.yaml
- **Source**: tn-review

### 28. Bidirectional `From<B256>` on every digest newtype permits cross-domain digest laundering
- **Severity (initial)**: Informational
- **Category**: Type confusion
- **Location**: `crates/types/src/crypto/mod.rs:177-194`
- **Claim**: The `digest_newtype` macro generates both `From<$name> for B256` and `From<B256> for $name` for every digest type, so `ConsensusHeaderDigest → B256 → EpochDigest` compiles via two `.into()` calls, partially defeating the type-separation purpose of the newtypes. All current conversion sites in the workspace were audited and no actual cross-domain misuse exists on this branch.
- **Key Question**: Should `From<B256>` be replaced with named constructors at the two legitimate B256-ingestion sites (e.g., `parent_beacon_block_root`) to keep the laundering door closed?
- **Relevant Files**: crates/types/src/crypto/mod.rs, crates/storage/src/consensus_pack.rs, crates/node/src/manager/node.rs, crates/tn-reth/src/payload.rs
- **Source**: tn-review

### 29. `Header::validate` digest-integrity check downgraded to `debug_assert_eq!`
- **Severity (initial)**: Informational
- **Category**: Bugs
- **Location**: `crates/types/src/primary/header.rs:114-117`
- **Claim**: Main returned `HeaderError::InvalidHeaderDigest` when `Hash::digest(self) != self.digest()`; the branch removes the check and error variant, replacing it with `debug_assert_eq!` (not a production guard). The invariant is now structurally enforced: `HeaderInner` is module-private, all four construction paths compute the digest, and the cached digest is `#[serde(skip)]` so a peer cannot ship a mismatched digest. The old check was already a tautological no-op for deserialized headers.
- **Key Question**: Does any path construct `HeaderInner` without setting the digest (tuple-construction sites confined to header.rs)?
- **Relevant Files**: crates/types/src/primary/header.rs, crates/types/src/error.rs
- **Source**: tn-review

## Verification Results

Verified by the `findings-verifier` agent (independent re-trace of every claim from source, attempting refutation), with orchestrator spot-checks of all Critical/High mechanisms against branch code (`output.rs:240-244`, `subscriber.rs:448-501`, `all_peers.rs:155-174`, `consensus.rs:489-518` + `consensus_pack.rs:882-896`, `epoch.rs:96/113/136`).

| Severity | Submitted | Confirmed | Refuted | Notes |
|----------|-----------|-----------|---------|-------|
| Critical | 1 | 1 | 0 | #1 |
| High | 4 | 4 | 0 | #2–#5 |
| Medium | 9 | 9 | 0 | #11 and #12 flagged borderline-Low (impact bounded/transient), severity held |
| Low | 13 | 13 | 0 | — |
| Informational | 2 | 2 | 0 | spot-checked only; no production guard at risk |

**29 submitted → 27 confirmed (Tier 1–3) + 2 informational spot-checked. 0 false positives.** Every mechanism reproduced from source. Pre-existing-on-`main` flags retained for separate triage on #5, #9, #10, #20, and partially #25 — these are valid bugs but not introduced by this branch.

## Proposed Fixes

- **#1 / #2 (Critical/High, one root cause):** In `Subscriber::fetch_batches`, stop pushing duplicate digests into `batch_digests` so it stays 1:1 with the executed `batches` (`flatten_batches()`). This realigns `get_batch_digest(batch_index)` and makes `close_epoch_for_last_batch` reach the final index. Separately, promote the `debug_assert_eq!(batch_digests.len(), batches.len())` in `execute_consensus_output` (`payload_builder.rs:51-55`) to a hard error so any future desync fails loudly in release instead of silently corrupting block fields.
- **#3 (High):** In `upsert_peer`, resolve the existing record by stable BLS identity first (look up `PeerIdentity::Confirmed(bls_public_key)` before falling back to the network-key-derived `Unidentified`), migrate via `update_net`, and remove the stale `bls_by_peer_id[old_peer_id]` entry (also closes #18a).
- **#4 (High):** In `ConsensusChain::save_consensus_output`, guard the `current_pack` write with `consensus.sub_dag().leader_epoch() == pack.epoch()`; route epoch-mismatched outputs to the static-pack branch. In the inner `Pack::save_consensus_output`, reject `consensus_number < start_consensus_number` with `InvalidConsensusNumber` instead of flooring to index 0 via `saturating_sub`.
- **#5 (High, pre-existing):** Replace `return epoch - 1` with `return epoch.saturating_sub(1)` at epoch.rs:96/113/136, and clamp the collector's `last_epoch = next.max(last_epoch)` so a bad epoch-0 response cannot poison the watermark.
- **#6 (Medium):** Make `save_consensus_output` return an error (not `Ok(())`) when an output cannot be persisted to any pack, and stop swallowing `open_append_exists` failures with `.ok()` in `ConsensusChain::new` — a damaged pack at startup should surface, not silently disable persistence.
- **#7 / #21 / #22 (Medium/Low durability):** Add `fsync_directory(&self.base_path)` after the `stream_import` rename and after each `epoch-N` `create_dir_all`. Log (don't `let _ =`) create-path fsync failures for consistency with the rename path. In `DataFile::rename`, fsync the parent directory *before* updating `self.data_file_path`, or treat a post-rename fsync error as non-fatal-but-logged so callers don't see a false failure for a rename that landed.
- **#8 (Medium):** In `stream_import`, after the record loop add an `ExtraBatches` check on the residual `batches` set (mirroring the per-`Consensus`-record validation) so trailing batch records are rejected at import rather than producing a `CorruptPack` on next open.
- **#9 (Medium, pre-existing):** In `LatestConsensus::new`, fall back to `(0,0)` (or pack-derived recovery via `latest_consensus_header_from_pack`) for a CRC-failing slot instead of propagating the error — honoring the dual-slot design's single-corruption tolerance. Consider temp-file+rename for slot updates to avoid torn in-place writes.
- **#10 (Medium, pre-existing):** In `LayeredDatabase::db_run`, on a failed persist do not clear the memory overlay; surface the error to a health signal or halt rather than silently losing the write. Requires a policy decision (durability-vs-availability).
- **#11 (Medium→borderline-Low):** Restore a termination guard for the backward walk so `number == 0` ends the walk (`Done`/`None`) instead of entering perpetual `Retry`; bound or release the held backfill slot.
- **#12 (Medium→borderline-Low):** Replace the unconditional `send_replace` at handler.rs:290-292 with the existing atomic `publish_consensus_num_hash_if_newer` helper; apply `set_request_missing_epoch_if_newer` at the two `requested_missing_epoch` call sites (epoch.rs:478-479, 496-497).
- **#13 (Medium, human decision):** Decide whether the public `tn` RPC digest fields should serialize as 0x-hex (preserve API) or bs58 (current). If hex is required, give the digest newtypes a human-readable serde impl that emits 0x-hex, and add a JSON-RPC wire-format regression test.
- **#14 (Medium, latent):** Before any sync/network path deserializes `ConsensusOutput`, have `deserialize` validate `batch_digests`/`batches`/`address` against the `sub_dag` header payloads, and fix the false doc comment at output.rs:53.
- **#15 (Low):** In `DigestVisitor::visit_str`, check the bs58 decoded length equals `DIGEST_LEN` and reject otherwise.
- **#16 (Low):** Have `add_known_peer` call `forgive_temporarily_banned(peer_id)` when it applies committee membership, matching `update_committees`.
- **#17 (Low):** Track as documented future work (late-gossip acceptance) — exempt inbound committee peers once identity resolves, or hold gossip judgment until identity is known.
- **#18 (Low):** Add a pruning path for `Unknown`/known-but-never-connected `Confirmed` peers and drop orphaned `bls_by_peer_id` entries on key rotation (paired with #3).
- **#19 (Low):** Guard `remove_banned_peer` to decrement `total` only when an entry was actually removed, or skip the call in `add_trusted_peer` when the peer has no known IPs.
- **#20 (Low, pre-existing):** Have `CommittedSubDag::deserialize` reject empty `headers` so `leader()`'s invariant holds at the serde boundary.
- **#23 (Low):** Re-check `already_streaming_epoch`/`is_epoch_complete` inside the retry loop and break on a confirmed concurrent import instead of re-downloading.
- **#24 (Low):** Restore the backward scan in `request_missing_packs` (or add a separate sweep) so incomplete packs *behind* the latest processed epoch are re-requested for peer-serving.
- **#25 (Low):** After re-subscribing in `spawn_track_recent_consensus`, seed the queue with the current watch value so a publication landing in the inter-epoch gap is not lost.
- **#26 (Low):** Log only the duplicate digest and certificate digest, not the full `batch_digests` list.
- **#27 (Low, authorization):** Confirm the CI-bypass allowlist additions (`MavenRain`, `Huwonk`) are authorized; consider making `ci-success` fail when required jobs were skipped rather than only on `failure`.
- **#28 / #29 (Informational):** Optionally replace `From<B256>` with named constructors at the legitimate ingestion sites; the `debug_assert_eq!` in `Header::validate` is acceptable given the structural invariant (private `HeaderInner`, `#[serde(skip)]` digest recomputed on deserialize).
