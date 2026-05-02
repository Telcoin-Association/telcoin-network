# Phase 3: State Inconsistency Cross-Check — issue-629

_Git hash: 9b398938_
_Mode: FULL_
_Generated: 2026-04-27_
_Scope: NEW or MODIFIED state-mutating functions in the issue-629 diff. Phase 1's GAP rows are the priority list; Phase 1's VERIFIED rows are not re-litigated unless a new break is found._

---

## 3A. Mutation Matrix

Every state piece touched by the in-scope diff and its read/write boundary, in one table. "Tx" = transaction boundary that makes the multi-step update atomic. "Crash-safe?" = "does the invariant survive a process kill at any point during the write?"

| State Variable | Functions That Write | Functions That Read | Lock / Tx Boundary | Crash-Safe? |
|---|---|---|---|---|
| `data: Pack<PackRecord>` (epoch data file, append-only) | `Inner::stream_import` (consensus_pack.rs:705,741,768); `Inner::save_consensus_output` (consensus_pack.rs:819,832); `Inner::trunc_and_heal` (consensus_pack.rs:485,487,523 via `data.truncate`); `Inner::open_append` first record (consensus_pack.rs:558); `PackInner::Drop` `commit()` (pack.rs:163-164) | `Inner::get_consensus_output` (consensus_pack.rs:856,893); `Inner::contains_consensus_header` (`pos < data.file_len()`, consensus_pack.rs:923); `consensus_header_by_*` (930-947); `Inner::files_consistent` (440-471) | None across multi-record sequences. Per-record: `data.append` is sync but **NOT followed by an fsync** until `Inner::persist`. `commit()` runs in `PackInner::Drop` but only flushes + syncs at drop time. | **NO**. Process kill between `data.append` and `<index>.save` leaves the data file longer than `data_file_length` markers. Recovery via `trunc_and_heal` truncates `data` to `consensus_final` but that operation is itself non-atomic w.r.t. digest indexes. |
| `consensus_idx: PositionIndex` (round-offset → file pos) | `Inner::stream_import` (consensus_pack.rs:775); `Inner::save_consensus_output` (consensus_pack.rs:837); `Inner::trunc_and_heal` (508,516) | `Inner::get_consensus_output` (851); `consensus_header_by_number` (944-946); `read_last_committed` (974); `latest_consensus_header` (967-968); `files_consistent` (453); `read_latest_commit_with_final_reputation_scores` (994) | None. The save itself is a single index file write; no enclosing tx with `consensus_digests`. | **NO**. A crash between `consensus_digests.save` (770) and `consensus_idx.save` (775) — albeit a small window — leaves `consensus_digests` with an entry that has no `consensus_idx` partner. After `trunc_and_heal`, `consensus_idx` is shortened but `consensus_digests` is **deliberately not truncated** (489-493 comment). |
| `consensus_digests: HdxIndex` (header digest → file pos) | `Inner::stream_import` (consensus_pack.rs:770); `Inner::save_consensus_output` (consensus_pack.rs:834); never truncated — see comment at consensus_pack.rs:489-493 | `Inner::contains_consensus_header` (922); `consensus_header_by_digest` (931); `files_consistent` (447) | None. Per-record `set_data_file_length` is in-memory; `sync()` only at `Inner::persist`. | **NO**. Stale entries persist by design after `trunc_and_heal`. The `pos` they hold may point past `data.file_len()` after recovery. |
| `batch_digests: HdxIndex` (batch digest → file pos) | `Inner::stream_import` (consensus_pack.rs:743); `Inner::save_consensus_output` (consensus_pack.rs:822); never truncated by `trunc_and_heal` | `Inner::get_consensus_output` (888); `files_consistent` (448) | None. | **NO**. Same issue: a batch is appended to `data` and indexed in `batch_digests` BEFORE the consensus header that authorizes it arrives. On crash between batch-write and header-arrival, in the import dir, `data` contains the batch and `batch_digests` indexes it; `consensus_idx` is unchanged; recovery is via wholesale `remove_dir_all(import-{epoch})` if and only if the live `stream_import` returned `Err(...)` rather than being killed. |
| `epoch_meta: EpochMeta` (first record of pack) | `Inner::stream_import` (consensus_pack.rs:705 via `data.append`); `Inner::open_append` (558) | `Inner::open_append` equality check (552-555); used in start_consensus_number arithmetic everywhere | None | YES once written; never updated post-creation. |
| `data_file_length` markers on each HdxIndex | Set after every `data.append` in `stream_import` (746-748, 778-780) and `save_consensus_output` (824-826, 840-842) | `Inner::files_consistent` (447-449); `Inner::trunc_and_heal` (481-482) | None — stamp is set after the index `save`, separate from the index `save` itself. | **NO**. If process is killed between `data.append` (success, file grew) and `set_data_file_length` (call), the marker is stale; on recovery, `files_consistent` returns false but `trunc_and_heal` truncates `data` back to the last marker — effectively rolling back the just-appended record. The data is lost; no invariant is broken. |
| `pending_epoch_requests: HashMap<(BlsPublicKey, B256), PendingEpochStream>` | `process_epoch_stream` insert (mod.rs:703); `cleanup_stale_pending_requests` retain (mod.rs:493-495); `process_inbound_stream` remove (mod.rs:798-800) | `process_epoch_stream` peer count + same-key get (mod.rs:679, 698); `cleanup_stale` retain (493-495) | parking_lot Mutex on the map. | YES (in-memory only; no persistence). Permit Drop releases on entry drop. |
| `epoch_stream_semaphore: Arc<Semaphore>` (cap 5) | `try_acquire_owned` in `process_epoch_stream` (mod.rs:674); permit dropped via `_permit` field on `PendingEpochStream::Drop`; permit dropped on rejection branch (688) and on existing-key replacement when `pending_map.insert` returns `Some(old)` | implicit (semaphore counter) | None — drop-based. | YES, modulo "what if the prune loop panics?" addressed in §3D. |
| `pending_batch_requests` (worker network) | `process_request_batches_stream` insert (worker/mod.rs:342); `cleanup_stale_pending_requests` retain (worker/mod.rs:436-441); `process_inbound_stream` remove (worker/mod.rs:418-420) | `process_request_batches_stream` peer count + same-key get (worker/mod.rs:313, 332) | parking_lot Mutex. | YES (in-memory). |
| `batch_stream_semaphore` (cap 5) | identical pattern to `epoch_stream_semaphore` | implicit | None — drop-based. | YES. |
| `current_pack: Arc<Mutex<Option<ConsensusPack>>>` (consensus.rs:281) | `ConsensusChain::new_epoch` rotates (346); `stream_import` clears if epoch matches (404-410); `new` inits (297-299) | `current_pack()` clone (consensus.rs:678-680, used at 480, 511, 530, 562) | parking_lot Mutex; held briefly to swap `Option`. | YES — the Option is a single pointer swap. |
| `recent_packs: Arc<Mutex<VecDeque<ConsensusPack>>>` (cap 10) | `new_epoch` push_back (consensus.rs:342); `get_static` push_back (702); `stream_import` retain (428) | `get_static` iter (691); `latest_consensus_header_from_pack` (572-586) | parking_lot Mutex on each access (no held-across-await guarantee since it's the std-blocking parking_lot variant). | YES. |
| `LatestConsensus.state` (in-memory) + `consensus_slot1` / `consensus_slot2` files | `LatestConsensus::update` (consensus.rs:229-249) — bumps state then sends `Update` cmd to bg thread; bg thread writes 16 bytes + CRC + conditional sync (137-167) | `LatestConsensus::epoch / number / current_slot` (259-272) | `state.lock()` for in-memory; bg thread serialises file writes; sync_all only fires on epoch change (165-167) | **PARTIAL**. State is updated in memory **before** the bg thread writes the slot. A crash with no `persist()` since the last `update` leaves the in-mem epoch/number ahead of disk. A/B slot scheme tolerates a torn slot file by picking the max-(epoch, number) slot at startup. |
| `tx_last_consensus_header: watch::Sender<Option<ConsensusHeader>>` | `state-sync/consensus.rs:65` (gossip-fetch path; after `ConsensusHeaderCache.insert`); NOT touched by `stream_import` | All RPC subscribers; `behind_consensus`; e2e tests | None. | YES; advanced ONLY after `ConsensusHeaderCache.insert` succeeds. |
| `tx_last_published_consensus_num_hash: watch::Sender<(Epoch, u64, BlockHash)>` | `handler.rs:282` after 1/3+1 sigs verified; `state-sync/epoch.rs:157` | `state-sync/consensus.rs:103,138`; `behind_consensus` | None. Old-data check at 246 (`hash == old_hash || old_number >= number`). | YES (monotonically advancing). |
| `epoch_request_queue` (mpsc, capacity 10_000) | `request_epoch_pack_file` (consensus_bus.rs:551-553) — no dedup; `spawn_track_recent_consensus` (consensus.rs:116) | `get_next_epoch_pack_file_request` (consensus_bus.rs:558-560); `spawn_fetch_consensus` (consensus.rs:171) | mpsc internal; tokio::Mutex on receiver. | YES; loss only if dropped. |
| `auth_last_vote: HashMap<AuthorityIdentifier, TokioMutex<Option<...>>>` | `RequestHandler::new` initialises with current committee (handler.rs:82-87); `vote()` take/replace under per-auth TokioMutex (handler.rs:367-435) | `vote()` (365-405) | per-authority TokioMutex; map itself is keyed once at construction — no map mutation post-construction. | YES (in-memory). |
| `consensus_certs: HashMap<BlockHash, u32>` | `process_gossip` Consensus path (handler.rs:285,288); cleared at 270 and 283 | `process_gossip` get (265) | parking_lot Mutex. | YES (in-memory). |
| `requested_parents: BTreeMap<(Round, CertificateDigest), AuthorityIdentifier>` | `check_for_missing_parents` insert (handler.rs:743); pruned by `retain` at limit (731-737) | `try_accept_unknown_certs` get (765-771) | parking_lot Mutex. | YES (in-memory). |
| `import-{epoch}` directory on disk | `stream_import` `create_dir_all` (consensus_pack.rs:692); `data.append` writes (705,741,768); `<index>.save` writes (743-777); `consensus.rs:357` calls `remove_dir_all(&path)` BEFORE `stream_import` (379) and on Err (434); on success: `fs::rename(path_base_dir, base_dir)` (consensus.rs:422) | `ConsensusPack::stream_import` reads its own writes (no external readers — name is per-epoch staging) | None across the rename window. The `if exists(base_dir) { remove_dir_all(base_dir); } rename(path_base_dir, base_dir);` (consensus.rs:415-422) is acknowledged-non-atomic — comment at line 416-419. | **NO** (acknowledged TOCTOU window). On process kill mid-import: dir survives, `PackInner::Drop` runs `commit()` (pack.rs:163-164) flushing pending bytes. On next boot, no recovery code targets the `import-{epoch}` dir — see Open Q5. |
| `epoch-{epoch}/` directory (live pack location) | `new_epoch` writes via `open_append` (consensus.rs:344); `save_consensus_output` writes via `current_pack` (consensus_pack.rs:819,832); `stream_import` rename target (consensus.rs:422); `stream_import` removes pre-rename (420) | `get_static` (689); `consensus_header_by_*`; `get_epoch_stream` (446-471) | None across the (remove → rename) sequence. | **NO**. Concurrent reader between `remove_dir_all(&base_dir)` (line 420) and `rename` (line 422) gets `ENOENT`. Acknowledged in code comment. |

---

## 3B. Coupled-Pair Walk

Each pair is interrogated against the Phase 1 GAP rows. Anything not surfaced as a new break or a deeper read of an existing GAP is omitted.

### Pair 1 — `consensus_idx` ↔ `consensus_digests` (header indexes)

- **Invariant:** for every persisted consensus header at `data` position `pos`, `consensus_idx[round_offset] == pos` AND `consensus_digests[header.digest()] == pos`. Bidirectional: `consensus_digests[d] == pos` ⟹ ∃ unique round_offset such that `consensus_idx[round_offset] == pos`.
- **Order that preserves it:** `data.append` → `consensus_digests.save` → `consensus_idx.save` (the order in `stream_import:767-777` and `save_consensus_output:834-839`).
- **Order that breaks it:** any failure between the two index saves. In `stream_import` (770-777) the saves are immediately consecutive — there is no interruptible step between them in user code, but the `data_file_length` markers (lines 778-780) are set AFTER both saves, and the `set_data_file_length` is in-memory; even a `data.flush()` failure between the two `.save` calls is not a Rust language possibility because the saves don't take `&mut data`. Failure mode that DOES exist: process kill between the two `index.save` syscalls — the digest index file got an entry but consensus_idx didn't.
- **Tx boundary:** **none**. Three independent files. No fsync between, no rollback hook.
- **Crash window:** kill after `consensus_digests.save` (writes its own index file) and before `consensus_idx.save` (writes a different index file). Both files are unsynced until `Inner::persist`. On reboot, `trunc_and_heal` reconciles `data` ↔ `consensus_idx` — see pair 3 — but **explicitly leaves stale `consensus_digests` entries** (consensus_pack.rs:489-493).
- **Reader observation of inconsistent state:** `Inner::contains_consensus_header` (918-927) defensively checks `pos < data.file_len()` to filter out stale digest entries that point past truncated data. **`Inner::consensus_header_by_digest` (930-934) does NOT have that defense** — it does `consensus_digests.load(digest).ok()?` then `data.fetch(pos).ok()?`. If `pos` is < `data.file_len()` but at a record that was overwritten by a *later* successful re-stream, `data.fetch(pos)` returns SOME header, possibly not the one whose digest was used as the lookup key. (See finding F2.)
- **Verdict:** GAP — pair 1 is desynchronised after recovery, and reader paths handle the desync inconsistently.

### Pair 2 — header-indexes ↔ `batch_digests`

- **Invariant:** every batch reachable via `consensus_idx` (i.e. payload-referenced by some persisted header) must be present in `batch_digests`. Conversely, every entry in `batch_digests` must eventually be referenced by a persisted header in the same pack.
- **Order that preserves it (forward direction):** in `save_consensus_output`, all batches are appended + indexed (817-826) BEFORE the header is appended + indexed (829-839). On Err during batch loop, header is never written, so the second invariant ("batches must be referenced") is violated, but recovery can detect it via `files_consistent` length check.
- **Order that breaks it (in `stream_import`):** the protocol streams batches first, header second. Per-record, the loop at consensus_pack.rs:737-749 writes `data.append(Batch)` THEN `batch_digests.save(...)`. The HashSet `batches` (line 730) is populated, then on a `Consensus` record at 750-781 the set is drained and emptied; if drained-to-zero, the header is appended.
- **Failure mode A (within stream):** stream errors after batchN but before headerK. The HashSet still has digests in it; the in-memory check at 754-763 never runs (we never reached the next Consensus record). The data file in the import dir has the batches; `batch_digests` has them too; no header references them. On `Err` return, `consensus.rs:434` does `remove_dir_all(&path)`. Net: pollution lives only in the staging dir, then the dir is wiped.
- **Failure mode B (process kill mid-stream):** `remove_dir_all` never runs. `PackInner::Drop` runs `commit()` flushing pending bytes (pack.rs:163-164). The import dir persists with orphan batches in `data` + matching entries in `batch_digests`. **There is no boot-time reaper for `import-{epoch}` directories** — confirmed by reading `ConsensusChain::new` (consensus.rs:294-303) which only opens `current_pack` via `open_append_exists`. (See finding F4.)
- **Failure mode C (rename succeeds with partial pack):** `consensus.rs:386-401` validates `epoch_record.final_consensus.{number,hash}` matches the imported pack's `latest_consensus_header()` BEFORE `fs::rename`. So rename-on-partial cannot happen.
- **Verdict:** Failure mode A is recovered. Failure modes B and C — only B persists. **GAP confirmed for crash-resilience, see F4.**

### Pair 3 — `data` file ↔ all 3 indexes

- **Invariant:** for each index, every stored `pos` < `data.file_len()`, and the record at `pos` decodes to the type implied by the index (header for `consensus_*`, batch for `batch_digests`).
- **Order that preserves it:** `data.append` first (returns `pos`), then `index.save(key, pos)`. This order is consistently followed in `stream_import` and `save_consensus_output`.
- **Order that breaks it (recovery direction):** `trunc_and_heal` (consensus_pack.rs:474-527) **shrinks `data`** but **does not shrink `consensus_digests` or `batch_digests`**. After trunc, the digest indexes can hold `pos` values ≥ `data.file_len()`.
- **Tx boundary:** none.
- **Crash window:** see Pair 1 + Pair 2.
- **Reader observation:** `contains_consensus_header` filters by `pos < data.file_len()` and returns false for stale entries — safe. `consensus_header_by_digest` does NOT filter; if the stale `pos` happens to be < `data.file_len()` (because the data file was *re-extended* by a subsequent successful run), `data.fetch(pos)` decodes whatever record is at that position and returns it. Whether that decoded header has digest equal to the *requested* `digest` is **never re-verified**. (See finding F2.)
- **Verdict:** GAP — confirmed by the deliberate code comment at consensus_pack.rs:489-493 and the defensive guard's omission from `consensus_header_by_digest`.

### Pair 4 — `pending_epoch_requests` map size ↔ `epoch_stream_semaphore` permits

- **Invariant:** `permits + map.len() == MAX_CONCURRENT_EPOCH_STREAMS = 5`, AND for any peer P, `count(map.keys() where peer == P) ≤ MAX_PENDING_REQUESTS_PER_PEER = 2`.
- **Order that preserves it:** `try_acquire_owned()` succeeds → take the map mutex → check per-peer count → if reject, drop permit (685-688); if accept, insert entry that owns the permit (702-711). Reject branch's permit is dropped at end of arm because the value is bound to a local `permit` that goes out of scope.
- **Order that breaks it — idempotent replacement (commit cb0e147c):** at line 703, `pending_map.insert((peer, request_digest), pending)` returns `Some(old)`. The OLD entry is dropped right there, releasing the OLD `_permit`. The NEW entry already owns the freshly-acquired NEW permit. Net: no leak. **However**: between line 674 (NEW permit acquired) and line 703 (OLD permit released), the semaphore briefly counts both — `permits + map.len()` transiently overshoots `MAX_CONCURRENT_EPOCH_STREAMS - 1`. This is held under the `pending_map.lock()` so no concurrent observer can see the wrong value, but if a third concurrent `process_epoch_stream` call arrives, it competes for a single permit: the duplicate-replacement path "consumes" one of the 5 global permits even though it's replacing an existing slot. **Effective global cap during heavy duplicate traffic is `MAX_CONCURRENT_EPOCH_STREAMS - 1 = 4`.** This is not a correctness break but a soft-cap reduction. (Finding F5.)
- **Order that breaks it — panic in cleanup_stale_pending_requests:** see Pair 6.
- **Verdict:** SYNCED in the steady state; mild soft-cap reduction during replacement (F5).

### Pair 5 — `pending_epoch_requests` entry lifetime ↔ importer task

- **Invariant:** either an active task is running on behalf of this entry, or the entry is removed.
- **Order on the responder side (the only side this map serves):** entry inserted at `process_epoch_stream:703` (RPC ack), removed at `process_inbound_stream:798-800` (when the requester opens the stream). **Between insert and the inbound-stream attach, NO task is running on behalf of the entry** — it is purely awaiting. Reclamation of stuck-pending entries depends on `cleanup_stale_pending_requests` running every 15 s and reclaiming entries older than 30 s. (Per Phase 1 GAP row 5.)
- **Crash / cancellation between insert and stream-arrival:** entry sits up to 30 s. With `MAX_CONCURRENT_EPOCH_STREAMS = 5` and `MAX_PENDING_REQUESTS_PER_PEER = 2`, three malicious peers each holding 2 entries fill all 5 permits, locking out the 6th peer for up to 30 s — domain-pattern attack #3 (slot-leak DoS).
- **Verdict:** GAP-by-design — confirmed Phase 1 finding holds. Not a new break.

### Pair 6 — `epoch_stream_semaphore` permits ↔ active import work

- **Invariant:** `permits + active_concurrent_streams = 5`. "Active" = entry-in-map OR spawned-task-still-running-after-remove.
- **The closure in `process_inbound_stream:803-805` captures `opt_pending_req: Option<PendingEpochStream>`.** Drop happens when the closure returns. So permit lifetime correctly extends from RPC-ack through the spawned task's end. ✅
- **Panic in spawned task `process_request_epoch_stream`:** tokio task panic isolation drops the closure; `PendingEpochStream::Drop` runs; permit released. ✅
- **Panic in `cleanup_stale_pending_requests` (mod.rs:491-496):** parking_lot `Mutex` does NOT poison on panic. The function holds the lock across `retain`. If the closure passed to `retain` panics (it can't here — only field access — but if `Instant::now()` ever panicked the whole task ends), the outer `select!` task ends. The task is "critical" (`spawn_critical_task` at mod.rs:464); whether the supervisor restarts it is **out of scope for this audit**, but the absence of a `catch_unwind` here means **a single panic stops both event processing and prune** — entries are never reclaimed and permits leak indefinitely. (Finding F6.)
- **Critical observation:** the prune loop is the ONLY reclamation mechanism for stuck-pending entries (since `process_inbound_stream` only fires when a stream arrives). If the outer task dies, `pending_epoch_requests` becomes append-only from that moment — every new RPC consumes one of the 5 global permits, and since no inbound streams may arrive (the dispatch is in the same dead task), permits never come back.
- **Verdict:** GAP — operational fragility; no panic guard.

### Pair 7 — `tx_last_consensus_header` watch ↔ validated DB headers

- **Invariant:** the watch's value is a header that has been verified (digest matches request) AND has been inserted into a durable store (`ConsensusHeaderCache` table or `consensus_chain` pack).
- **Path 1 — `state-sync/consensus.rs:51-65`:** `db.insert::<ConsensusHeaderCache>(&header.number, &header)` then `last_consensus_header().send_replace(Some(header))`. The header was verified by `network.request_consensus(number, hash)` (`state-sync/consensus.rs:49`) — the contract guarantees `header.digest() == hash`. Insert-then-send is correct ordering.
- **Path 2 — `stream_import` does NOT touch the watch.** Confirmed by reading `consensus_pack.rs:673-785` and `consensus.rs:357-438` — no `last_consensus_header()` call.
- **Open Phase 1 question:** "Does the watch advance during a stream_import that ultimately fails?" Re-confirmed: NO, because the watch path is gossip-driven and references `ConsensusHeaderCache`, a different store from the pack file. A failed `stream_import` does not touch the watch.
- **Subtlety not surfaced in Phase 1:** the watch advances when `ConsensusHeaderCache.insert` succeeds. But the corresponding `consensus_chain` pack file may not yet contain the header — the gossip-fetch path inserts into the cache, but the pack file gets the data via the *separate* `stream_import` flow. So `last_consensus_header().borrow()` can be `Some(h)` while `consensus_chain.consensus_header_by_digest(epoch, h.digest())` returns `None`. **This is by design** (the cache is a fast lookup, the pack is durable storage), but it means downstream watch consumers cannot assume the header is in the pack. (Finding F7 — informational.)
- **Verdict:** SYNCED for the path-1 invariant ("inserted into ConsensusHeaderCache"); UNSYNCED if a downstream consumer interprets the watch as "in pack file."

### Pair 8 — `epoch_request_queue` ↔ idempotency dedup

- **Invariant claim per Phase 1:** "channel never has two live entries for the same epoch."
- **Reality:** `request_epoch_pack_file` (consensus_bus.rs:551-553) sends to the mpsc with no dedup. `spawn_track_recent_consensus` (state-sync/consensus.rs:111-121) checks `consensus_chain.consensus_header_by_number(epoch_record.final_consensus.number)` — this returns `Some` only after the pack is COMPLETE. So between fetch-start and fetch-complete, a re-trigger (e.g. another gossip event firing the loop again) DOES re-enqueue. (See finding F8.)
- **`spawn_fetch_consensus` per-task semaphore** (state-sync/consensus.rs:174): `Arc::new(Semaphore::new(1))` is created INSIDE the function body. Each call to `spawn_fetch_consensus` produces a new task with its own permit-of-1. If multiple `spawn_fetch_consensus` tasks run (the comment at line 157 says "several of these will run"), they each have their own `next_sem` and can run TWO different epochs in parallel. They share the `epoch_request_queue` (an mpsc), so each `next_epoch` call dequeues one record. The semaphore caps each task at 1 in-flight, but doesn't coordinate ACROSS tasks.
- **Collision: same-epoch in two `spawn_fetch_consensus` tasks.** If both dequeue the same epoch (e.g. duplicate enqueue), they both call `consensus_chain.stream_import` for that epoch. Inside `stream_import` (consensus.rs:377): `let path = self.base_path.join(format!("import-{epoch}"));` — same path for both. Line 379: `let _ = std::fs::remove_dir_all(&path);` — task B's `remove_dir_all` at the start of its run wipes task A's in-progress staging directory. Task A's subsequent `Pack::open(...)` calls will see `ENOENT` on already-opened files (Linux: open file descriptors survive unlink), but `data.append` writes go to the unlinked inode. Task A's `pack.persist()` (consensus.rs:386) syncs the unlinked inode. Then `pack.latest_consensus_header()` (consensus.rs:387) reads from the unlinked inode — succeeds. Then `fs::rename(&path_base_dir, &base_dir)` at line 422: `path_base_dir = path.join(format!("epoch-{epoch}"))` — but `path` was wiped, so the rename source doesn't exist. Result: task A returns `ConsensusChainError::IoError(ENOENT)`. Task B is racing in parallel. (Finding F8.)
- **Verdict:** GAP — confirmed two-task collision creates lost-import + error-cascade.

### Pair 9 — peer-score (libp2p PeerId) ↔ `report_penalty` target (BLS)

- **Invariant:** the BLS key passed to `report_penalty` resolves to the libp2p PeerId of the peer that actually sent the data we're penalising.
- **`request_epoch_pack` flow (mod.rs:342-381):** the `peer: BlsPublicKey` field is extracted from the response payload `RequestEpochStream { ack, peer }`. The same `peer` is then passed to `open_stream(peer)` and `report_penalty(peer, Mild)`.
- **What protects this:** `open_stream(peer)` resolves the BLS to a PeerId via the trusted committee mapping (out-of-scope but assumed safe). `send_request_any` returns a response from SOME peer — the network layer doesn't verify that the `peer` field in the response equals the on-wire PeerId.
- **Attack:** peer A claims `peer = B` in the response. Node calls `open_stream(B)` — opens a stream to actual peer B (different libp2p connection). Now peer B is the responder for the ensuing stream. If B's stream fails, B is rightly penalised. If B is the attacker, they self-penalise. **What's the actual attack?** Look more carefully: does `open_stream(peer)` even use the peer's BLS or does it use the libp2p PeerId of the original responder? Phase 1 Open Q2 calls this out — needs Phase 4 verification. If `open_stream` uses the BLS-mapped PeerId, the attack is "peer A sends a response, node opens a stream to B; if B is honest B will reject the stream (no pending request from us); the failure is then reported as `Penalty::Mild` against B". **This IS a griefing vector.** (Finding F9.)
- **Verdict:** GAP — confirmed Phase 1 finding; needs Phase 4 to resolve `open_stream` semantics.

### Pair 10 — `auth_last_vote` ↔ current epoch

- **Invariant:** entries in `auth_last_vote` reflect the current epoch's committee only.
- **Construction:** `RequestHandler::new` (handler.rs:81-87) preloads with current committee. The handler is owned by `PrimaryNetwork` (mod.rs:438), spawned via `epoch_task_spawner.spawn_critical_task` (mod.rs:464). **Phase 4 must verify whether `PrimaryNetwork` is rebuilt per-epoch.** If yes, the map IS scoped per-epoch by construction. If no, the map carries forward.
- **`vote()` cross-epoch consideration (handler.rs:330-440):** the digest-equality short-circuit at line 371 (`last_digest == header.digest()`) returns the cached response without inspecting `last_epoch` vs `header.epoch()`. If two epochs ever produce identical `header.digest()` for the same `author`, the cached response (e.g. an old `Vote`) is replayed. `header.digest()` includes the epoch (per `tn_types::Header::digest`), so the collision should require a SHA-256 collision — out of practical attack range. (Finding F10 — informational, mitigated by digest including epoch.)
- **`reset_for_epoch` (consensus_bus.rs:304-307):** explicitly resets only `tx_committed_round_updates` and `tx_primary_round_updates`. Does NOT touch any of the maps owned by `RequestHandler`. So if the same handler instance crosses an epoch boundary, the maps are stale. (Finding F11.)
- **Verdict:** GAP for `auth_last_vote` if (and only if) `PrimaryNetwork` lifetime exceeds one epoch — depends on Phase 4 q4.

---

## 3C. Partial Operation × Ordering Findings (Rule 4 — gold)

These are the intersections where Phase 0/2 ordering concerns and Phase 1 partial-state concerns coincide — the gold-standard nemesis findings.

### F1 — `Inner::stream_import` write-before-validate orphan-batch scar (anchor 2)

**Invariant statement:** for every `(batch_digest, pos)` entry in `batch_digests`, the corresponding header that authorises the batch must be persisted in `consensus_idx` AND `consensus_digests` of the same pack.

**Breaking operation:** `consensus_pack.rs:737-749` writes `data.append(Batch)` and `batch_digests.save(...)` for each `Batch` record. The HashSet `batches` (line 730) only validates "every payload digest in the next header is in the set" at line 754-763 when a `Consensus` record arrives. If the stream errors out (timeout, CRC mismatch, peer RST) AFTER batchN but BEFORE headerK, the import dir holds N orphan batches.

**Trigger sequence:**
1. Receiver opens stream. `import-{epoch}` is freshly created (consensus.rs:379 wiped any prior).
2. Peer streams 5 valid batch records. `data` grows; `batch_digests` gets 5 entries.
3. Peer feeds invalid CRC on the 6th record (a header). `next()` returns `Err(PackError::ReadError(...))`.
4. `Inner::stream_import` returns `Err`. Control returns to `ConsensusPack::stream_import` (consensus_pack.rs:254).
5. Control returns to `ConsensusChain::stream_import`; line 432-435 runs `remove_dir_all(&path)`.
6. **Net: import dir wiped; no on-disk pollution. Recovered.**

**Process-kill variant:**
1. Steps 1-3 as above.
2. SIGKILL between step 3 and step 5 — or even step 3 and `PackInner::Drop`'s `commit()`.
3. `import-{epoch}` survives. `data` has 5 batches plus partial header bytes (depending on where in `data.append(PackRecord::Consensus(...))` the kill landed); `batch_digests` has 5 entries pointing into `data`; `consensus_idx` is empty.
4. On reboot, `ConsensusChain::new` does NOT inspect or recover from `import-*` directories. It only opens `current_pack` (consensus.rs:298) — a different directory.
5. The orphan staging dir lives forever (until the next `stream_import(epoch)` call's `remove_dir_all` at line 379). On a fresh fetch attempt for a different epoch, the orphan persists.
6. Cumulatively, multiple kills produce a junk-file scar.

**Downstream consequence:** disk-space exhaustion under repeated crash scenarios; potential to confuse a future re-import of the same epoch (the wipe at line 379 is unconditional, so this is bounded), but more importantly: **if a partial `import-{epoch}` survives and a later code path (perhaps a future PR) ever decides to "resume" an existing import dir instead of wiping it, the orphan batches become legitimised.**

**Severity hint:** MEDIUM — current flow wipes pre-attempt, so live consensus is not corrupted. Becomes HIGH if future code resumes partial imports.

**Evidence:**
```rust
// consensus_pack.rs:737-749
PackRecord::Batch(batch) => {
    let batch_digest = batch.digest();
    batches.insert(batch_digest);
    let position = data
        .append(&PackRecord::Batch(batch))
        .map_err(|e| PackError::Append(e.to_string()))?;
    batch_digests
        .save(batch_digest, position)
        .map_err(|e| PackError::IndexAppend(format!("batch {e}")))?;
    let len = data.file_len();
    consensus_digests.set_data_file_length(len);
    batch_digests.set_data_file_length(len);
}
```

### F2 — `consensus_header_by_digest` lacks the defensive `pos < data.file_len()` filter (anchor — Rule 5)

**Invariant statement:** an entry `(digest → pos)` in `consensus_digests` must point to a record in `data` whose decoded form is a `ConsensusHeader` with `header.digest() == digest`.

**Breaking operation:** `Inner::trunc_and_heal` (consensus_pack.rs:474-527) truncates `data` and `consensus_idx` but **deliberately leaves `consensus_digests` and `batch_digests` un-truncated**. Comment at lines 489-493: *"Note we leave the digest indexes with potentially some missing digests."*

**Trigger sequence:**
1. Pack file at epoch E is at `data.file_len() == 1000`. `consensus_digests` has digest D1 → 800, D2 → 900.
2. Process kill mid-`save_consensus_output` for D3 — `data.append` succeeded growing to 1100 with partial header, but `consensus_digests.save(D3, 1000)` happened first (line 834-836 then `consensus_idx.save` at 837-839 — **wait**: read again).
3. Re-reading the order in `save_consensus_output` (consensus_pack.rs:828-839): `data.append(Consensus(...))` (832) → `consensus_digests.save(D3, position)` (834) → `consensus_idx.save(consensus_idx, position)` (837). So if kill happens between line 834 and 837: `consensus_digests` has D3, `consensus_idx` does not, `data` has the header bytes. But wait — neither index has been `sync()`ed yet (sync only at `Inner::persist`).
4. Reboot. `Inner::open_append_exists` calls `trunc_and_heal`. `pack_len` = 1100 (the data was flushed by `PackInner::Drop`'s commit, which DOES sync — pack.rs:163-164, 273); `consensus_final` = `consensus_digests.data_file_length()` — **what is this?** The line 825 marker was set in the previous batch's iteration; the line 841 marker would have been set after the `consensus_idx.save` (which never happened). So `consensus_final = 1000` (pre-D3 length). `consensus_idx` was last saved at the previous header. Since `pack_len (1100) > consensus_final (1000)`, line 487 truncates `data` to 1000.
5. After trunc: `data.file_len() == 1000`. `consensus_digests` STILL contains D3 → 1000 (the comment's "missing digests" actually meant "stale digests pointing at truncated-away data"; D3's `pos == 1000` now points exactly at `data.file_len()`).
6. Caller invokes `consensus_chain.consensus_header_by_digest(epoch, D3)`. This goes through `consensus_pack.rs:930` `consensus_header_by_digest`:
   ```rust
   let pos = self.consensus_digests.load(digest).ok()?;  // = 1000
   let rec = self.data.fetch(pos).ok()?;                 // pos == file_len: read past EOF
   rec.into_consensus().ok()                              // None (EOF)
   ```
   → returns `None` correctly *because* `data.fetch` errors at EOF.
7. Now consider a slightly worse case: `pos < data.file_len()` because subsequent appends extended `data`. After trunc, the next legitimate `save_consensus_output(D4)` runs. `data.append(D4)` now writes at offset 1000 (new). `data.file_len() == 1100` (assuming D4 is also 100 bytes). `consensus_digests.save(D4, 1000)` — **but D4 ≠ D3, so this is a different key, no collision in the digest index.** OK.
8. The actual hazard: `consensus_digests` still has D3 → 1000. A subsequent `consensus_header_by_digest(D3)` returns the header at offset 1000 — which is now D4's header. `data.fetch(pos)` succeeds; `rec.into_consensus()` succeeds; **the function returns D4's header even though the caller asked for D3's**. There is NO check that the returned header's `.digest() == requested digest`.

**Downstream consequence:** any caller that trusts `consensus_header_by_digest(D)` to return either the unique header with digest D or `None` is wrong. RPC callers requesting a specific digest can get a different header. State-sync callers using `request_consensus(number, hash)` then comparing hash will detect the mismatch and re-fetch — but this depends on the digest-comparison happening at the call site.

**Severity hint:** HIGH — silent return of wrong header to digest-keyed lookups after any crash-and-recover sequence.

**Evidence:**
```rust
// consensus_pack.rs:917-934
fn contains_consensus_header(&mut self, digest: B256) -> bool {
    // Defensive — explains the invariant break
    if let Ok(pos) = self.consensus_digests.load(digest) {
        pos < self.data.file_len()    // <-- this filter
    } else {
        false
    }
}

fn consensus_header_by_digest(&mut self, digest: B256) -> Option<ConsensusHeader> {
    let pos = self.consensus_digests.load(digest).ok()?;
    let rec = self.data.fetch(pos).ok()?;          // <-- no pos < file_len() check
    rec.into_consensus().ok()                       // <-- no digest re-comparison
}
```

### F3 — `import-{epoch}` directory collision under concurrent `spawn_fetch_consensus` tasks (anchor 9 + Phase 1 GAP 5)

**Invariant statement:** the `import-{epoch}` staging directory is owned exclusively by one in-progress `stream_import` per epoch.

**Breaking operation:** `consensus.rs:377-379`:
```rust
let path = self.base_path.join(format!("import-{epoch}"));
// We need to start with a clean import dir since we do not restart.
let _ = std::fs::remove_dir_all(&path);
```
The path is a function only of `epoch`, not of any per-task identifier. Two concurrent calls for the same `epoch` produce the same `path`.

**Trigger sequence:**
1. `spawn_fetch_consensus` task 1 dequeues `EpochRecord(epoch=42)`. Task 1 is mid-`stream_import` writing record #20 of 50 to `import-42/`.
2. `spawn_track_recent_consensus` re-enqueues `EpochRecord(epoch=42)` (because no dedup at enqueue per pair 8).
3. Task 2 dequeues — wait, both tasks share the same `epoch_request_queue` mpsc, but each task has its OWN `next_sem: Arc<Semaphore::new(1)>` (consensus.rs:174 — the Arc is created INSIDE the async fn). So task 2 is a SEPARATE `spawn_fetch_consensus` invocation. Multiple invocations are possible — comment at consensus.rs:157 says "Several of these will run."
4. Task 2 dequeues `EpochRecord(epoch=42)` (the dup). Task 2 calls `consensus_chain.stream_import(...)` for epoch 42.
5. Task 2 reaches `consensus.rs:379`: `remove_dir_all(&path)`. **Wipes task 1's staging dir.**
6. On Linux, task 1's open file descriptors (`data`, `consensus_idx`, `consensus_digests`, `batch_digests`) still reference the now-unlinked inodes. `data.append(...)` writes still succeed but go to the orphan inodes. When task 1's process-kill or normal completion drops the `Pack`, `commit()` syncs the orphan inodes to disk-but-no-name. Bytes are flushed but the directory entry is gone.
7. Task 1 reaches `consensus.rs:386`: `pack.persist().await` — succeeds (sync on orphan inodes).
8. Task 1 reaches `consensus.rs:387`: `pack.latest_consensus_header().await` — succeeds (reads from orphan-inode FDs).
9. Task 1 reaches `consensus.rs:422`: `fs::rename(&path_base_dir, &base_dir)` where `path_base_dir = import-42/epoch-42`. **The directory `import-42/` was unlinked by task 2; if it has been re-created by task 2 (line 692 in consensus_pack.rs `create_dir_all(&base_dir)`), then `import-42/epoch-42` may or may not exist at task 1's rename point.**
10. Two outcomes:
    - (a) Task 1's rename fails (`ENOENT`); task 1 returns `Err`; task 1 runs `remove_dir_all(&path)` at line 434 — wipes task 2's IN-PROGRESS staging.
    - (b) Task 1's rename succeeds (race won) — it renames task 2's freshly-created (mostly-empty) `import-42/epoch-42` to `epoch-42/`, leaving task 2's import data destroyed, and task 1's actually-imported orphan inodes to be lost on FD close.

**Downstream consequence:**
- **Lost epoch import.** Both tasks fail with confusing errors, the epoch never makes it into the live `epoch-{epoch}/` directory.
- **CPU/network burn** — both tasks streamed full epoch packs from peers, all wasted.
- **Mutual eviction** — repeated re-enqueues (per pair 8 GAP) in a heavy gossip flow could keep the epoch indefinitely failing.

**Severity hint:** HIGH — under heavy gossip churn this can stall epoch sync indefinitely; the only mitigation is "spawn only one `spawn_fetch_consensus` task," which is not enforced in code.

**Evidence:**
```rust
// state-sync/consensus.rs:174 — per-task semaphore
let next_sem = Arc::new(Semaphore::new(1));

// storage/consensus.rs:377-379 — race target
let path = self.base_path.join(format!("import-{epoch}"));
let _ = std::fs::remove_dir_all(&path);
```

### F4 — process-kill mid-`stream_import` leaves orphan `import-{epoch}` dir; no boot-time reaper

**Invariant statement:** at startup, the only on-disk consensus state is the live `epoch-{N}/` directories tracked by `LatestConsensus`; any `import-*/` dirs are stale.

**Breaking operation:** `ConsensusChain::new` (consensus.rs:294-303) opens only `current_pack` (line 297-299 — `ConsensusPack::open_append_exists(&base_path, latest_consensus.epoch())`) and the `EpochRecordDb` (line 301). Nothing iterates `import-*` directories.

**Trigger sequence:**
1. Process is mid-`stream_import` for epoch 42; `import-42/` exists with partial data.
2. `kill -9` or hardware power loss.
3. `PackInner::Drop` may or may not run (depends on cleanup). For `kill -9`: not at all.
4. Process restart: `ConsensusChain::new` ignores `import-42/`. Subsequent fetch for epoch 42 calls `stream_import` which `remove_dir_all(&path)` at line 379, which DOES clean it up.

**Downstream consequence:** disk-space leak proportional to `(number of crash-during-imports) × (max partial pack size)`. NOT consensus corruption (the orphan dir never participates in live state). The disk-space leak is bounded ONLY by the next attempt for the same epoch wiping it.

**Severity hint:** LOW — bounded by next attempt; no consensus impact.

**Evidence:** consensus.rs:294-303; consensus.rs:377-379.

### F5 — Idempotent replacement transiently inflates permit count (anchor 11)

**Invariant statement:** at any moment, `epoch_stream_semaphore` available permits + `pending_epoch_requests.len()` ≤ `MAX_CONCURRENT_EPOCH_STREAMS`.

**Breaking operation:** `process_epoch_stream` (mod.rs:666-749). The flow is:
1. Line 674: `try_acquire_owned()` — if cap is full this fails; if 4-of-5 used, acquires permit #5.
2. Line 676: `pending_map.lock()`.
3. Line 698-701: `pending_map.get(&(peer, request_digest))` — if entry exists, preserve `created_at`.
4. Line 702-703: `pending_map.insert(...)` — **OVERWRITES** existing entry. The OLD `PendingEpochStream` is dropped → OLD `_permit` dropped → 1 permit returned to semaphore.

Between line 674 and line 703, NEW permit is held AND OLD permit is held. The map size has NOT increased (replacement, not insert). So `permits_held = old + new = 2` for a single map entry. The invariant `permits + map.len() == 5` becomes `permits = 5 - map.len()`, but transiently we hold 2 permits for 1 entry.

**Trigger sequence:**
1. Peer P has 1 pending entry. Global semaphore has 4 permits free.
2. Peer P re-requests same epoch (rapid retry). `process_epoch_stream` acquires permit (3 free).
3. Lock acquired. Peer count check passes (P has 1 pending, max is 2).
4. `insert` replaces. OLD permit dropped (4 free). NEW permit held by NEW entry.
5. Net: 4 free → 4 free. ✅ Invariant holds AFTER drop.

But: **DURING** steps 2-4, the semaphore counts 3 free even though only 1 logical slot is occupied. Concurrent `process_epoch_stream` calls from a different peer would see 3 free — fine. But if 4 different peers simultaneously do replacement-trick: 4 NEW permits acquired (1 free), 4 OLD permits not yet released. Concurrent honest peer #5 sees 1 free, takes it. After all replacements settle: 5 OLD permits dropped → semaphore has 5 + 1 = 6 free **MOMENTARILY** — but the semaphore is cap 5; wait, `OwnedSemaphorePermit::Drop` calls `add_permits(1)` — `tokio::sync::Semaphore` does NOT cap added permits. **Is this a permit-count corruption?**

Let me re-check: `Semaphore::new(5)` then 5 acquire + 5 drop = back to 5. If we acquire-then-replace 4 times (+4 acquires, -4 implicit drops via insert returning Some → Drop): net 0 change. The transient over-acquire is bounded by the `try_acquire_owned` cap — you can never acquire more than 5 at once. So at any instant, AT MOST 5 permits are held by alive locals. After the `insert`, the OLD value is dropped synchronously (single-threaded code path under the mutex), so the net `add_permits` is +1 immediately. The cap-of-5 holds.

**Re-evaluation:** F5 is **DISMISSED** — the invariant holds because `try_acquire_owned` enforces the cap and the replacement's old-permit-drop happens synchronously inside the mutex. No transient violation observable to other threads.

**Severity hint:** N/A — DISMISSED.

### F6 — `cleanup_stale_pending_requests` has no panic guard (Phase 1 GAP 4)

**Invariant statement:** the prune loop runs every 15 s for the lifetime of the network task.

**Breaking operation:** `mod.rs:482-484`:
```rust
_ = prune_requests.tick() => {
    self.cleanup_stale_pending_requests();
}
```
`cleanup_stale_pending_requests` (mod.rs:491-496):
```rust
fn cleanup_stale_pending_requests(&mut self) {
    let now = Instant::now();
    self.pending_epoch_requests
        .lock()
        .retain(|_, pending| now.duration_since(pending.created_at) < PENDING_REQUEST_TIMEOUT);
}
```

**Trigger sequence:**
1. Suppose `Instant::now()` somehow panics (overflow on certain platforms — extremely rare, but `Instant` arithmetic is one of the few stdlib panics not behind `checked_*`).
2. Or `pending_map.lock()` holds a poisoned guard — parking_lot doesn't poison so this is moot.
3. Or `retain`'s closure panics — closure does only field access on `pending: &mut PendingEpochStream`; safe.
4. **The realistic panic vector is upstream** — the same task's other arm (`process_network_event`) processing a malformed event. Phase 0 noted that `process_inbound_stream` and `process_epoch_stream` spawn their own subtasks; their panics are isolated. But if `process_network_event` itself panics on the dispatch (e.g. an unhandled enum variant introduced by a future PR), the outer task ends.
5. After the outer task ends, the `pending_epoch_requests` map continues receiving inserts via... wait — inserts come from `process_epoch_stream`, which is called from `process_network_event`, which is in the same dead task. So new inserts cannot happen either. **Net consequence:** map is frozen at last state; existing entries are never reclaimed; permits leak; semaphore stays at whatever count it had at task death.
6. No supervised restart of `spawn_critical_task` is visible in the audited diff. Whether `tn-types` or task-spawner has supervisor logic is out of scope.

**Downstream consequence:** if the outer task dies mid-operation with N permits held, the semaphore has `5 - N` permits forever. Attackers who got 5 permits then triggered the panic would lock the node out of all epoch sync forever (until restart).

**Severity hint:** MEDIUM — depends on supervisor; the task is named "critical" but a hardening review should add `catch_unwind` around the prune call or verify the task supervisor restarts on panic.

**Evidence:** mod.rs:463-487, 491-496.

### F7 — `tx_last_consensus_header` watch can advance ahead of pack-file (informational)

**Invariant statement (claim):** the watch points to a header that is durable in `consensus_chain`'s pack file.

**Reality:** the watch is advanced after `ConsensusHeaderCache.insert` succeeds — that's a `node_storage` table, not the pack file. Downstream code that does `consensus_chain.consensus_header_by_digest(epoch, watch_value.digest())` may get `None`.

**Trigger sequence:**
1. Gossip event for header N. `state-sync/consensus.rs:49` — `network.request_consensus(N, hash)` returns the verified header.
2. Line 51 — `db.insert::<ConsensusHeaderCache>(&N, &header)` succeeds.
3. Line 65 — `consensus_bus.last_consensus_header().send_replace(Some(header))`.
4. RPC subscriber wakes on watch change. Calls `consensus_chain.consensus_header_by_digest(header.epoch(), header.digest())`.
5. `consensus_chain` looks up via the pack file (consensus.rs:506-522). The pack file may not yet contain this header (it's only added via `save_consensus_output` for live consensus, or `stream_import` for historical pack download).
6. Returns `None`. RPC subscriber retries; eventually pack catches up.

**Downstream consequence:** transient `None` returns from `consensus_header_by_digest` for a header that "exists" per the watch. Causes RPC retry storms but no consensus corruption.

**Severity hint:** LOW — informational; not a coupling bug per se but an asymmetry between two storage layers that share a watch.

**Evidence:** state-sync/consensus.rs:51-65; storage/consensus.rs:506-522.

### F8 — `epoch_request_queue` lacks enqueue dedup; `spawn_fetch_consensus` semaphore is per-task (Phase 1 GAP 8)

**Invariant statement:** for any given epoch E, at most one in-flight `request_epoch_pack` exists at a time.

**Breaking operation:**
- `request_epoch_pack_file` (consensus_bus.rs:551-553) enqueues without checking pending or in-flight.
- `spawn_fetch_consensus` (state-sync/consensus.rs:174) creates a per-task `Arc<Semaphore::new(1)>`; multiple invocations don't coordinate.

**Trigger sequence:** see F3 (the collision *is* the consequence of this gap).

**Downstream consequence:** F3.

**Severity hint:** HIGH (root cause of F3; if dedup is added, F3 becomes unreachable from this path).

**Evidence:** consensus_bus.rs:551-553; state-sync/consensus.rs:174.

### F9 — `request_epoch_pack` penalty mis-targets BLS (Phase 1 GAP 9, anchor — domain pattern #5)

**Invariant statement:** `report_penalty(peer, ...)` targets the libp2p PeerId of the actual responder.

**Breaking operation:** `mod.rs:342, 358, 381`:
```rust
if let PrimaryResponse::RequestEpochStream { ack, peer } =
    self.handle.send_request_any(request.clone()).await?.await??
{
    // ...
    let mut stream = self.handle.open_stream(peer).await??;
    // ...
    if res.is_err() {
        self.report_penalty(peer, Penalty::Mild).await;
    }
}
```
The `peer` in `RequestEpochStream { ack, peer }` is the responder's CLAIM in the response payload. There is no cross-check.

**Trigger sequence:**
1. Attacker peer A sends response `RequestEpochStream { ack: true, peer: B }` where B is an honest validator.
2. Receiver calls `open_stream(B)` — opens a libp2p stream to actual peer B.
3. Two sub-cases:
   - (a) B is online and accepts the stream. B reads 32 bytes, looks up `pending_epoch_requests` keyed by `(B, request_digest)`. Receiver never inserted such an entry (the receiver is the requester, not the responder; pending map is only used on the responding side). Wait — re-reading: `process_inbound_stream` looks up the receiver's `pending_epoch_requests` — but the receiver in `request_epoch_pack` is the REQUESTER, not the responder. **The requester's pending map is irrelevant here.** Let me re-trace.
4. **Re-traced:** when peer A says "ack=true, my BLS is B" in response to our `StreamEpoch` request, our node next calls `open_stream(B)`. `open_stream(peer: BlsPublicKey)` (sigil at mod.rs:358) likely does a BLS→PeerId resolution via the trusted committee mapping. If B is a real committee member with a valid PeerId binding, the stream opens to the real B. If A and B are different libp2p PeerIds, the stream goes to B, not A. B's `process_epoch_stream` handler runs — but B never received our `StreamEpoch` request, so B has no pending entry. B's `process_inbound_stream` looks up `(self.bls_public_key=B, request_digest)` in B's own `pending_epoch_requests` — but B never accepted the request, so the lookup returns None. The handler returns `PrimaryNetworkError::UnknownStreamRequest(request_digest)`. B replies with an error response over the stream OR closes it. From the requester's POV, `consensus_chain.stream_import(stream.compat(), ...)` reads zero bytes / EOF / error. `res.is_err() = true`. `self.report_penalty(B, Penalty::Mild)` — **penalises honest B**.
5. Attacker A walks away unpenalised.

**Downstream consequence:** asymmetric griefing — A burns B's reputation with no cost. With `Penalty::Mild` non-escalating, this requires many iterations to actually evict B, but it's a positive-cost-zero attack on A's side.

**Severity hint:** MEDIUM-to-HIGH — depends on penalty accumulation and committee size. The mitigation is to penalise based on the libp2p PeerId of the actual responder, which is known at the libp2p layer.

**Evidence:** mod.rs:322-395.

### F10 — `auth_last_vote` digest-equality cache replay across epochs (Phase 1 GAP 10)

**Invariant statement:** the cached vote response in `auth_last_vote[author]` is valid only for the epoch it was created in.

**Breaking operation:** `vote()` at handler.rs:371-405:
```rust
if last_digest == header.digest() {
    match last_response {
        // ...
        Some(res) => return Ok(res),  // <-- returns cached without checking last_epoch == header.epoch()
    }
}
```

**Pre-condition for break:** the cached `last_digest` equals the new `header.digest()`. Since `Header::digest()` includes the epoch field (per `tn_types::Header`), this requires either a SHA collision (cryptographically infeasible) or carrying the same `Header` across epochs (which is not how epoch transitions work).

**Verdict:** invariant is preserved by `Header::digest()` including epoch in its preimage. **DISMISSED** for cryptographic reasons. The Phase 1 concern stands ONLY if a future change to `Header::digest()` removes epoch from the preimage; flag for code-review-time monitoring.

**Severity hint:** N/A — DISMISSED.

### F11 — `reset_for_epoch` does not clear `RequestHandler` maps (Phase 1 GAP 10 supplement)

**Invariant statement:** at every epoch boundary, all per-epoch state held in `RequestHandler` must be cleared or invalidated.

**Breaking operation:** `ConsensusBusApp::reset_for_epoch` (consensus_bus.rs:304-307) only resets `tx_committed_round_updates` and `tx_primary_round_updates`. It does NOT reset:
- `auth_last_vote` (lives in `RequestHandler`, not `ConsensusBus`)
- `consensus_certs` (lives in `RequestHandler`)
- `requested_parents` (lives in `RequestHandler`)
- `pending_epoch_requests` (lives in `PrimaryNetwork`)
- `tx_last_consensus_header`, `tx_last_published_consensus_num_hash` (in `ConsensusBus` but watch values not reset)

**Trigger sequence:** depends on whether `RequestHandler` survives an epoch boundary. The handler is owned by `PrimaryNetwork`, spawned via `epoch_task_spawner.spawn_critical_task` (mod.rs:464). The naming `epoch_task_spawner` strongly suggests per-epoch lifetime, but **no code in the audited diff confirms `PrimaryNetwork` is rebuilt per epoch**.

**Downstream consequence:** if the handler does survive the epoch transition:
- `consensus_certs` accumulates entries from the previous epoch's gossip; gossip dedup at handler.rs:246 (`hash == old_hash || old_number >= number`) only checks the most recent published header, so cross-epoch sigs can accumulate without being cleared.
- `requested_parents` retains entries with old `(Round, CertificateDigest)` keys; the prune-by-round at handler.rs:731-737 eventually clears them but only as new rounds advance.
- `pending_epoch_requests` carries pending entries from the previous epoch; the 30 s prune handles this.

**Severity hint:** LOW assuming `PrimaryNetwork` is rebuilt per epoch (Open Q4 from Phase 1); MEDIUM if not.

**Evidence:** consensus_bus.rs:304-307.

---

## 3D. Defensive Code Signals (Rule 5)

Each defensive pattern in the diff reveals an invariant that's broken underneath. Trace which writes leave the broken state and which readers can be fooled.

### Signal 1 — `pos < data.file_len()` in `contains_consensus_header` (consensus_pack.rs:923)

- **Mask:** filters out digest-index entries that point past the truncated `data` file.
- **What writes leave stale entries:** `Inner::trunc_and_heal` (consensus_pack.rs:474-527) truncates `data` and `consensus_idx` only; the comment at lines 489-493 explicitly admits that `consensus_digests` and `batch_digests` are left with stale entries.
- **Readers fooled by absence of the defense:** `Inner::consensus_header_by_digest` (consensus_pack.rs:930-934). It does `let pos = self.consensus_digests.load(digest).ok()?; let rec = self.data.fetch(pos).ok()?;`. If `pos < data.file_len()` (because the file was re-extended by a later successful import or save_consensus_output), it returns whatever record sits at `pos` — possibly a different header. **No re-comparison of the returned header's digest against the requested digest.**
- **Broken invariant the mask is papering over:** the digest-index can hold entries that point at *deleted-then-overwritten* data. The `pos < data.file_len()` check works only when the data file has been truncated and not re-extended. Once re-extended, the check passes and stale entries deliver wrong data.
- **See finding F2.**

### Signal 2 — `let _ = std::fs::remove_dir_all(...)` in `stream_import` (consensus.rs:379, 393, 399, 420, 434)

- **Mask:** ignores errors from removing directories that may not exist.
- **What's underneath:** `_ =` is suppressing `Err` returns from `remove_dir_all`. For most calls (line 379, 434) the dir may not exist and ENOENT is fine. But at line 420 (the rename-pre-cleanup path), an actual permission/IO error in `remove_dir_all(&base_dir)` would be silently suppressed, and the subsequent `fs::rename(&path_base_dir, &base_dir)` would then fail with `EEXIST` (the existing directory not empty) or unexpectedly succeed if `base_dir` was partially-removed.
- **Broken invariant the mask is papering over:** the swap is non-atomic; callers must accept the TOCTOU window between unlink and rename. The error suppression hides ANY filesystem-level failure that would otherwise abort the import.

### Signal 3 — `peer_count = pending_map.keys().filter(|(p, _)| *p == peer).count()` (mod.rs:679)

- **Mask:** `O(n)` linear scan instead of a per-peer counter.
- **What's underneath:** the design rejected maintaining a separate `peer_in_flight_count: HashMap<peer, u8>`. Why? A separate map would require atomic updates with the main `pending_epoch_requests` map. By computing `count()` on demand under the same mutex, the developer avoids the dual-map atomicity problem entirely.
- **Broken invariant under a counter design:** with a separate counter, `cleanup_stale_pending_requests` would have to decrement on retain-removed entries; missing such a decrement would corrupt the counter. The linear-scan design is correct-by-construction.
- **Verdict:** GOOD defensive choice — the mask is principled.

### Signal 4 — `created_at` preservation in idempotent replacement (mod.rs:698-701)

- **Mask:** when replacing an existing pending entry, copy the OLD entry's `created_at` to prevent the cleanup timer from being rearmed.
- **What's underneath:** comment at lines 693-697 admits that without this preservation, "a peer could hold a slot indefinitely by re-requesting before the 30 s timeout."
- **Broken invariant the mask is papering over:** the cleanup timer is expressed as `created_at + 30s`. If `created_at` resets on every re-request, a peer can grief the slot indefinitely. The mask correctly closes that vector.
- **Verdict:** GOOD defensive code. (This is what commit cb0e147c does.)

### Signal 5 — `if !batches.is_empty() { return Err(PackError::ExtraBatches); }` (consensus_pack.rs:761-763)

- **Mask:** rejects a pack stream where a header doesn't claim all the previously-streamed batches.
- **What's underneath:** the protocol *implicitly* requires that every batch streamed must be claimed by the next header. The check ensures the malicious peer can't smuggle extra batches into the data file alongside an honest header.
- **Broken invariant the mask is papering over:** in the stream protocol, batches are written to disk BEFORE the header validates. If the check failed and `batches` was non-empty, those batches are already in `data` and `batch_digests`. The check correctly returns `Err`, which the caller turns into `remove_dir_all(&path)` — net recovery. **But the check executes only on the consensus record arrival; if the stream ends mid-batches with no consensus record, the loop simply returns `Ok(Self {...})`** — wait, re-read line 731-783: the `while let Some(record) = next(...)` loop completes when stream ends. **If stream ends with leftover batches (no final consensus record), `Ok(Self { ... })` is returned with `batches` still non-empty.** Caller (`ConsensusChain::stream_import` at consensus.rs:380-386) then `pack.persist().await?` — the pack with orphan batches gets persisted. Then line 387 `pack.latest_consensus_header()` — this may return `None` if no consensus headers were written. Line 397-401 catches `None` → `EmptyImport` error → `remove_dir_all(&path)`. ✅ Net recovery.
- **What if `latest_consensus_header()` returns `Some(stale_last_header)` but there are extra orphan batches after it?** That requires the stream to send `header(K), batch1, batch2, batch3, EOF`. After `header(K)` is processed at consensus_pack.rs:750-781, `parent_digest = header(K).digest()`. Then `batch1` arrives at the loop top, hits the `Batch` arm, gets written. EOF — loop ends. `Ok(Self {...})` with `batches = {batch1, batch2, batch3}`. Caller `pack.latest_consensus_header()` returns `Some(header(K))`. Caller checks `epoch_record.final_consensus.number == header(K).number` — if K is the final consensus number, this passes. **The orphan batches are now in the imported pack.** Caller proceeds to `fs::rename` line 422. **The pack is committed with orphan batches in `data` and `batch_digests`.**
- **Severity:** the orphan batches are referenced by no header (their digest is in `batch_digests` but no header in `consensus_idx`/`consensus_digests` payload-references them). When `Inner::get_consensus_output` is called for any consensus number, it traverses `cert.header().payload()` — the orphan batches are NEVER looked up because they aren't in any header's payload. Net: they consume disk space but cause no functional harm.
- **Verdict:** the mask catches the case where a header arrives after the orphan batches; the gap is "stream ends mid-batches without a follow-up header." That gap is real but consequence is limited to disk waste.

### Signal 6 — `epoch_record.final_consensus.number == last_header.number && epoch_record.final_consensus.hash == last_header.digest()` (consensus.rs:387-401, 449-451)

- **Mask:** validates that the imported pack actually ends with the expected final-consensus header before promoting it to live.
- **What's underneath:** the pack-import protocol does NOT cryptographically verify intermediate consensus headers. The only header verified-by-quorum is the FINAL one (the epoch certificate's `final_consensus`). The mask is the only defense against a peer streaming a long alternate history that ends in the right hash.
- **Broken invariant the mask is papering over:** any intermediate header in the stream could be fabricated, AS LONG AS the parent-hash chain links and the final hash matches. **The epoch certificate's quorum signature only attests to the final hash, not to the intermediate ones.** Two peers serving different "alternate histories" could both produce streams that pass this check — the receiving node accepts whichever it sees first.
- **Severity:** depends on consensus invariants. If consensus determinism requires every committed header to be quorum-signed, then alternate histories should fail to execute at downstream layers. Phase 4 should verify whether intermediate header validation happens elsewhere.
- **Severity hint:** UNRESOLVED — needs Phase 4.

---

## State-Anchor Concern Coverage

| Anchor | Concern | Verdict | Finding |
|---|---|---|---|
| 2 | write-before-validate orphan batches in stream_import | **SUBSTANTIATED** | F1 (also Signal 5 caveat) |
| 4 | race on per-peer pending count (atomicity gap) | **DISMISSED** — same `pending_map.lock()` serialises check + insert (Phase 1 confirmed) | n/a |
| 6 | three-index atomicity (no enclosing REDB txn) | **SUBSTANTIATED** | F2 (the gap is real, the consequence is "wrong-header lookups after recovery"). Phase 1 already noted this; F2 deepens by showing reader inconsistency. |
| 9 | pending-prune relies on background interval (slot leak) | **SUBSTANTIATED** | F6 (panic vector) + Phase 1 GAP 5 (slot-DoS for 30 s) |
| 11 | idempotent replacement cancels in-flight import (state vs. channel desync) | **DISMISSED** — replacement is on the responder side; no import is in flight at the moment of replacement (entry is just-pending, awaiting inbound stream). The OLD permit is dropped synchronously, no transient invariant break. | n/a |
| 12 | watch channel advances during import (downstream readers see transient state) | **DISMISSED** — `stream_import` does not touch `tx_last_consensus_header`. The watch is advanced only by the gossip-fetch path after `ConsensusHeaderCache.insert`. The asymmetry between cache and pack file is F7 (informational). | F7 |

Non-state anchors (1, 3, 5, 7, 8, 10) — **Phase 2 owns these**.

---

## Findings

| ID | File:Line | Severity Hint | Title |
|---|---|---|---|
| F1 | crates/storage/src/consensus_pack.rs:737-749 | MEDIUM | `Inner::stream_import` writes batches to data + index before header validation; partial state lives in the import dir until error-handler `remove_dir_all` |
| F2 | crates/storage/src/consensus_pack.rs:930-934 | HIGH | `Inner::consensus_header_by_digest` lacks `pos < data.file_len()` defense AND lacks digest re-comparison; can return wrong header after `trunc_and_heal` + re-extend |
| F3 | crates/storage/src/consensus.rs:377-422 + crates/state-sync/src/consensus.rs:174 | HIGH | `import-{epoch}` path collision under concurrent `spawn_fetch_consensus` tasks; two tasks for same epoch mutually destroy each other's staging dir |
| F4 | crates/storage/src/consensus.rs:294-303 | LOW | No boot-time reaper for orphan `import-*/` directories after process kill mid-import |
| F5 | crates/consensus/primary/src/network/mod.rs:674-703 | DISMISSED | (Investigated and dismissed — replacement is synchronous under mutex; semaphore cap holds.) |
| F6 | crates/consensus/primary/src/network/mod.rs:482-496 | MEDIUM | `cleanup_stale_pending_requests` has no panic guard; if outer task dies, all permits leak indefinitely |
| F7 | crates/state-sync/src/consensus.rs:51-65 vs. crates/storage/src/consensus.rs:506-522 | LOW | `tx_last_consensus_header` advances after `ConsensusHeaderCache.insert` but pack file may not yet contain header (informational asymmetry between cache and pack) |
| F8 | crates/consensus/primary/src/consensus_bus.rs:551-553 + crates/state-sync/src/consensus.rs:174 | HIGH | `epoch_request_queue` has no enqueue dedup AND `spawn_fetch_consensus` semaphore is per-task (root cause of F3) |
| F9 | crates/consensus/primary/src/network/mod.rs:342, 358, 381 | MEDIUM-HIGH | `request_epoch_pack` penalty applied to `peer: BlsPublicKey` from response payload, not to libp2p PeerId of actual responder — mis-attribution griefing |
| F10 | crates/consensus/primary/src/network/handler.rs:371-405 | DISMISSED | `auth_last_vote` digest-equality short-circuit — DISMISSED because `Header::digest()` includes epoch |
| F11 | crates/consensus/primary/src/consensus_bus.rs:304-307 | LOW-to-MEDIUM | `reset_for_epoch` does not clear `RequestHandler` maps; severity depends on whether `PrimaryNetwork` is rebuilt per epoch (Open Q4) |

### F1 — Detail card

- **Invariant:** every entry in `batch_digests` is referenced by some persisted header in the same pack.
- **Breaking operation:** `consensus_pack.rs:737-749` writes batches before the header arrives.
- **Trigger sequence:**
  1. Receiver opens stream, allocates fresh `import-{epoch}` (line 692).
  2. Peer streams batches 1..N. `data` and `batch_digests` get N entries each in the staging dir.
  3. Peer sends RST or invalid CRC. `Inner::stream_import` returns `Err`.
  4. `ConsensusChain::stream_import` (consensus.rs:432-435) catches `Err` and runs `remove_dir_all(&path)`.
  5. **Recovered** — no live state corruption.
- **Process-kill variant trigger sequence:**
  1. As above, but SIGKILL between step 3 and step 4. `PackInner::Drop` may or may not run (depends on whether kill is `SIGTERM` with cleanup or `SIGKILL` instant).
  2. `import-{epoch}` survives with N orphan batches.
  3. Reboot. No reaper. Disk-space leak (see F4).
- **Downstream consequence (current):** disk space; no functional impact.
- **Downstream consequence (future risk):** if a future PR introduces "resume partial import" logic, the orphan batches become legitimised.
- **Code excerpt:** consensus_pack.rs:737-749 (cited above in 3C).

### F2 — Detail card

- **Invariant:** `consensus_header_by_digest(D)` returns either the unique header with `digest() == D` or `None`.
- **Breaking operation:** `consensus_pack.rs:930-934`:
  ```rust
  fn consensus_header_by_digest(&mut self, digest: B256) -> Option<ConsensusHeader> {
      let pos = self.consensus_digests.load(digest).ok()?;
      let rec = self.data.fetch(pos).ok()?;
      rec.into_consensus().ok()
  }
  ```
- **Trigger sequence:**
  1. Process kill mid-`save_consensus_output` for header D3 — kill happens between `consensus_digests.save(D3, P3)` (line 834) and `consensus_idx.save(idx3, P3)` (line 837). `PackInner::Drop`'s `commit()` flushes `data` (header bytes at P3) and the digest-index file (D3 entry).
  2. Reboot. `Inner::open_append_exists` runs `trunc_and_heal`. Sees `pack_len > consensus_final` (because consensus_digests was flushed but `set_data_file_length` wasn't called or was called and stamped the new length — depending on order). Either way, `trunc_and_heal` truncates `data` to a length that may evict P3. `consensus_digests` keeps D3 by design (489-493 comment).
  3. New `save_consensus_output(D4)` runs. `data.append(D4)` writes at the truncated tail — same offset P3 (or near it). `consensus_digests.save(D4, P3)` — different key, no in-place collision.
  4. Caller invokes `consensus_chain.consensus_header_by_digest(epoch, D3)`.
  5. `consensus_digests.load(D3)` returns `Ok(P3)` (stale entry).
  6. `data.fetch(P3)` returns `Ok(record)` — but the record at P3 is now D4's header, not D3's.
  7. `record.into_consensus().ok()` returns `Some(D4_header)`.
  8. **Function returns `Some(D4_header)` for a request that asked for `D3`.**
- **Downstream consequence:** any call site that looks up by digest and trusts the returned header without re-checking `returned.digest() == requested_digest` proceeds with wrong data. Examples:
  - `ConsensusChain::consensus_header_by_digest` (consensus.rs:506-522) — direct passthrough; same vulnerability bubbles up.
  - RPC handler `retrieve_consensus_header` (handler.rs:821-834) — gets the header by digest after waiting for the right number; no digest-recheck.
- **Severity:** HIGH for any path that uses digest-keyed lookups for non-recoverable decisions.
- **Mitigation:** add either `pos < data.file_len()` filter (matching `contains_consensus_header`) OR re-compare the returned header's digest against the requested key.
- **Code excerpts:** consensus_pack.rs:489-493, 918-934 (cited above).

### F3 — Detail card

- **Invariant:** `import-{epoch}` is owned by exactly one `stream_import` call at a time.
- **Breaking operation:** consensus.rs:377-379 derives the path from `epoch` only; consensus.rs:379 unconditionally `remove_dir_all`.
- **Trigger sequence:** see 3C, F3.
- **Downstream consequence:** mutual eviction of two parallel imports of the same epoch; both fail; epoch sync stalls until queue drains.
- **Severity:** HIGH under heavy gossip churn.
- **Mitigation candidates:**
  - Dedup at the enqueue site (per F8).
  - Use a per-task suffix on the import path: `import-{epoch}-{task-uuid}`.
  - Hold a global `Mutex<HashSet<Epoch>>` of in-flight epochs.

### F4 — Detail card

- **Invariant:** at startup, no orphan staging directories exist.
- **Breaking operation:** `ConsensusChain::new` (consensus.rs:294-303) does not iterate or clean `import-*` dirs.
- **Trigger sequence:** see 3C, F4.
- **Downstream consequence:** disk-space leak bounded by next attempt for the same epoch.
- **Severity:** LOW.
- **Mitigation:** add a glob over `base_path.join("import-*")` at `ConsensusChain::new` and `remove_dir_all` each.

### F6 — Detail card

- **Invariant:** the prune loop runs every 15 s for the lifetime of the process.
- **Breaking operation:** mod.rs:482-496; no `catch_unwind` or supervised restart visible in audit scope.
- **Trigger sequence:** see 3C, F6.
- **Downstream consequence:** if outer task dies, all held permits leak indefinitely; new requests are rejected at `try_acquire_owned`.
- **Severity:** MEDIUM (depends on supervisor).
- **Mitigation:** wrap `cleanup_stale_pending_requests()` and `process_network_event(...)` in `tokio::task::spawn_blocking(... catch_unwind)` or document the supervisor's restart policy.

### F8 — Detail card

- **Invariant:** at most one `request_epoch_pack` per epoch in flight at a time.
- **Breaking operation:** consensus_bus.rs:551-553 (no dedup); state-sync/consensus.rs:174 (per-task semaphore).
- **Trigger sequence:** see F3.
- **Downstream consequence:** F3.
- **Severity:** HIGH (root cause of F3).
- **Mitigation:** lift the semaphore to a parameter passed by the spawner; add `Mutex<HashSet<Epoch>>` of in-flight epochs at the requester side.

### F9 — Detail card

- **Invariant:** `report_penalty(peer, ...)` targets the libp2p PeerId of the actual responder.
- **Breaking operation:** mod.rs:342 reads `peer` from the response payload; mod.rs:381 penalises that BLS.
- **Trigger sequence:** see 3C, F9.
- **Downstream consequence:** asymmetric griefing — attacker A burns honest B's reputation.
- **Severity:** MEDIUM-to-HIGH.
- **Mitigation:** at the libp2p layer, expose the actual responder's PeerId or BLS in `send_request_any`'s return, and use that for the penalty target.

---

## Open Questions for Phase 4 Feedback Loop

These are the questions where Feynman + state interrogation must collide to settle the verdict.

1. **F2 root-cause sequence.** The trigger sequence in F2 assumes a process kill between `consensus_digests.save` and `consensus_idx.save`. Two unknowns:
   - (a) Are those saves' file writes buffered, sync'd to disk, or memory-mapped? If unsync'd, the kill-window is much wider than two syscalls — it spans up to the next `Inner::persist`. Phase 4: characterise the actual durability behavior of `HdxIndex::save` and `PositionIndex::save`.
   - (b) Does `trunc_and_heal` reliably truncate `data` *before* the next `data.append` extends it? If yes, the F2 hazard requires re-extending `data` to position `pos` again — quantify how often this happens in practice (i.e. does the pack normally re-write the same offset?).

2. **F3 spawn count.** How many `spawn_fetch_consensus` tasks does the application spawn? The comment at state-sync/consensus.rs:157 says "several" — Phase 4 must trace the actual call sites.

3. **F6 task-supervisor behavior.** If the outer "primary network events" task dies, what does `tn-types::TaskSpawner::spawn_critical_task` do? Is there a supervisor that restarts? Is the panic surfaced to the application as a fatal error?

4. **F9 `open_stream(peer: BlsPublicKey)` semantics.** Does the libp2p layer enforce that the BLS-mapped PeerId matches a peer we're already connected to? If yes, the attack is constrained to peers in the connected set; if no, it's broader.

5. **F11 `PrimaryNetwork` lifetime.** Is `PrimaryNetwork` rebuilt per epoch? The `epoch_task_spawner` parameter at `spawn` (mod.rs:464) suggests yes, but the `RequestHandler` is constructed in `PrimaryNetwork::new` (mod.rs:438) so its lifetime is the same as `PrimaryNetwork`'s. Phase 4: trace the call chain that constructs `PrimaryNetwork` and confirm whether it's per-application or per-epoch.

6. **Signal 6 — intermediate-header validation.** Does any code path validate intermediate consensus headers' signatures during stream import? If only the final-consensus hash is validated, how does the receiver detect that an intermediate header was fabricated by a malicious peer? Likely answer: the receiver's downstream execution layer recomputes headers from sub-DAGs and compares; if so, F-Signal-6 collapses. Phase 4: confirm.

7. **F1 future-resume risk.** Is there any roadmap-level pressure to add "resume partial import" functionality? If yes, F1's MEDIUM becomes HIGH because the orphan-batches scar gets persisted into the live pack.

8. **`auth_last_vote` map mutation.** The map is initialised once and never mutated as a map (only the inner `TokioMutex<Option<_>>` values are mutated). If a new authority joins mid-epoch (committee change), they have no entry — `vote()` at handler.rs:365-438 returns `Err(HeaderError::UnknownAuthority(...))`. Is this the intended behavior? Phase 4: confirm committee-change cadence vs. handler lifetime.
