# Phase 2: Feynman Interrogation — issue-629

_Git hash: 9b398938_
_Mode: FULL — every entry-point function in scope was interrogated._
_Generated: 2026-04-27_
_Language: Rust (libp2p + bcs + custom Pack/HdxIndex/PositionIndex storage)._

---

## Function Verdicts (full coverage)

| File | Function | Verdict | Notes |
|---|---|---|---|
| `crates/storage/src/archive/pack_iter.rs:135-160` | `AsyncPackIter::read_record_file` | **VULNERABLE** | Untrusted `val_size as usize` — peer can request arbitrary allocation up to ~4 GiB per record. Anchor 1. |
| `crates/storage/src/archive/pack_iter.rs:59-87` | `PackIter::read_record_file` (sync) | SUSPECT | Same uncapped resize as async; only matters for local data, but `Inner::open_append` opens local file with this iter — a corrupt local file (e.g. half-imported during attacker-driven crash) can OOM on reopen. |
| `crates/storage/src/archive/pack_iter.rs:163-171` | `AsyncPackIter::next` | SOUND | Pure delegation. |
| `crates/storage/src/archive/pack.rs:313-355` | `PackInner::read_record` / `record_size` | SUSPECT | Same `value_buffer.resize(val_size as usize, 0)` shape; same lack of cap. Used for local pack reads — primary risk is an attacker-corrupted local file. |
| `crates/storage/src/consensus_pack.rs:254-269` | `ConsensusPack::stream_import` (public wrapper) | SUSPECT | Calls `Inner::stream_import` to completion BEFORE spawning the bg thread. So Inner::stream_import runs on the caller's task, fully ingesting all peer bytes before returning. Means every byte that fails inside Inner-stream is buffered into the Inner that then is dropped — the data file gets fsynced via `Drop` of `PackInner`. Anchor 2. |
| `crates/storage/src/consensus_pack.rs:673-785` | `Inner::stream_import` | **VULNERABLE** | Write-before-validate ordering for batches. Batches written to data file & batch_digests AND `set_data_file_length` advanced BEFORE the consensus header that references them validates the parent-hash chain. On failure the temp dir is removed in caller, BUT the per-record state is observably advanced inside the Inner being constructed, AND if the failure happens after a successful header on record N+1, the orphan window for batches between header N and the failed record persists in the Inner index. Anchor 2. |
| `crates/storage/src/consensus_pack.rs:680-690` | `Inner::stream_import::next` (helper) | SUSPECT | Wraps each record in `tokio::time::timeout(timeout, iter.next())`. Per-record cap only — no whole-stream cap. Anchor 10. |
| `crates/storage/src/consensus_pack.rs:531-592` | `Inner::open_append` | SOUND | Calls `trunc_and_heal` with explicit divergence comment at 489-493 (digest indexes intentionally not truncated). |
| `crates/storage/src/consensus_pack.rs:474-527` | `Inner::trunc_and_heal` | SUSPECT | Recovery deliberately leaves stale entries in `consensus_digests`/`batch_digests`. Defensive `pos < data.file_len()` filter exists in `contains_consensus_header` (line 923) but NOT in `consensus_header_by_digest` (line 930) or `batch` (line 1023). |
| `crates/storage/src/consensus_pack.rs:918-927` | `Inner::contains_consensus_header` | SOUND | Defensive bound check `pos < data.file_len()` is present. |
| `crates/storage/src/consensus_pack.rs:930-934` | `Inner::consensus_header_by_digest` | SUSPECT | Loads `pos` from `consensus_digests`, calls `data.fetch(pos)`. NO `pos < data.file_len()` defensive check (unlike sibling at 918-927). Stale digest entry post-`trunc_and_heal` can return an unexpected fetch outcome (CrcFailed → Err converted to None). |
| `crates/storage/src/consensus_pack.rs:1022-1032` | `Inner::batch` | SUSPECT | Same shape — no `pos < file_len()` check. |
| `crates/storage/src/consensus_pack.rs:788-845` | `Inner::save_consensus_output` | SUSPECT | Same write-before-validate ordering as stream_import: batches written to `data` and `batch_digests` BEFORE the consensus header is written. If a mid-loop `data.append` succeeds but `<index>.save` fails, data file leads indexes; `trunc_and_heal` repairs on next open. |
| `crates/storage/src/consensus_pack.rs:950-958` | `Inner::persist` | SOUND | Issues `data.commit` then sync on each index. Order is data first, then indexes — consistent with crash-recovery model (data leads, indexes catch up at rest). |
| `crates/storage/src/consensus_pack.rs:850-908` | `Inner::get_consensus_output` | SOUND | Read path; bounds-checked via `consensus_idx.load`. |
| `crates/storage/src/consensus.rs:357-438` | `ConsensusChain::stream_import` | **VULNERABLE** | (a) line 379 unconditionally `remove_dir_all(&path)` of the import dir — concurrent fetches for same epoch destroy each other; (b) "tiny window before rename" acknowledged in code comment at line 417. Anchor 12 + adversarial #5. |
| `crates/storage/src/consensus.rs:325-348` | `ConsensusChain::new_epoch` | SOUND | Ordered: `persist()` old, push to recent_packs, then open new. |
| `crates/storage/src/consensus.rs:442-472` | `ConsensusChain::get_epoch_stream` | SOUND | Verifies final_consensus matches before returning the stream reader. |
| `crates/storage/src/consensus.rs:476-491` | `ConsensusChain::save_consensus_output` | SOUND | Forwards to current_pack; updates `latest_consensus`. |
| `crates/storage/src/consensus.rs:683-704` | `ConsensusChain::get_static` | SOUND | Cache eviction at len > PACK_CACHE_SIZE. |
| `crates/consensus/primary/src/network/mod.rs:322-395` | `PrimaryNetworkHandle::request_epoch_pack` | **VULNERABLE** | Trusts `peer: BlsPublicKey` from response payload (line 342) for both `open_stream` AND `report_penalty` — peer is responder's claim, not transport-bound. Anchor 7. |
| `crates/consensus/primary/src/network/mod.rs:463-488` | `PrimaryNetwork::spawn` | SOUND | Spawns critical task; tokio::select on events vs prune tick. |
| `crates/consensus/primary/src/network/mod.rs:491-496` | `PrimaryNetwork::cleanup_stale_pending_requests` | SUSPECT | No internal panic guard; the function is called from a critical task — panic ends task → process shutdown. The bug surface is a slow-leak via integer overflow in `Instant::duration_since` (no overflow possible in practice but worth flagging for crash-loop investigation). Anchor 9. |
| `crates/consensus/primary/src/network/mod.rs:666-749` | `PrimaryNetwork::process_epoch_stream` | SUSPECT | Permit acquisition is BEFORE map.lock; an aborted future between `try_acquire_owned()` (line 674) and `pending_map.lock()` (line 676) leaks the permit. Synchronous code so unlikely in practice. Anchor 4 + idempotency analysis. |
| `crates/consensus/primary/src/network/mod.rs:772-813` | `PrimaryNetwork::process_inbound_stream` | SUSPECT | The 5s timeout for digest read covers only the digest, not the rest of the stream; then handoff to `process_request_epoch_stream` which has its own 10s buffer timeout per write but no whole-stream cap. Anchor 10. |
| `crates/consensus/primary/src/network/handler.rs:941-990` | `RequestHandler::process_request_epoch_stream` | SOUND | Returns UnknownStreamRequest when no pending entry. Properly closes stream on exit. |
| `crates/consensus/primary/src/network/handler.rs:916-938` | `RequestHandler::send_epoch_over_stream` | SOUND | 16 KB buffer, per-write timeout. |
| `crates/consensus/primary/src/network/handler.rs:188-327` | `RequestHandler::process_gossip` | SOUND (in scope) | Validates committee membership and BLS signature. |
| `crates/consensus/primary/src/network/handler.rs:330-440` | `RequestHandler::vote` | SUSPECT | Cross-epoch cached-response replay vector at line 371: `last_digest == header.digest()` returns cached response without a check that the cached entry's `last_epoch == header.epoch()`. (Mitigated by digest-includes-epoch property of `Header::digest()` but verify in Phase 3.) |
| `crates/consensus/primary/src/network/message.rs` | `PrimaryRequest::StreamEpoch`, `PrimaryResponse::RequestEpochStream` | SUSPECT | The `peer` field in `RequestEpochStream` is unauthenticated (Anchor 3 / Anchor 7). The wire framing has no application-layer signature on the StreamEpoch request itself either. |
| `crates/consensus/primary/src/error/network.rs:70-132` | `From<&PrimaryNetworkError> for Option<Penalty>` | SUSPECT | `Penalty::Mild` for both `UnknownStreamRequest` and `InvalidRequest`; `StreamUnavailable` returns `None`. No escalation ladder for repeat misbehavior. Anchor 5. |
| `crates/consensus/primary/src/consensus_bus.rs:551-553` | `ConsensusBusApp::request_epoch_pack_file` | SUSPECT | Fire-and-forget; no dedup. Combined with 3 spawn_fetch_consensus workers each having own per-task semaphore, two duplicate enqueues can run in parallel. Anchor 12. |
| `crates/state-sync/src/consensus.rs:20-81` | `get_consensus_header` | SUSPECT | Calls `consensus_bus.last_consensus_header().send_replace(Some(header))` at line 65 BEFORE the header is in the persisted pack. Watch can advance to a header that lookups via consensus_chain return None for. Anchor 12. |
| `crates/state-sync/src/consensus.rs:85-152` | `spawn_track_recent_consensus` | SOUND (in scope) | Drives the trigger path; dedup is by "is final header already in DB". |
| `crates/state-sync/src/consensus.rs:159-219` | `spawn_fetch_consensus` | **VULNERABLE** | `let next_sem = Arc::new(Semaphore::new(1));` on line 174 INSIDE the function — each of 3 workers has its own permit. Two duplicate epoch records dequeued by two workers run concurrent stream_import on same `import-{epoch}` dir. Anchor 12 / adversarial #5. |
| `crates/state-sync/src/epoch.rs:46-161` | `collect_epoch_records` | SOUND (in scope) | Verifies committee, parent-hash, and BLS aggregate signature before save. |
| `crates/consensus/worker/src/network/mod.rs:155-180` | `WorkerNetwork::spawn` | SOUND | Same shape as primary spawn. |
| `crates/consensus/worker/src/network/mod.rs:280-387` | `WorkerNetwork::process_request_batches_stream` | SOUND | Caps batch_digests at 500. Per-peer count enforced under same mutex critical section as the insert. |
| `crates/consensus/worker/src/network/mod.rs:392-433` | `WorkerNetwork::process_inbound_stream` | SUSPECT | Same shape concern as primary process_inbound_stream — but worker uses `generate_batch_request_id(&batch_digests)` which is content-addressed (not cross-session collidable). |
| `crates/consensus/worker/src/network/mod.rs:436-441` | `WorkerNetwork::cleanup_stale_pending_requests` | SUSPECT | Same as primary version. No panic guard. |

---

## Anchor Concerns Coverage

### Anchor 1 — Unbounded allocation from peer-supplied `val_size` (`pack_iter.rs`)

- **Status:** **SUBSTANTIATED — F-1**
- **Evidence:** `crates/storage/src/archive/pack_iter.rs:147-150`:
  ```rust
  crc32_hasher.update(&val_size_buf);
  let val_size = u32::from_le_bytes(val_size_buf);
  buffer.resize(val_size as usize, 0);
  file.read_exact(buffer).await?;
  ```
- **Reasoning:** `val_size` is read directly from peer-controlled bytes; CRC32 covers the size+payload but does not constrain the size, so a peer can compute any `(val_size, payload, crc32)` tuple they want. `buffer.resize(val_size as usize, 0)` is invoked before any sanity check. With `u32::MAX` the request is ~4 GiB. The whole call is wrapped in `tokio::time::timeout(PACK_RECORD_TIMEOUT_SECS=10s)` at `consensus_pack.rs:684`, but the OS-level allocator behaves differently across platforms: on Linux with overcommit, `Vec::resize` succeeds quickly and then page-faults (delayed OOM-kill); on macOS / no-overcommit Linux the allocation immediately ENOMEMs which `resize` will panic on. Either way, repeatable from any peer that has cleared the libp2p handshake.
- **Trigger sequence:**
  1. Attacker P clears libp2p handshake and is mesh peer.
  2. Victim V issues `request_epoch_pack` — `send_request_any` reaches P; P returns `RequestEpochStream { ack: true, peer: P_bls }`.
  3. V opens stream to P; writes 32-byte digest. P responds with valid 28-byte `DataHeader`, valid `EpochMeta` record (passes the `next` helper), then a single record header with `val_size = 0xFFFF_FFFF` and matching CRC.
  4. `read_record_file` calls `buffer.resize(4 GiB, 0)` → panic / OOM-kill.
- **Consequence:** Process termination or thrashing. Repeatable via any one peer; the per-record timeout does NOT prevent the allocation.
- **Severity hint:** **CRITICAL** (single-peer process kill).
- **State variables touched:** `Vec<u8>` (ephemeral). No persistent state corruption.

### Anchor 2 — Write-before-validate ordering in `Inner::stream_import`

- **Status:** **SUBSTANTIATED — F-2** (with caveats; see Adjacent Bugs for the precise scar surface).
- **Evidence:** `crates/storage/src/consensus_pack.rs:737-749`:
  ```rust
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
  And then at lines 750-781, the consensus arm finally validates `parent_digest` and the `batches` HashSet.
- **Reasoning:** Batches are written into `data`, into `batch_digests` index, AND `set_data_file_length` is advanced on both digest indexes BEFORE the consensus header that consumes them is parsed. If the stream errors on any record after the batches but before the matching header (timeout, malformed record, RST, or simply a header whose `parent_digest` mismatches), the staged `import-{epoch}` directory contains data file bytes + index entries for batches whose owning header never validates. The cleanup at `consensus.rs:434` (`remove_dir_all(&path)`) runs in the caller's `Err` branch and removes the dir → no permanent orphan. **However**, see Adjacent Bug 1 below: the staging dir survives a process kill mid-import, but the next reopen of `import-{epoch}` is via `consensus.rs:379` (unconditional `remove_dir_all`), so the orphan is wiped. Net: structurally protected by surrounding cleanup, but **the protection is one `remove_dir_all` away from being defeated**.
- **Trigger sequence (the survivable scar):**
  1. Two `spawn_fetch_consensus` workers each dequeue the same epoch (Anchor 12 vector).
  2. Worker A starts streaming into `import-{epoch}`, writes 5 batch records.
  3. Worker B starts; runs `remove_dir_all(&path)` at `consensus.rs:379`, wiping A's in-progress staging dir.
  4. Worker A's open file descriptor still points to the unlinked inode; A continues writing for a while.
  5. Worker A's stream errors (peer-driven or because B's `remove_dir_all` raced into the middle of an open call). A's `Err` branch tries `remove_dir_all(&path)` again — but the dir is already B's now. **A unlinks B's in-progress import dir.**
  6. Worker B sees vanished files → various I/O errors → its own Err branch → `remove_dir_all` (no-op since A already removed).
  7. Both fail, no successful import for this epoch. Honest peer's epoch is now unfetchable until the gossip cycle re-triggers, by which time the malicious peer can re-trigger collisions.
- **Consequence:** Repeatable DoS on epoch sync; possible left-behind bytes if a process crash interleaves with `remove_dir_all`.
- **Severity hint:** **HIGH** (DoS on sync; prevents catch-up).
- **State variables touched:** `import-{epoch}/data` file, `import-{epoch}/idx`, `import-{epoch}/hash`, `import-{epoch}/bhash`, plus the in-memory Inner (which is dropped on Err).

### Anchor 3 — No application-layer signature on `StreamEpoch` RPC

- **Status:** **SUBSTANTIATED — F-3**
- **Evidence:** `crates/consensus/primary/src/network/message.rs:132-136` defines `PrimaryRequest::StreamEpoch { epoch: Epoch }` with no signature/auth fields. `PrimaryResponse::RequestEpochStream { ack: bool, peer: BlsPublicKey }` (lines 247-258) likewise has no signature. Routing is `send_request_any` (`mod.rs:343`).
- **Reasoning:** Authentication is purely on libp2p session level. Any peer that has cleared the handshake can answer. The `peer` field in the response is the **responder's claim** of which BLS pubkey they are, NOT the libp2p PeerId-mapped BLS. Since `auth_to_peer` (libp2p side) maps BLS→PeerId via locally-known peers, the requester's subsequent `open_stream(peer_claim)` (`mod.rs:358`) will actually go to the BLS-mapped peer's PeerId (which may be Q, not the actual responder P). This decouples response identity from stream-fulfillment identity. The penalty at `mod.rs:381` `report_penalty(peer, Penalty::Mild)` then targets the BLS claimed by P, not P itself.
- **Trigger sequence:**
  1. Attacker P answers `StreamEpoch` saying `RequestEpochStream { ack: true, peer: Q_bls }` where Q is an honest validator.
  2. V's `open_stream(Q_bls)` resolves Q's PeerId via `auth_to_peer` → opens stream to Q (NOT P).
  3. Q has no pending entry for this digest → Q's `process_inbound_stream` returns `UnknownStreamRequest` → Q applies `Penalty::Mild` to V (the requester).
  4. V's `consensus_chain.stream_import` returns Err (Q closed the stream / never wrote) → V applies `Penalty::Mild` to Q.
  5. Net: P framed Q, Q framed V, P incurred zero penalty.
- **Consequence:** Asymmetric griefing. With repeats, Q's score degrades; eventually Q is banned by V (and any other victim using same trick). Combined with no escalation ladder (Anchor 5), the attacker has a knob with zero feedback.
- **Severity hint:** **HIGH** (peer-score griefing → liveness degradation of honest validators).
- **State variables touched:** libp2p peer-score state (out of audit scope); `pending_epoch_requests` map on Q.

### Anchor 4 — Race on `MAX_PENDING_REQUESTS_PER_PEER`

- **Status:** **DISMISSED**
- **Evidence:** `crates/consensus/primary/src/network/mod.rs:674-723`:
  ```rust
  let ack = match self.epoch_stream_semaphore.clone().try_acquire_owned() {
      Ok(permit) => {
          let mut pending_map = self.pending_epoch_requests.lock();
          let peer_count = pending_map.keys().filter(|(p, _)| *p == peer).count();
          if peer_count >= MAX_PENDING_REQUESTS_PER_PEER {
              false
          } else {
              ...
              pending_map.insert((peer, request_digest), pending) ...
              true
          }
      }
      Err(_) => false,
  };
  ```
- **Reasoning:** The count check (line 679) and the insert (line 703) execute under the SAME `parking_lot::Mutex` critical section (`pending_map.lock()` at 676 holds the guard for the entire scope). Two concurrent inbound RPC handlers must serialize on this mutex. The first to enter increments effective count; the second sees count=2 and rejects. Race is impossible with the current code.
- **Note:** `try_acquire_owned()` happens BEFORE the lock — so two callers can both grab a permit, both lock the map serially, second one rejects and drops permit. Permit accounting is correct (Drop on the rejected permit fires when the `false` arm exits). No leak.

### Anchor 5 — `Penalty::Mild` only — no escalation on repeat failure

- **Status:** **SUBSTANTIATED — F-4**
- **Evidence:** `crates/consensus/primary/src/error/network.rs:115-118`:
  ```rust
  | PrimaryNetworkError::InvalidRequest(_)
  | PrimaryNetworkError::UnknownConsensusHeaderDigest(_)
  | PrimaryNetworkError::UnknownStreamRequest(_)
  | PrimaryNetworkError::UnknownConsensusHeaderCert(_) => Some(Penalty::Mild),
  ```
  And `StreamUnavailable(_)` returns `None` (line 128).
- **Reasoning:** No counter, no rate-of-misbehavior, no escalation. A peer who repeatedly sends bogus inbound streams pays only `Penalty::Mild` per event — and an unknown stream costs no permit on the responder side (the path goes through `process_inbound_stream` which doesn't acquire a permit). Combined with `request_digest = digest(epoch.to_le_bytes())` (Anchor 8) which is publicly computable, the attacker can probe receivers' state without holding any resource.
- **Trigger sequence:**
  1. Attacker P opens 1000 inbound streams to V over time, writes a known `request_digest` for V's published epoch numbers.
  2. None match a pending entry → 1000 `Penalty::Mild` reports.
  3. P's score drops by `1000 * mild_weight`, but stays above ban threshold for any reasonable `mild_weight` chosen for "occasional honest miss" semantics.
- **Consequence:** Attack budget is unbounded; reputation system has no defensive depth.
- **Severity hint:** **MEDIUM** (depends on libp2p score weighting — Phase 3 should compute the actual ban-after-N from the scoring config).
- **State variables touched:** libp2p peer scores.

### Anchor 6 — Three-index atomicity gap (no single REDB transaction)

- **Status:** **SUBSTANTIATED — F-5** (well-documented partial recovery; complete recovery is one bug)
- **Evidence:** `crates/storage/src/consensus_pack.rs:737-781` (stream_import) and `788-845` (save_consensus_output) both have the pattern:
  ```rust
  let position = self.data.append(&PackRecord::Batch(batch))?;     // (1) data file write
  self.batch_digests.save(batch_digest, position)?;                 // (2) hdx index write
  let len = self.data.file_len();
  self.consensus_digests.set_data_file_length(len);                 // (3) digest index marker
  self.batch_digests.set_data_file_length(len);                     // (4) batch index marker
  ```
  And for consensus headers:
  ```rust
  let position = self.data.append(&PackRecord::Consensus(...))?;    // (1)
  self.consensus_digests.save(consensus_digest, position)?;          // (2)
  self.consensus_idx.save(consensus_idx, position)?;                 // (3)
  ```
  Recovery is via `Inner::trunc_and_heal` (lines 474-527). Lines 489-493 explicitly comment:
  > "Note we leave the digest indexes with potentially some missing digests. This should be OK since they will have to be overwritten with same digests when they are readded and lookups should handle this."
- **Reasoning:** No REDB-style transactional boundary across the three+four files. The recovery contract is "data file leads (it's the source of truth); indexes catch up at append; trunc_and_heal trims data to whichever index/marker is shortest". The sib-comment admits digest indexes are NOT trimmed in heal. So after a crash that leaves data shorter than digest entries, the digest indexes have entries pointing to positions BEYOND the truncated `data.file_len()`. **`Inner::contains_consensus_header` (line 922-923) defensively bounds-checks**, but `Inner::consensus_header_by_digest` (line 930) and `Inner::batch` (line 1023) do NOT — they call `data.fetch(pos)` directly. `data.fetch` will then read past file end → `FetchError::IO(UnexpectedEof)` or read garbage CRC → `FetchError::CrcFailed`, both caught and converted to `None`. Net visible effect: those callers silently return `None`/error → caller sees "header not found" even though it WAS recorded — split-brain visibility post-crash.
- **Trigger sequence (post-crash divergence):**
  1. Live system: header H committed at position P. `data` file ends at P+sizeof(H). `consensus_idx` has entry → P. `consensus_digests` has H.digest → P. `batch_digests` data_file_length marker = P+sizeof(H).
  2. Process killed mid-`Inner::save_consensus_output` between lines 833 (`consensus_digests.save`) and 837 (`consensus_idx.save`). On disk: data file leads, `consensus_digests` has H, `consensus_idx` does NOT.
  3. On reopen, `trunc_and_heal` truncates data to the shorter `batch_final` or `consensus_final` (whichever is smaller), but DOES NOT truncate `consensus_digests`.
  4. Now `consensus_digests` contains H.digest → some position P' that's beyond the new (truncated) `data.file_len()`. `consensus_header_by_digest(H.digest)` calls `data.fetch(P')` → returns `Err(FetchError::IO(...))` → caller gets `None`.
- **Consequence:** Inconsistent reader visibility. Not data corruption per se (no garbage returned), but unexpected `None` for headers a digest-aware caller knows exist.
- **Severity hint:** **MEDIUM** (correctness drift, not value loss; observable to RPC subscribers).
- **State variables touched:** `consensus_digests`, `batch_digests`, `data` file content.

### Anchor 7 — Initiator trusts response identity

- **Status:** **SUBSTANTIATED — see F-3** (this anchor is the same root as Anchor 3; restated here for completeness).
- **Evidence:** `mod.rs:342-381`:
  ```rust
  if let PrimaryResponse::RequestEpochStream { ack, peer } =
      self.handle.send_request_any(request.clone()).await?.await??
  { ...
      let mut stream = self.handle.open_stream(peer).await??;
      ...
      if res.is_err() {
          self.report_penalty(peer, Penalty::Mild).await;
      }
  }
  ```
  The `peer` is the responder's claim. There is no `assert_eq!(peer, actual_responder)` — `send_request_any` doesn't even surface the actual responder. `auth_to_peer(peer)` then resolves THAT BLS to a PeerId, which may not be the real sender of the response.
- **Reasoning:** Confirmed orthogonal binding. `report_penalty(peer, Mild)` (line 381) targets a BLS the responder NAMED, not the wire-level identity that ACKed. Same vector as F-3.
- **Severity hint:** **HIGH** (covered by F-3).

### Anchor 8 — `request_digest` correlation only — cross-epoch / replay / uniqueness

- **Status:** **SUBSTANTIATED — F-6**
- **Evidence:** `mod.rs:333-336` and `mod.rs:690-692`:
  ```rust
  let mut hasher = tn_types::DefaultHashFunction::new();
  hasher.update(&epoch.to_le_bytes());
  let request_digest = B256::from_slice(hasher.finalize().as_bytes());
  ```
- **Reasoning:** Digest is a pure function of `epoch`. Lacks: committee_id, requester BLS, responder BLS, version, nonce. Implications:
  - **Cross-peer collision:** Two requesters asking peer P for the same epoch produce the same digest. The `(peer, request_digest)` map key on the responder side disambiguates by libp2p-connected peer (BlsPublicKey of the inbound libp2p connection), so the responder's pending map IS distinct per requester. ✓ no responder-side collision.
  - **Cross-session replay:** A peer that disconnected mid-stream, on reconnect, can re-present the same digest. The responder's map entry was removed in `process_inbound_stream` line 798-800 OR pruned 30s later. If the same peer re-connects within 30s and writes the same digest, no entry matches → `UnknownStreamRequest` → `Penalty::Mild`. ✓ stale digest does NOT match.
  - **Cross-epoch within same peer:** Different epoch → different digest. ✓ no aliasing.
  - **Predictability:** Any peer can compute the digest for any epoch they know is being requested — combined with Anchor 5 lets attacker probe pending state at low cost (always returns UnknownStreamRequest → Mild).
- **Trigger sequence (probing):**
  1. Attacker observes V publishing consensus block N; computes `epoch = epoch_for(N)`.
  2. Attacker opens inbound stream to V, writes `digest(epoch.to_le_bytes())`.
  3. V looks up `(attacker_bls, that_digest)` — entry doesn't exist (V is requester, not responder, for this epoch usually) → V issues `Penalty::Mild` against attacker.
  4. Each probe cost ≈ `mild_weight`. With low mild_weight, probing is sustainable.
- **Consequence:** Combined with Anchor 5, low-cost reconnaissance and griefing.
- **Severity hint:** **MEDIUM**.
- **State variables touched:** `pending_epoch_requests` map (read), peer-score state.

### Anchor 9 — Pending-request prune relies on background interval (no panic guard / heartbeat / restart)

- **Status:** **DISMISSED-WITH-CAVEAT**
- **Evidence:** `mod.rs:463-488`:
  ```rust
  pub fn spawn(mut self, epoch_task_spawner: &TaskSpawner) {
      epoch_task_spawner.spawn_critical_task("primary network events", async move {
          let mut prune_requests = tokio::time::interval(PENDING_REQUEST_PRUNE_INTERVAL);
          loop {
              tokio::select! {
                  next = self.network_events.recv() => { ... }
                  _ = prune_requests.tick() => {
                      self.cleanup_stale_pending_requests();
                  }
              }
          }
      });
  }
  ```
  And `task_manager.rs:400/411` shows critical task panic produces `CriticalExitError` → triggers shutdown.
- **Reasoning:** A panic inside `cleanup_stale_pending_requests` (which holds `pending_epoch_requests.lock()` during `retain`) would propagate out of the task. The task is `spawn_critical_task` so panic = process shutdown via the task manager. So we DO NOT have a silent leak; instead we have a fail-fast crash. That's better than silent leak but is still a DoS lever IF the panic is reachable. The function only does:
  - `Instant::now()` — never panics.
  - `parking_lot::Mutex::lock()` — never poisons / panics.
  - `HashMap::retain` with closure that does `now.duration_since(pending.created_at) < PENDING_REQUEST_TIMEOUT` — `Instant::duration_since` returns 0 on saturating-sub semantics in std (does NOT panic). The comparison can't panic.
  - **Potential panic:** none reachable from this code with normal types.
- **Verdict:** No reachable panic in `cleanup_stale_pending_requests`. The original concern about "silent slot leak" is mitigated by critical-task crash semantics.
- **Caveat (Adjacent Bug):** If `cleanup_stale_pending_requests` has no panic AND the spawn_critical_task DOES propagate panics → the system is fail-fast on prune issues. The vulnerability ladder shifts: an attacker who can force the prune task to crash (via a different path, e.g. spawn poisoning) would bring the WHOLE node down, not just leak slots.

### Anchor 10 — Per-record `PACK_RECORD_TIMEOUT_SECS = 10` but no whole-stream cap

- **Status:** **SUBSTANTIATED — F-7**
- **Evidence:** `crates/state-sync/src/consensus.rs:14`:
  ```rust
  const PACK_RECORD_TIMEOUT_SECS: u64 = 10;
  ```
  Passed to `network.request_epoch_pack(..., Duration::from_secs(PACK_RECORD_TIMEOUT_SECS))` at line 185, then to `consensus_chain.stream_import(..., timeout)` at `mod.rs:374`, then to `Inner::stream_import(... timeout)` at `consensus_pack.rs:264`, where it's used per-record:
  ```rust
  match tokio::time::timeout(timeout, iter.next()).await { ... }
  ```
  No outer `tokio::time::timeout(WHOLE_STREAM_TIMEOUT, ...)` wrapping the entire `Inner::stream_import` future.
- **Reasoning:** A peer that sends 1 valid record every 9.99 seconds keeps the import alive forever. With ~10⁵ consensus blocks per epoch (or more for active epochs), an attacker holds the slot for ~10⁶ seconds = ~11.5 days uninterrupted. Realistically the requester process will restart before then, but the attacker can pace records to keep the slot occupied for the duration of the requester's operational window.
- **Trigger sequence:**
  1. Attacker P answers `StreamEpoch` for E, gets V to open a stream.
  2. P sends 1 record every 9 seconds. Per-record timeout never fires.
  3. V is stuck importing → V's other 2 fetch workers can still process other epochs IF the queue has more. But subsequent requests for E that get re-queued (by gossip churn) collide with this in-flight import via the import-{epoch} dir collision (see F-2).
  4. P sustains for hours. V never completes E from a different peer (because stream_import early-return at consensus.rs:365-374 only fires if E is COMPLETE on disk; partial in-flight doesn't gate).
- **Consequence:** Slot occupation; combined with F-2 (dir collision) and predictable digests, an attacker can hold V's import pipeline indefinitely.
- **Severity hint:** **MEDIUM** (DoS on epoch sync; combine with F-2 for reliability).
- **State variables touched:** the worker's logical execution slot in `spawn_fetch_consensus`.

### Anchor 11 — Idempotency model from commit `cb0e147c`

- **Status:** **DISMISSED-with-caveat (the dedup is ON THE WRONG SIDE)**
- **Evidence:** Commit `cb0e147c` referenced at `mod.rs:693-702`:
  ```rust
  // If the same peer re-requests the same epoch while a prior entry
  // is still pending, preserve the original `created_at` so the
  // cleanup timer is not rearmed. Without this, a peer could hold a
  // slot indefinitely by re-requesting before the 30s timeout.
  // A second stream open is still punished as a protocol violation.
  let created_at = pending_map
      .get(&(peer, request_digest))
      .map(|p| p.created_at)
      .unwrap_or_else(Instant::now);
  ```
- **Reasoning:** The idempotent replacement is on the **responder** side: when a peer re-requests the same epoch from us, we keep the original `created_at` so they can't extend their slot indefinitely. **On the requester side there is no dedup.** `request_epoch_pack_file` (`consensus_bus.rs:551-553`) is fire-and-forget and `spawn_fetch_consensus` has 3 worker tasks each with own `Semaphore::new(1)` — duplicate enqueues run in parallel. So the idempotency does NOT prevent the import-dir collision (F-2 / Anchor 12) at all.
- **Verdict:** Anchor question 11.a "dedupes concurrent fetches" — DISMISSED on the responder side, but the protection sits on the wrong side of the protocol. Anchor question 11.b "doesn't accidentally cancel an in-flight successful import" — VERIFIED that the responder's idempotent-replace doesn't cancel the importer (the importer is on the REQUESTER side; the responder never imports).
- **Severity hint:** N/A (informational note pointing to F-8, the requester-side gap).

### Anchor 12 — Cross-component invariant (`consensus_chain.latest_consensus_header()` during pack-file import)

- **Status:** **SUBSTANTIATED — F-8** (more precise: the watch advances PRE-pack via state-sync, not pack-file import per se)
- **Evidence:** `crates/state-sync/src/consensus.rs:50-66`:
  ```rust
  match network.request_consensus(number, hash).await {
      Ok(header) => {
          if let Err(e) = db.insert::<ConsensusHeaderCache>(&header.number, &header) {
              error!(target: "state-sync", ?e, "error saving a consensus header to cache storage!");
          }
          ...
          if header.number > last_seen_header_number {
              consensus_bus.last_consensus_header().send_replace(Some(header));
          }
          ...
      }
      Err(e) => { ... }
  }
  ```
  And `crates/storage/src/consensus.rs:357-438` (`stream_import`) does NOT touch `last_consensus_header` watch directly. AND: `crates/state-sync/src/consensus.rs:159-219` uses `Arc::new(Semaphore::new(1))` PER-TASK at line 174.
- **Reasoning:** Two distinct invariants:
  - **(A) Watch advancing prior to durable pack:** `last_consensus_header` watch is sent the moment a header is inserted into `ConsensusHeaderCache` (which is on `node_storage`, NOT the consensus pack file). Downstream consumers of the watch (`behind_consensus`, `consensus_header_latest`, JSON-RPC subscribers) read the watch and may make decisions assuming the header is also in the consensus DB pack — which it might NOT be yet (the pack-file is fetched by `spawn_fetch_consensus` separately). Subsequent calls to `consensus_chain.consensus_header_by_digest` may return None for a header the watch claims is the latest. **Visible inconsistency window** but not corruption.
  - **(B) Concurrent imports of same epoch collide:** `spawn_fetch_consensus` (3 workers, each own `Semaphore::new(1)`, share `epoch_request_queue_rx` mpsc receiver). If gossip enqueues the same epoch twice (two `consensus_bus.request_epoch_pack_file(epoch_record)` calls before either dequeue), two different workers can process them in parallel. They both call `network.request_epoch_pack` → both reach `ConsensusChain::stream_import` (`storage/consensus.rs:357`). Line 379 unconditionally `let _ = std::fs::remove_dir_all(&path);` blows away the in-progress import dir of the other worker. See F-2 trigger sequence for full disaster.
- **Trigger sequence (B — the worse one):**
  1. Gossip event 1 reaches V: `request_epoch_pack_file(E)` enqueues E.
  2. Worker 1 dequeues E, starts `request_epoch_pack(...)` → `stream_import` → starts writing `import-E/data`.
  3. Gossip event 2 (different validator, same E reached quorum) reaches V: `request_epoch_pack_file(E)` enqueues E AGAIN.
  4. Worker 2 dequeues E, calls `stream_import`. The `get_static(epoch)` check at line 365-374 is racy: at this point `epoch-E` doesn't exist yet on disk (Worker 1 is mid-import). So Worker 2 proceeds.
  5. Worker 2 hits `let _ = std::fs::remove_dir_all(&path);` (line 379) — wipes Worker 1's `import-E/`.
  6. Worker 1's open file descriptors point to unlinked inode; Worker 1 keeps writing into the void; eventually Worker 1's `pack.persist()` succeeds against the unlinked file; Worker 1's `fs::rename(import-E/epoch-E, epoch-E)` FAILS (path doesn't exist) → returns Err → `remove_dir_all(&path)` (line 434) — but `path` is now Worker 2's! → wipes Worker 2's import-in-progress.
  7. Both fail. E is unfetchable until next gossip cycle. Adversarial peer who controls gossip timing can sustain this.
- **Consequence:** Sync stalls. With sustained gossip churn (or attacker reissuing), epoch import blocked indefinitely.
- **Severity hint:** **HIGH** (DoS with non-deterministic file-system corruption).
- **State variables touched:** `import-{epoch}` directory tree (the data file, all 3 indexes).

---

## SUSPECT / VULNERABLE Findings

### F-1 — Unbounded `val_size` resize in pack iterator (CRITICAL)

- **File:** `crates/storage/src/archive/pack_iter.rs:147-150` (also `pack_iter.rs:74-77`, `pack.rs:319-321`, `pack.rs:343-345` — same pattern repeated 4×)
- **Code excerpt:**
  ```rust
  crc32_hasher.update(&val_size_buf);
  let val_size = u32::from_le_bytes(val_size_buf);
  buffer.resize(val_size as usize, 0);
  file.read_exact(buffer).await?;
  ```
- **Trigger sequence:**
  1. Attacker peer P responds `RequestEpochStream { ack: true, peer: P_bls }` to V's epoch fetch.
  2. V opens a stream to P. P writes valid 28-byte `DataHeader` (CRC-clean), valid `EpochMeta` record.
  3. P writes a record with `val_size = 0xFFFF_FFFF` (4 GiB), 4 GiB of any byte payload, and the matching CRC32 over (size_le || payload).
  4. V's `read_record_file`: `val_size_buf = [0xFF, 0xFF, 0xFF, 0xFF]` → `val_size = u32::MAX = 4_294_967_295`. `buffer.resize(4_294_967_295, 0)`.
  5. On Linux + overcommit: succeeds at allocation, then page-faults during `read_exact` write → OOM-killer or crash.
  6. On macOS / no-overcommit: `Vec::resize` panics with `capacity overflow` or returns zero-init failure → process abort.
- **Consequence:** Process termination / OOM. Repeatable per peer.
- **State variables touched:** `value_buffer` (ephemeral). No persistent corruption. **Tag for Phase 3:** verify which task wrapper catches the panic vs propagates it (`spawn_fetch_consensus` is `spawn_critical_task` → process shutdown).
- **Adjacent bugs:** see Adjacent §1 below — same pattern in 4 places.

### F-2 — Write-before-validate orphans batches in import dir, race-accelerated by F-8 (HIGH)

- **File:** `crates/storage/src/consensus_pack.rs:737-749` (write loop) and `crates/storage/src/consensus.rs:357-438` (cleanup wrapper)
- **Code excerpt (the staging of un-validated batches):**
  ```rust
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
- **Trigger sequence:** see Anchor 2 / Anchor 12 trigger.
- **Consequence:** Combined with F-8's directory collision, the `import-{epoch}` dir contains structurally-validated-but-not-semantically-validated batches that can be wiped by another concurrent fetch. With process kill in the wrong moment, dir survives partially populated; the next `stream_import` for same epoch unconditionally wipes it. So there's no true persistent scar — but there IS a guaranteed sync-DoS pattern.
- **State variables touched:** `import-{epoch}/data`, `import-{epoch}/idx`, `import-{epoch}/hash`, `import-{epoch}/bhash`. **Tag for Phase 3:** the in-memory Inner survives across the loop iteration boundary; verify whether early returns drop the Inner cleanly (PackInner Drop does fsync — see Adjacent §3).
- **Adjacent bugs:** see Adjacent §3.

### F-3 — Penalty mis-attribution via unauthenticated BLS-in-payload (HIGH)

- **File:** `crates/consensus/primary/src/network/mod.rs:342-381`
- **Code excerpt:**
  ```rust
  if let PrimaryResponse::RequestEpochStream { ack, peer } =
      self.handle.send_request_any(request.clone()).await?.await??
  {
      ...
      let mut stream = self.handle.open_stream(peer).await??;
      ...
      let res = consensus_chain.stream_import(...).await
          .map_err(|e| NetworkError::RPCError(format!("failed to stream pack file: {e}")));
      if res.is_err() {
          self.report_penalty(peer, Penalty::Mild).await;
      }
      ...
  }
  ```
- **Trigger sequence:** see Anchor 3.
- **Consequence:** Two-way penalty: P framed Q at V (V → Q penalty); Q framed V (V opens unsolicited stream to Q → Q penalizes V). Repeatable. Net: attacker P invisible.
- **State variables touched:** libp2p peer-score state on V and Q. **Tag for Phase 3:** quantify the score delta required to evict an honest validator.
- **Adjacent bugs:** Anchor 5 (no escalation ladder) makes this strictly cheaper for the attacker.

### F-4 — `Penalty::Mild` for both `UnknownStreamRequest` and `InvalidRequest`; `StreamUnavailable` returns None (MEDIUM)

- **File:** `crates/consensus/primary/src/error/network.rs:115-128`
- **Code excerpt:** see Anchor 5 evidence.
- **Trigger sequence:** see Anchor 5 / Anchor 8 — predictable digest probe loop.
- **Consequence:** No reputation cost ever exceeds linear-in-events; no ban path for sustained low-grade misbehavior.
- **State variables touched:** libp2p peer scores.
- **Adjacent bugs:** `StreamUnavailable(_)` returning None means a peer can repeatedly request streams for epochs we don't have at zero cost.

### F-5 — `trunc_and_heal` digest-index staleness; `consensus_header_by_digest` & `batch` lack defensive bound (MEDIUM)

- **File:** `crates/storage/src/consensus_pack.rs:474-527` (heal), `930-934` (lookup), `1022-1032` (batch lookup)
- **Code excerpt (lookup, no defensive check):**
  ```rust
  fn consensus_header_by_digest(&mut self, digest: B256) -> Option<ConsensusHeader> {
      let pos = self.consensus_digests.load(digest).ok()?;
      let rec = self.data.fetch(pos).ok()?;
      rec.into_consensus().ok()
  }

  fn batch(&mut self, digest: BlockHash) -> Option<Batch> {
      if let Ok(pos) = self.batch_digests.load(digest) {
          if let Ok(batch) = self.data.fetch(pos) { ... }
      }
      None
  }
  ```
  Compare with `contains_consensus_header` (lines 918-927) which DOES defensive-check.
- **Trigger sequence:** see Anchor 6 trigger.
- **Consequence:** Post-crash split-brain: caller may `contains_consensus_header(d) → false` while a separate path silently returns `None` from `consensus_header_by_digest(d)`. Internally consistent but masks recoverability state.
- **State variables touched:** `consensus_digests`, `batch_digests`, `data` file. **Tag for Phase 3:** trace whether any caller distinguishes "header truly absent" from "header masked by truncation"; if not, this is correctness-only.

### F-6 — `request_digest = digest(epoch.to_le_bytes())` lacks domain separation (MEDIUM)

- **File:** `crates/consensus/primary/src/network/mod.rs:333-336` and `mod.rs:690-692`
- **Code excerpt:** see Anchor 8 evidence.
- **Trigger sequence:** see Anchor 8 — probing without holding a slot.
- **Consequence:** Predictability + low-cost probe combine to amplify Anchor 5.
- **State variables touched:** `pending_epoch_requests` map (read-only via probe).
- **Adjacent bugs:** Worker uses `generate_batch_request_id(&batch_digests)` (worker/mod.rs:325) which IS content-addressed over the digest set — much harder to predict. **Symmetry break:** primary's digest is weaker than worker's despite both being on the same threat model. Opportunity for unifying primitive.

### F-7 — No whole-stream timeout cap (MEDIUM)

- **File:** `crates/consensus/primary/src/network/mod.rs:373-378` (caller never wraps `stream_import` in outer timeout) and `crates/storage/src/consensus_pack.rs:680-690` (per-record timeout only)
- **Code excerpt:** see Anchor 10 evidence.
- **Trigger sequence:** see Anchor 10.
- **Consequence:** Attacker holds a logical fetch slot indefinitely with paced records. Combined with F-8 to fully starve epoch sync.
- **State variables touched:** `spawn_fetch_consensus` worker task slot.
- **Adjacent bugs:** even worse — no cap on **TOTAL** records emitted, so peer can sustain the import for as long as their pacing allows.

### F-8 — Concurrent epoch fetches collide on `import-{epoch}` directory (HIGH)

- **File:** `crates/state-sync/src/consensus.rs:174` (per-task semaphore) and `crates/storage/src/consensus.rs:377-379` (unconditional cleanup)
- **Code excerpt:**
  ```rust
  // state-sync/src/consensus.rs:174
  let next_sem = Arc::new(Semaphore::new(1));   // INSIDE function body — per-task instance
  ```
  ```rust
  // storage/src/consensus.rs:377-379
  let path = self.base_path.join(format!("import-{epoch}"));
  // We need to start with a clean import dir since we do not restart.
  let _ = std::fs::remove_dir_all(&path);
  ```
- **Trigger sequence:** see Anchor 12 trigger (B).
- **Consequence:** Sync stalls, file-descriptor confusion, both workers fail.
- **State variables touched:** `import-{epoch}/` tree; the in-flight `Pack<PackRecord>` of the losing worker.
- **Adjacent bugs:** the `current_pack.lock()` ordering (consensus.rs:403-413) is correct for serializing the FINAL stages, but the first stage (write the import dir) is unprotected.

### F-9 — `vote()` cached-response replay across epochs (LOW-MEDIUM, needs Phase 3)

- **File:** `crates/consensus/primary/src/network/handler.rs:368-405`
- **Code excerpt:**
  ```rust
  if let Some((last_epoch, last_round, last_digest, last_response)) =
      auth_last_vote.take()
  {
      if last_digest == header.digest() {
          match last_response {
              ...
              Some(res) => return Ok(res),
          }
      }
      ...
  }
  ```
- **Trigger sequence:**
  1. End of epoch N: validator A has cached `(N, R, digest_X, vote_response)` in `auth_last_vote[A]`.
  2. Start of epoch N+1: validator A (or attacker spoofing A) sends a vote request with header h such that `h.digest() == digest_X` (collision is hard for a hash, BUT `Header::digest` may serialize in a way that two epochs can produce the same struct shape if A re-uses a header — possible for attacker A).
  3. Cached response is returned immediately, bypassing all of `vote_inner`'s freshness checks.
- **Consequence:** Equivocation if attacker can produce a same-digest header across epochs. Rust strong typing of `Hash` for `Header` likely makes collision infeasible (depends on `Header` containing `epoch` field — verify in Phase 3).
- **State variables touched:** `auth_last_vote`. **Tag for Phase 3:** confirm `Header::digest()` includes `epoch` field in the hashed pre-image; if it does, this is dismissed as not-exploitable; if not, escalate.

### F-10 — `last_consensus_header` watch advances pre-durable (LOW)

- **File:** `crates/state-sync/src/consensus.rs:65`
- **Code excerpt:**
  ```rust
  if header.number > last_seen_header_number {
      consensus_bus.last_consensus_header().send_replace(Some(header));
  }
  ```
- **Trigger sequence:** Subscriber reads watch; subsequent `consensus_chain.consensus_header_by_*` returns None because pack file hasn't been fetched yet.
- **Consequence:** Inconsistent visibility window; flaky retries in JSON-RPC consumers.
- **State variables touched:** `tx_last_consensus_header`. **Tag for Phase 3:** identify all readers and confirm they're tolerant to "watch ahead of DB".

### F-11 — `process_epoch_stream` rejection produces no peer cost (LOW)

- **File:** `crates/consensus/primary/src/network/mod.rs:686-688, 722`
- **Code excerpt:**
  ```rust
  if peer_count >= MAX_PENDING_REQUESTS_PER_PEER {
      ...
      false  // permit drops; ack=false; no penalty
  } ...
  Err(_) => false,    // semaphore exhausted; no penalty
  ```
- **Trigger sequence:** Attacker spams `StreamEpoch` requests, gets `ack=false` 100% of the time, pays nothing. Each call still consumes responder CPU + lock contention.
- **Consequence:** Cheap CPU-DoS on the responder.
- **State variables touched:** `pending_epoch_requests` (lock contention).

---

## Adjacent Bugs Surfaced

### §1 — Same uncapped resize in 4 locations

- `pack_iter.rs:76` (sync iter), `pack_iter.rs:149` (async iter), `pack.rs:320` (`read_record`), `pack.rs:344` (`record_size`).
- The async one is the hot peer-fed path. The sync one is for local data, but it's invoked at startup via `Inner::open_append`'s `data.fetch(DATA_HEADER_BYTES as u64)` (line 552) and via `trunc_and_heal`'s `data.record_size(last_record)` (line 504). If a malicious peer can corrupt local state to introduce a record with `val_size = u32::MAX` (e.g. via partially-imported pack on disk that survived a crash), the next `open_append` will OOM. **Cross-component reachability:** stream_import writes the `data` file with peer bytes including the size header. If a peer crashes V mid-import (e.g. via Anchor 1 OOM), V's file may have a partially written record with attacker-set size. Reopen of `import-{epoch}/data` next time goes through the SAME unbounded resize path. Two bugs cooperate to make the OOM persistent.

### §2 — `EpochMeta` wire-format implicit trust

- `consensus_pack.rs:697-704`:
  ```rust
  let epoch_meta = if let Some(meta) = next(&mut stream_iter, timeout).await? {
      meta.into_epoch()?
  } else {
      return Err(PackError::NotEpoch);
  };
  if epoch != epoch_meta.epoch {
      return Err(PackError::InvalidEpoch);
  }
  ```
  After this check, `epoch_meta.committee`, `epoch_meta.start_consensus_number`, `epoch_meta.genesis_consensus`, `epoch_meta.genesis_exec_state` are TRUSTED for the rest of the import. The `genesis_consensus.hash` becomes `parent_digest` (line 729). If a peer constructs an `EpochMeta` with a forged `genesis_consensus` and matching first ConsensusHeader's `parent_hash`, the parent-hash chain check passes for the WRONG genesis.
  - **Caveat:** `previous_epoch.final_consensus.hash` (line 729) is sourced from the LOCAL EpochRecord (which was verified by `collect_epoch_records` via committee BLS quorum), so the `parent_digest` initialization is **trusted**. Wait, line 729 reads:
    ```rust
    let mut parent_digest = previous_epoch.final_consensus.hash;
    ```
    `previous_epoch` is the function parameter (the LOCAL trusted epoch record) — NOT `epoch_meta.genesis_consensus`. So the chain DOES anchor to trusted state. ✓ Good.
  - BUT: `epoch_meta.start_consensus_number` is used at line 774 to compute `consensus_idx_pos`. A peer that claims `start_consensus_number = u64::MAX - 5` makes `saturating_sub` produce a small index, which then `consensus_idx.save` writes — corrupting the index layout. **Verify in Phase 3 that `start_consensus_number` is checked against `previous_epoch.final_consensus.number + 1`.** It is NOT checked.
  - **New finding:** F-12 (below).

### §3 — `PackInner::Drop` calls `commit()` (fsync to disk) — orphan persistence on Err path

- `pack.rs:158-167`:
  ```rust
  impl<V> Drop for PackInner<V> { fn drop(&mut self) { if !self.read_only { let _ = self.commit(); } } }
  ```
- When `Inner::stream_import` returns Err, the local `Inner` (containing `data: Pack<PackRecord>`) is dropped. That fsyncs partial bytes to `import-{epoch}/data`. Next: caller (`consensus.rs:434`) does `remove_dir_all(&path)`. So the fsynced bytes are unlinked — fine on success. But if `remove_dir_all` itself panics or partially fails (e.g. EACCES on an inotify-locked directory, or process killed mid-removal), the orphan persists. On next stream_import, `consensus.rs:379` does `remove_dir_all` again. Recovery is `remove_dir_all`-resilient ONLY if it's called eventually before the next reopen.
- **Cross-platform note:** On Windows, `remove_dir_all` can fail with `ERROR_SHARING_VIOLATION` if any FD is open on a file inside. The bg thread (`run_pack_loop`) holds FDs. Linux unlinks fine while open.

### §4 — `epoch_meta.start_consensus_number` not validated against previous epoch (F-12)

- `consensus_pack.rs:702-706` accepts `epoch_meta` if epoch matches; does NOT check that `epoch_meta.start_consensus_number == previous_epoch.final_consensus.number + 1`.
- `consensus_pack.rs:774`:
  ```rust
  let consensus_idx_pos =
      consensus_number.saturating_sub(epoch_meta.start_consensus_number);
  consensus_idx.save(consensus_idx_pos, position) ...
  ```
  With attacker-chosen `start_consensus_number` very high, `saturating_sub` produces small indices for every header → repeated writes to the same `consensus_idx_pos` → either index error or overwrite. With `start_consensus_number` very low, indices grow unreasonably large.
- **Trigger:** peer sends `EpochMeta { start_consensus_number: u64::MAX }` for epoch E. First ConsensusHeader has `consensus_number = 100` → `consensus_idx_pos = 0`. Second has `consensus_number = 101` → `consensus_idx_pos = 0`. Index has only one entry; subsequent writes either error (DuplicateKey if PositionIndex enforces) or overwrite.
- **Severity hint:** **HIGH** — corrupts index layout silently in the staging dir. On rename to `epoch-{epoch}`, the corrupted index is now durable. Lookups by number return wrong positions → wrong headers returned to local consumers.
- **Status:** to fully verify, Phase 3 must trace `PositionIndex::save` semantics for duplicate index. If it errors, the import fails (good); if it overwrites, this is a CRITICAL bug.

### §5 — `epoch_meta.committee` not cross-validated against `previous_epoch.next_committee`

- Same surface as §4. `epoch_meta.committee` is used by `Inner::get_consensus_output` (line 900) for executor address lookup. A malicious committee in `epoch_meta` would mis-attribute reward addresses on later replay.
- **Trigger:** peer crafts `EpochMeta { committee: attacker_authorities }`. Stream completes structurally. Local node's later `get_consensus_output` returns batches with attacker addresses for `CertifiedBatch::address`. (Actual execution uses different paths; verify in Phase 3 whether downstream consumers trust this committee.)
- **Severity hint:** **MEDIUM-HIGH** depending on consumer trust.

### §6 — Worker `process_request_batches_stream` truncates oversized requests silently

- `worker/mod.rs:289-300`: if `batch_digests.len() > MAX_BATCH_DIGESTS_PER_REQUEST`, truncates with `.take(500)`. The `request_digest` is then computed over the TRUNCATED set, not the original. So a peer that requested 1000 batches gets a digest mismatched against their own computation. The protocol expects responder + requester to agree on `request_digest`. **Mismatch → requester opens stream with a digest that the responder (who truncated) didn't insert under** → responder's `process_inbound_stream` returns UnknownStreamRequest → Penalty::Mild against requester for an oversized request that the responder silently truncated. **Mis-attribution.** This is a worker-side analog of F-3.

### §7 — `consensus_certs` and `requested_parents` not cleared on epoch transition

- `consensus_bus.rs:304-307` `reset_for_epoch` does NOT clear `auth_last_vote`, `consensus_certs`, `requested_parents`, `pending_epoch_requests`, `last_consensus_header`, `last_published_consensus_num_hash`. Already noted in Phase 1 1C row 10. Verify in Phase 3 whether these are intended persistent or epoch-local.

---

## Open Questions for Phase 3

1. **Does `Header::digest()` include `epoch` in its hash pre-image?** — gates F-9.
2. **Does `PositionIndex::save` overwrite or error on duplicate index?** — gates F-12 (Adjacent §4) escalation.
3. **What is the libp2p score weighting for `Penalty::Mild`?** — gates F-3, F-4, F-6 attacker cost.
4. **Does any reader of `Inner::consensus_header_by_digest` distinguish "absent" from "error"?** — gates F-5 severity.
5. **Can the `import-{epoch}` directory survive process kill in any window where the next start is NOT preceded by a cleaner?** — gates F-2 persistence.
6. **Does `task_manager`'s `spawn_critical_task` panic propagation actually reach the `until_exit` handler in time to prevent slot leak?** — gates Anchor 9 dismissal.
7. **Are there other readers of `tx_last_consensus_header` watch that perform consensus-affecting decisions?** — gates F-10 severity.
8. **Worker's `generate_batch_request_id(&batch_digests)`: is it content-addressed (hash of sorted digests) or insertion-ordered?** — gates Adjacent §6 severity.
9. **Does the `epoch_meta.committee` field flow into any consensus or execution-affecting path post-import?** — gates Adjacent §5 severity.

---

## Summary

- **12/12 anchor concerns interrogated.** Substantiated: 1, 2, 3, 5, 6, 7, 8, 10, 12 (9 anchors). Dismissed with code evidence: 4, 9, 11 (3 anchors).
- **12 SUSPECT/VULNERABLE findings** raised (F-1 through F-12, with F-12 captured as Adjacent §4 pending Phase 3).
- **Top-3 by severity hint:**
  - **F-1 (CRIT):** uncapped `val_size as usize` in pack iterator; single-peer process kill.
  - **F-3 (HIGH):** unauthenticated `peer` field in `RequestEpochStream` enables penalty mis-attribution / honest-validator griefing.
  - **F-8 (HIGH):** per-task `Semaphore::new(1)` in `spawn_fetch_consensus` lets concurrent same-epoch fetches collide on `import-{epoch}` dir.
- **State variables flagged for Phase 3 tracing:** `consensus_digests`, `batch_digests`, `consensus_idx`, `data` file, `pending_epoch_requests`, `epoch_stream_semaphore`, `tx_last_consensus_header`, `auth_last_vote`, `epoch_request_queue`, libp2p peer-score state.
