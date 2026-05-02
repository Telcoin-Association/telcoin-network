# Nemesis Scan — Raw Report (issue-629)

## Metadata
- Branch: issue-629
- HEAD: 9b398938
- Diff: main..HEAD
- Generated: 2026-04-27
- Pipeline: Phase -1 + Phase 0 through Phase 7
- Verifier mode: anti-confirmation

## All Findings (in order surfaced)

### F-1 — Unbounded `val_size` resize in pack iterator
- **First surfaced:** Phase 0 (Hit-List #1) → Phase 2 (Anchor 1) as F-1
- **File:line:** `crates/storage/src/archive/pack_iter.rs:147-150` (also `pack_iter.rs:74-77`, `pack.rs:319-321`, `pack.rs:343-345`)
- **Phase 4 unified severity:** CRITICAL
- **Phase 6 final severity:** CRITICAL
- **Status:** CONFIRMED
- **Code excerpt:**
  ```rust
  crc32_hasher.update(&val_size_buf);
  let val_size = u32::from_le_bytes(val_size_buf);
  buffer.resize(val_size as usize, 0);
  file.read_exact(buffer).await?;
  ```
- **Trigger sequence:**
  1. Attacker peer P clears libp2p handshake; appears in `auth_to_peer` map.
  2. V issues `request_epoch_pack(E)`; `send_request_any` reaches P.
  3. P answers `RequestEpochStream { ack: true, peer: P_bls }`.
  4. V opens stream to P; writes 32-byte correlator `digest(E.to_le_bytes())`.
  5. P writes valid `DataHeader` + `EpochMeta`, then a record header with `val_size = 0xFFFF_FFFF`.
  6. V's `read_record_file` calls `buffer.resize(4_294_967_295, 0)` → OOM/abort.
- **Consequence:** Process kill of any node fetching from this peer; crash-loop on restart because gossip re-enqueues E and `send_request_any` selects P with non-trivial probability.
- **Compensating-control search:** Searched for `MAX_RECORD_SIZE`, `MAX_VAL_SIZE`, `MAX_PACK_RECORD`, `MAX_PAYLOAD`, `MAX_BATCH_SIZE`, `RECORD_SIZE_LIMIT` across `crates/storage`, `crates/state-sync`, `crates/consensus/primary` — no matches. Per-record `tokio::time::timeout(10s)` wraps the future but only fires AFTER the allocation panic. CRC32 covers `(size_le || payload)` — does not constrain size.
- **Final reasoning:** `Vec::resize` is infallible at the type level; allocation failure aborts. Single-peer exploit with no length cap. CRITICAL.

### F-2 — Write-before-validate orphan-batch staging + final-header-only check
- **First surfaced:** Phase 0 (Hit-List #2) → Phase 2 (Anchor 2) as F-2 → Phase 3 F1 → Phase 4 / Phase 5 J5.4 (joint J-4)
- **File:line:** `crates/storage/src/consensus_pack.rs:737-749` (write-before-validate batch arm) + `crates/storage/src/consensus.rs:387-401` (final-only validation)
- **Phase 4 unified severity:** HIGH
- **Phase 6 final severity:** HIGH
- **Status:** CONFIRMED
- **Code excerpt:**
  ```rust
  PackRecord::Batch(batch) => {
      let batch_digest = batch.digest();
      batches.insert(batch_digest);
      let position = data.append(&PackRecord::Batch(batch))
          .map_err(|e| PackError::Append(e.to_string()))?;
      batch_digests.save(batch_digest, position)
          .map_err(|e| PackError::IndexAppend(format!("batch {e}")))?;
      ...
  }
  ```
- **Trigger sequence:**
  1. Attacker P answers V's `StreamEpoch` for E.
  2. P streams `(batch_real_1, header_real_1, ..., batch_real_K, forged_header_K, ..., header_real_N)` where forged headers are internally consistent (parent-hash chain + batch HashSet drains) and the LAST header equals `epoch_record.final_consensus`.
  3. `Inner::stream_import` accepts; `pack.persist().await` flushes.
  4. `consensus.rs:387-401` final-header check passes; rename succeeds.
  5. Forged intermediate headers are now durable in V's pack DB.
- **Consequence:** Forged intermediate headers admitted to local pack; re-served to other peers via `get_epoch_stream`; reward attribution may be corrupted.
- **Compensating-control search:** Searched for `verify_signature`, `verify_with_cert`, `quorum`-style check inside `Inner::stream_import` — none. `EpochRecord` carries only `final_consensus: BlockNumHash` (no per-header attestations). Whether downstream execution re-validates intermediate headers' BLS sigs is the gating uncertainty (deferred to follow-up audit).
- **Final reasoning:** Only the final header is cross-checked against trusted state. Process-kill variant of the orphan-batch concern is recovered by `remove_dir_all(&path)` at consensus.rs:434, but the forged-history admission survives because the structural loop accepts any chain that anchors on a real final hash.

### F-3 — Penalty mis-attribution via unauthenticated `peer: BlsPublicKey` in response payload
- **First surfaced:** Phase 0 (Hit-List #3) → Phase 1 GAP 9 → Phase 2 (Anchor 3 / Anchor 7) F-3 → Phase 3 F-9 → Phase 4 / Phase 5 J5.3 (joint J-3)
- **File:line:** `crates/consensus/primary/src/network/mod.rs:342, 358, 381`
- **Phase 4 unified severity:** HIGH
- **Phase 6 final severity:** HIGH
- **Status:** CONFIRMED
- **Code excerpt:**
  ```rust
  if let PrimaryResponse::RequestEpochStream { ack, peer } =
      self.handle.send_request_any(request.clone()).await?.await??
  {
      let mut stream = self.handle.open_stream(peer).await??;
      ...
      if res.is_err() {
          self.report_penalty(peer, Penalty::Mild).await;
      }
  }
  ```
- **Trigger sequence:**
  1. Attacker A receives V's `StreamEpoch { epoch: E }` via `send_request_any`.
  2. A responds `RequestEpochStream { ack: true, peer: B_bls }` (B is honest committee member).
  3. V calls `open_stream(B_bls)` → `auth_to_peer(B_bls)` resolves to B's PeerId.
  4. B has no pending entry → returns `UnknownStreamRequest` → applies `Penalty::Mild` to V.
  5. V's `stream_import` errors → V applies `Penalty::Mild` to B.
  6. A is invisible. Repeat at ~5s cadence; B's score crosses ban threshold (~100 events) in ~83 minutes.
- **Consequence:** Attacker can systematically evict any honest committee member from a victim's mesh at zero cost; degrades liveness as honest peers get banned.
- **Compensating-control search:** No `peer == peer_actual` rebinding in `request_epoch_pack`; `send_request_any` doesn't surface the on-wire PeerId. `open_stream(peer: BlsPublicKey)` resolves via `peer_manager.auth_to_peer(peer)` (`network-libp2p/src/consensus.rs:716`) — the stream goes to whoever holds the claimed BLS, not the responder.
- **Final reasoning:** The BLS field in the response payload is the responder's claim and is never bound to the actual transport-layer PeerId.

### F-4 — `Penalty::Mild` only for stream errors; no escalation
- **First surfaced:** Phase 0 (Hit-List #7) → Phase 2 (Anchor 5) F-4
- **File:line:** `crates/consensus/primary/src/error/network.rs:115-128`
- **Phase 4 unified severity:** MEDIUM
- **Phase 6 final severity:** MEDIUM
- **Status:** CONFIRMED (with nuance: Medium and Fatal exist for OTHER variants, but stream-error variants are all Mild or None)
- **Code excerpt:**
  ```rust
  | PrimaryNetworkError::InvalidRequest(_)
  | PrimaryNetworkError::UnknownConsensusHeaderDigest(_)
  | PrimaryNetworkError::UnknownStreamRequest(_)
  | PrimaryNetworkError::UnknownConsensusHeaderCert(_) => Some(Penalty::Mild),
  ...
  | PrimaryNetworkError::StreamUnavailable(_) => None,
  ```
- **Trigger sequence:** Any sequence of probes / mismatched streams pays linear `Penalty::Mild`; never escalates regardless of repeat count.
- **Consequence:** Reputation system has no defensive depth. Combined with F-3 (mis-attribution) and F-6 (predictable digest), produces the J5.3 griefing triangle.
- **Compensating-control search:** No counter, no rate-of-misbehavior tracker visible in `error/network.rs` or libp2p score config (out of scope).
- **Final reasoning:** Confirmed at MEDIUM. The earlier wording "Mild is the entire ladder" is overstated globally but accurate for the streaming surface.

### F-5 — `consensus_header_by_digest` and `batch` lack defensive `pos < data.file_len()` filter
- **First surfaced:** Phase 0 (Hit-List #2 partial) → Phase 1 GAP 2 → Phase 2 (Anchor 6) F-5 → Phase 3 F2 → Phase 4 (joint J-5)
- **File:line:** `crates/storage/src/consensus_pack.rs:474-527` (heal), `919-928` (defensive sibling), `930-935` (non-defensive lookup), `1024-1033` (non-defensive batch lookup)
- **Phase 4 unified severity:** MEDIUM
- **Phase 6 final severity:** MEDIUM
- **Status:** CONFIRMED
- **Code excerpt:**
  ```rust
  // 919-928 (defensive)
  fn contains_consensus_header(&mut self, digest: B256) -> bool {
      if let Ok(pos) = self.consensus_digests.load(digest) { pos < self.data.file_len() }
      else { false }
  }
  // 930-935 (NOT defensive)
  fn consensus_header_by_digest(&mut self, digest: B256) -> Option<ConsensusHeader> {
      let pos = self.consensus_digests.load(digest).ok()?;
      let rec = self.data.fetch(pos).ok()?;
      rec.into_consensus().ok()
  }
  ```
- **Trigger sequence:** Process kill mid-`save_consensus_output` between `consensus_digests.save(D3)` and `consensus_idx.save(D3)` → `trunc_and_heal` truncates `data` and `consensus_idx` but leaves `consensus_digests` stale (explicit code comment lines 489-493). Subsequent `consensus_header_by_digest(D3)` returns whatever record now sits at the stale position with no digest re-comparison.
- **Consequence:** Split-brain visibility — `contains_consensus_header(D)` returns false while `consensus_header_by_digest(D)` returns Some(other_header). `data.fetch(pos)` past EOF errors safely (returning None), but if `pos < file_len()` because re-extension occurred, it returns the wrong header.
- **Compensating-control search:** Phase 6 reading `data.fetch` semantics shows that `read_exact` returns `UnexpectedEof` if `pos > file_len`, which converts to `FetchError::IO` → caller sees None. So the worst-case outcome is silent None or wrong-header rather than garbage — bumps severity DOWN from initial Phase 2 framing.
- **Final reasoning:** Cross-API consistency observably broken in post-`trunc_and_heal` states. Severity stays MEDIUM (correctness drift, not value loss).

### F-6 — `request_digest = digest(epoch.to_le_bytes())` lacks domain separation
- **First surfaced:** Phase 0 (Hit-List #3 partial) → Phase 2 (Anchor 8) F-6
- **File:line:** `crates/consensus/primary/src/network/mod.rs:333-336` (requester) and `mod.rs:690-692` (responder)
- **Phase 4 unified severity:** MEDIUM
- **Phase 6 final severity:** MEDIUM
- **Status:** CONFIRMED
- **Code excerpt:**
  ```rust
  let mut hasher = tn_types::DefaultHashFunction::new();
  hasher.update(&epoch.to_le_bytes());
  let request_digest = B256::from_slice(hasher.finalize().as_bytes());
  ```
- **Trigger sequence:** Any peer can compute `request_digest` for any known epoch; combined with F-4, attacker probes V's pending map at `Penalty::Mild` per probe.
- **Consequence:** Predictable correlator amplifies F-3 (peer attribution) and F-4 (no escalation); enables low-cost reconnaissance.
- **Compensating-control search:** Map key is `(BlsPublicKey, B256)`; the BLS half disambiguates per-peer. Lifetime bounded by 30s prune and `process_inbound_stream` removal. Worker uses content-addressed `generate_batch_request_id(&batch_digests)` — primary digest is weaker by design asymmetry.
- **Final reasoning:** Operational concern is predictability + low-cost probe, not collision per se. MEDIUM stands.

### F-7 — No whole-stream timeout cap
- **First surfaced:** Phase 0 (Hit-List #6) → Phase 2 (Anchor 10) F-7
- **File:line:** `crates/consensus/primary/src/network/mod.rs:373-378` (call site, no outer timeout) and `consensus_pack.rs:680-690` (per-record timeout helper)
- **Phase 4 unified severity:** MEDIUM
- **Phase 6 final severity:** MEDIUM
- **Status:** CONFIRMED
- **Code excerpt:**
  ```rust
  async fn next<R: AsyncRead + Unpin>(
      iter: &mut AsyncPackIter<PackRecord, R>,
      timeout: Duration,
  ) -> Result<Option<PackRecord>, PackError> {
      match tokio::time::timeout(timeout, iter.next()).await { ... }
  }
  ```
- **Trigger sequence:** Attacker streams 1 record every 9 seconds (under `PACK_RECORD_TIMEOUT_SECS = 10`). Per-record timeout never fires; whole-stream import can run for hours holding a fetcher slot.
- **Consequence:** Slot occupation; combined with F-8 dir collision and 3 fetcher tasks, completes the J5.6 starvation attack.
- **Compensating-control search:** No `MAX_RECORDS_PER_PACK`. No outer `tokio::time::timeout(STREAM_CAP, ...)` wrapping `consensus_chain.stream_import(...)`.
- **Final reasoning:** Confirmed MEDIUM.

### F-8 — Concurrent same-epoch fetches collide on `import-{epoch}` directory
- **First surfaced:** Phase 0 (Hit-List #5) → Phase 1 GAP 8 → Phase 2 F-8 → Phase 3 F-3+F-8 → Phase 4 (joint J-4)
- **File:line:** `crates/state-sync/src/consensus.rs:174` (per-task semaphore), `crates/node/src/manager/node.rs:286-299` (3 fetcher tasks), `crates/storage/src/consensus.rs:377-379` (unconditional cleanup), `crates/consensus/primary/src/consensus_bus.rs:551-553` (no enqueue dedup)
- **Phase 4 unified severity:** HIGH
- **Phase 6 final severity:** HIGH
- **Status:** CONFIRMED
- **Code excerpt:**
  ```rust
  // state-sync/consensus.rs:174 — per-invocation Arc
  let next_sem = Arc::new(Semaphore::new(1));
  // node.rs:286-299 — 3 of these
  for i in 0..3 { node_task_manager.spawn_critical_task(..., spawn_fetch_consensus(...)); }
  // storage/consensus.rs:377-379 — unconditional cleanup
  let path = self.base_path.join(format!("import-{epoch}"));
  let _ = std::fs::remove_dir_all(&path);
  ```
- **Trigger sequence:**
  1. Gossip events re-trigger `request_epoch_pack_file(E)` — no dedup.
  2. Two of the 3 fetcher workers dequeue same E (independent `Semaphore::new(1)` per worker).
  3. Worker B's `remove_dir_all(&path)` at line 379 wipes Worker A's in-progress staging dir.
  4. Worker A's open FDs still write to unlinked inodes; A's `fs::rename` later sees `ENOENT` → Err → wipes Worker B's in-progress dir.
  5. Both fail; epoch unfetchable until next gossip cycle.
- **Consequence:** Permanent epoch sync DoS under sustained gossip churn (reachable in normal operation, not just adversarial).
- **Compensating-control search:** No file lock on `import-{epoch}/`; no `flock`. Early-return at `consensus.rs:365-374` only sees COMPLETED packs. `state-sync/consensus.rs:111-117` only checks for completed pack header. No coordination across workers.
- **Final reasoning:** Reachable under normal gossip churn. HIGH.

### F-9 — `auth_last_vote` cached-response replay across epochs
- **First surfaced:** Phase 1 GAP 10 → Phase 2 F-9 → Phase 3 F10
- **File:line:** `crates/consensus/primary/src/network/handler.rs:368-405`
- **Phase 4 unified severity:** DISMISSED
- **Phase 6 final severity:** DISMISSED
- **Status:** DISMISSED
- **Code excerpt:**
  ```rust
  if last_digest == header.digest() {
      match last_response {
          Some(res) => return Ok(res),
          ...
      }
  }
  ```
- **Trigger sequence:** Hypothetical: same-digest header replayed across an epoch boundary returns a cached Vote without epoch check.
- **Consequence (theoretical):** Equivocation if attacker can produce same-digest header across epochs.
- **Compensating-control search:** Phase 4 Q1 — `crates/types/src/primary/header.rs:14-38, 245-253` — `Header::digest()` calls `encode(&self).as_ref()` on the full struct including non-skipped `epoch: Epoch` field. SHA-256 collision required across epochs (cryptographically infeasible). Phase 4 Q7 — `RequestHandler` is rebuilt per epoch via `epoch_task_manager` (`node/manager/node/epoch.rs:110-114, 1010-1019`), so the map doesn't survive an epoch boundary anyway.
- **Final reasoning:** Two independent reasons make this unreachable. Stays DISMISSED.

### F-10 — `tx_last_consensus_header` watch advances pre-durable
- **First surfaced:** Phase 0 (in Q3 watch tracing) → Phase 2 F-10 → Phase 3 F7
- **File:line:** `crates/state-sync/src/consensus.rs:65` vs `crates/storage/src/consensus.rs:506-522`
- **Phase 4 unified severity:** LOW
- **Phase 6 final severity:** LOW
- **Status:** CONFIRMED (informational)
- **Code excerpt:**
  ```rust
  if header.number > last_seen_header_number {
      consensus_bus.last_consensus_header().send_replace(Some(header));
  }
  ```
- **Trigger sequence:** Watch advances after `ConsensusHeaderCache.insert` (in `node_storage`). Pack file via `consensus_chain` may not yet contain header. Subsequent `consensus_header_by_digest` returns None.
- **Consequence:** RPC retry storms; transient None lookups for headers the watch claims are latest.
- **Compensating-control search:** Phase 6 — JSON-RPC and `behind_consensus` consumers tolerate stale-pack lookups returning None.
- **Final reasoning:** Informational asymmetry between two storage layers. LOW.

### F-11 — `process_epoch_stream` rejection has no peer cost
- **First surfaced:** Phase 1 LOW item 11 → Phase 2 F-11
- **File:line:** `crates/consensus/primary/src/network/mod.rs:686-688, 722`
- **Phase 4 unified severity:** LOW
- **Phase 6 final severity:** LOW
- **Status:** CONFIRMED
- **Code excerpt:**
  ```rust
  if peer_count >= MAX_PENDING_REQUESTS_PER_PEER {
      false  // permit drops; ack=false; no penalty
  }
  ```
- **Trigger sequence:** Attacker spams `StreamEpoch`; gets `ack=false`; pays nothing. Each call still consumes responder CPU + lock contention.
- **Consequence:** Cheap CPU-DoS on responder.
- **Compensating-control search:** None. By design, denial is silent.
- **Final reasoning:** Cheap responder griefing; LOW.

### F-12 — `epoch_meta.start_consensus_number` not validated
- **First surfaced:** Phase 2 Adjacent §4 → Phase 4 Q2/Q3 reclassification
- **File:line:** `crates/storage/src/consensus_pack.rs:697-706, 773-777`
- **Phase 4 unified severity:** Originally CRITICAL, downgraded to MEDIUM
- **Phase 6 final severity:** MEDIUM
- **Status:** CONFIRMED RECLASSIFIED
- **Code excerpt:**
  ```rust
  // 702-706
  if epoch != epoch_meta.epoch { return Err(PackError::InvalidEpoch); }
  // 773-777
  let consensus_idx_pos = consensus_number.saturating_sub(epoch_meta.start_consensus_number);
  consensus_idx.save(consensus_idx_pos, position)?;
  ```
- **Trigger sequence:** Attacker sets `start_consensus_number = u64::MAX - 5`. First header's `saturating_sub` clamps to 0, save succeeds. Second header also clamps to 0, save errors with `AppendError::SerializeValue` (strict-sequential check at `position_index/index.rs:194-206`).
- **Consequence:** Import fails repeatably from this peer (sync DoS). No persistent corruption — staging dir wiped.
- **Compensating-control search:** Phase 4 Q2 — `PositionIndex::save` requires `self.len() == key as usize`, errors otherwise. This bounds the exploit to import-DoS rather than corruption.
- **Final reasoning:** Originally framed as CRITICAL data corruption; the strict-sequential index guard changes it to import-fail-DoS. MEDIUM.

### F-13 — `cleanup_stale_pending_requests` has no panic guard (was Phase 3 F6)
- **First surfaced:** Phase 1 GAP 4 → Phase 2 (Anchor 9) → Phase 3 F6 → Phase 4 J-2 reclassification
- **File:line:** `crates/consensus/primary/src/network/mod.rs:482-496`
- **Phase 4 unified severity:** Originally MEDIUM, downgraded to LOW
- **Phase 6 final severity:** LOW
- **Status:** CONFIRMED RECLASSIFIED
- **Code excerpt:**
  ```rust
  fn cleanup_stale_pending_requests(&mut self) {
      let now = Instant::now();
      self.pending_epoch_requests
          .lock()
          .retain(|_, pending| now.duration_since(pending.created_at) < PENDING_REQUEST_TIMEOUT);
  }
  ```
- **Trigger sequence:** Hypothetical panic in `cleanup_stale_pending_requests` ends the outer `primary network events` task → permits leak forever.
- **Consequence:** If outer task dies, all 5 permits potentially leak.
- **Compensating-control search:** Phase 6 — `Instant::now()` doesn't panic; `parking_lot::Mutex::lock()` doesn't panic; `HashMap::retain` with `Duration` comparison doesn't panic. No reachable panic vector. `spawn_critical_task` propagates panics to `CriticalExitError` → process restart, bounding leak window.
- **Final reasoning:** No reachable panic AND critical-task propagation reset state on restart. LOW.

### F-14 — No boot-time reaper for orphan `import-*/` directories
- **First surfaced:** Phase 3 F4
- **File:line:** `crates/storage/src/consensus.rs:294-303`
- **Phase 4 unified severity:** LOW
- **Phase 6 final severity:** LOW
- **Status:** CONFIRMED
- **Code excerpt:**
  ```rust
  pub fn new(base_path: PathBuf) -> Result<Self, ConsensusChainError> {
      let latest_consensus = LatestConsensus::new(&base_path)?;
      let current_pack = ConsensusPack::open_append_exists(&base_path, latest_consensus.epoch())?;
      ...
  }
  ```
- **Trigger sequence:** SIGKILL mid-`stream_import` leaves `import-{epoch}/` populated. `ConsensusChain::new` doesn't iterate `import-*/`.
- **Consequence:** Disk-space leak bounded by next attempt's `remove_dir_all` at line 379.
- **Compensating-control search:** Line 379 unconditional cleanup on next attempt is the only mechanism.
- **Final reasoning:** Bounded; no consensus impact. LOW.

### F-15 — `reset_for_epoch` doesn't clear `RequestHandler` maps
- **First surfaced:** Phase 1 GAP 10 → Phase 2 Adjacent §7 → Phase 3 F11 → Phase 4 Q7/Q8 reclassification
- **File:line:** `crates/consensus/primary/src/consensus_bus.rs:304-307`
- **Phase 4 unified severity:** Originally LOW-MEDIUM, downgraded to LOW
- **Phase 6 final severity:** LOW (informational)
- **Status:** CONFIRMED LOW
- **Compensating-control search:** Phase 4 Q7 — `PrimaryNetwork::new` is invoked on per-epoch `epoch_task_manager` (`node/manager/node/epoch.rs:110-114, 1010-1019`). Handler is rebuilt per epoch; maps are implicitly reset.
- **Final reasoning:** Documented reset path doesn't clear maps, but operational invariant holds via per-epoch handler rebuild. Informational only.

### F-16 — Worker oversized-request silent truncation causes mis-attributed penalty
- **First surfaced:** Phase 2 Adjacent §6
- **File:line:** `crates/consensus/worker/src/network/mod.rs:289-300, 324-325`
- **Phase 4 unified severity:** MEDIUM
- **Phase 6 final severity:** MEDIUM
- **Status:** CONFIRMED
- **Trigger sequence:**
  1. Requester R sends 501-digest batch request (>`MAX_BATCH_DIGESTS_PER_REQUEST = 500`).
  2. Responder Q truncates with `.take(500)` silently; computes `request_digest` over truncated set.
  3. R computes its own digest over full 501-set; the digests differ.
  4. R opens stream with full-set digest; Q's `process_inbound_stream` fails the lookup → `UnknownStreamRequest` → `Penalty::Mild` against R.
- **Consequence:** R is penalised for an oversized request that Q silently agreed to truncate. Worker analog of F-3 mis-attribution.
- **Final reasoning:** Asymmetric griefing on the worker network. MEDIUM.

### F-17 — `epoch_meta.committee` not validated; flows into `CertifiedBatch::address`
- **First surfaced:** Phase 2 Adjacent §5 → Phase 4 J-6 (joint flag)
- **File:line:** `crates/storage/src/consensus_pack.rs:697-704` (no validation), `consensus_pack.rs:900-901` (read site)
- **Phase 4 unified severity:** MEDIUM-HIGH (flag)
- **Phase 6 final severity:** MEDIUM-HIGH (flag for follow-up audit)
- **Status:** CONFIRMED FLAG
- **Code excerpt:**
  ```rust
  // consensus_pack.rs:900-901
  let address =
      self.epoch_meta.committee.authority(cert.origin()).map(|a| a.execution_address());
  ```
- **Trigger sequence:** Attacker streams `EpochMeta { committee: <attacker_choice> }`. Stream completes; `Inner::get_consensus_output` later returns `CertifiedBatch::address` from attacker-set committee.
- **Consequence:** Reward attribution corruption if downstream execution trusts `CertifiedBatch::address`.
- **Compensating-control search:** Phase 6 — `EpochRecord.next_committee` (`primary/epoch.rs:28`) is `Vec<BlsPublicKey>` only — no execution addresses to cross-check against. `cert.origin()` is signed (trusted key); but the lookup MAP is not.
- **Final reasoning:** Severity hinges on whether downstream execution trusts this committee field for reward attribution; cannot resolve from this audit's scope. Strongly recommend follow-up audit of `Inner::get_consensus_output` consumers.

### J-1 — Transient `current_pack = None` window during same-epoch `stream_import`
- **First surfaced:** Phase 4 (NEW joint finding)
- **File:line:** `crates/storage/src/consensus.rs:403-422`
- **Phase 4 unified severity:** MEDIUM
- **Phase 6 final severity:** MEDIUM
- **Status:** CONFIRMED
- **Code excerpt:**
  ```rust
  let mut current_pack = self.current_pack.lock();
  ...
  if replace_current { *current_pack = None; }
  drop(pack);
  drop(current_pack);
  if std::fs::exists(&base_dir).unwrap_or_default() {
      let _ = std::fs::remove_dir_all(&base_dir);
  }
  std::fs::rename(&path_base_dir, &base_dir)?;
  self.recent_packs.lock().retain(|p| p.epoch() != epoch);
  ```
- **Trigger sequence:** Same-epoch `stream_import(E)` (in-place replacement). Between `*current_pack = None` (line 411) and `fs::rename` (line 422), concurrent reader sees None, falls through to `recent_packs` (may serve stale handle to unlinked inode on Linux), or `open_static` returns ENOENT.
- **Consequence:** Flapping availability of `consensus_header_by_*` lookups during the rename window. Worse if rename fails: `current_pack = None` permanently until next epoch; live pack handle lost (error path doesn't restore).
- **Compensating-control search:** All `current_pack()` callers handle None gracefully (fall through to `get_static`); no panics. But error path at lines 432-435 doesn't reinstate prior `current_pack`.
- **Final reasoning:** Visible inconsistency window; no permanent corruption unless rename fails. MEDIUM.

## Joint Findings (Phase 4 Step C — already absorbed)

- **J-2** — F-1 OOM × F-13 prune slot leak: confirmed bounded by critical-task propagation → process restart. Downgraded F-13 to LOW. Captured under F-13.
- **J-3** — F-3 × F-4 × F-6 griefing triangle: confirmed HIGH. Captured as the J5.3 journey backed by F-3.
- **J-4** — F-2 × F-8 collision: confirmed HIGH. Captured as J5.4 journey backed by F-2 + F-8.
- **J-5** — F-5 × `trunc_and_heal`: identical to F-5 standalone. Captured under F-5.
- **J-6** — F-17 committee unvalidated × `Inner::get_consensus_output` read site: identical to F-17 flag. Captured under F-17.

## Anchor Concern Audit Trail (1-12)

| # | Concern | Phase 0 | Phase 1 | Phase 2 | Phase 3 | Phase 4 | Phase 6 | Final State | Reasoning |
|---|---|---|---|---|---|---|---|---|---|
| 1 | Uncapped `val_size` resize | Hit-List #1 | (storage scope) | SUBSTANTIATED F-1 | n/a (non-state) | CONFIRMED CRITICAL | CONFIRMED | F-1 CRITICAL | Single-peer process kill; no length cap exists. |
| 2 | Write-before-validate ordering | Hit-List #2 | GAP pair 2 | SUBSTANTIATED F-2 | F1 | CONFIRMED HIGH (joint J-4) | CONFIRMED | F-2 HIGH | Final-header-only check admits forged intermediates. |
| 3 | No app-layer signature on StreamEpoch | Hit-List #3 | GAP pair 9 | SUBSTANTIATED F-3 | F-9 | CONFIRMED HIGH (joint J-3) | CONFIRMED | F-3 HIGH | BLS in payload not bound to libp2p PeerId. |
| 4 | Race on `MAX_PENDING_REQUESTS_PER_PEER` | Hit-List candidate | (atomicity question) | DISMISSED | DISMISSED | DISMISSED | CONFIRMED DISMISSED | (no finding) | Same-mutex serialization enforces cap; race impossible. |
| 5 | `Penalty::Mild` only — no escalation | Hit-List #7 | LOW item 11 | SUBSTANTIATED F-4 | n/a | CONFIRMED MEDIUM | CONFIRMED (with nuance) | F-4 MEDIUM | Stream-error variants are all Mild or None; joint J-3 amplifies to HIGH. |
| 6 | Three-index atomicity (no REDB tx) | Hit-List #2 | GAP pair 1+3 | SUBSTANTIATED F-5 | F2 | CONFIRMED MEDIUM (joint J-5) | CONFIRMED | F-5 MEDIUM | `trunc_and_heal` divergent; `consensus_header_by_digest` non-defensive. |
| 7 | Initiator trusts response identity | Hit-List #3 | GAP pair 9 | SUBSTANTIATED (same as #3) | n/a | CONFIRMED (same root as #3) | CONFIRMED | F-3 HIGH | Same root cause as anchor 3. |
| 8 | `request_digest` lacks domain separation | Hit-List #4 | (correlation question) | SUBSTANTIATED F-6 | n/a | CONFIRMED MEDIUM | CONFIRMED | F-6 MEDIUM | Predictability + low-cost probing. |
| 9 | Pending-request prune relies on background interval | Hit-List #10 | GAP pair 4/6 | DISMISSED-WITH-CAVEAT | F6 | DOWNGRADED LOW (J-2) | CONFIRMED LOW | F-13 LOW | No reachable panic; restart-bounded leak window. |
| 10 | No whole-stream timeout cap | Hit-List #6 | (timeout audit) | SUBSTANTIATED F-7 | n/a | CONFIRMED MEDIUM | CONFIRMED | F-7 MEDIUM | Per-record only; sustainable slow-stream attack. |
| 11 | Idempotent request replacement | (commit cb0e147c context) | (cross-pair pair 4 mention) | SUBSTANTIATED with caveat | DISMISSED | RECLASSIFIED INFORMATIONAL | CONFIRMED INFORMATIONAL | (no finding) | Mechanism correct on responder side; requester-side gap captured by F-8. |
| 12 | Cross-component invariant during pack import | Hit-List #6 partial | GAP pair 7 | SUBSTANTIATED (mixed F-8 + F-10) | DISMISSED for watch | RECLASSIFIED SPLIT | CONFIRMED SPLIT | F-8 HIGH + F-10 LOW + J-1 MEDIUM | Two distinct concerns conflated; disentangled. |

## Phase 4 reclassifications and withdrawals

- **Anchor 11 → INFORMATIONAL.** Mechanism in cb0e147c is correct. Substantive concern (requester-side dedup) captured by F-8.
- **Anchor 12 → SPLIT.** Two distinct concerns conflated in Phase 2; Phase 3 disentangled them. Watch advance is on the gossip-fetch path (F-10 LOW); dir collision is its own finding (F-8 HIGH); transient `current_pack=None` is a NEW joint finding (J-1 MEDIUM).
- **F-9 (vote-replay) → DISMISSED.** Q1: `Header::digest()` includes epoch (collision infeasible). Q7: handler rebuilt per epoch.
- **F-11 / F-15 (handler-map clearing) → DOWNGRADED to LOW.** Q7: handler is rebuilt per-epoch via `epoch_task_manager`; explicit clearing is moot.
- **F-12 (start_consensus_number poisoning) → RECLASSIFIED MEDIUM.** Q2: `PositionIndex::save` strict-sequential check bounds exploit to import-fail-DoS, not corruption. Was originally hypothesised CRITICAL.
- **F-13 / F-6 (prune panic guard) → DOWNGRADED to LOW.** J-2: `spawn_critical_task` panic propagation triggers restart, bounding leak window.
- **Phase 3 F5 (idempotent-replacement permit inflation) → SELF-DISMISSED in Phase 3.** OLD permit drops synchronously inside the same mutex critical section; cap holds. Confirmed dismissed in Phase 4.

## Phase 6 dismissals with counter-evidence

- **F-9 (cross-epoch vote replay):** STAYS DISMISSED.
  - Counter-evidence: `crates/types/src/primary/header.rs:14-38, 248-252`. `Header { ..., epoch: Epoch, ... }` non-`#[serde(skip)]`; `Header::digest()` calls `encode(&self).as_ref()` on full struct → SHA-256 collision required.
  - Plus Q7 per-epoch handler rebuild (independent reason).

- **Anchor 4 (`MAX_PENDING_REQUESTS_PER_PEER` race):** STAYS DISMISSED.
  - Counter-evidence: `mod.rs:676-723`. `pending_map.lock()` at line 676 holds the guard for both the count check at line 679 AND the insert at line 703. Two concurrent inbound RPC handlers serialize on this mutex.

- **Anchor 11 (idempotent replacement):** STAYS RECLASSIFIED INFORMATIONAL.
  - Counter-evidence: `mod.rs:698-701`. `created_at` preservation runs entirely under `pending_map.lock()`; OLD permit drop and NEW permit grab are net-zero per replacement.

## Cross-references

### Adversarial journeys (J5.1-J5.10)

- **J5.1** — Single-peer OOM amplification + restart-state hazard. Composes F-1, F-14, F-8. CRITICAL.
- **J5.2** — Concurrent same-epoch fetch collision (3-way race). Composes F-8, F-3, F-6, J-1. HIGH.
- **J5.3** — Penalty mis-attribution honest-validator eviction. Composes F-3, F-4, F-6. HIGH.
- **J5.4** — Forged-intermediate-header pack admission. Composes F-2, J-4, F-17. HIGH.
- **J5.5** — Cross-epoch staleness via attacker-set `start_consensus_number`. Composes F-12, F-7, F-3. MEDIUM.
- **J5.6** — Slot exhaustion via slow-stream + small records. Composes F-7, F-4, F-11, F-3. HIGH.
- **J5.7** — Transient `current_pack=None` reader race. Composes J-1, F-10. MEDIUM.
- **J5.8** — Restart-after-OOM compounded with idempotent replacement. Composes F-1, F-14, F-13, F-8. HIGH.
- **J5.9** — Probe-driven peer-score asymmetric drain. Composes F-6, F-4, F-11. MEDIUM.
- **J5.10** — Worker batch-stream mis-attribution mirror. Composes F-16, F-3, F-4. MEDIUM.

### Cross-journey patterns

- **F-3 + F-4 + F-6 form a "griefing triangle":** identity confusion (F-3) + no escalation (F-4) + predictable correlator (F-6) appears in J5.3, J5.6, J5.9, J5.10 and partially in J5.1 / J5.2 / J5.5.
- **F-1 + F-8 + F-14 form a "crash-loop triangle":** OOM vector + immediate re-fetch + no startup cleanup. Appears in J5.1 and J5.8.
- **F-2 + final-header-only check** is the unique gold-finding for J5.4 — silent data-poison vector that depends on whether downstream execution re-validates intermediate headers.
- **Highest-leverage findings by reuse:** F-3 (5 journeys), F-6 (5), F-4 (4), F-1 / F-7 / F-8 (2-3 each).
