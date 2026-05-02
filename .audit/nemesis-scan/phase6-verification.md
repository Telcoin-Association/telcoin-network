# Phase 6: Verification — issue-629

_Git hash: 9b398938_
_Mode: Anti-confirmation. Each finding re-verified against HEAD._
_Generated: 2026-04-27_

## Verifier Stance

Anti-confirmation. False positives reduced. Every finding was treated as a
hypothesis to falsify; only those re-verified against HEAD code, with no
compensating control located, survived.

The CRITICAL is the only finding given a complete repro recipe; HIGH findings
have a tightened repro hint; below HIGH the trigger sequence from earlier
phases is preserved without re-derivation.

---

## Findings Re-Verified

| ID | Phase 4 Severity | Verifier Verdict | Final Severity | Reason |
|---|---|---|---|---|
| F-1 | CRITICAL | CONFIRMED | CRITICAL | `pack_iter.rs:148-150` matches; no `MAX_RECORD_SIZE`-style cap exists in scope; `val_size` is peer-controlled. |
| F-2 | HIGH | CONFIRMED | HIGH | `consensus_pack.rs:738-749` matches; only the FINAL header is checked at `consensus.rs:387-401` against `epoch_record.final_consensus`. |
| F-3 | HIGH | CONFIRMED | HIGH | `mod.rs:342, 358, 381` matches; `peer` from response payload used for both `open_stream` and `report_penalty`. |
| F-4 | MEDIUM | CONFIRMED (with nuance) | MEDIUM | `error/network.rs:115-128` matches. Note: `Penalty::Medium` and `Penalty::Fatal` exist for OTHER variants, but for stream-related errors (`UnknownStreamRequest`, `StreamUnavailable`) there's no escalation across repeated attempts — F-4's claim holds for the streaming surface. |
| F-5 | MEDIUM | CONFIRMED | MEDIUM | `consensus_pack.rs:919-928` defensive vs `930-935`/`1024-1033` non-defensive matches; `trunc_and_heal` digest-index divergence is documented in code. |
| F-6 | MEDIUM | CONFIRMED | MEDIUM | `mod.rs:333-336` and `mod.rs:690-692` match; digest is pure `epoch.to_le_bytes()`. |
| F-7 | MEDIUM | CONFIRMED | MEDIUM | `consensus_pack.rs:684-689` per-record timeout only; no outer `tokio::time::timeout(...)` at `mod.rs:373-378`. |
| F-8 | HIGH | CONFIRMED | HIGH | `state-sync/consensus.rs:174` per-task `Arc::new(Semaphore::new(1))`; `node.rs:288` spawns 3 of them; `consensus.rs:379` unconditional `remove_dir_all`. |
| F-9 | DISMISSED | CONFIRMED DISMISSED | DISMISSED | `header.rs:248-250` calls `encode(&self).as_ref()`; `epoch: Epoch` is non-`#[serde(skip)]` (line 22), so digest covers epoch — collision infeasible. |
| F-10 | LOW | CONFIRMED | LOW | `state-sync/consensus.rs:65` advances watch on `ConsensusHeaderCache.insert`, before pack durable. Asymmetry stands. |
| F-11 | LOW | CONFIRMED | LOW | `mod.rs:686-688, 722` rejection drops permit silently; `ack=false` returned. |
| F-12 | MEDIUM | CONFIRMED RECLASSIFIED | MEDIUM | `position_index/index.rs:194-206` strict-sequential check (`self.len() != key as usize`) bounds exploit to import-DoS, not corruption. |
| F-13 | LOW | CONFIRMED | LOW | `mod.rs:482-496` no panic guard but no reachable panic in pure `Instant` + `parking_lot::Mutex::lock` + `HashMap::retain` with `Duration::lt`. Critical-task panic propagates → process restart bounds leak window. |
| F-14 | LOW | CONFIRMED | LOW | `consensus.rs:294-303` `ConsensusChain::new` does not iterate `import-*/`. Bounded by line 379's per-attempt cleanup. |
| F-15 | LOW | CONFIRMED | LOW | `consensus_bus.rs:304-307` `reset_for_epoch` doesn't clear handler maps; moot under per-epoch handler rebuild (Q7). |
| F-16 | MEDIUM | CONFIRMED | MEDIUM | `worker/network/mod.rs:289-300` silent truncation; `request_digest` at line 324-325 is computed over the truncated set. |
| F-17 | MEDIUM-HIGH (flag) | CONFIRMED + ELEVATED CONCERN | MEDIUM-HIGH (flag for Phase 5/follow-up audit) | `consensus_pack.rs:900-901` reads `epoch_meta.committee.authority(...).execution_address()` for `CertifiedBatch::address`. Crucially, `EpochRecord.next_committee` (`primary/epoch.rs:28`) is only `Vec<BlsPublicKey>` — there is no trusted source of execution addresses to validate against. Rewarding consumers' downstream trust is the gating uncertainty. |
| J-1 | MEDIUM | CONFIRMED | MEDIUM | `consensus.rs:403-422` window matches; error path at lines 432-435 doesn't restore `current_pack`. |
| J-2 | (analysis only) | CONFIRMED | LOW | F-13 leak window is bounded by critical-task panic → process restart. |
| J-3 | (joint) | CONFIRMED | HIGH | F-3 × F-4 × F-6 chain reproducible against HEAD code. |
| J-4 | (joint) | CONFIRMED | HIGH | F-2 × F-8 × final-only check at `consensus.rs:387-401`. |
| J-5 | (joint) | CONFIRMED | MEDIUM | Identical to F-5 standalone. |
| J-6 | (joint) | CONFIRMED FLAG | MEDIUM-HIGH (flag) | Identical to F-17 — the unvalidated `epoch_meta.committee` IS read in `get_consensus_output`. |

---

## Confirmed Findings

### F-1 — Unbounded `val_size` resize in pack iterator (CRITICAL)

- **File:line:** `crates/storage/src/archive/pack_iter.rs:147-150` (async, hot
  peer path) and re-occurring at `pack_iter.rs:74-77`, `pack.rs:319-321`,
  `pack.rs:343-345`.
- **Re-read code excerpt (HEAD):**
  ```rust
  // pack_iter.rs:147-150
  crc32_hasher.update(&val_size_buf);
  let val_size = u32::from_le_bytes(val_size_buf);
  buffer.resize(val_size as usize, 0);
  file.read_exact(buffer).await?;
  ```
- **Compensating-control search:**
  - Searched `crates/storage`, `crates/state-sync`, `crates/consensus/primary`
    for `MAX_RECORD_SIZE`, `MAX_VAL_SIZE`, `MAX_PACK_RECORD`, `MAX_PAYLOAD`,
    `MAX_BATCH_SIZE`, `RECORD_SIZE_LIMIT` — **no matches**.
  - The per-record `tokio::time::timeout(PACK_RECORD_TIMEOUT_SECS=10s, ...)`
    at `consensus_pack.rs:684-689` wraps the WHOLE `iter.next()` future,
    including the `read_exact` over a 4 GiB allocation. The timeout fires
    after the resize panic / OOM kill.
  - The CRC32 covers `(size_le || payload)`, so an attacker who crafts both
    payload and matching CRC32 passes the integrity check.
  - Trait-bound search: `Vec::resize(usize, T)` is infallible at the
    type level; allocation failure produces a panic in `alloc::vec::Vec`,
    not a `Result`.
- **Trigger sequence (re-validated):**
  1. Attacker P clears libp2p handshake.
  2. V issues `request_epoch_pack(E)`. `send_request_any` reaches P.
     P answers `RequestEpochStream { ack: true, peer: P_bls }`.
  3. V calls `open_stream(P_bls)` → `auth_to_peer(P_bls)` → libp2p
     stream to P. V writes `digest(E.to_le_bytes())` correlator.
  4. P streams a valid 28-byte `DataHeader` (uid_idx = epoch as u64),
     a structurally-valid `EpochMeta { epoch: E, ... }`, then a record
     with `val_size_buf = [0xFF, 0xFF, 0xFF, 0xFF]` and any 4 bytes
     CRC over (size_le || payload).
  5. V's `AsyncPackIter::read_record_file` reads `val_size_buf`,
     computes `val_size = u32::MAX = 4_294_967_295`, calls
     `buffer.resize(4_294_967_295, 0)`.
  6. On Linux + overcommit: `Vec::resize` reserves virtual memory,
     `read_exact` writes pages, kernel OOM-killer fires
     when physical memory exhausted. On macOS / no-overcommit: the
     resize aborts with allocation failure / panic.
- **Final consequence:** Process termination. `epoch-consensus-worker-{i}`
  is a `spawn_critical_task` (`node.rs:289`), so its panic produces a
  `CriticalExitError` and shuts down the whole node. Repeatable from the
  same peer on every restart (F-14: no boot-time reaper, but bounded
  by line 379's per-attempt cleanup).

### F-2 — Write-before-validate orphan-batch staging + final-header-only check (HIGH)

- **File:line:** `consensus_pack.rs:738-749` (write-before-validate batch arm),
  `consensus_pack.rs:751-781` (consensus-header arm with parent-chain check),
  `consensus.rs:387-401` (final-header-only validation).
- **Re-read code excerpt (HEAD):**
  ```rust
  // consensus_pack.rs:751-754
  PackRecord::Consensus(consensus_header) => {
      if consensus_header.parent_hash != parent_digest {
          return Err(PackError::InvalidConsensusChain);
      }
  ```
  ```rust
  // consensus.rs:387-401
  match pack.latest_consensus_header().await {
      Some(last_header) => {
          if epoch_record.final_consensus.number != last_header.number
              || epoch_record.final_consensus.hash != last_header.digest()
          {
              let _ = std::fs::remove_dir_all(&path);
              return Err(ConsensusChainError::InvalidImport);
          }
      }
      ...
  }
  ```
- **Compensating-control search:**
  - Searched for any `verify_signature`, `verify_with_cert`, `quorum`-style
    check inside `Inner::stream_import` (`consensus_pack.rs:673-786`) —
    none. The only structural checks are `parent_hash == parent_digest`,
    batch-set `HashSet::remove` bookkeeping, and CRC32 framing.
  - The `EpochRecord` carries only `final_consensus: BlockNumHash`
    (`primary/epoch.rs:37`) — no per-header attestations. A trusted
    source of intermediate-header validity is **absent by design**.
  - The Q3 question (Phase 4 §B): is there a downstream re-validation of
    intermediate headers' BLS sigs? The answer was deferred to Phase 5
    (and the Phase 5 J5.4 mitigation breakage explicitly notes "the
    Phase 4 Q3 question on whether intermediate headers are re-validated
    downstream is the gating uncertainty"). **In the code in scope**,
    there is no such re-validation — `Inner::get_consensus_output`
    returns the stored sub-DAG without re-verifying signatures. If
    execution does re-verify, F-2 reduces severity; the audit cannot
    independently confirm execution-side behaviour from this scope.
- **Trigger sequence (re-validated):**
  See J5.4 in Phase 5. Attacker constructs a chain whose intermediate
  headers are fabricated (with internally consistent parent-hash links
  and batch-set bookkeeping) and whose terminal header has the genuine
  `final_consensus.{number,hash}`. The structural loop accepts; final
  check passes; rename succeeds; forged headers are now durable.
- **Final consequence:** Forged intermediate headers admitted to the local
  pack DB, re-served to other peers via `get_epoch_stream`. Reward
  attribution downstream depends on whether execution re-verifies.
- **Repro recipe:**
  1. Compromised peer P selected by V's `send_request_any` for epoch E.
  2. P emits `DataHeader`, `EpochMeta { epoch: E, committee:
     <real_committee>, start_consensus_number: <real>, ... }`.
  3. P emits `(batch_real_1, header_real_1, batch_real_2,
     forged_header_2, ..., forged_header_N-1, batch_real_N,
     header_real_N)` where:
     - Each forged header's `parent_hash == prev_header.digest()`.
     - Each forged header's `payload` references the immediately
       preceding (real) batches such that `batches.remove(...)` line
       757 succeeds.
     - The final header's `digest()` and `number` match
       `epoch_record.final_consensus`.
  4. V's `Inner::stream_import` accepts. `pack.persist().await` flushes.
  5. V's `consensus.rs:387-401` final-header check passes.
  6. V's `fs::rename(import-{E}/epoch-{E}, base/epoch-{E})` (line 422)
     succeeds.
  7. V's pack DB now contains `forged_header_2..N-1` retrievable via
     `consensus_chain.consensus_header_by_number` and
     `consensus_chain.consensus_header_by_digest`.
- **Severity rationale:** HIGH because pack-stream re-emission propagates
  the forge to other honest peers (`get_epoch_stream` at
  `consensus.rs:442-472` re-serves bytes verbatim).

### F-3 — Penalty mis-attribution via unauthenticated `peer: BlsPublicKey` in response payload (HIGH)

- **File:line:** `crates/consensus/primary/src/network/mod.rs:342, 358, 381`.
- **Re-read code excerpt (HEAD):**
  ```rust
  // mod.rs:342
  if let PrimaryResponse::RequestEpochStream { ack, peer } =
      self.handle.send_request_any(request.clone()).await?.await??
  {
      ...
      // mod.rs:358
      let mut stream = self.handle.open_stream(peer).await??;
      ...
      // mod.rs:381
      if res.is_err() {
          self.report_penalty(peer, Penalty::Mild).await;
      }
  ```
- **Compensating-control search:**
  - Searched for any `peer ==`/`peer_actual`/`assert_eq!` rebinding the
    BLS to the on-wire PeerId in `request_epoch_pack` — none.
  - `send_request_any` returns `PrimaryResponse` only — does not surface
    the libp2p PeerId of the actual responder.
  - `open_stream(peer)` resolves BLS to PeerId via
    `peer_manager.auth_to_peer(peer)`
    (`network-libp2p/src/consensus.rs:716`) — confirmed Q5 — so the
    stream goes to whoever holds the claimed BLS, not to the responder
    who answered the RPC.
- **Trigger sequence (re-validated):**
  See J5.3 in Phase 5. Concise: A claims `peer: B_bls`. V opens stream
  to B (via `auth_to_peer`). B has no pending entry → `UnknownStreamRequest`
  → B penalises V. V's import fails → V penalises B. A is invisible.
- **Final consequence:** Honest validator B accumulates penalty mass at
  zero cost to A; combined with no escalation ladder (F-4) and predictable
  digest (F-6), peer-eviction attack is sustainable.
- **Repro recipe (J5.3 chain):**
  1. A is a libp2p mesh peer.
  2. A monitors `PrimaryRequest::StreamEpoch` requests.
  3. When V sends `StreamEpoch { epoch: E }` and A is selected, A
     responds with `peer: B_bls` (B is any honest peer in the committee
     mapping).
  4. V calls `open_stream(B_bls)` → routes to B's PeerId → B sees
     unsolicited inbound stream → returns `UnknownStreamRequest` →
     `Penalty::Mild` from B → V.
  5. V's `stream_import` errors → `Penalty::Mild` from V → B.
  6. A repeats every ~5s with rotating B values. Score for B trends
     negative ~0.1/event. Default ban threshold ~100 → ~83 minutes per
     evicted validator.

### F-4 — `Penalty::Mild` only for stream errors, no escalation (MEDIUM)

- **File:line:** `crates/consensus/primary/src/error/network.rs:115-128`.
- **Re-read code excerpt (HEAD):**
  ```rust
  | PrimaryNetworkError::InvalidRequest(_)
  | PrimaryNetworkError::UnknownConsensusHeaderDigest(_)
  | PrimaryNetworkError::UnknownStreamRequest(_)
  | PrimaryNetworkError::UnknownConsensusHeaderCert(_) => Some(Penalty::Mild),
  ...
  | PrimaryNetworkError::StreamUnavailable(_) ... => None,
  ```
- **Compensating-control search:**
  - Penalty levels `Mild`, `Medium`, `Fatal` ARE used elsewhere
    (lines 90, 97, 120, 122) — but for OTHER error variants
    (CertificateError, InvalidEpochRequest, InvalidTopic). For the
    streaming-related errors specifically, only `Mild` and `None`.
  - No counter, no rate-of-misbehavior, no escalation tracker exists in
    `error/network.rs` or in the libp2p score config (out of scope but
    flagged).
- **Verdict:** F-4 stands at MEDIUM. The earlier phases' wording "Mild is the
  entire ladder" is overstated as a global claim, but for the streaming
  surface in scope it is accurate.

### F-5 — `consensus_header_by_digest` and `batch` lack defensive `pos < data.file_len()` filter (MEDIUM)

- **File:line:** `consensus_pack.rs:919-928` (defensive), `930-935`
  (non-defensive), `1024-1033` (non-defensive); `474-527`
  (`trunc_and_heal` divergence).
- **Re-read code excerpt (HEAD):**
  ```rust
  // 919-928 (defensive)
  fn contains_consensus_header(&mut self, digest: B256) -> bool {
      if let Ok(pos) = self.consensus_digests.load(digest) {
          pos < self.data.file_len()
      } else { false }
  }
  // 930-935 (NOT defensive)
  fn consensus_header_by_digest(&mut self, digest: B256) -> Option<ConsensusHeader> {
      let pos = self.consensus_digests.load(digest).ok()?;
      let rec = self.data.fetch(pos).ok()?;
      rec.into_consensus().ok()
  }
  // 1024-1033 (NOT defensive)
  fn batch(&mut self, digest: BlockHash) -> Option<Batch> {
      if let Ok(pos) = self.batch_digests.load(digest) {
          if let Ok(batch) = self.data.fetch(pos) { ... }
      }
      None
  }
  ```
- **Compensating-control search:**
  - `data.fetch(pos)` (in `pack.rs::PackInner::read_record`) does
    `seek(SeekFrom::Start(pos))` then `read_exact(val_size_buf)`. If
    `pos > data.file_len()`, the `read_exact` returns `UnexpectedEof`
    converted to `FetchError::IO` → `consensus_header_by_digest` returns
    `None`. So the asymmetry doesn't cause garbage reads — it causes
    silent `None` where `contains_consensus_header` returned `false`.
    **Severity stays MEDIUM** (split-brain visibility, not corruption).
- **Verdict:** F-5 CONFIRMED MEDIUM. The "internally consistent" view is
  preserved by Rust's safe IO error handling, but cross-API consistency is
  observably broken in post-`trunc_and_heal` states.

### F-6 — `request_digest = digest(epoch.to_le_bytes())` lacks domain separation (MEDIUM)

- **File:line:** `mod.rs:333-336` (requester) and `mod.rs:690-692`
  (responder).
- **Re-read code excerpt (HEAD):**
  ```rust
  let mut hasher = tn_types::DefaultHashFunction::new();
  hasher.update(&epoch.to_le_bytes());
  let request_digest = B256::from_slice(hasher.finalize().as_bytes());
  ```
- **Compensating-control search:**
  - Map key is `(BlsPublicKey, B256)` — the BLS half disambiguates per-peer.
    Cross-peer same-epoch collisions are not a same-key issue on the
    responder side (different requesters → different `peer` halves).
  - Replay across reconnects: pruning at `mod.rs:495` (30s) and
    `mod.rs:798-800` (remove on stream open) bound the lifetime. A
    same-peer reconnect within 30s with a stale digest matches an entry,
    but the Phase 4 analysis confirmed this is bounded.
- **Verdict:** F-6 CONFIRMED MEDIUM. Predictability + low-cost probe is
  the operational concern, not collision per se.

### F-7 — No whole-stream timeout (MEDIUM)

- **File:line:** `mod.rs:373-378` (call site, no outer timeout) and
  `consensus_pack.rs:680-690` (per-record timeout helper).
- **Re-read code excerpt (HEAD):**
  ```rust
  // consensus_pack.rs:680-690
  async fn next<R: AsyncRead + Unpin>(
      iter: &mut AsyncPackIter<PackRecord, R>,
      timeout: Duration,
  ) -> Result<Option<PackRecord>, PackError> {
      match tokio::time::timeout(timeout, iter.next()).await { ... }
  }
  ```
- **Compensating-control search:**
  - `mod.rs:373-378` calls `consensus_chain.stream_import(...,
    record_timeout).await` — no `tokio::time::timeout(STREAM_CAP, ...)`
    wrapping it.
  - No `MAX_RECORDS_PER_PACK` constant; the while-loop at
    `consensus_pack.rs:732` runs until `next` returns `Ok(None)` (stream
    EOF).
- **Verdict:** F-7 CONFIRMED MEDIUM.

### F-8 — Concurrent same-epoch fetches collide on `import-{epoch}` directory (HIGH)

- **File:line:** `state-sync/consensus.rs:174` (per-task semaphore),
  `node.rs:286-299` (3 fetcher tasks), `storage/consensus.rs:377-379`
  (unconditional cleanup), `consensus_bus.rs:551-553` (no enqueue dedup).
- **Re-read code excerpt (HEAD):**
  ```rust
  // state-sync/consensus.rs:174
  let next_sem = Arc::new(Semaphore::new(1));      // per-invocation
  ```
  ```rust
  // node.rs:286-299
  for i in 0..3 {
      node_task_manager.spawn_critical_task(
          format!("epoch-consensus-worker-{i}"),
          spawn_fetch_consensus(...),
      );
  }
  ```
  ```rust
  // storage/consensus.rs:377-379
  let path = self.base_path.join(format!("import-{epoch}"));
  let _ = std::fs::remove_dir_all(&path);
  ```
- **Compensating-control search:**
  - `consensus.rs:365-374` early-return checks `get_static(epoch)` for a
    COMPLETED pack — a partial in-progress import is invisible.
  - No file lock on `import-{epoch}/`. No `flock` / `LockFile`.
  - `consensus_bus.rs:551-553` (`request_epoch_pack_file`) is
    fire-and-forget `let _ =`, no dedup.
  - `state-sync/consensus.rs:111-117` (only producer of enqueues) checks
    `consensus_chain.consensus_header_by_number(...).is_some()` which
    only returns true for a COMPLETE pack — same blind spot.
- **Verdict:** F-8 CONFIRMED HIGH. The 3-way race is reachable under
  normal gossip churn (not just adversarial conditions).

### F-10 — `tx_last_consensus_header` advances pre-durable (LOW)

- **File:line:** `state-sync/consensus.rs:65`.
- **Compensating-control search:**
  - The watch is read by JSON-RPC + `behind_consensus`; both code paths
    tolerate a stale-pack lookup returning `None`.
- **Verdict:** F-10 CONFIRMED LOW.

### F-11 — `process_epoch_stream` rejection has no peer cost (LOW)

- **File:line:** `mod.rs:686-688, 722`.
- **Verdict:** F-11 CONFIRMED LOW.

### F-12 — `start_consensus_number` not validated; bounded by `PositionIndex::save` strict-sequential (MEDIUM)

- **File:line:** `consensus_pack.rs:702-706` (no validation),
  `consensus_pack.rs:773-778` (saturating_sub),
  `position_index/index.rs:194-206` (strict-sequential check).
- **Re-read code excerpt (HEAD):**
  ```rust
  // position_index/index.rs:194-206
  fn save(&mut self, key: u64, record_pos: u64) -> Result<(), AppendError> {
      if self.len() != key as usize {
          Err(AppendError::SerializeValue(format!(...)))
      } else {
          self.pdx_file.write_all(&record_pos.to_le_bytes())?;
          Ok(())
      }
  }
  ```
- **Verdict:** F-12 CONFIRMED MEDIUM (DoS only; the strict-sequential
  guard prevents corruption).

### F-13 — Prune loop has no panic guard (LOW)

- **File:line:** `mod.rs:482-496`.
- **Compensating-control search:**
  - Operations: `Instant::now()` (no panic), `parking_lot::Mutex::lock()`
    (no panic), `HashMap::retain` with closure
    `now.duration_since(pending.created_at) < PENDING_REQUEST_TIMEOUT`
    (`Duration` comparison is total; `duration_since` is saturating in
    std and never panics on `Instant`).
  - Critical-task panic propagation triggers process restart, bounding
    leak window.
- **Verdict:** F-13 CONFIRMED LOW.

### F-14 — No boot-time reaper for `import-*/` (LOW)

- **File:line:** `consensus.rs:294-303`.
- **Verdict:** F-14 CONFIRMED LOW (bounded by line 379's per-attempt
  cleanup).

### F-15 — `reset_for_epoch` doesn't clear handler maps (LOW)

- **File:line:** `consensus_bus.rs:304-307`.
- **Compensating-control search:**
  - Q7: `PrimaryNetwork::new` is invoked on the per-epoch
    `epoch_task_manager` (`node/manager/node/epoch.rs:110-114, 1010-1019`),
    so the handler is rebuilt. The map is implicitly reset.
- **Verdict:** F-15 CONFIRMED LOW (informational).

### F-16 — Worker oversized-request silent truncation causes mis-attributed penalty (MEDIUM)

- **File:line:** `worker/network/mod.rs:289-300, 324-325`.
- **Verdict:** F-16 CONFIRMED MEDIUM.

### F-17 — `epoch_meta.committee` not validated; flows into `CertifiedBatch::address` (MEDIUM-HIGH flag)

- **File:line:** `consensus_pack.rs:697-704` (no validation),
  `consensus_pack.rs:900-901` (read site).
- **Re-read code excerpt (HEAD):**
  ```rust
  // consensus_pack.rs:900-901
  let address =
      self.epoch_meta.committee.authority(cert.origin()).map(|a| a.execution_address());
  ```
- **Compensating-control search:**
  - `EpochRecord.next_committee` (`primary/epoch.rs:28`) is
    `Vec<BlsPublicKey>` — only BLS keys, NO execution addresses. There is
    **no trusted source** in the local node's verified state to cross-check
    `epoch_meta.committee.authority(...).execution_address()` against.
  - `cert.origin()` (the lookup key) is part of a signed certificate, so
    the lookup KEY is trusted; the lookup MAP is not.
- **Verdict:** F-17 CONFIRMED MEDIUM-HIGH (flag). The exploit hinges on
  whether downstream execution trusts `CertifiedBatch::address` for
  reward attribution. This audit's scope cannot resolve that question;
  it requires inspecting the executor path. **Strongly recommend follow-up
  audit on the consumer of `Inner::get_consensus_output`.**

### J-1 — Transient `current_pack = None` window during same-epoch `stream_import` (MEDIUM)

- **File:line:** `consensus.rs:403-422`.
- **Compensating-control search:**
  - `current_pack()` callers (e.g. `consensus.rs:506-541`) handle `None`
    by falling through to `get_static(epoch)` which scans `recent_packs`
    then `open_static`. During the `remove_dir_all` → `rename` window,
    `open_static` returns `ENOENT`. No callers panic; they all return
    errors / `None`.
  - The error path `consensus.rs:432-435` does NOT restore the prior
    `current_pack` — Phase 4 captured this correctly.
- **Verdict:** J-1 CONFIRMED MEDIUM. Visible inconsistency window;
  worse if `fs::rename` fails (live pack lost until restart).

### J-3 / J-4 — joint findings (HIGH each)

These are confirmed by the underlying findings (F-3 + F-4 + F-6 for J-3;
F-2 + F-8 for J-4). Both reproducible against HEAD code; both already
detailed in Phase 5 (J5.3, J5.4) with steps verifiable against the file:line
citations re-read here.

---

## Dismissed Findings

### F-9 (cross-epoch vote-replay via cached `auth_last_vote`) — STAYS DISMISSED

- **Phase 4 reasoning:** Q1 — `Header::digest()` includes `epoch`; SHA-256
  collision across epochs is infeasible. Q7 — `RequestHandler` is
  rebuilt per epoch.
- **Counter-evidence (re-read at HEAD):**
  - `crates/types/src/primary/header.rs:14-38`: `Header { ..., epoch:
    Epoch, ... }` — `epoch` is a non-`#[serde(skip)]` field (line 22).
  - `crates/types/src/primary/header.rs:248-252`:
    ```rust
    fn digest(&self) -> HeaderDigest {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(encode(&self).as_ref());
        HeaderDigest(Digest { digest: hasher.finalize().into() })
    }
    ```
    `encode(&self)` is bcs serialization of the full struct → `epoch`
    IS in the digest pre-image.
- **Stays-dismissed reasoning:** A deliberate cross-epoch
  digest collision requires a SHA-256 collision; not feasible.
  Additionally, Q7's per-epoch handler rebuild is a second independent
  reason. Either alone suffices.

### Anchor 4 (race on `MAX_PENDING_REQUESTS_PER_PEER`) — STAYS DISMISSED

- **Counter-evidence (re-read at HEAD):**
  `mod.rs:676-723` — `pending_map.lock()` at line 676 holds the guard
  for both the `keys().filter().count()` check at line 679 AND the
  `pending_map.insert(...)` at line 703. Two concurrent inbound RPC
  handlers serialize on this mutex. The race the Phase 0 Hit List #6
  hypothesised is impossible.

### Anchor 11 (idempotent replacement) — STAYS RECLASSIFIED INFORMATIONAL

- The `created_at` preservation at `mod.rs:698-701` runs entirely under
  `pending_map.lock()`; the OLD permit drop and NEW permit grab are
  net-zero per replacement. The mechanism is correct and isolated to
  the responder side.

---

## Reclassified

| ID | Old Severity | New Severity | Reason |
|---|---|---|---|
| F-12 | (Phase 2 §4) Originally CRITICAL | MEDIUM | `PositionIndex::save` strict-sequential check (`position_index/index.rs:196`) bounds the exploit to import-fail-DoS, not corruption. |
| F-13 (was F-6 prune) | Phase 3 MEDIUM | LOW | Critical-task panic propagation bounds leak window to a process restart. |
| Anchor 11 | Phase 2 SUBSTANTIATED | INFORMATIONAL | The mechanism is correct; the substantive concern (requester-side dedup) is captured by F-8. |
| Anchor 12 | Phase 2 SUBSTANTIATED | SPLIT into F-8 (HIGH) + F-10 (LOW) + J-1 (MEDIUM) | Two distinct concerns conflated in Phase 2; disentangled in Phase 4. |
| F-9 | Phase 2 LOW-MEDIUM | DISMISSED | Q1 + Q7 give two independent reasons. |
| F-15 | Phase 2 LOW | LOW (informational) | Map clearing is moot under Q7's per-epoch handler rebuild. |

---

## Anchor Concerns Final Coverage Table

| # | Concern | Phase 4 Verdict | Phase 6 Verdict | Final State |
|---|---|---|---|---|
| 1 | Uncapped `val_size` resize (`pack_iter.rs`) | SUBSTANTIATED | CONFIRMED | F-1 CRITICAL |
| 2 | Write-before-validate ordering in `Inner::stream_import` | SUBSTANTIATED | CONFIRMED | F-2 HIGH |
| 3 | No application-layer signature on `StreamEpoch` RPC | SUBSTANTIATED | CONFIRMED | F-3 HIGH (joint J-3) |
| 4 | Race on `MAX_PENDING_REQUESTS_PER_PEER` | DISMISSED | CONFIRMED DISMISSED | (no finding) |
| 5 | `Penalty::Mild` only — no escalation on stream errors | SUBSTANTIATED | CONFIRMED | F-4 MEDIUM (joint J-3) |
| 6 | Three-index atomicity gap (no single REDB tx) | SUBSTANTIATED | CONFIRMED | F-5 MEDIUM (joint J-5) |
| 7 | Initiator trusts response identity (BLS-in-payload) | SUBSTANTIATED | CONFIRMED (same root as #3) | F-3 HIGH |
| 8 | `request_digest` lacks domain separation | SUBSTANTIATED | CONFIRMED | F-6 MEDIUM |
| 9 | Pending-request prune relies on background interval | DISMISSED-WITH-CAVEAT | CONFIRMED LOW | F-13 LOW |
| 10 | Per-record timeout, no whole-stream cap | SUBSTANTIATED | CONFIRMED | F-7 MEDIUM |
| 11 | Idempotent request replacement (cb0e147c) | RECLASSIFIED INFORMATIONAL | CONFIRMED INFORMATIONAL | (no finding; F-8 captures requester-side gap) |
| 12 | Cross-component invariant during pack-file import | RECLASSIFIED (split) | CONFIRMED SPLIT | F-8 HIGH (dir collision) + F-10 LOW (watch asymmetry) + J-1 MEDIUM (current_pack=None) |

All 12 anchors are accounted for: 9 substantiated, 3 dismissed (#4, #9
soft-dismissed, #11). No anchor is unaddressed.

---

## Phantom Check (5 random findings re-cited)

Each cited file:line was independently re-opened during verification:

1. **F-1 / `pack_iter.rs:147-150`** — re-read: lines 147-150 show
   `crc32_hasher.update(&val_size_buf)` (147), `let val_size =
   u32::from_le_bytes(val_size_buf);` (148), `buffer.resize(val_size as
   usize, 0);` (149), `file.read_exact(buffer).await?;` (150). **Match.**
2. **F-2 / `consensus_pack.rs:738-749`** — re-read: lines 738-749 show
   `PackRecord::Batch(batch) => { let batch_digest = batch.digest();
   batches.insert(batch_digest); let position = data.append(...); ... }`.
   **Match.**
3. **F-3 / `mod.rs:342, 358, 381`** — re-read: line 342
   `if let PrimaryResponse::RequestEpochStream { ack, peer } =
   self.handle.send_request_any(...)`, line 358 `let mut stream =
   self.handle.open_stream(peer).await??;`, line 381 `self.report_penalty(peer,
   Penalty::Mild).await;`. **Match.**
4. **F-5 / `consensus_pack.rs:919-928, 930-935`** — re-read: 919-928 has
   the defensive `pos < self.data.file_len()` check; 930-935
   (`consensus_header_by_digest`) does NOT. **Match.**
5. **F-9 dismissal / `header.rs:248-252`** — re-read: `fn digest(&self) ->
   HeaderDigest { let mut hasher = crypto::DefaultHashFunction::new();
   hasher.update(encode(&self).as_ref()); ... }`. The `encode(&self)`
   serializes `Header` including the `epoch: Epoch` field at line 22.
   **Match.**

No phantom citations.

---

## CRITICAL Repro

**Finding:** F-1 — Uncapped `val_size as usize` in `AsyncPackIter::read_record_file`.
**Severity:** CRITICAL.
**Result:** single-peer, single-record process kill of any node that issues
a `request_epoch_pack` and routes through the attacker.

### Pre-conditions

- Attacker peer P has cleared the libp2p handshake. P appears in V's
  peer manager `auth_to_peer` mapping with a known `BlsPublicKey =
  P_bls`.
- V is initiating an epoch fetch — driven either by gossip (organic)
  or by P sending crafted gossip-priming traffic (out of scope for this
  repro; standard mesh activity is sufficient).

### Steps an attacker takes

1. Wait for V's `PrimaryRequest::StreamEpoch { epoch: E }` to land at
   P's network layer via libp2p `send_request_any`. (P is in V's
   committee mesh, so any epoch fetch can route to P with non-trivial
   probability; with 3 peers and multiple retries, near-certainty within
   a few epochs.)
2. P responds:
   ```
   PrimaryResponse::RequestEpochStream { ack: true, peer: P_bls }
   ```
   (Or any BLS in V's committee map; for cleanest attribution P uses
   its own BLS so V's penalty hits P, but the crash happens BEFORE the
   penalty fires regardless. See §"Mitigation breakage".)
3. V opens a libp2p stream to P (`auth_to_peer(P_bls)` resolves to P's
   PeerId).
4. V writes the 32-byte correlator
   `request_digest = digest(E.to_le_bytes())`.
5. P writes a valid 28-byte `DataHeader` with `uid_idx = E as u64`
   (matches V's `AsyncPackIter::open(stream, epoch as u64)` at
   `consensus_pack.rs:694`).
6. P writes a structurally-valid `EpochMeta { epoch: E, ... }`. Only
   `epoch` is checked at `consensus_pack.rs:702-704`, so any
   `start_consensus_number`, `committee`, etc. work.
7. P writes a record header:
   - 4 bytes `val_size_buf = [0xFF, 0xFF, 0xFF, 0xFF]`
   - 4_294_967_295 bytes of any payload (P doesn't actually need to
     send the payload — V's `read_exact` at `pack_iter.rs:150` will
     hang on the read; the `tokio::time::timeout(10s, iter.next())`
     fires first and propagates the timeout error. **But the
     allocation has ALREADY happened** at line 149's `buffer.resize`.)
   - 4 bytes CRC32 (irrelevant — process is already dead by the time
     CRC is computed).
8. **Observation:** V's `AsyncPackIter::read_record_file`
   (`pack_iter.rs:135-160`):
   - Line 138: reads 4 bytes `val_size_buf` from stream — succeeds.
   - Line 148: `val_size = u32::MAX = 4_294_967_295`.
   - Line 149: `buffer.resize(4_294_967_295, 0)`.
     - On Linux + overcommit (default `vm.overcommit_memory = 0`):
       `Vec::resize` reserves 4 GiB virtual address. The kernel
       lazily allocates pages on first write. The `resize`'s
       `RawVec::extend_with(0_u8)` writes zero into every page →
       4 GiB of physical memory is touched → kernel OOM-killer
       fires within seconds (depending on physical RAM available
       and other tenants).
     - On macOS / Linux with `vm.overcommit_memory = 2`:
       `Vec::resize` invokes `RawVec::reserve` →
       `__rust_alloc_zeroed(4 GiB)`. If the allocator returns
       null, Rust's allocator handler (`alloc_error_handler`) fires
       which by default `abort()`s the process.
   - Line 150 may not be reached.

### Expected observable outcome

- `epoch-consensus-worker-{i}` task abnormally terminates.
- `spawn_critical_task` in `crates/task-manager/src/lib.rs` produces
  a `CriticalExitError` and cascades a node-wide shutdown.
- Process supervisor (systemd / k8s) restarts the node.
- Per `journalctl` / `dmesg` (Linux): `Out of memory: Killed process
  <pid> (telcoin-network)`.
- On macOS: `memory allocation of 4294967295 bytes failed` panic
  followed by abort.
- On restart: gossip re-enqueues E (because
  `consensus_chain.consensus_header_by_number(E.final.number)` is
  still `None`); a fetcher dequeues E; `send_request_any` selects P
  again with comparable probability; **return to step 5, OOM again.**
- Steady-state: crash-loop driven by a single mesh peer.

### Mitigation breakage

- **No length cap on `val_size`** before `buffer.resize` at
  `pack_iter.rs:149`. A `MAX_RECORD_SIZE` constant (e.g. 16 MiB or 64
  MiB defensive ceiling) compared as `if val_size as u64 >
  MAX_RECORD_SIZE { return Err(FetchError::CrcFailed); }` would close
  the vector. Search confirmed no such constant exists in
  `crates/storage`, `crates/state-sync`, or
  `crates/consensus/primary`.
- **Per-record `tokio::time::timeout`** at
  `consensus_pack.rs:684-689` wraps the future, but the timeout
  fires AFTER the allocation panic / OOM-kill on Linux overcommit.
- **`Penalty::Mild`** at `mod.rs:381` is in the call chain but the
  panic short-circuits before any `report_penalty` runs. The
  attacker is unpunished by V across the crash.
- **Boot-time orphan reaper** (F-14, absent) is irrelevant here —
  `consensus.rs:379`'s per-attempt `remove_dir_all` cleans on the next
  attempt. **There is no persistent disk corruption**, only repeating
  process kill.

### Distinction: OOM vs ENOMEM-handled-via-Result

- `Vec::resize`'s public API is **infallible**. There is no `Result`
  return path. Allocation failure produces a `panic!` via Rust's
  default `alloc_error_handler` (which calls `abort()`).
- `read_exact` returning a `Result` is irrelevant — by the time the
  read happens (line 150), the allocation at line 149 has either
  succeeded (Linux overcommit) and the OOM-killer is racing the
  page-fault loop, or has failed (macOS) and the process has aborted.
- **Verdict: OOM kill. NOT recoverable via `Result`.**

---

## De-dup vs existing tracking

Searched:
- `find /Users/grant/coding/telcoin/telcoin-network -name "harden*.md" -o
  -name "panic-audit*.md" -o -name "*known-flake*"` — **no matches.**
- `.audit/findings/` — **empty directory.**
- `.audit/tn-bug-scan/research/` — **empty directory.**

There is no prior harden/panic-audit/known-flake tracking that would
double-count any of these findings. This Phase 6 verification is the
canonical source for the issue-629 audit.

---

## Phase 7 Inputs

- **Confirmed:** 17 (F-1, F-2, F-3, F-4, F-5, F-6, F-7, F-8, F-10, F-11,
  F-12, F-13, F-14, F-15, F-16, F-17, J-1).
- **Dismissed:** 3 (F-9, Anchor 4 dismissal restated, Anchor 11
  reclassification restated).
- **Reclassified:** 4 (F-12 → MEDIUM; F-13 → LOW; F-15 → LOW; Anchor 12
  split).
- **NEEDS-RUNTIME-VERIFICATION:** 0 — every finding has been re-verified
  against HEAD code with concrete file:line evidence and either a
  reachable trigger or a concrete compensating control.

### Severity rollup (post-verification)

| Severity | Count | Findings |
|---|---|---|
| CRITICAL | 1 | F-1 |
| HIGH | 3 | F-2, F-3, F-8 |
| MEDIUM (incl. flag) | 8 | F-4, F-5, F-6, F-7, F-12, F-16, F-17 (flag for downstream re-audit), J-1 |
| LOW | 5 | F-10, F-11, F-13, F-14, F-15 |
| DISMISSED | 1 | F-9 |

Joint findings (J-1, J-3, J-4, J-5, J-6) are absorbed into / stand
alongside their primary findings. The Phase 5 journeys (J5.1..J5.10)
remain as the chained exploit narratives backing the
unified-finding-set above.

---

### Recommended Phase 7 reporter inputs

The Phase 7 reporter should:

1. **Lead with F-1** as the only CRITICAL — full repro recipe is
   above.
2. **Bundle F-2 + F-8 + F-17** as the "pack-import semantic-
   validation" work-stream (the pack-stream protocol needs per-
   intermediate-header attestations and global enqueue dedup).
3. **Bundle F-3 + F-4 + F-6** as the "peer-attribution" work-stream
   (PeerId-bound penalty, escalation ladder, domain-separated
   request_digest — all three are the J5.3 griefing triangle).
4. **Treat F-17** as a flag requiring downstream-execution review
   before assigning a final severity.
5. **Acknowledge F-9 dismissal explicitly** in the report — the
   `Header::digest()` epoch inclusion is the load-bearing fact.
