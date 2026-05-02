# Nemesis Scan — Verified Report (issue-629)

## Executive summary

`issue-629` introduces a streaming epoch-pack-file historical state-sync protocol over a libp2p stream channel. A node fetches an epoch certificate via gossip, verifies it via committee BLS quorum, then issues a `StreamEpoch` request to a peer. The peer streams a sequence of framed `(record_kind, val_size, crc32, bytes)` records — alternating worker batches and consensus headers — that are written into the local consensus DB pack files inline as bytes are received. Structural validation (parent-hash chain, batch-set bookkeeping, CRC32) is performed; per-intermediate-header BLS signatures are NOT verified — only the final header's `(number, hash)` is cross-checked against the trusted `EpochRecord`.

The threat actor is a malicious peer in the libp2p mesh that has cleared the network handshake. Because every byte of the pack stream is written to durable consensus storage before any cryptographic check on the contents, and because penalty attribution trusts a BLS field carried in the response payload (not the on-wire libp2p PeerId), this surface contains the chain's largest new attack class. The CRITICAL is unbounded length-prefixed allocation; HIGH findings are forged-intermediate-header admission, penalty mis-attribution, and concurrent-fetch directory collision.

- Counts: **CRIT 1, HIGH 3, MED 8, LOW 5, DISMISSED 1.**
- Top 3 most consensus-critical findings:
  1. **F-1** — uncapped `val_size` in `pack_iter::read_record_file` causes single-peer OOM-kill of any node fetching from the attacker.
  2. **F-2** — write-before-validate ordering plus final-header-only check admits forged intermediate consensus headers into the local pack DB.
  3. **F-3** — penalty applied to attacker-claimed `BlsPublicKey` instead of the on-wire libp2p PeerId enables systematic peer eviction at zero attacker cost.

## Recommended fix order (highest journey-coverage first)

1. **Cap `val_size` in `pack_iter::read_record_file` (and 3 sibling sites)** — closes J5.1 and J5.8 (process kill + crash loop). Closes F-1.
2. **Bind `report_penalty` to the actual libp2p PeerId of the responder, not the BLS claim in the response payload** — closes J5.3 and J5.10; removes amplifier from J5.2, J5.5, J5.6, J5.9. Closes F-3.
3. **Single shared `Arc<Semaphore>` across all `spawn_fetch_consensus` workers AND a dedup window in `request_epoch_pack_file`** — closes J5.2; removes amplifier from J5.1, J5.6, J5.8. Closes F-8.
4. **Add a whole-stream timeout cap and a record-count cap** — closes J5.6.
5. **Validate `epoch_meta.start_consensus_number` and `epoch_meta.committee` against the trusted `previous_epoch`** — closes J5.5 and de-fangs F-17.
6. **Re-validate intermediate consensus headers (per-header attestations or quorum hints) at stream-import time** — closes the silent-data-poison core of J5.4 (F-2). Highest-impact, highest-cost fix.
7. **Add `pos < data.file_len()` defensive check (or digest re-comparison) in `consensus_header_by_digest` and `batch`** — closes F-5 cross-API consistency.
8. **Add domain separation to `request_digest` (commit `committee_id || requester_bls || epoch || version`)** — closes F-6 and aligns with worker's content-addressed digest.

## CRITICAL Findings

### F-1 — Unbounded `val_size as usize` resize in pack iterator

- **File:line:** `crates/storage/src/archive/pack_iter.rs:147-150` (also `pack_iter.rs:74-77`, `pack.rs:319-321`, `pack.rs:343-345`)
- **Severity:** CRITICAL
- **Threat model:** Any libp2p mesh peer that has cleared the handshake. Attack triggers on any `request_epoch_pack` whose `send_request_any` routes to the attacker — driven organically by gossip on every catching-up node.
- **Code excerpt:**
  ```rust
  // pack_iter.rs:147-150 (async hot peer-fed path)
  crc32_hasher.update(&val_size_buf);
  let val_size = u32::from_le_bytes(val_size_buf);
  buffer.resize(val_size as usize, 0);
  file.read_exact(buffer).await?;
  ```
- **Trigger sequence:**
  1. Attacker peer P clears libp2p handshake; appears in V's `peer_manager.auth_to_peer` map.
  2. V issues `request_epoch_pack(E)`. `send_request_any` selects P.
  3. P answers `RequestEpochStream { ack: true, peer: P_bls }`.
  4. V calls `open_stream(P_bls)` → libp2p stream to P. V writes 32-byte correlator.
  5. P writes valid 28-byte `DataHeader` with `uid_idx = E as u64`, then a structurally valid `EpochMeta`.
  6. P writes a record header: 4 bytes `[0xFF, 0xFF, 0xFF, 0xFF]` (`val_size = u32::MAX`), then any payload, then 4 bytes CRC32.
  7. V's `read_record_file` calls `buffer.resize(4_294_967_295, 0)`. On Linux + overcommit the kernel OOM-kills mid-page-fault; on macOS / no-overcommit `Vec::resize` aborts immediately via `alloc_error_handler`.
- **Consequence:** Process termination of any victim node. `epoch-consensus-worker-{i}` is `spawn_critical_task`, so the panic produces `CriticalExitError` and shuts down the whole node. On restart, gossip re-enqueues E; same peer is reselected with non-trivial probability → crash-loop driven by a single peer. No persistent disk corruption (per-attempt `remove_dir_all` at `consensus.rs:379`).
- **Repro recipe:**
  1. Run a 2-node telcoin-network testnet. Configure node A to be a normal validator and node B to be the attacker.
  2. Patch B's `RequestHandler::send_epoch_over_stream` to write a record with `val_size = 0xFFFF_FFFF` after the `DataHeader` and `EpochMeta`. Concretely, override the writer to emit `[size_le_u32, 0_u8 * payload_len, crc32]` where `payload_len` can be 0 (the receiver dies before the read).
  3. Trigger a state-sync fetch on A (e.g. by stopping A for an epoch then restarting so A is behind).
  4. Expected: A's `epoch-consensus-worker-{i}` task dies with `memory allocation of 4294967295 bytes failed` (macOS) or `OOM killed process <pid>` in dmesg (Linux). Process supervisor restarts A; cycle repeats.
  5. Verification command from a workstation:
     ```
     # Run the e2e harness with a malicious-pack hook (one needs to be added under
     # crates/e2e-tests/tests/it/restarts.rs as a new test case):
     cargo nextest run -p tn-e2e-tests --test e2e --no-capture e2e::restarts::oom_during_pack_import
     ```
     Expect the test to assert that the node restarts at least once and that the next request_epoch_pack call OOMs again, demonstrating the crash-loop without a fix. With a fix in place the test should fail in a way that confirms the cap (the import returns `Err(PackError::ReadError)` instead of crashing).
- **Recommended remediation:**
  Introduce a `MAX_RECORD_SIZE` constant in `crates/storage/src/archive/pack_iter.rs` (a defensive ceiling around 16 MiB or 64 MiB that comfortably exceeds the largest legitimate batch+header). Compare BEFORE the cast and resize:
  ```rust
  const MAX_RECORD_SIZE: u32 = 64 * 1024 * 1024; // 64 MiB
  ...
  let val_size = u32::from_le_bytes(val_size_buf);
  if val_size > MAX_RECORD_SIZE {
      return Err(FetchError::CrcFailed); // or a new RecordTooLarge variant
  }
  buffer.resize(val_size as usize, 0);
  ```
  Apply identically in the 3 sibling sites (`pack_iter.rs:74-77`, `pack.rs:319-321`, `pack.rs:343-345`).
- **Adjacent improvements:**
  - Add a `MAX_RECORDS_PER_PACK` constant; reject `Inner::stream_import` streams that exceed it.
  - Apply `Penalty::Medium` (or a new `Penalty::Strong`) on `RecordTooLarge` instead of relying on the OOM-kill to be the punishment.
  - Sanity-check `EpochMeta::start_consensus_number` and `EpochMeta::committee` at the same time (closes F-12 and de-fangs F-17 via the same code path).

## HIGH Findings

### F-2 — Write-before-validate orphan-batch staging + final-header-only validation

- **File:line:** `crates/storage/src/consensus_pack.rs:737-781` (per-record loop) + `crates/storage/src/consensus.rs:387-401` (final-header check)
- **Severity:** HIGH
- **Threat model:** Any libp2p mesh peer; same trust profile as F-1.
- **Code excerpt:**
  ```rust
  // consensus_pack.rs:737-749 (batch arm — written before header validates)
  PackRecord::Batch(batch) => {
      let batch_digest = batch.digest();
      batches.insert(batch_digest);
      let position = data.append(&PackRecord::Batch(batch))
          .map_err(|e| PackError::Append(e.to_string()))?;
      batch_digests.save(batch_digest, position)?;
      ...
  }
  // consensus.rs:387-401 (final-header-only validation)
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
- **Trigger sequence:**
  1. Attacker peer P answers `StreamEpoch { epoch: E }` for V.
  2. P streams `(batch_real_1, header_real_1, ..., batch_real_K, forged_header_K, ..., header_real_N)` where each forged header has internally-consistent `parent_hash == prev.digest()` and a `payload` referencing the immediately preceding batches.
  3. The very last header's `digest()` and `number` must equal `epoch_record.final_consensus`.
  4. Loop accepts every record. `pack.persist().await` flushes. `consensus.rs:387-401` final check passes. `fs::rename` promotes the staged pack to canonical.
  5. V's local pack DB now contains forged intermediate headers. Other peers fetching epoch E from V via `get_epoch_stream` get the same poisoned bytes verbatim.
- **Consequence:** Forged consensus headers in the local pack DB; propagation to other peers via re-serving; potential reward attribution corruption depending on whether the executor re-validates (see F-17).
- **Recommended remediation:**
  Add per-intermediate-header attestations to the pack-stream protocol. Concretely: every consensus header in the stream must carry a quorum-binding attestation (e.g. a BLS aggregate signature over `header.digest()` from the epoch's committee) or, equivalently, the stream emits a chain of `Certificate<ConsensusHeader>` where each is verified against the local trusted `committee` using `BlsAggregateSignature::verify`. Validate inside `Inner::stream_import` at `consensus_pack.rs:751-754` BEFORE `data.append` runs for any header. The `EpochRecord.committee` is already the trusted source of validators; thread it through `stream_import`.

  Cheaper interim fix: defer-then-verify. Buffer all headers in memory; only after the loop ends and the `final_consensus` check passes, re-walk the chain and verify each header's signature. This avoids per-header attestation changes but requires a memory cap (combine with F-1 fix).
- **Adjacent improvements:**
  - Audit `Inner::get_consensus_output` consumers to determine if `epoch_meta.committee` (F-17) is trusted for execution-affecting decisions; if so, the severity of admitting forged headers escalates.
  - Add a `MAX_HEADERS_PER_PACK` cap to bound the buffering required for the deferred-verify approach.

### F-3 — Penalty mis-attribution via unauthenticated `peer: BlsPublicKey` in response payload

- **File:line:** `crates/consensus/primary/src/network/mod.rs:342, 358, 381`
- **Severity:** HIGH
- **Threat model:** Any libp2p mesh peer that can answer `send_request_any` on `PrimaryRequest::StreamEpoch`.
- **Code excerpt:**
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
  }
  ```
- **Trigger sequence:**
  1. Attacker A intercepts V's `StreamEpoch { epoch: E }` via `send_request_any`.
  2. A responds `RequestEpochStream { ack: true, peer: B_bls }` where B is an honest committee member.
  3. V calls `open_stream(B_bls)`. `peer_manager.auth_to_peer(B_bls)` resolves to B's PeerId. The stream goes to **B**, not A.
  4. B's `process_inbound_stream` reads the digest, finds no entry in B's `pending_epoch_requests` → `UnknownStreamRequest` → B applies `Penalty::Mild` to V.
  5. V's `stream_import` errors → V applies `Penalty::Mild` to B (the BLS in the response payload).
  6. A pays nothing. A repeats the trick at ~5s cadence; with default `Penalty::Mild` weight, B's score crosses the eviction threshold (~100) in ~83 minutes per evicted validator.
- **Consequence:** Systematic peer-eviction attack: A can bleed reputation from any honest committee member at zero cost. Combined with F-4 (no escalation) and F-6 (predictable digest), this is the J5.3 griefing triangle.
- **Recommended remediation:**
  Surface the actual on-wire libp2p PeerId from `send_request_any` and use it as the penalty target. Concretely:
  1. Modify `send_request_any` (in `crates/network-libp2p`) to return `(BlsPublicKey_actual, PrimaryResponse)` where `BlsPublicKey_actual` is the BLS bound to the responder's libp2p PeerId at handshake.
  2. In `request_epoch_pack`, cross-check `peer_actual == peer_claimed` (the BLS in the response payload). On mismatch, apply `Penalty::Medium` (intentional misrepresentation) to `peer_actual` and skip the stream.
  3. On import error (line 381), apply `Penalty::Mild` to `peer_actual`, never to `peer_claimed`.
- **Adjacent improvements:**
  - Apply the same fix at the other `report_penalty` sites in `mod.rs` (626, 736, 763, 809) where the peer source is already trusted, to keep semantics uniform.
  - The worker network has the same shape — apply the same audit to `crates/consensus/worker/src/network/mod.rs`.

### F-8 — Concurrent same-epoch fetches collide on `import-{epoch}` directory

- **File:line:** `crates/state-sync/src/consensus.rs:174` (per-task semaphore), `crates/node/src/manager/node.rs:286-299` (3 fetcher tasks), `crates/storage/src/consensus.rs:377-379` (unconditional cleanup), `crates/consensus/primary/src/consensus_bus.rs:551-553` (no enqueue dedup)
- **Severity:** HIGH
- **Threat model:** Reachable under normal gossip churn (not just adversarial); any sustained gossip flow on the same epoch produces collisions.
- **Code excerpt:**
  ```rust
  // state-sync/consensus.rs:174 — per-invocation Arc
  let next_sem = Arc::new(Semaphore::new(1));
  // node.rs:286-299 — 3 of these
  for i in 0..3 {
      node_task_manager.spawn_critical_task(format!("epoch-consensus-worker-{i}"),
          spawn_fetch_consensus(...));
  }
  // storage/consensus.rs:377-379 — unconditional cleanup
  let path = self.base_path.join(format!("import-{epoch}"));
  let _ = std::fs::remove_dir_all(&path);
  ```
- **Trigger sequence:**
  1. Multiple gossip events for the same epoch boundary fire `tx_last_published_consensus_num_hash` rapidly.
  2. `spawn_track_recent_consensus` calls `request_epoch_pack_file(E)` for each event; the mpsc channel accumulates duplicates with no dedup.
  3. Two of the 3 fetcher workers each dequeue an `EpochRecord(E)` (independent `Arc<Semaphore::new(1)>` per worker).
  4. Worker A starts `stream_import(E)`; line 379 `remove_dir_all(&path)`; opens fresh files; begins streaming.
  5. Worker B enters; line 379 wipes Worker A's in-progress staging directory.
  6. On Linux, A's open FDs persist on unlinked inodes; A continues writing into orphaned bytes.
  7. A's `fs::rename(import-{E}/epoch-{E}, base/epoch-{E})` (line 422) sees `ENOENT` (path was wiped by B) → returns Err. Line 434 runs `remove_dir_all(&path)` — wipes Worker B's in-progress staging.
  8. Both fail. Epoch E is unfetchable until next gossip cycle. Repeat indefinitely under sustained churn.
- **Consequence:** Epoch sync stalls indefinitely; node falls progressively further behind; eventually drops out of CVV.
- **Recommended remediation:**
  Two mitigations needed; either alone is insufficient.
  1. **Move the semaphore out of the function body.** In `crates/node/src/manager/node.rs:286-299`, construct one `Arc<Semaphore::new(1)>` outside the loop and pass it as a parameter to all 3 `spawn_fetch_consensus` invocations. This makes only one fetcher run at a time globally (or use `Semaphore::new(N)` if parallelism across DIFFERENT epochs is desired, with the understanding that no two fetchers will ever target the same epoch).
  2. **Add per-epoch dedup at the requester side.** Replace the fire-and-forget `request_epoch_pack_file` with a method that consults a `Mutex<HashSet<Epoch>>` of in-flight epoch fetches. If the epoch is already in flight, drop the enqueue.
- **Adjacent improvements:**
  - Use `flock`/file-lock on `import-{epoch}/.lock` for cross-process safety (e.g. if a separate utility ever runs).
  - Make the import path include a per-task UUID (`import-{epoch}-{task_id}`) as defense-in-depth; rename to `epoch-{epoch}` on success.

## MEDIUM Findings

### F-4 — `Penalty::Mild` only for stream errors; no escalation

- **File:line:** `crates/consensus/primary/src/error/network.rs:115-128`
- A peer accumulates linear `Penalty::Mild` per stream-related error (`UnknownStreamRequest`, `InvalidRequest`, etc.); `StreamUnavailable` returns `None` (zero cost). No counter, no rate-of-misbehavior, no escalation. Combined with F-3 / F-6 this produces the J5.3 griefing triangle.
- **Recommended remediation:** Add an in-memory penalty-rate tracker keyed by `BlsPublicKey`; promote to `Penalty::Medium` after N events in T seconds, `Penalty::Fatal` after M events. Wire this into the `From<&PrimaryNetworkError> for Option<Penalty>` impl by passing the peer + a rate-limiter handle.

### F-5 — `consensus_header_by_digest` and `batch` lack defensive `pos < data.file_len()` filter

- **File:line:** `crates/storage/src/consensus_pack.rs:919-928` (defensive sibling), `930-935` and `1024-1033` (non-defensive lookups). `trunc_and_heal` at lines 474-527 explicitly leaves stale digest entries (comment at 489-493).
- After a crash recovery, `consensus_digests` can hold entries pointing past the truncated `data` file end, or pointing at re-extended positions that now hold a different header. The defensive sibling `contains_consensus_header` filters these out; the lookup APIs do not. Worst case: silent `None` for headers the caller knows exist; or — if `pos < data.file_len()` post re-extension — a returned header whose digest does not match the requested digest.
- **Recommended remediation:** Add to `consensus_header_by_digest`:
  ```rust
  let pos = self.consensus_digests.load(digest).ok()?;
  if pos >= self.data.file_len() { return None; }
  let rec = self.data.fetch(pos).ok()?;
  let header = rec.into_consensus().ok()?;
  if header.digest() != digest { return None; }
  Some(header)
  ```
  Apply analogously in `batch` (line 1022). Re-comparison handles the re-extension edge case the bound-check alone misses.

### F-6 — `request_digest = digest(epoch.to_le_bytes())` lacks domain separation

- **File:line:** `crates/consensus/primary/src/network/mod.rs:333-336` and `mod.rs:690-692`
- The digest is a pure function of `epoch`, publicly computable by any peer. Combined with F-4, attacker probes V's pending state at `Penalty::Mild` per probe. The worker network's content-addressed `generate_batch_request_id(&batch_digests)` is much stronger by symmetry.
- **Recommended remediation:** Domain-separate the digest:
  ```rust
  hasher.update(b"telcoin/stream-epoch/v1");
  hasher.update(committee_id.as_bytes());
  hasher.update(&epoch.to_le_bytes());
  hasher.update(requester_bls.as_bytes());
  hasher.update(&nonce.to_le_bytes()); // 16-byte random per request
  ```
  The nonce ensures different in-flight requests for the same epoch don't collide. Update both requester and responder sites in lockstep.

### F-7 — No whole-stream timeout cap

- **File:line:** `crates/consensus/primary/src/network/mod.rs:373-378` (caller never wraps `stream_import` in outer timeout) + `crates/storage/src/consensus_pack.rs:680-690` (per-record only)
- Attacker streams 1 record every 9 seconds (under `PACK_RECORD_TIMEOUT_SECS = 10`); per-record timeout never fires; whole-stream import can run for hours. Combined with F-8 and 3 fetcher tasks completes the J5.6 starvation attack.
- **Recommended remediation:** Wrap the `stream_import` call in an outer timeout (e.g. `WHOLE_STREAM_TIMEOUT = 5 * 60` seconds for typical-sized epochs):
  ```rust
  match tokio::time::timeout(WHOLE_STREAM_TIMEOUT,
      consensus_chain.stream_import(stream.compat(), ...)).await {
      Ok(Ok(())) => {}
      Ok(Err(e)) => { ... }
      Err(_) => { return Err(PackError::WholeStreamTimeout); }
  }
  ```
  Also add a `MAX_RECORDS_PER_PACK` constant; reject in `Inner::stream_import`'s while-loop if exceeded.

### F-12 — `epoch_meta.start_consensus_number` not validated

- **File:line:** `crates/storage/src/consensus_pack.rs:697-706, 773-777`
- Attacker sets `start_consensus_number = u64::MAX - 5`; first header's `saturating_sub` clamps to 0 (save succeeds); second header errors via `PositionIndex::save` strict-sequential check. Net: import-fail-DoS, repeatable per malicious peer. No persistent corruption.
- **Recommended remediation:** Add at line 702-704 (after the epoch check):
  ```rust
  if epoch_meta.start_consensus_number != previous_epoch.final_consensus.number + 1 {
      return Err(PackError::InvalidEpochMeta(
          "start_consensus_number doesn't follow previous_epoch.final_consensus".into()
      ));
  }
  ```
  Same site can validate `epoch_meta.committee == previous_epoch.next_committee` — closes F-17 in tandem.

### F-16 — Worker oversized-request silent truncation causes mis-attributed penalty

- **File:line:** `crates/consensus/worker/src/network/mod.rs:289-300, 324-325`
- Responder Q silently truncates a 501-digest request to 500, computes `request_digest` over the truncated set; requester R computes its digest over the full set; the digests differ; R opens stream with full-set digest; Q can't find it → R is penalised for an oversized request that Q silently agreed to truncate.
- **Recommended remediation:** Either reject oversized requests outright with `Penalty::Mild` against the requester (the explicit-rejection variant), or compute `request_digest` over the FULL original set on both sides and have the responder serve only 500 of the requested batches without truncating the digest. Prefer the first (explicit reject) — protocol invariants stay simpler.

### F-17 — `epoch_meta.committee` not validated; flows into `CertifiedBatch::address` (FLAG)

- **File:line:** `crates/storage/src/consensus_pack.rs:697-704` (no validation), `consensus_pack.rs:900-901` (read site)
- Attacker streams `EpochMeta { committee: <attacker_choice> }`; `Inner::get_consensus_output` later reads `pack.epoch_meta.committee.authority(cert.origin()).map(|a| a.execution_address())` for `CertifiedBatch::address`. Whether downstream execution trusts this for reward attribution is the gating uncertainty — `EpochRecord.next_committee` is `Vec<BlsPublicKey>` only (no execution addresses), so there is no trusted source to cross-check against in the local node's verified state.
- **Recommended remediation:** Two fixes needed in tandem.
  1. At `consensus_pack.rs:702`, validate `epoch_meta.committee == previous_epoch.next_committee` (using whatever equality semantics the `Committee` type provides).
  2. Schedule a follow-up audit of `Inner::get_consensus_output` consumers and the executor to determine whether `CertifiedBatch::address` is trusted for reward attribution. If yes, the impact escalates from "node-local correctness" to "rewards corruption affecting multiple nodes after import".

### J-1 — Transient `current_pack = None` window during same-epoch `stream_import`

- **File:line:** `crates/storage/src/consensus.rs:403-422`
- Between `*current_pack = None` (line 411) and `fs::rename(...)` (line 422), concurrent readers see None and may serve stale data from `recent_packs` or fail with ENOENT. The error path at lines 432-435 doesn't reinstate `current_pack` if rename fails — `current_pack` stays None until the next epoch transition; the live pack handle is effectively lost until restart.
- **Recommended remediation:**
  1. Save the prior `Option<ConsensusPack>` before nulling. On error path at lines 432-435, restore it.
  2. Optionally: invalidate `recent_packs` BEFORE `remove_dir_all` to reduce the window where a stale handle can serve unlinked data on Linux.
  3. Add a comment summarising the contract: "current_pack is None only between line 411 and rename; readers must fall through to recent_packs / open_static; rename failure paths must restore the prior pack".

## LOW Findings

- **F-10** — `crates/state-sync/src/consensus.rs:65`. `tx_last_consensus_header` watch advances after `ConsensusHeaderCache.insert` but pack file may not yet contain header. Recommendation: document the asymmetry in `last_consensus_header` Rustdoc; consumers already tolerate stale-pack lookups returning None.

- **F-11** — `crates/consensus/primary/src/network/mod.rs:686-688, 722`. `process_epoch_stream` rejection silently returns `ack=false` with no peer cost; cheap responder CPU-DoS. Recommendation: apply `Penalty::Mild` on rejection-due-to-per-peer-limit AND on rejection-due-to-global-cap.

- **F-13** — `crates/consensus/primary/src/network/mod.rs:482-496`. `cleanup_stale_pending_requests` has no panic guard; bounded by `spawn_critical_task` panic propagation → process restart. Recommendation: add `catch_unwind` defensively in case future edits introduce a panic vector. No reachable panic in current code.

- **F-14** — `crates/storage/src/consensus.rs:294-303`. No boot-time reaper for `import-*/`. Recommendation: at `ConsensusChain::new`, glob `base_path.join("import-*")` and `remove_dir_all` each. Cheap; closes a small disk-leak vector under crash-during-import scenarios.

- **F-15** — `crates/consensus/primary/src/consensus_bus.rs:304-307`. `reset_for_epoch` doesn't clear `RequestHandler` maps; moot under per-epoch handler rebuild. Recommendation: leave as-is (the implicit reset is correct); add a comment to `reset_for_epoch` noting that handler-owned maps are reset by handler re-construction.

## DISMISSED — but worth knowing

These findings were initially flagged but are not exploitable; future audits should NOT re-raise them without new code that breaks the load-bearing assumption.

- **F-9 (cross-epoch vote replay via `auth_last_vote`)** — DISMISSED. Two independent reasons: (1) `Header::digest()` includes `epoch` in its bcs-encoded preimage (`crates/types/src/primary/header.rs:14-38, 248-252`), so cross-epoch digest collision requires SHA-256 collision (cryptographically infeasible). (2) `RequestHandler` is rebuilt per epoch via `epoch_task_manager`, so `auth_last_vote` doesn't survive an epoch boundary. **Future-watch flag:** if `Header::digest()` is ever changed to skip `epoch`, this finding revives.

- **Anchor 4 (race on `MAX_PENDING_REQUESTS_PER_PEER`)** — DISMISSED. The count check (`mod.rs:679`) and insert (`mod.rs:703`) execute under the same `pending_map.lock()` critical section; concurrent inbound RPC handlers serialize on this mutex. **Future-watch flag:** if either operation is moved out of the lock (e.g. for performance), the race revives.

- **Anchor 11 (idempotent request replacement from cb0e147c)** — RECLASSIFIED INFORMATIONAL. The `created_at` preservation correctly prevents responder-side slot-hogging via re-request. The OLD permit drops synchronously inside the same mutex critical section as the NEW permit grab; net change is zero per replacement. The substantive concern (no requester-side dedup) is captured by F-8.

- **Anchor 12 watch-channel poisoning during failed import** — RECLASSIFIED. `stream_import` does NOT touch `tx_last_consensus_header`. The watch is advanced only by the gossip-fetch path (state-sync/consensus.rs:65) after `ConsensusHeaderCache.insert`. The original concern split into F-8 (HIGH dir collision), F-10 (LOW watch asymmetry), and J-1 (MEDIUM transient `current_pack=None`). **Future-watch flag:** if `stream_import` ever starts updating `tx_last_consensus_header` mid-stream, this concern revives as a real coupling break.

## Anchor concerns coverage table

| # | Concern | Verdict | Finding ID (if substantiated) |
|---|---|---|---|
| 1 | `val_size` OOM | SUBSTANTIATED | F-1 |
| 2 | Write-before-validate orphan batches in stream_import | SUBSTANTIATED | F-2 |
| 3 | No app-layer signature on `StreamEpoch` RPC | SUBSTANTIATED | F-3 |
| 4 | Race on `MAX_PENDING_REQUESTS_PER_PEER` | DISMISSED | (none) |
| 5 | `Penalty::Mild` only — no escalation | SUBSTANTIATED | F-4 |
| 6 | Three-index atomicity gap | SUBSTANTIATED | F-5 |
| 7 | Initiator trusts response identity | SUBSTANTIATED | F-3 (same root as #3) |
| 8 | `request_digest` lacks domain separation | SUBSTANTIATED | F-6 |
| 9 | Pending-request prune relies on background interval | DISMISSED-WITH-CAVEAT | F-13 (LOW) |
| 10 | No whole-stream timeout cap | SUBSTANTIATED | F-7 |
| 11 | Idempotent request replacement (cb0e147c) | RECLASSIFIED INFORMATIONAL | (none; F-8 captures requester gap) |
| 12 | Cross-component invariant during pack-file import | SPLIT | F-8 (HIGH), F-10 (LOW), J-1 (MEDIUM) |

## Cross-cutting recommendations

1. **Pack-stream protocol re-spec.** The current protocol relies on "structural validity + final-header equality with trusted EpochRecord" as the sole admission contract. This is insufficient. The protocol should be tightened to either (a) carry per-header attestations from the epoch's committee or (b) emit `Certificate<ConsensusHeader>` for every header. F-2 is unfixable without one of these.

2. **Bind ALL penalty sites to libp2p PeerId.** F-3 is the headline penalty mis-attribution, but the same anti-pattern can creep into future code. Make `report_penalty` only accept a `PeerId` (or a wrapper that's only constructable from a PeerId). The BLS field in any response payload should never reach `report_penalty` directly.

3. **Stream-import audit log.** Every successful or failed `stream_import` should emit a structured log line capturing `(epoch, peer_actual, peer_claimed, bytes_received, records_received, outcome, duration)`. This catches J5.3 and J5.4 in production by making mis-attribution and over-long imports visible to operators.

4. **Boot-time reaper and crash-resilience hardening.** F-14 alone is LOW, but combined with future code that may not always run `remove_dir_all` (e.g. resume-partial-import hypothetical) it becomes the foundation for permanent corruption. Cheap to add; do it now.

5. **Domain-separated correlators across the protocol family.** F-6 is one instance; future stream-protocols (worker, snapshot, etc.) should use a shared helper that constructs `request_digest = H(domain_tag || protocol_version || committee_id || requester_bls || ...)`. Add this helper to `tn_types::network`.

6. **End-to-end fuzz testing of the pack-stream input parser.** Given that F-1, F-2, F-12, F-17 all live in the deserialization/validation surface, a `cargo-fuzz` harness over `Inner::stream_import` (mocking the AsyncRead stream from a byte vec) would catch the next class of these issues before audit.

## Adversarial Journey Catalog

The 10 journeys from Phase 5; full step-by-step traces are in `.audit/nemesis-scan/phase5-journeys.md`.

- **J5.1 — Single-peer OOM amplification + restart-state hazard (CRITICAL).** F-1 + F-14 + F-8. One peer kills the victim with one record; restart re-enqueues the same epoch; same peer is re-selected; OOM repeats. Crash-loop driven by a single mesh peer. Closed by the F-1 fix.

- **J5.2 — Concurrent same-epoch fetch collision 3-way race (HIGH).** F-8 + F-3 + F-6 + J-1. Three honest gossip events plus one attacker-paced response create a triple-race on `import-{E}/`; no worker reaches durable state. Permanent epoch sync DoS. Closed by the F-8 dedup fix.

- **J5.3 — Penalty mis-attribution honest-validator eviction (HIGH).** F-3 + F-4 + F-6. Attacker A claims B's BLS in response payload; V opens stream to actual B; B has no pending entry → B penalises V; V penalises B; A is invisible. Repeat → B is evicted. Closed by the F-3 PeerId-binding fix.

- **J5.4 — Forged-intermediate-header pack admission (HIGH).** F-2 + J-4 + F-17. Attacker streams a chain whose intermediate headers are fabricated (internally consistent + correct final header); receiver accepts; forged headers durable in pack DB; re-served to other peers. Closed only by per-header attestation in the protocol (F-2 remediation).

- **J5.5 — Cross-epoch staleness via attacker-set `start_consensus_number` (MEDIUM).** F-12 + F-7 + F-3. Attacker pokes a deterministic wedge on header 2; sync DoS for that epoch from that peer. Closed by F-12 validation fix.

- **J5.6 — Slot exhaustion via slow-stream + small records (HIGH).** F-7 + F-4 + F-11 + F-3. Attacker paces 1 valid record every 9 seconds across 3 attacker BLS positions; pins all 3 fetcher workers indefinitely. Closed by F-7 whole-stream cap.

- **J5.7 — Transient `current_pack=None` reader race (MEDIUM).** J-1 + F-10. Brief window during same-epoch `stream_import` where lookups return None or stale data; flapping RPC visibility. Closed by J-1 restore-on-error fix.

- **J5.8 — Restart-after-OOM compounded with idempotent replacement (HIGH).** F-1 + F-14 + F-13 + F-8. F-1 kills the victim; restart re-fetches; orphan dir cycles cleanly but cycle itself is the attack. Closed by the F-1 fix.

- **J5.9 — Probe-driven peer-score asymmetric drain (MEDIUM).** F-6 + F-4 + F-11. Sustained low-grade reconnaissance + `Penalty::Mild` no-escalation lets attacker churn libp2p score state. Closed by F-4 escalation ladder.

- **J5.10 — Worker batch-stream mis-attribution mirror (MEDIUM).** F-16 + F-3 + F-4. Worker analog of J5.3; oversized request silently truncated by responder; requester penalised. Closed by F-16 explicit-reject fix.

## Test surface gaps

Per the test-surface-gaps section of `.audit/domain-patterns.md`, the existing `crates/e2e-tests/tests/it/{epochs,restarts,common}.rs` files do not cover any of the following adversarial behaviors. Each gap maps to one or more journeys.

- **Malicious peer with malformed pack file.** No e2e test injects a peer that responds to `StreamEpoch` with crafted byte sequences. Add tests for: (a) `val_size = u32::MAX` (F-1 / J5.1 / J5.8), (b) forged-intermediate-header chain (F-2 / J5.4), (c) attacker-set `start_consensus_number` (F-12 / J5.5), (d) attacker-set `committee` (F-17), (e) oversized-request worker truncation (F-16 / J5.10).

- **Crash mid-import with consistency check on recovery.** No test SIGKILLs a node mid-`stream_import` and verifies that on restart, the live consensus DB is uncorrupted and the orphan `import-{E}/` is reclaimed. Adds coverage for F-14 and F-2's process-kill variant.

- **Slow-stream peer cannot starve the importer pool.** No test verifies that a peer pacing 1 record / 9 seconds is bounded by a whole-stream cap; the per-record timeout is the only thing tested today (implicitly). Adds coverage for F-7 / J5.6.

- **Watch channel does not advance during a failing import.** No test asserts `tx_last_consensus_header.borrow()` invariants while a `stream_import` is in error path. Adds coverage for J-1 / J5.7.

- **Per-peer rate limit holds under concurrent inbound requests.** No test fires concurrent `StreamEpoch` from the same peer and asserts the cap holds. Anchor 4 was DISMISSED but the test would catch a regression.

- **Penalty target binding.** No test verifies that `report_penalty` after a failed `request_epoch_pack` targets the libp2p PeerId of the actual responder, not the BLS field in the response. Adds coverage for F-3 / J5.3.

- **Concurrent same-epoch fetches.** No test triggers two `request_epoch_pack_file(E)` enqueues and asserts only one fetcher runs. Adds coverage for F-8 / J5.2.

A focused proptest suite over the pack-stream parser (`Inner::stream_import` driven by an `AsyncRead` over arbitrary bytes) is also missing. Given the deserialization-trust surface, this is the highest-ROI test addition.
