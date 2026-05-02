# Phase 4: Nemesis Feedback Loop — Iteration 1

_Git hash: 9b398938_
_Mode: Targeted, single iteration (most-impactful, of up to 3)._
_Generated: 2026-04-27_

This phase reconciles Phase 2 (Feynman) and Phase 3 (State) verdicts, resolves
the cross-phase open questions with concrete code citations, and surfaces the
Rule-4-gold (partial-state × ordering) findings that are invisible to either
phase alone.

---

## Step A: Reconciled Disagreements

### Anchor 11 — Idempotent request replacement (commit `cb0e147c`) — **RECLASSIFIED → INFORMATIONAL**

- **Phase 2 verdict:** SUBSTANTIATED — *"idempotency works on the responder side, doesn't help the requester-side problem"*. Treated as a pointer to F-8 (requester-side gap), not a bug in the idempotency mechanism itself.
- **Phase 3 verdict:** DISMISSED — *"replacement is on the responder side; no in-flight import to cancel; OLD permit dropped synchronously, no transient invariant break"*.
- **Code evidence (file:line + excerpt):**
  - Diff of cb0e147c (`crates/consensus/primary/src/network/mod.rs:684-710`):
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
    let pending = PendingEpochStream { epoch, created_at, _permit: permit };
    if pending_map.insert((peer, request_digest), pending).is_some() { ... }
    ```
  - Worker analog at `crates/consensus/worker/src/network/mod.rs:323-353` (same shape: `created_at` preservation + replacement).
  - The replacement runs ENTIRELY under `pending_map.lock()` (`mod.rs:676`). The permit lifetime is in the value being replaced; `pending_map.insert(...)` returns `Some(old)` and `old` is dropped synchronously inside the same critical section, releasing the OLD `_permit`. The NEW permit was acquired BEFORE the lock at `mod.rs:674`. Net effect: net change of `_permit` count = 0 per replacement.
- **Final verdict:** **RECLASSIFIED → INFORMATIONAL.** The mechanism itself is correctly placed and correctly counted.
- **Reasoning:** The replacement is on the SERVER (responder) side. There is NO importer running at this point — the entry is awaiting the requester's `open_stream` call. So Phase 3's claim "no in-flight import to cancel" is correct. Phase 2's "idempotency works on the responder side, doesn't help requester-side" is also correct — but the requester-side problem is its own finding (F-8), not a defect of cb0e147c. The **substantive** finding is captured under F-8 (epoch_request_queue lacks dedup); cb0e147c is good defensive code for what it covers (slot-hogging via re-request).
- **Net status:** Anchor 11 is no longer a finding by itself; F-8 absorbs the requester-side gap.

### Anchor 12 — Watch advance during stream import — **RECLASSIFIED**

- **Phase 2 verdict:** SUBSTANTIATED — F-8 (concurrent imports collide on `import-{epoch}`). Phase 2 ALSO listed F-10 separately (watch advance pre-durable via the gossip-fetch path). It mixed the two concerns under "Anchor 12".
- **Phase 3 verdict:** DISMISSED for the watch claim — *"`stream_import` does not touch `last_consensus_header` watch"*. Phase 3 separately substantiated the dir-collision claim under F3+F8.
- **Code evidence (file:line + excerpt):**
  - **Watch writers:** `grep -n "last_consensus_header\|send_replace" crates/state-sync/src/consensus.rs crates/storage/src/consensus.rs crates/storage/src/consensus_pack.rs` returns ONLY `state-sync/consensus.rs:65` (`consensus_bus.last_consensus_header().send_replace(Some(header))`) — the gossip-fetch path. No write inside `stream_import` (`storage/consensus.rs:357-438`) or `Inner::stream_import` (`storage/consensus_pack.rs:673-785`).
  - **Per-record analogue (`Inner::latest_consensus_header`)** at `consensus_pack.rs:963-970`:
    ```rust
    fn latest_consensus_header(&mut self) -> Option<ConsensusHeader> {
        if self.consensus_idx.is_empty() {
            return None;
        }
        let latest_number =
            self.epoch_meta.start_consensus_number + self.consensus_idx.len() as u64 - 1;
        self.consensus_header_by_number(latest_number).ok()
    }
    ```
    This DOES advance per-record (consensus_idx grows mid-stream), but **the Inner being mutated lives in the staging directory only**. There is no live reader path that touches the import-staging Inner — `current_pack` (`storage/consensus.rs:281`) is the live handle and is NOT swapped to point at the import-Inner until after `fs::rename` (`storage/consensus.rs:422`).
- **Final verdict:** **RECLASSIFIED.** Anchor 12 splits cleanly into two distinct findings, both already captured:
  - **F-8 (HIGH)** — `import-{epoch}` directory collision under concurrent `spawn_fetch_consensus` tasks (the dominant Anchor 12 concern).
  - **F-10/F-7 (LOW)** — `tx_last_consensus_header` watch advances on gossip-fetch path before the pack-file is durable; informational asymmetry between `ConsensusHeaderCache` and pack file.
- **Reasoning:** The watch advance hazard does NOT occur inside `stream_import`; it occurs on the SEPARATE gossip-fetch path that populates `ConsensusHeaderCache`. Phase 2 conflated these because both involve the `last_consensus_header` watch in some way; Phase 3 disentangled them correctly. There is also a NEW transient-visibility concern (current_pack briefly = None during stream_import — see Step C joint finding J-1 below) that Phase 2 implied but neither phase finalized.

---

## Step B: Open Questions Resolved

### Q1 — Does `Header::digest()` include `epoch`? — **YES**

- **Question:** gates F-9 (auth_last_vote cross-epoch replay) severity.
- **Evidence:** `crates/types/src/primary/header.rs:14-38, 245-253`:
  ```rust
  pub struct Header {
      pub author: AuthorityIdentifier,
      pub round: Round,
      pub epoch: Epoch,                                       // <-- field
      pub created_at: TimestampSec,
      pub payload: IndexMap<BlockHash, WorkerId>,
      pub parents: BTreeSet<CertificateDigest>,
      pub latest_execution_block: BlockNumHash,
      #[serde(skip)]
      pub digest: OnceCell<HeaderDigest>,                     // skipped from serialization
  }

  impl Hash<{ crypto::DIGEST_LENGTH }> for Header {
      fn digest(&self) -> HeaderDigest {
          let mut hasher = crypto::DefaultHashFunction::new();
          hasher.update(encode(&self).as_ref());                  // <-- encodes the full struct
          HeaderDigest(Digest { digest: hasher.finalize().into() })
      }
  }
  ```
  `encode(&self)` is bcs serialization of the full struct. `epoch: Epoch` is a non-`#[serde(skip)]` field, therefore included in the digest pre-image.
- **Answer:** **YES**, `Header::digest()` includes `epoch`. A SHA-256 collision across epochs is cryptographically infeasible.
- **Severity impact on findings:**
  - **F-9 (vote-replay across epochs):** **DISMISSED.** Two epochs cannot produce the same `Header::digest()` for the same author without breaking SHA-256.
  - **F-10 (`auth_last_vote` cross-epoch replay) per Phase 3:** **DISMISSED** (same reason).

### Q2 — Does `PositionIndex::save` overwrite or error on duplicates? — **ERRORS** (strict-sequential-only)

- **Question:** gates F-12 (`start_consensus_number` poisoning) severity.
- **Evidence:** `crates/storage/src/archive/position_index/index.rs:194-206`:
  ```rust
  impl Index<u64> for PositionIndex {
      fn save(&mut self, key: u64, record_pos: u64) -> Result<(), AppendError> {
          if self.len() != key as usize {
              Err(AppendError::SerializeValue(format!(
                  "{} must add the next item by position, expected {} got {key}",
                  self.pdx_file.path().to_string_lossy(),
                  self.len()
              )))
          } else {
              self.pdx_file.write_all(&record_pos.to_le_bytes())?;
              Ok(())
          }
      }
      ...
  }
  ```
- **Answer:** **`PositionIndex::save` errors on any non-sequential key** — it requires `self.len() == key`. Duplicate-key writes (where the new key < current length) and skip-key writes (where new key > current length) both return `AppendError::SerializeValue`.
- **Severity impact on findings:**
  - **F-12 (start_consensus_number poisoning):** **RECLASSIFIED MEDIUM-LOW (DoS only).** With attacker-supplied `start_consensus_number = u64::MAX`:
    - First header: `consensus_idx_pos = consensus_number.saturating_sub(u64::MAX) = 0` → `consensus_idx.save(0, pos)` succeeds (len was 0).
    - Second header: `consensus_idx_pos = (consensus_number + 1).saturating_sub(u64::MAX) = 0` → `consensus_idx.save(0, pos)` errors because `self.len() = 1 ≠ 0`.
    - Stream import returns `Err(PackError::IndexAppend(...))`. Caller `consensus.rs:432-435` runs `remove_dir_all(&path)`. **No persistent corruption** — the import dir is wiped; the live state is untouched.
    - Net effect: an attacker can force an import to fail by supplying an attacker-chosen `start_consensus_number`. This is a SYNC-DOS vector (the epoch fetch fails repeatably from this peer), but does not corrupt local state.
  - F-12 is **downgraded from CRITICAL to MEDIUM** (DoS, not data corruption). The defensive `PositionIndex::save` strict-sequential check **is** the invariant guard Phase 2 §4 was missing.

### Q3 — Is `epoch_meta.start_consensus_number` validated against `previous_epoch.final_consensus.number + 1`? — **NO**

- **Question:** Phase 2 §4 F-12 flagged it as not validated. Confirm or refute. Exploit?
- **Evidence:** `crates/storage/src/consensus_pack.rs:697-704`:
  ```rust
  let epoch_meta = if let Some(meta) = next(&mut stream_iter, timeout).await? {
      meta.into_epoch()?
  } else {
      return Err(PackError::NotEpoch);
  };
  if epoch != epoch_meta.epoch {
      return Err(PackError::InvalidEpoch);
  }
  data.append(&PackRecord::EpochMeta(epoch_meta.clone()))
      .map_err(|e| PackError::Append(e.to_string()))?;
  ```
  Only `epoch_meta.epoch` is checked. There is NO check that `epoch_meta.start_consensus_number == previous_epoch.final_consensus.number + 1`. There is also no check that `epoch_meta.committee == previous_epoch.next_committee` (Phase 2 Adjacent §5).
- **Answer:** **Not validated.** But the exploit Phase 2 hypothesised is bounded by Q2's finding: `PositionIndex::save`'s strict-sequential check kicks in on the second header and aborts the import.
- **Exploit (post-Q2 reduction):**
  1. Attacker sets `epoch_meta.start_consensus_number = u64::MAX - 5`.
  2. Stream succeeds for headers whose `consensus_number ≥ u64::MAX - 5` ... but the first header's `consensus_number` is some normal value (e.g. ~1e6 if epoch boundaries are sane), so `saturating_sub` produces 0.
  3. First save at key=0 succeeds. Second header attempts key=0 again → `PositionIndex::save` errors → import fails.
  4. **Net: SYNC-DOS, not corruption.**
- **Adjacent exploit (committee field):** Phase 2 Adjacent §5 — `epoch_meta.committee` is not cross-checked against `previous_epoch.next_committee`. This IS a corruption vector if any downstream consumer trusts `epoch_meta.committee` for execution-affecting decisions. **Need to confirm:**
  - `Inner::get_consensus_output` at `consensus_pack.rs:900` does `pack.epoch_meta.committee.clone()` for the committee field of the output. The committee from the stored `epoch_meta` flows into `ConsensusOutput::committee()` for the imported epoch. Whether downstream execution trusts this for reward attribution depends on whether the executor uses the LOCAL `EpochRecord` (trusted) or `ConsensusOutput.committee` (potentially attacker-set).
  - Out of scope for this iteration; flag for future audit.
- **Severity impact:**
  - F-12 confirmed at **MEDIUM** for `start_consensus_number` (DoS, not corruption).
  - Adjacent §5 (committee field) escalates to **MEDIUM-HIGH** if execution layer reads `epoch_meta.committee` — flag for Phase 5/6 deeper review.

### Q4 — `spawn_fetch_consensus` per-task vs shared semaphore — **PER-TASK; 3 INSTANCES**

- **Question:** Phase 2 F-8 / Phase 3 F-3+F-8 root-cause this. Determine actual semaphore lifetime + concurrent invocations.
- **Evidence:**
  - `crates/state-sync/src/consensus.rs:159-219`:
    ```rust
    pub async fn spawn_fetch_consensus(...) -> eyre::Result<()> {
        ...
        // When can we accept more work (a new epoch).
        let next_sem = Arc::new(Semaphore::new(1));    // line 174 — INSIDE the function
        loop {
            tokio::select! {
                Some((_permit, epoch_record)) = next_epoch(&consensus_bus, &next_sem) => {
                    ...
                }
                ...
            }
        }
    }
    ```
  - `crates/node/src/manager/node.rs:286-299`:
    ```rust
    // spawn three critical workers that will fetch epoch pack files from an epoch work queue.
    for i in 0..3 {
        node_task_manager.spawn_critical_task(
            format!("epoch-consensus-worker-{i}"),
            spawn_fetch_consensus(
                self.node_shutdown.subscribe(),
                self.consensus_bus.clone(),
                primary_network_handle.clone(),
                i,
                self.consensus_chain.clone(),
            ),
        );
    }
    ```
- **Answer:** Each `spawn_fetch_consensus` call creates its own `Arc<Semaphore::new(1)>` (consensus.rs:174). The node spawns **3 concurrent invocations** at startup (node.rs:288). The semaphores are independent — `permits = 3` total across the cluster of fetcher tasks, not 1.
- **Severity impact on findings:**
  - **F-3/F-8 (import-{epoch} collision):** CONFIRMED HIGH. Up to 3 fetcher tasks can dequeue the same epoch in parallel if it is enqueued multiple times by gossip. With only mpsc-channel-FIFO ordering and no enqueue dedup, this race is reachable in normal operation under heavy gossip churn — not just adversarial conditions.
  - The mitigation is to either (a) dedup at enqueue OR (b) make `next_sem` a SHARED `Arc` passed to all three workers (effectively a global `Semaphore::new(1)` across all fetchers).

### Q5 — `open_stream(peer)` BLS→PeerId semantics — **RESOLVED VIA PEER MANAGER MAP**

- **Question:** Does the `peer: BlsPublicKey` arg get translated to a PeerId via the peer manager?
- **Evidence:**
  - `crates/network-libp2p/src/types.rs:547-556` (handle entry):
    ```rust
    pub async fn open_stream(&self, peer: BlsPublicKey) -> NetworkResult<NetworkResult<Stream>> {
        let (reply, rx) = oneshot::channel();
        self.sender.send(NetworkCommand::OpenStream { peer, reply }).await?;
        rx.await.map_err(Into::into)
    }
    ```
  - `crates/network-libp2p/src/consensus.rs:714-738` (command handler):
    ```rust
    NetworkCommand::OpenStream { peer, reply } => {
        // Look up the peer's PeerId from their BLS key
        let peer_id = match self.swarm.behaviour().peer_manager.auth_to_peer(peer) {
            Some((id, _addrs)) => id,
            None => {
                debug!(target: "network", ?peer, "OpenStream: peer not found");
                let _ = reply.send(Err(NetworkError::PeerMissing));
                return Ok(());
            }
        };
        ...
        self.swarm.behaviour_mut().stream.open_stream(peer_id, reply);
    }
    ```
- **Answer:** `open_stream(peer: BlsPublicKey)` resolves the BLS to a libp2p PeerId via `peer_manager.auth_to_peer(peer)` — the trusted committee mapping. **The stream actually goes to whichever peer holds the claimed BLS key**, NOT to the libp2p sender of the response.
- **Severity impact on F-9 (penalty mis-attribution):**
  - The attack flow is: attacker A sends `RequestEpochStream { ack: true, peer: B_bls }` where B is honest. Receiver calls `open_stream(B_bls)` → opens a libp2p stream to **the actual peer B** (not A). B's `process_inbound_stream` runs (B is now the responder for this stream). B looks up `(receiver_bls, request_digest)` in B's pending map — never inserted (B never received our `StreamEpoch`). B's handler returns `UnknownStreamRequest` → B applies `Penalty::Mild` against the receiver. The receiver's `stream_import` sees a closed stream → `res.is_err() = true` → applies `Penalty::Mild` against B (line 381).
  - **Net: B gets penalized twice (once by us, once via reciprocal), and A is invisible.** Confirmed F-9 HIGH severity.
  - Mitigation: penalty target should be the libp2p PeerId of the actual responder (the source of the response message), surfaced from `send_request_any`'s response metadata, NOT the BLS field in the response payload.

### Q6 — Idempotent replacement scope (commit cb0e147c) — **RESPONDER-SIDE ONLY**

- **Question:** Does cb0e147c dedupe on the responder, the requester, both, or something else?
- **Evidence (`git show cb0e147c`):** files modified are:
  - `crates/consensus/primary/src/network/mod.rs` — `process_epoch_stream` insert path (responder side)
  - `crates/consensus/worker/src/network/mod.rs` — `process_request_batches_stream` insert path (responder side)
  - `crates/consensus/worker/src/network/error.rs` — penalty escalation for `UnknownStreamRequest` (responder-error variant)
  - test files
- **Answer:** **Responder-side only.** The diff modifies only the SERVER paths (`process_*_stream` insert into `pending_*_requests`). It does NOT touch:
  - `request_epoch_pack` (REQUESTER, mod.rs:322) — no dedup added.
  - `request_epoch_pack_file` enqueue path (consensus_bus.rs:551) — still no dedup.
  - `spawn_fetch_consensus` (state-sync/consensus.rs:174) — still per-task semaphore.
- **Severity impact:** F-8 stands as the requester-side analog gap; cb0e147c does not address it.

### Q7 — Is `PrimaryNetwork` per-epoch or persistent across epochs? — **PER-EPOCH**

- **Question:** gates digest-replay severity for anchor 8/10 (auth_last_vote, consensus_certs, etc.)
- **Evidence:**
  - `crates/node/src/manager/node/epoch.rs:110-114`:
    ```rust
    // The task manager that resets every epoch and manages
    // short-running tasks for the lifetime of the epoch.
    let mut epoch_task_manager = TaskManager::new(EPOCH_TASK_MANAGER);
    ```
  - `crates/node/src/manager/node/epoch.rs:1010-1019`:
    ```rust
    PrimaryNetwork::new(
        rx_event_stream,
        network_handle.clone(),
        consensus_config.clone(),
        consensus_bus.app().clone(),
        state_sync,
        epoch_task_spawner.clone(), // tasks should abort with epoch
        self.consensus_chain.clone(),
    )
    .spawn(&epoch_task_spawner);
    ```
  - The function containing this construction is invoked once per epoch in the outer epoch-manager loop; tasks are spawned on `epoch_task_manager` whose comment explicitly states "resets every epoch".
- **Answer:** **`PrimaryNetwork` is rebuilt per epoch.** The `RequestHandler` is constructed inside `PrimaryNetwork::new` and dies at the epoch boundary along with all its maps (`auth_last_vote`, `consensus_certs`, `requested_parents`).
- **Severity impact on findings:**
  - **F-9 (vote replay across epochs):** **DISMISSED** (already dismissed by Q1 too — handler rebuilt is a second independent reason).
  - **F-11 (`reset_for_epoch` not clearing handler maps):** **DOWNGRADED to LOW (informational).** The handler is rebuilt per epoch, so `reset_for_epoch` not clearing its maps is irrelevant — they don't survive the epoch boundary.
  - `pending_epoch_requests` (in `PrimaryNetwork`) is also reset per epoch — confirms the 30s prune timeout is bounded by epoch lifetime.

### Q8 — `auth_last_vote` clear on epoch transition — **NOT EXPLICITLY CLEARED, BUT REBUILT VIA Q7**

- **Question:** Phase 1 marked VERIFIED-broken. Confirm by tracing.
- **Evidence:**
  - `crates/consensus/primary/src/network/handler.rs:75-97`:
    ```rust
    pub(crate) fn new(...) -> Self {
        let mut auth_last_vote: AuthEquivocationMap = Default::default();
        // Preload with each committee auth id.
        for auth in consensus_config.committee().authorities() {
            auth_last_vote.insert(auth.id(), TokioMutex::new(None));
        }
        Self { ..., auth_last_vote: Arc::new(auth_last_vote), ... }
    }
    ```
  - `RequestHandler` is constructed once per `PrimaryNetwork::new` (mod.rs:438), which is per-epoch (Q7).
  - `consensus_bus.rs:304-307` `reset_for_epoch` does NOT touch `auth_last_vote` — but this is moot because the entire handler is recreated.
- **Answer:** `auth_last_vote` is **not explicitly cleared by `reset_for_epoch`**, but is **implicitly recreated** when `PrimaryNetwork` is rebuilt at the epoch boundary. Phase 1's "VERIFIED-broken" verdict was based on the absence of an explicit clear; in practice, the handler-rebuild semantics make the map epoch-scoped.
- **Severity impact:** The Phase 1 GAP is technically real ("the documented reset path doesn't clear it") but the **operational invariant** (map is per-epoch) is preserved by a different mechanism. Rated LOW informational.

---

## Step C: Joint Findings (Rule 4 gold)

These findings are visible only at the intersection of Feynman ordering AND state-coupling lenses. Each was either implied but not finalized, or required an additional code trace.

### J-1 — Transient `current_pack = None` window during stream_import — **NEW (MEDIUM)**

- **File:line:** `crates/storage/src/consensus.rs:403-422`
- **Code excerpt:**
  ```rust
  let mut current_pack = self.current_pack.lock();
  let replace_current = if let Some(current_pack) = &*current_pack {
      current_pack.epoch() == epoch
  } else {
      false
  };
  if replace_current {
      *current_pack = None;          // <-- live handle nulled
  }
  drop(pack);
  drop(current_pack);                 // <-- mutex released
  // Make sure we don't have any cruft in the final dir.
  if std::fs::exists(&base_dir).unwrap_or_default() {
      let _ = std::fs::remove_dir_all(&base_dir);
  }
  std::fs::rename(&path_base_dir, &base_dir)?;        // <-- new pack in place
  // Invalidate the cache AFTER the rename ...
  self.recent_packs.lock().retain(|p| p.epoch() != epoch);
  ```
- **Joint trigger (Feynman ordering × state coupling):** between line 411 (`*current_pack = None`) and line 422 (`fs::rename` completing), ANY concurrent reader that calls `consensus_chain.consensus_header_*` for `epoch` will:
  1. See `current_pack = None` at line 480-484 (via `current_pack().is_some()` check) → fall through.
  2. Hit `recent_packs` cache — but the cache MAY still hold a stale handle to the OLD `epoch-{N}/` directory which was just `remove_dir_all`'d at line 420. The handle is to an unlinked inode. On Linux, reads still succeed via the open FD; on Windows, sharing-violation errors.
  3. If `recent_packs` cache misses, `get_static` (consensus.rs:683-704) calls `ConsensusPack::open_static(&self.base_path, epoch)` — **which targets a directory that does not yet exist** between lines 420 and 422 → `ENOENT` error.
  - The Feynman ordering concern is "old data unlinked BEFORE new data renamed in" (acknowledged in code comment at lines 416-419).
  - The state-coupling concern is `current_pack` ↔ `recent_packs` ↔ on-disk `epoch-{N}/` triple invariant: while one of the three is being swapped, the others can serve stale or absent data.
- **Consequence:** Transient `None`/`ENOENT` returns from `consensus_header_by_*` lookups during a same-epoch stream_import (i.e. when re-fetching the live epoch we're already in). RPC subscribers / state-sync consumers see flapping availability.
- **Severity:** **MEDIUM** — visible inconsistency, no permanent corruption. Worse if the in-flight rename fails (ENOSPC, IO error): then `epoch-{N}/` is GONE and `current_pack = None` permanently — the live pack file disappears, requiring full re-fetch. The error path at line 432-435 (`Err(...)`) does NOT restore `current_pack` from a saved copy; it just returns the error.
- **Why neither phase alone would have found it:**
  - Phase 2 (Feynman) noted the rename TOCTOU window as a hazard but didn't trace coupled state (`current_pack` ↔ `recent_packs` ↔ disk).
  - Phase 3 (State) noted `current_pack` writes and `recent_packs` writes separately but didn't sequence the mutations against the rename window.
  - **Joint discovery:** the ordered sequence `current_pack=None → drop → remove_dir_all → rename → recent_packs.retain` has FOUR distinct windows where different readers see different inconsistencies, and the error path drops the live handle without restoring it.

### J-2 — `pack_iter` OOM (F-1) cascades into `cleanup_stale_pending_requests` slot leak (F-6) → permanent slot loss — **NEW (HIGH)**

- **File:line:** `crates/storage/src/archive/pack_iter.rs:147-150` × `crates/consensus/primary/src/network/mod.rs:463-496`
- **Code excerpt:**
  ```rust
  // pack_iter.rs:147-150
  let val_size = u32::from_le_bytes(val_size_buf);
  buffer.resize(val_size as usize, 0);            // <-- OOM/panic vector
  file.read_exact(buffer).await?;
  ```
  ```rust
  // mod.rs:463-486 — same critical task hosts both the network event loop AND the prune loop
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
- **Joint trigger (Feynman ordering × state coupling):**
  - **Feynman:** an OOM/panic via F-1 happens in a `request_epoch_pack` invocation — but `request_epoch_pack` is called from `spawn_fetch_consensus` (a *separate* `spawn_critical_task`), NOT from the prune-hosting `primary network events` task. So a process-wide OOM panic propagates out of the fetcher task → critical-task panic → **process shutdown** via the task manager.
  - **State coupling:** during shutdown, the `pending_epoch_requests` map's permits are dropped via `PendingEpochStream::Drop` → released to `epoch_stream_semaphore`. This is NORMAL drop behavior. **No leak.**
  - **HOWEVER**, consider the **partial-shutdown** Feynman scenario: SIGTERM with a join-wait of 200ms (`epoch_task_manager.set_join_wait(200)` at `node/manager/node/epoch.rs:114`). If the prune task is mid-`retain` (holding `pending_map.lock()`) when SIGTERM arrives, `parking_lot::Mutex` does NOT poison, but the task can be aborted mid-iteration. The map's entries are still alive in memory; their permits are still held. The task abort + 200ms join wait + epoch transition produces a NEW `PrimaryNetwork` with a NEW `pending_epoch_requests` map and a NEW `epoch_stream_semaphore` — **so the permits DO get reset per-epoch by virtue of the entire structure being replaced.**
  - **REVISED joint scenario:** within one epoch lifetime, if F-1 OOM-kills the process and the OS does NOT run Drop, the in-memory state is gone; on restart, all maps are fresh. The "permanent slot loss" Phase 3 envisioned for F-6 requires the prune task to die WITHIN an epoch lifetime WITHOUT taking down the process. The `spawn_critical_task` hosting the prune loop will propagate panic → process shutdown → restart with fresh state. The "permanent slot loss" scenario is therefore **only reachable if `spawn_critical_task` does NOT actually propagate panics** — Phase 3 Open Q3.
- **Consequence:** lower severity than Phase 3 estimated. The realistic exploit is "F-1 OOM-kills the node" (already CRITICAL) and the F-6 slot-leak is a sub-effect that doesn't compound.
- **Severity:** **MEDIUM** for the joint case; F-1 alone remains CRITICAL.
- **Why neither phase alone would have found this:**
  - Phase 2 framed F-1 as ephemeral (no persistent corruption); it didn't trace through to F-6's task topology.
  - Phase 3 framed F-6 as "if the outer task dies, permits leak forever"; it didn't recognize that `spawn_critical_task` in this codebase produces a `CriticalExitError` that triggers shutdown (`task_manager.rs:400/411`), which means the leak window is bounded by the time-to-restart.
  - **Joint discovery:** the actual exploit chain is bounded — F-6's "permits leak indefinitely" is gated by the supervisor restarting the entire process, NOT by the prune task surviving without doing its job. This downgrades F-6 from MEDIUM to LOW.

### J-3 — Penalty mis-attribution (F-9) × no penalty-escalation (F-4) × predictable digest (F-6) — **CONFIRMED HIGH**

- **File:line:** `mod.rs:333-336, 342-381` × `error/network.rs:115-128` × `mod.rs:690-692`
- **Joint trigger:**
  - F-9 alone: attacker A frames B by claiming B's BLS in `RequestEpochStream { peer }`. B is penalized once.
  - F-9 × F-4: `Penalty::Mild` doesn't escalate. Attacker repeats the framing for hours; cumulative penalty against B grows linearly. With `Penalty::Mild` weight = `w`, attacker burn rate is `0` (A never gets penalized, since `open_stream` resolves to B via BLS — A is never the on-wire responder for the import stream); B's score drops at `w` per attack iteration. **Asymmetric: cost-zero attack on A, cumulative cost on B.**
  - F-9 × F-4 × F-6: predictable `request_digest = digest(epoch.to_le_bytes())` lets A attack any committee member B for any known epoch without coordination. A doesn't need to be in any specific stream; A just needs to answer ANY node's `StreamEpoch` request with a different B's BLS in the response.
- **Consequence:** under the joint attack, an attacker controlling a single peer position can bleed reputation from any honest committee member at near-zero cost. Combined with libp2p's typical eviction-on-low-score, this is a **PEER EVICTION ATTACK** — A can systematically evict B from the mesh.
- **Severity:** **HIGH** (was MEDIUM-HIGH separately for F-9; the joint chain confirms HIGH).
- **Why neither phase alone would have found this severity:**
  - Phase 2 noted F-3, F-4, F-6 as separate findings of varying severities.
  - Phase 3 noted F-9 standalone.
  - The **joint chain** — A's cost is zero (because BLS routing means A is never on-wire); B's cost accumulates linearly; F-6's predictable digest lets A pick targets — escalates the attack to "systematic peer eviction" which is HIGH on its own.

### J-4 — Concurrent same-epoch fetch collision (F-3/F-8) × import-dir orphan persistence (F-1/F-2) — **CONFIRMED HIGH**

- **File:line:** `state-sync/consensus.rs:174` × `storage/consensus.rs:377-379, 432-435` × `consensus_pack.rs:737-781`
- **Joint trigger:**
  - F-3/F-8: 3 fetcher tasks, no enqueue dedup → two tasks dequeue same epoch.
  - F-1/F-2: write-before-validate ordering means task A has already written N batches into `import-{epoch}/data` and `import-{epoch}/batch_digests` when task B's `remove_dir_all(&path)` (consensus.rs:379) wipes the staging dir.
  - PackInner Drop runs `commit()` (pack.rs:163-164) which fsyncs to the unlinked inode. On Linux, the bytes are flushed to a file with no directory entry — orphan inode leaked until reboot.
  - `fs::rename` racing in BOTH directions can leave either:
    - (a) Task A's rename succeeds → a partially-imported pack is in `epoch-{N}/`; subsequent `epoch_record.final_consensus.{number,hash}` check at line 387-401 catches this BUT only for the FINAL header — intermediate orphans persist.
    - (b) Task A's rename fails → `Err` → `remove_dir_all(&path)` at line 434 wipes task B's in-progress staging.
- **Consequence:** mutual eviction; orphan inodes; epoch sync stalls indefinitely under heavy gossip churn.
- **Severity:** **HIGH** — same as F-3 standalone; the joint analysis confirms the failure modes.
- **Why neither phase alone would have found the orphan-inode + final-header-only check interaction:**
  - Phase 2 noted F-1 (resize OOM) and F-2 (write-before-validate) and F-8 (collision) separately.
  - Phase 3 noted F-3 (the collision) and F-1 (write-before-validate orphan).
  - **Joint discovery:** the `epoch_record.final_consensus.{number,hash}` check at consensus.rs:387-401 only validates the LAST header — intermediate headers in a stream that wins the race can be fabricated by the responder as long as the parent-chain links and the FINAL hash matches. This is the same concern as Defensive Signal 6 in Phase 3, but neither phase finalized it. **Phase 5 must verify whether the execution layer revalidates intermediate headers' signatures.**

### J-5 — `consensus_header_by_digest` no defensive bound (F-2) × `trunc_and_heal` digest divergence (F-5) — **JOINT CONFIRMED HIGH**

- **File:line:** `consensus_pack.rs:474-527` × `consensus_pack.rs:930-934` × `contains_consensus_header` at 918-927
- **Joint trigger:** Phase 3 F2 already captured this: after a crash + `trunc_and_heal`, `consensus_digests` retains entries pointing to positions that may now hold DIFFERENT headers (via re-extension). `consensus_header_by_digest` does NOT filter by `pos < data.file_len()` AND does NOT re-compare the returned header's digest. It can silently return the wrong header.
- **Severity:** HIGH — confirmed by Phase 3.
- **Why this is joint-only:** Phase 2 noted both `trunc_and_heal`'s explicit "leave stale digests" comment AND the absence of the defensive guard at `consensus_header_by_digest`, but treated them as two findings. Phase 3 fused them into F2 by tracing the post-crash + re-extension sequence. **This pair is already finalized; flagging here for completeness.**

### J-6 — `epoch_meta.committee` not validated (Phase 2 §5) × `Inner::get_consensus_output` reads `epoch_meta.committee` (`consensus_pack.rs:900`) — **NEW finding to flag for Phase 5**

- **File:line:** `consensus_pack.rs:697-704` (no committee validation) × `consensus_pack.rs:900` (committee read)
- **Joint trigger:** attacker streams `EpochMeta { committee: <attacker_choice> }`. Stream completes, `fs::rename` succeeds (final-consensus check passes if the chain anchors correctly). Subsequent `Inner::get_consensus_output` reads `pack.epoch_meta.committee` for the imported epoch.
- **Consequence:** depends on whether downstream execution trusts this committee field. If it does (e.g. for reward calculation), an attacker can mis-attribute rewards.
- **Severity:** **MEDIUM-HIGH** — flagged for Phase 5 deep dive; out of scope for this iteration to trace the executor path.
- **Why neither phase alone:** Phase 2 raised the validation gap; Phase 3 didn't trace its consumer; the JOINT pairing is the un-validated write coupled with a trusted read.

---

## Step D: Convergence Status

- **Converged:** YES (for this scope). Step C produced 6 joint findings; 5 are previously-flagged interactions now finalized, 1 is new (J-1, transient `current_pack=None` window). No new ordering concerns or coupled-pair gaps emerged that require an iteration 2.
- **New findings count:** 1 (J-1).
- **Reclassified:** 5
  - F-9/F-10 (vote-replay) — DISMISSED (Q1: Header digest includes epoch; Q7: handler rebuilt per epoch).
  - F-11 (`reset_for_epoch` not clearing handler maps) — DOWNGRADED to LOW (Q7: handler rebuilt per epoch makes this irrelevant).
  - F-12 (`start_consensus_number` poisoning) — DOWNGRADED from CRITICAL to MEDIUM (Q2: `PositionIndex::save` strict-sequential check causes import-fail-DoS, not corruption).
  - F-6 (no panic guard on prune) — DOWNGRADED to LOW (J-2 / Phase 3 Open Q3: critical-task panic propagation triggers process restart, bounding the leak window).
  - Anchor 11 (idempotent replacement) — RECLASSIFIED INFORMATIONAL (Step A; the substantive concern is captured under F-8).
  - Anchor 12 (watch advance) — RECLASSIFIED (Step A; splits into F-8 dir-collision HIGH and F-7/F-10 watch-asymmetry LOW).
- **Withdrawn:** 1
  - F-5 (idempotent replacement transiently inflates permit count) — already self-dismissed in Phase 3; confirmed dismissed.
- **Phase 2 + Phase 3 anchor verdicts now REUNIFIED:** all 12 anchors have a unified verdict (see table at end).
- **Recommendation:** **Proceed to Phase 5** (journey tracing) and **Phase 6** (final report). No iteration 2 needed — the Step C joint analysis surfaced one new finding (J-1) and re-validated five existing ones; no untraced ordering concerns remain.

---

## Unified Finding Set (post-feedback)

| ID | Severity | File:Line | Title | Source(s) | Status |
|---|---|---|---|---|---|
| F-1 | CRITICAL | crates/storage/src/archive/pack_iter.rs:147-150 (also pack_iter.rs:74-77, pack.rs:319-321, 343-345) | Unbounded `val_size as usize` resize — peer kills process with one record | Phase 2 F-1 | Confirmed |
| F-2 | HIGH | crates/storage/src/consensus_pack.rs:737-749 + storage/src/consensus.rs:357-438 | Write-before-validate orphan-batch staging + final-header-only validation lets fabricated intermediate headers be accepted | Phase 2 F-2, Phase 3 F1, Joint J-4 | Confirmed |
| F-3 | HIGH | crates/consensus/primary/src/network/mod.rs:342, 358, 381 | Penalty mis-attribution via unauthenticated `peer: BlsPublicKey` in response payload — enables peer-eviction griefing | Phase 2 F-3, Phase 3 F-9, Joint J-3 | Confirmed |
| F-4 | MEDIUM | crates/consensus/primary/src/error/network.rs:115-128 | `Penalty::Mild` only — no escalation ladder; `StreamUnavailable` returns `None`. Joint with F-3/F-6 produces zero-cost attacker | Phase 2 F-4 | Confirmed |
| F-5 | MEDIUM | crates/storage/src/consensus_pack.rs:474-527, 918-927, 930-934 | `consensus_header_by_digest` lacks `pos < data.file_len()` defense AND digest re-comparison; returns wrong header after `trunc_and_heal` + re-extend | Phase 2 F-5, Phase 3 F2, Joint J-5 | Confirmed |
| F-6 | MEDIUM | crates/consensus/primary/src/network/mod.rs:333-336, 690-692 | `request_digest = digest(epoch.to_le_bytes())` lacks domain separation; predictable + low-cost probing | Phase 2 F-6 | Confirmed |
| F-7 | MEDIUM | crates/consensus/primary/src/network/mod.rs:373-378, consensus_pack.rs:680-690 | Per-record timeout but no whole-stream timeout; attacker holds slot for hours via 9s pacing | Phase 2 F-7 | Confirmed |
| F-8 | HIGH | crates/state-sync/src/consensus.rs:174 + crates/storage/src/consensus.rs:377-379 + crates/consensus/primary/src/consensus_bus.rs:551-553 | Concurrent same-epoch fetches collide on `import-{epoch}` dir; per-task semaphore + no enqueue dedup; 3 fetcher tasks running | Phase 2 F-8, Phase 3 F-3+F-8, Joint J-4 | Confirmed |
| F-9 (vote replay) | DISMISSED | crates/consensus/primary/src/network/handler.rs:368-405 | Cross-epoch cached-vote replay — DISMISSED by Q1 (`Header::digest()` includes epoch) and Q7 (handler rebuilt per epoch) | Phase 2 F-9, Phase 3 F10 | Dismissed |
| F-10 | LOW | crates/state-sync/src/consensus.rs:51-65 vs storage/consensus.rs:506-522 | `tx_last_consensus_header` advances after `ConsensusHeaderCache.insert` but pack file may not yet contain header — informational asymmetry | Phase 2 F-10, Phase 3 F7 | Confirmed |
| F-11 | LOW | crates/consensus/primary/src/network/mod.rs:686-688, 722 | `process_epoch_stream` rejection produces no peer cost — cheap responder CPU-DoS | Phase 2 F-11 | Confirmed |
| F-12 | MEDIUM | crates/storage/src/consensus_pack.rs:697-706, 773-777 | `epoch_meta.start_consensus_number` not validated against `previous_epoch.final_consensus.number + 1`; downgraded to import-fail-DoS by `PositionIndex::save` strict-sequential guard (Q2) | Phase 2 §4, Q2 | Reclassified MEDIUM |
| F-13 (was F-6 prune) | LOW | crates/consensus/primary/src/network/mod.rs:482-496 | `cleanup_stale_pending_requests` no panic guard — but `spawn_critical_task` propagates to process shutdown, bounding leak window. Downgraded by J-2. | Phase 1, Phase 3 F6, Joint J-2 | Reclassified LOW |
| F-14 | LOW | crates/storage/src/consensus.rs:294-303 | No boot-time reaper for orphan `import-*/` directories after process kill mid-import — disk-space leak only | Phase 3 F4 | Confirmed |
| F-15 | LOW | crates/consensus/primary/src/consensus_bus.rs:304-307 | `reset_for_epoch` doesn't clear handler maps — moot because handler rebuilt per epoch (Q7) | Phase 1, Phase 3 F11, Q7 | Reclassified LOW |
| F-16 | MEDIUM | crates/consensus/worker/src/network/mod.rs:289-300 | Worker `process_request_batches_stream` truncates oversized digest sets silently; `request_digest` mismatch causes mis-attribution penalty against the requester | Phase 2 Adjacent §6 | Confirmed |
| F-17 (Adj §5) | MEDIUM-HIGH (flag) | crates/storage/src/consensus_pack.rs:697-704, 900 | `epoch_meta.committee` not validated against `previous_epoch.next_committee`; flows into `Inner::get_consensus_output` | Phase 2 Adjacent §5, Joint J-6 | Flag for Phase 5 |
| J-1 | MEDIUM | crates/storage/src/consensus.rs:403-422 | Transient `current_pack = None` window during same-epoch `stream_import` produces flapping `None`/`ENOENT` from `consensus_header_*` lookups; error path doesn't restore handle | Joint Phase 4 (NEW) | NEW |

---

## Anchor Concerns Final Verdicts (1-12)

1. **Anchor 1 (uncapped `val_size` resize):** **SUBSTANTIATED** — F-1 CRITICAL.
2. **Anchor 2 (write-before-validate ordering):** **SUBSTANTIATED** — F-2 HIGH.
3. **Anchor 3 (no application-layer signature on `StreamEpoch`):** **SUBSTANTIATED** — F-3 HIGH (joint with F-4 and F-6).
4. **Anchor 4 (race on `MAX_PENDING_REQUESTS_PER_PEER`):** **DISMISSED** — same-mutex serialization enforces the cap.
5. **Anchor 5 (`Penalty::Mild` only — no escalation):** **SUBSTANTIATED** — F-4 MEDIUM (HIGH in joint J-3).
6. **Anchor 6 (three-index atomicity gap):** **SUBSTANTIATED** — F-5 MEDIUM.
7. **Anchor 7 (initiator trusts response identity):** **SUBSTANTIATED** — F-3 (same root as Anchor 3).
8. **Anchor 8 (`request_digest` lacks domain separation):** **SUBSTANTIATED** — F-6 MEDIUM.
9. **Anchor 9 (prune relies on background interval):** **DISMISSED-WITH-CAVEAT** — Phase 3 F6 found no reachable panic; J-2 confirms the leak window is bounded by `spawn_critical_task` panic propagation. Downgraded to F-13 LOW.
10. **Anchor 10 (no whole-stream timeout cap):** **SUBSTANTIATED** — F-7 MEDIUM.
11. **Anchor 11 (idempotent request replacement):** **RECLASSIFIED INFORMATIONAL** — the mechanism in cb0e147c is correct on the responder side; the requester-side gap is captured by F-8.
12. **Anchor 12 (cross-component invariant during pack import):** **RECLASSIFIED** — splits into F-8 (HIGH dir-collision) and F-10 (LOW watch-asymmetry); plus NEW J-1 (MEDIUM transient `current_pack=None`).
