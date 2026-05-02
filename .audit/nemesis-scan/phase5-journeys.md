# Phase 5: Adversarial Journeys — issue-629

_Git hash: 9b398938_
_Generated: 2026-04-27_
_Source: chains together the unified findings F-1..F-17 + J-1..J-6 from Phase 4._

This phase composes individual findings into multi-step adversarial sequences.
Each journey weaves 2+ Phase 4 findings; the journey-level severity is set by
the chain's observable end state, not by any single component.

---

## Journey Index

| ID    | Title                                                          | Severity | Findings Composed              | Goal                                           |
|-------|----------------------------------------------------------------|----------|--------------------------------|------------------------------------------------|
| J5.1  | Single-peer OOM amplification + restart-state hazard           | CRITICAL | F-1, F-14, F-8                 | Process kill + persistent disk-state confusion |
| J5.2  | Concurrent same-epoch fetch collision (3-way race)             | HIGH     | F-8, F-3, F-6, J-1             | Permanent epoch-sync DoS                       |
| J5.3  | Penalty mis-attribution honest-validator eviction              | HIGH     | F-3, F-4, F-6                  | Evict honest validator from mesh               |
| J5.4  | Forged-intermediate-header pack admission                      | HIGH     | F-2, J-4, F-17                 | Insert fabricated headers into local pack DB   |
| J5.5  | Cross-epoch staleness via attacker-set start_consensus_number  | MEDIUM   | F-12, F-7, F-3                 | Repeatable per-peer sync DoS for one epoch     |
| J5.6  | Slot exhaustion via slow-stream + small records                | HIGH     | F-7, F-4, F-11, F-3            | Permanent fetch-queue starvation               |
| J5.7  | Transient `current_pack=None` reader race                      | MEDIUM   | J-1, F-10                      | Flapping RPC visibility, watch-channel skew    |
| J5.8  | Restart-after-OOM compounded with idempotent replacement       | HIGH     | F-1, F-14, F-13, F-8           | Cumulative disk waste + post-restart wedge     |
| J5.9  | Probe-driven peer-score asymmetric drain (extra)               | MEDIUM   | F-6, F-4, F-11                 | Bleed the responder's compute budget           |
| J5.10 | Worker batch-stream mis-attribution mirror (extra)             | MEDIUM   | F-16, F-3, F-4                 | Worker analog of J5.3                          |

---

## J5.1 — Single-peer OOM amplification + restart-state hazard

**Goal:** Use one carelessly framed record to kill the victim process repeatedly,
while the per-attempt orphan staging directory accumulates on disk and the
pre-restart in-memory permits/queue state is unobservable to the recovered
process. Steady-state harm: the victim is in a crash-loop driven by one peer
who has already cleared the libp2p handshake.

**Findings composed:** F-1 (uncapped `val_size`), F-14 (no boot-time reaper of
`import-{epoch}/`), F-8 (no enqueue dedup so re-enqueue re-fires immediately).

**Severity (journey-level):** **CRITICAL** — process kill is repeatable from a
single mesh peer; restart does not break the cycle.

### Steps

1. **[network]** Gossip event for new committed consensus advances
   `tx_last_published_consensus_num_hash` —
   `crates/state-sync/src/consensus.rs:102-103`. The watch fires
   `spawn_track_recent_consensus`.
2. **[victim]** `spawn_track_recent_consensus` enqueues `EpochRecord(E)` on
   `epoch_request_queue` (capacity 10_000) —
   `crates/consensus/primary/src/consensus_bus.rs:551-553`. No dedup at this
   call site (F-8).
3. **[victim]** One of three `spawn_fetch_consensus` workers
   (`crates/node/src/manager/node.rs:286-299`) dequeues E. Each worker holds an
   independent `Arc<Semaphore::new(1)>` —
   `crates/state-sync/src/consensus.rs:174`.
4. **[victim]** Worker calls `request_epoch_pack(E)` —
   `crates/consensus/primary/src/network/mod.rs:322-395`. `send_request_any`
   reaches attacker peer P; P answers
   `RequestEpochStream { ack: true, peer: P_bls }` —
   `crates/consensus/primary/src/network/mod.rs:342`.
5. **[victim]** Victim opens stream to P, writes `digest(E.to_le_bytes())`
   correlator (F-6 — public, predictable) —
   `crates/consensus/primary/src/network/mod.rs:333-336, 359`.
6. **[attacker]** P writes a valid 28-byte `DataHeader`, then a valid
   `EpochMeta` (Q3 — only `epoch` is checked at
   `crates/storage/src/consensus_pack.rs:702-704`), then **one record framed
   `(val_size = 0xFFFF_FFFF, payload, valid_crc32)`**.
7. **[victim]** `AsyncPackIter::read_record_file` —
   `crates/storage/src/archive/pack_iter.rs:147-150` — calls
   `buffer.resize(4_294_967_295usize, 0)`. On Linux + overcommit the allocator
   reserves; on the first page-fault during `read_exact` the kernel OOM-kills
   the process. On macOS / no-overcommit the `Vec::resize` aborts immediately.
8. **[victim restart]** Process restarts. `ConsensusChain::new` —
   `crates/storage/src/consensus.rs:294-303` — opens only `current_pack` via
   `open_append_exists`. **It does not iterate `import-*/` directories** (F-14).
   The orphan `import-{E}/` from step 7 survives on disk with partial bytes
   that `PackInner::Drop` may have committed at `crates/storage/src/archive/pack.rs:163-164`.
9. **[network]** Gossip continues; the next gossip update re-enqueues E
   (because the local pack file for E is still incomplete:
   `consensus_chain.consensus_header_by_number(...)` returns `None` at
   `crates/state-sync/src/consensus.rs:114-117`).
10. **[victim]** Worker dequeues E again, re-enters
    `ConsensusChain::stream_import` —
    `crates/storage/src/consensus.rs:357`. Line 379 unconditionally
    `remove_dir_all(&path)` — this DOES wipe the prior orphan, so
    state pre-step-7 is mostly cleaned. **But** `request_epoch_pack` again
    routes to P (or any peer P co-locates with), which answers identically
    → **return to step 6, OOM again**.
11. **[steady-state]** The crash-loop iterates indefinitely. `pending_epoch_requests`
    on the responder side (P's view of any victim that requested) is naturally
    cleared on restart since it's in-memory only —
    `crates/consensus/primary/src/network/mod.rs:417`. The semaphore (cap 5,
    `crates/consensus/primary/src/network/mod.rs:412`) is also recreated.
    No state leaks across restarts EXCEPT the orphan `import-{E}/` directory,
    which is bounded by the next attempt's `remove_dir_all`.

### Final state

- Victim process is in a crash-loop driven by a single mesh peer.
- `import-{E}/` orphan dir survives only between steps 7 and 10; bounded.
- Disk-space leak: `O(partial_pack_size)` per crash, bounded by next
  `remove_dir_all`.
- **Liveness:** if the victim is in CVV, repeated crashes blow through any
  liveness budget. Out-of-CVV nodes simply cannot sync.

### Detection signals

- `error!` from `task_manager` `CriticalExitError` for the
  `epoch-consensus-worker-{i}` task.
- Process supervisor (systemd / k8s) shows `RestartCount` climbing.
- `Vec::resize` panic message: `memory allocation of 4294967295 bytes failed`
  on macOS or `OOM killed process` in dmesg on Linux.
- Disk metric: `import-{E}/` directories accumulate transiently between
  crash and next-attempt cleanup.
- `request_epoch_pack` log line at `crates/consensus/primary/src/network/mod.rs:366-371`
  fires immediately before crash → tight crash-time correlation.

### Mitigation breakage

- **F-1 cap (absent):** there is no `MAX_RECORD_SIZE` constant compared
  against `val_size` before `buffer.resize`.
- **F-14 reaper (absent):** `ConsensusChain::new` has no `walk_dir` over
  `import-*/` to clean stale staging.
- **`Penalty::Mild` on stream fail (F-4, F-3):** the panic happens BEFORE
  `report_penalty` runs (panic mid-`stream_import`); the attacker is never
  penalised for the OOM record. Even if the penalty fired, `Penalty::Mild`
  doesn't escalate.

---

## J5.2 — Concurrent same-epoch fetch collision (3-way race)

**Goal:** Three honest gossip events plus one attacker-paced response create
a triple-race on `import-{E}/` such that all three workers fail and E never
imports. This is the F-3/F-8 scenario fully traced.

**Findings composed:** F-8 (per-task semaphore + no enqueue dedup),
F-3 (predictable digest probe), F-6 (predictable digest), J-1 (transient
`current_pack=None`).

**Severity (journey-level):** **HIGH** — under sustained gossip churn, sync
stalls indefinitely.

### Steps

1. **[network]** Three different honest committee members publish quorum
   for the same epoch boundary header within a short window. Each fires
   `tx_last_published_consensus_num_hash` —
   `crates/consensus/primary/src/network/handler.rs:281-282`.
2. **[victim]** `spawn_track_recent_consensus` wakes three times. Each wake
   walks `epochs.record_by_epoch(current_fetch_epoch)` —
   `crates/state-sync/src/consensus.rs:111-117` — and calls
   `consensus_bus.request_epoch_pack_file(epoch_record)` for any unfetched
   `epoch_record`. The `is_some()` check at line 114 only sees a complete
   pack; an in-progress import is invisible. **Three duplicate
   `EpochRecord(E)` enter the mpsc.**
3. **[victim]** All three `spawn_fetch_consensus` workers
   (`crates/node/src/manager/node.rs:286-299`) — each with its own
   `Arc<Semaphore::new(1)>` — race to dequeue. Three workers end up holding
   `EpochRecord(E)` simultaneously.
4. **[victim Worker 1]** Worker 1 calls
   `consensus_chain.stream_import(stream_1, E, ...)` —
   `crates/storage/src/consensus.rs:357`. Line 365's `get_static(E)` returns
   either `NoEpoch` or a stale partial pack; line 379 runs
   `remove_dir_all(&path)` — clean start. Worker 1 begins streaming records
   into `import-{E}/data`, `batch_digests`, `consensus_idx`,
   `consensus_digests`.
5. **[victim Worker 2]** A few hundred millis later Worker 2 enters the same
   function; line 379 runs `remove_dir_all(&path)` — **wipes Worker 1's
   in-progress staging dir**.
6. **[Linux semantics]** Worker 1's open FDs on `data`, the three indexes,
   point to unlinked inodes. `data.append(...)` writes still succeed but go
   to the orphan inodes (`crates/storage/src/archive/pack.rs:78-80` →
   `append_inner`).
7. **[victim Worker 2]** Worker 2 races into `ConsensusPack::stream_import`
   (`crates/storage/src/consensus_pack.rs:673-785`). Calls
   `create_dir_all(&base_dir)` (line 692) — creates a new
   `import-{E}/epoch-{E}/` directory, opens fresh files in it. Worker 2
   begins streaming.
8. **[victim Worker 3]** Worker 3 enters and runs
   `remove_dir_all(&path)` — **wipes Worker 2's freshly-created files**.
   Worker 2's FDs are now also pointing at orphan inodes.
9. **[steady-state collision]** Each subsequent restart of any worker via
   step 4's clean-slate behaviour wipes whoever was newest. With three
   workers cycling, none of them ever reach `pack.persist()` →
   `pack.latest_consensus_header()` → `fs::rename(&path_base_dir, &base_dir)`
   in a state where the source directory still exists.
10. **[victim Worker N]** Eventually one worker's `fs::rename` (consensus.rs:422)
    fires with `path_base_dir = path.join("epoch-{E}")` not present →
    `ENOENT` → `Err` → `remove_dir_all(&path)` at line 434 — **wipes whoever
    is currently mid-import**.
11. **[network]** All three workers eventually return `Err`; they retry
    after `PACK_DOWNLOAD_RETRY_SECS` —
    `crates/state-sync/src/consensus.rs:198`. Meanwhile `request_digest =
    digest(E.to_le_bytes())` (F-6) is identical across all three workers,
    so attacker P (probing per F-6) can deterministically time a stream-stall
    against any worker — making the collision more frequent.

### Final state

- Epoch E is never imported. `consensus_chain.consensus_header_by_number(E.final_consensus.number)`
  returns `None` indefinitely.
- `import-{E}/` cycles between empty/partial states; no permanent disk
  state.
- All three fetcher workers loop forever between
  `request_epoch_pack` and `tokio::time::sleep(PACK_DOWNLOAD_RETRY_SECS)`
  — bounded CPU but no progress.
- Node falls progressively further out of sync.

### Detection signals

- Repeating `error!` log: `"failed to request epoch pack for epoch {E}: ..."`
  from `crates/state-sync/src/consensus.rs:189-190`.
- `tracing` ENOENT errors during `fs::rename` at `consensus.rs:422`.
- Three near-simultaneous `info!` lines `"epoch consensus fetcher {i}
  retrieving epoch {E}"` at `crates/state-sync/src/consensus.rs:182` for the
  same E.
- No `info!` `"streamed epoch pack file"` line at
  `crates/consensus/primary/src/network/mod.rs:387-388` ever fires for E.

### Mitigation breakage

- **No enqueue dedup (F-8):** `request_epoch_pack_file` is fire-and-forget
  at `crates/consensus/primary/src/consensus_bus.rs:551-553`.
- **Per-task `Arc::new(Semaphore::new(1))` (F-8):** workers don't coordinate
  globally — `crates/state-sync/src/consensus.rs:174`.
- **J-1 transient `current_pack=None`:** even if one worker wins, between
  lines 410 and 422 of `consensus.rs` other workers' `get_static(E)` can
  see `None`, race their own `stream_import`, and contend.
- **F-6 predictable digest:** the attacker can pre-time a stall to maximise
  collision frequency.

---

## J5.3 — Penalty mis-attribution honest-validator eviction

**Goal:** Attacker A bleeds reputation from honest validator B at zero cost
to A, until B's libp2p score crosses the eviction threshold and B is removed
from the mesh.

**Findings composed:** F-3 (unauthenticated `peer` field), F-4 (Mild only,
no escalation), F-6 (predictable digest enables coordinated probing).

**Severity (journey-level):** **HIGH** — systematic peer eviction without
ever being on-wire as the responder.

### Steps

1. **[attacker A]** A is a libp2p mesh peer. Listens for any node V's
   `StreamEpoch` requests (request type at
   `crates/consensus/primary/src/network/message.rs:132-136`).
2. **[victim V]** V publishes `StreamEpoch { epoch: E }` via
   `send_request_any` —
   `crates/consensus/primary/src/network/mod.rs:343`. The libp2p layer
   selects A (or A is among candidates).
3. **[attacker A]** A responds
   `RequestEpochStream { ack: true, peer: B_bls }` where B is an honest
   committee member. Wire format:
   `crates/consensus/primary/src/network/message.rs:247-258` — `peer` is
   not authenticated (Q5).
4. **[victim V]** V calls `self.handle.open_stream(B_bls)` —
   `crates/consensus/primary/src/network/mod.rs:358`. The network handle
   resolves B_bls → B's actual PeerId via
   `peer_manager.auth_to_peer(peer)` —
   `crates/network-libp2p/src/consensus.rs:716` (Q5). The libp2p stream
   opens to actual peer B.
5. **[victim V]** V writes `request_digest = digest(E.to_le_bytes())`
   (F-6) to the stream — `mod.rs:359`.
6. **[honest B]** B's `process_inbound_stream` —
   `crates/consensus/primary/src/network/mod.rs:772-813` — reads the
   32-byte digest. At line 798, B looks up
   `(B.bls_public_key, request_digest)` in B's
   `pending_epoch_requests`. **B never received V's `StreamEpoch`
   request** (V sent it to A), so B has no entry → `opt_pending_req =
   None`.
7. **[honest B]** B's `process_request_epoch_stream` —
   `crates/consensus/primary/src/network/handler.rs:941-990` — sees `None`,
   returns `PrimaryNetworkError::UnknownStreamRequest(request_digest)`.
   `(&err).into()` produces `Some(Penalty::Mild)` —
   `crates/consensus/primary/src/error/network.rs:117`. B applies
   `Penalty::Mild` against **V** (the libp2p peer who opened the stream) —
   `crates/consensus/primary/src/network/mod.rs:809`.
8. **[honest B]** B closes the stream.
9. **[victim V]** V's `consensus_chain.stream_import` —
   `crates/storage/src/consensus.rs:357-438` — sees a closed stream,
   `AsyncPackIter::read_record_file` returns EOF or `read` error. The
   import fails, returning `Err(...)`. Back in `request_epoch_pack` at
   line 379 of `mod.rs`, `res.is_err() = true` → V applies
   `Penalty::Mild` against **B** (the BLS in the response payload).
10. **[score arithmetic]** Each iteration of steps 1-9:
    - A pays 0 (A is invisible: never the on-wire responder for the
      stream itself).
    - B receives 1× `Penalty::Mild` from V's perspective.
    - V receives 1× `Penalty::Mild` from B's perspective.
    Net per-iteration: A burns reputation from BOTH V and B.
11. **[escalation]** F-4: `Penalty::Mild` is the entire ladder
    (`crates/consensus/primary/src/error/network.rs:115-118`). There is no
    `Penalty::Strong` triggered after N mild infractions. With libp2p's
    typical score weights (penalty units approximate), N iterations drop
    a peer's score by `N × mild_weight`. A typical `mild_weight = -0.1`
    and a default ban threshold around `-100` puts the eviction at
    N ≈ 1000 events. Assuming 1 request every 5 seconds (conservative
    epoch fetch cadence under churn), that's ~83 minutes from
    intact-score to ban for B.
12. **[steady-state]** Once B's score crosses the eviction threshold, V's
    libp2p layer disconnects B from V's mesh. A repeats the trick against
    every honest committee member until V's mesh is dominated by attackers.

### Final state

- B (and any other honest validator A targets) is evicted from V's mesh.
- A's mesh position is unchanged.
- V's view of the committee is degraded — fewer honest peers reachable
  → harder to reach quorum → degraded liveness.
- Repeatable for each (attacker_position, target_validator) pair.

### Detection signals

- `warn!` `"failed to read request digest from stream"` ≈ never (the
  digest read succeeds; it's the import that fails). Search for the more
  specific `warn!` `"error processing request batches stream"` at
  `crates/consensus/primary/src/network/mod.rs:807`. **This log fires
  every time A's frame reaches B.**
- `report_penalty` invocations with peer = B_bls from V at
  `crates/consensus/primary/src/network/mod.rs:381` AND with peer = V from
  B at `mod.rs:809` — symmetric pattern.
- libp2p score deltas for B trending negative on V's side AND for V
  trending negative on B's side, with no corresponding traffic from A.

### Mitigation breakage

- **F-3 / Q5:** the `peer` field in `RequestEpochStream` is not bound to
  the libp2p sender; `report_penalty` targets the BLS, not the actual
  on-wire responder.
- **F-4 (no escalation):** repeated misbehaviour never triggers an
  asymmetric penalty against the actual sender.
- **F-6 (predictable digest):** A doesn't need to coordinate with B —
  any epoch number lets A frame B.

---

## J5.4 — Forged-intermediate-header pack admission

**Goal:** A peer streams a pack file whose intermediate consensus headers are
fabricated; the receiving node accepts the pack as authoritative because
only the FINAL header is cross-checked against the trusted `EpochRecord`.
The fabricated intermediates land in the persistent consensus DB.

**Findings composed:** F-2 (write-before-validate / final-only check),
J-4 (joint dir-collision + final-header-only validation), F-17
(committee field unvalidated).

**Severity (journey-level):** **HIGH** — silent insertion of attacker-shaped
history into the local pack DB; downstream consumers (state-sync stream
back to other peers, RPC) serve poisoned data.

### Steps

1. **[attacker P]** P answers V's `StreamEpoch { epoch: E }` —
   `crates/consensus/primary/src/network/mod.rs:343` — with
   `RequestEpochStream { ack: true, peer: P_bls }`.
2. **[victim V]** V opens stream to P, writes correlator —
   `crates/consensus/primary/src/network/mod.rs:358-364`.
3. **[attacker P]** P streams `DataHeader` then `EpochMeta {
   epoch: E, start_consensus_number: <consistent>, committee:
   <attacker-chosen>, genesis_consensus: <attacker-chosen>,
   genesis_exec_state: <attacker-chosen> }`.
   `Inner::stream_import` —
   `crates/storage/src/consensus_pack.rs:697-704` — only checks
   `epoch == epoch_meta.epoch` (Q3 — start number not checked,
   committee not checked). Pass.
4. **[attacker P]** P chooses the chain anchor:
   `parent_digest = previous_epoch.final_consensus.hash` — line 729 — is
   sourced from V's local trusted EpochRecord; this is the only trusted
   anchor. P MUST link the first ConsensusHeader's `parent_hash` to this
   value.
5. **[attacker P]** P streams `(batch_real_1, header_real_1, batch_real_2,
   header_real_2, ..., batch_K, header_FORGED_K, ..., batch_N,
   header_real_N)`. For the forged headers `K..M`, P needs:
   - Correct `parent_hash` chain (each forged header's `parent_hash`
     equals the previous header's `digest()`).
   - Each forged header's `payload` references the immediately preceding
     batches such that `Inner::stream_import` line 754-763's HashSet
     drain succeeds.
   - The very last header's `digest()` must equal
     `epoch_record.final_consensus.hash` and its `number` must equal
     `epoch_record.final_consensus.number` —
     `crates/storage/src/consensus.rs:387-401`. **THIS is the only
     application-layer check.**
6. **[validation pass]** During the stream loop —
   `crates/storage/src/consensus_pack.rs:731-783` — every record passes
   the structural check (parent-hash chain is internally consistent;
   batch HashSet drains cleanly). Each batch is `data.append`ed and
   indexed (line 740-748) BEFORE its header arrives (F-2). Each header
   is `data.append`ed and double-indexed (line 767-777).
7. **[victim V]** Loop completes. `Inner::stream_import` returns
   `Ok(Self {...})`. `ConsensusPack::stream_import` —
   `crates/storage/src/consensus_pack.rs:254-269` — returns Ok.
   `ConsensusChain::stream_import` runs `pack.persist().await` —
   `crates/storage/src/consensus.rs:386` — flushes data + indexes to
   `import-{E}/`.
8. **[victim V]** Line 387-401 checks
   `epoch_record.final_consensus.number == last_header.number &&
   epoch_record.final_consensus.hash == last_header.digest()`. If P
   constructed the chain to anchor on the correct final header, **this
   passes** — the check examines ONLY the last header.
9. **[victim V]** Lines 403-422 perform the rename swap. After
   `fs::rename(import-{E}/epoch-{E}, base_path/epoch-{E})`, the
   attacker-shaped pack is now durable as the canonical epoch E.
10. **[downstream propagation]** A different honest peer Q later requests
    epoch E from V. V's `RequestHandler::send_epoch_over_stream` —
    `crates/consensus/primary/src/network/handler.rs:916-938` — opens
    `consensus_chain.get_epoch_stream(E)` —
    `crates/storage/src/consensus.rs:442-472`. The stream re-emits **all
    P's bytes including the forged middle**. Q ingests via the same
    `Inner::stream_import` path; Q's final-header check passes for the
    same reason. **Q is now poisoned identically to V.**
11. **[F-17 escalation]** Any consumer that reads
    `Inner::get_consensus_output` —
    `crates/storage/src/consensus_pack.rs:850-908` — gets the attacker's
    `epoch_meta.committee` for that epoch (line 900). If the executor
    layer trusts this for reward attribution (Phase 5 flag in F-17),
    rewards are mis-attributed for epoch E across every node that
    imported via this path.
12. **[orphan persistence — J-4]** If steps 5-7 race against another
    fetcher (J5.2), one of the `remove_dir_all` calls can leave orphan
    inodes via `PackInner::Drop`'s commit. Combined with the dir
    collision, the orphan can outlive the actual import — but the
    relevant fork in this journey is admission, not persistence.

### Final state

- V's local pack file for epoch E contains attacker-fabricated headers
  for any range `K..M < N` where the final header is genuine.
- `consensus_chain.consensus_header_by_number(E, k)` for `k ∈ K..M`
  returns the forged header.
- Re-served to other peers via `get_epoch_stream` — propagates.
- Reward attribution potentially corrupted (F-17) if executor reads
  `epoch_meta.committee`.

### Detection signals

- **NOTHING in the receiver layer** — every check passes by design.
- A separate validation pass (e.g. an external archiver, or a re-quorum
  check on intermediate headers) would be the only signal. The Phase 4
  Q3 question on whether intermediate headers are re-validated downstream
  is the gating uncertainty: if execution re-verifies BLS sigs on every
  consensus header, the attack is caught at execution time. If it doesn't,
  poisoning is silent.
- **ONLY indirect signal:** divergence between V's pack stream output and
  another honest peer's pack stream output for the same epoch (a third
  observer comparing).

### Mitigation breakage

- **F-2 (write-before-validate):** intermediate headers are committed to
  the import dir before any quorum-binding check.
- **Final-header-only validation** (`consensus.rs:387-401`): the
  `epoch_record.final_consensus` is the ONLY quorum-bound anchor; it
  doesn't transitively verify the chain.
- **F-17 (committee field unvalidated):** `epoch_meta.committee` is
  trusted as part of the imported pack; never cross-checked against
  `previous_epoch.next_committee`.
- The parent-hash chain is internally consistent — but internally
  consistent ≠ canonically committed.

---

## J5.5 — Cross-epoch staleness via attacker-set `start_consensus_number`

**Goal:** Attacker poisons `epoch_meta.start_consensus_number` to deterministically
wedge the import on the second header. Repeatable per peer; sync DoS for
that epoch from that peer.

**Findings composed:** F-12 (start number not validated), F-7 (no
whole-stream timeout amplifies the wedge), F-3 (mis-attribution mitigates
the cost-to-attacker).

**Severity (journey-level):** **MEDIUM** — repeatable sync DoS but not data
corruption (Q2: `PositionIndex::save` strict-sequential check fires on
header 2 and aborts the import).

### Steps

1. **[attacker P]** P answers `StreamEpoch { epoch: E }` from V with
   `RequestEpochStream { ack: true, peer: P_bls }`.
2. **[victim V]** V opens stream and writes correlator.
3. **[attacker P]** P streams `DataHeader`, then `EpochMeta { epoch: E,
   start_consensus_number: <very_large_value>,
   committee: previous_epoch.next_committee, ... }`. The epoch check
   passes (`crates/storage/src/consensus_pack.rs:702-704`); start number
   is not checked (Q3).
4. **[victim V]** V's `Inner::stream_import` accepts. `data.append` writes
   `EpochMeta` at line 705. Indexes opened (lines 707-728).
5. **[attacker P]** P streams `(batch_1, header_1)`. `header_1.number` is
   chosen so `header_1.number.saturating_sub(epoch_meta.start_consensus_number)
   = 0`. Specifically: with `start_consensus_number = u64::MAX - 5`,
   `header_1.number = small_value` → `saturating_sub` clamps to 0.
6. **[victim V]** Line 773-777:
   ```rust
   let consensus_idx_pos = consensus_number.saturating_sub(
       epoch_meta.start_consensus_number);
   consensus_idx.save(consensus_idx_pos, position)?;
   ```
   With `consensus_idx_pos = 0` and
   `consensus_idx.len() = 0`, the strict-sequential check at
   `crates/storage/src/archive/position_index/index.rs:194-206`:
   `self.len() != key as usize` → `0 != 0` → false → save succeeds.
7. **[attacker P]** P streams `(batch_2, header_2)`. `header_2.number`
   also clamps to 0 via `saturating_sub` (because the start number is at
   the saturation boundary).
8. **[victim V]** Line 776: `consensus_idx.save(0, position)`. Now
   `consensus_idx.len() = 1`. Check: `1 != 0` → returns
   `AppendError::SerializeValue("PositionIndex must add the next item by
   position, expected 1 got 0")`. `Inner::stream_import` returns
   `Err(PackError::IndexAppend("consensus number ..."))`.
9. **[victim V]** `consensus_chain.stream_import` at
   `crates/storage/src/consensus.rs:432-435` runs `remove_dir_all(&path)`
   — staging dir wiped. V's local consensus DB is untouched.
10. **[victim V]** `request_epoch_pack` at
    `crates/consensus/primary/src/network/mod.rs:379-381`: `res.is_err()`
    → V applies `Penalty::Mild` to **the BLS in the response payload**
    (F-3). If P claimed `peer = B_bls`, B is penalised; otherwise P is
    penalised mildly.
11. **[attacker amplification]** With F-7 (no whole-stream timeout), P can
    pace records every 9 seconds, forcing V to spend
    `2 × records × 9s = 18s+` per attempt. With 3 fetcher workers each
    racing to retry, the per-epoch wedge consumes at least one slot
    sustained for the entire `PACK_DOWNLOAD_RETRY_SECS` cycle.
12. **[steady-state]** V's `request_epoch_pack` retries up to 3 peers per
    invocation (`mod.rs:338`). If P is in any random-3 selection, P wedges
    again. With P co-locating multiple BlsPublicKeys in the mesh, all 3
    selections can route to P-controlled peers.

### Final state

- V cannot import epoch E from any P-controlled peer.
- V's `consensus_chain` for E remains uninitialized.
- No persistent corruption (Q2: `PositionIndex::save` aborts before any
  rename).
- Recovery: V eventually selects an honest peer in the random-3 (if any
  exist in mesh) → import succeeds. Without an honest peer in the mesh,
  E is unfetchable.

### Detection signals

- `error!` log at `state-sync/consensus.rs:189-190`:
  `"failed to request epoch pack for epoch {E}: ... IndexAppend(...consensus
  number ...)"`.
- `PackError::IndexAppend` propagated wrapped in
  `ConsensusChainError::PackError`.
- Alert: same epoch fails N times in a row from same peer set.

### Mitigation breakage

- **F-12 (start number not validated):** the import-time check is missing.
- **`saturating_sub` (`crates/storage/src/consensus_pack.rs:773-774`):**
  silently clamps to 0 instead of erroring.
- **F-3 (mis-attribution):** P avoids penalty by claiming a different
  peer's BLS in the response.
- **F-7 (no whole-stream cap):** P can pace records to maximise slot hold
  time per attempt.

---

## J5.6 — Slot exhaustion via slow-stream + small records

**Goal:** With 5 streams (one per global importer slot or one per
attacker peer position), feed tiny valid-CRC records every 9 seconds
forever. Import never finishes; no honest peer can sync; victim falls out
of CVV permanently.

**Findings composed:** F-7 (no whole-stream cap), F-4 (no escalation),
F-11 (deny path costs nothing), F-3 (BLS spoof avoids per-peer cap).

**Severity (journey-level):** **HIGH** — permanent fetch-queue starvation;
victim cannot rejoin consensus.

### Steps

1. **[setup]** Attacker controls ≥3 libp2p mesh peer positions (3 BLS
   keys mapped to 3 PeerIds). The global cap is
   `MAX_CONCURRENT_EPOCH_STREAMS = 5`
   (`crates/consensus/primary/src/network/mod.rs:60`); per-peer is
   `MAX_PENDING_REQUESTS_PER_PEER = 2`
   (`mod.rs:65`). `3 attackers × 2 = 6 > 5` permits — they can fill the
   global cap when the victim is the responder. **But this journey targets
   the REQUESTER side**: the attackers occupy the victim's
   `spawn_fetch_consensus` worker tasks (3 of them — `node.rs:286-299`).
2. **[network]** Gossip drives the victim to enqueue
   `EpochRecord(E_n)` for each of multiple unfetched epochs.
3. **[victim]** Worker 1 dequeues E_1, calls `request_epoch_pack(E_1)`,
   `send_request_any` reaches attacker peer P1. P1 acks with `peer =
   P1_bls`. V opens stream to P1.
4. **[attacker P1]** P1 streams a valid 28-byte `DataHeader`, then a
   valid `EpochMeta { epoch: E_1, ... }` (Q3 — only epoch checked).
5. **[attacker P1]** P1 then streams `(batch_1, header_1, batch_2,
   header_2, ...)` at 1 record every 9 seconds (under the per-record
   `PACK_RECORD_TIMEOUT_SECS = 10` —
   `crates/state-sync/src/consensus.rs:14`). All records pass CRC, the
   parent-hash chain links correctly. **There is no whole-stream cap
   (F-7).**
6. **[victim]** Worker 1 is stuck in `Inner::stream_import` —
   `crates/storage/src/consensus_pack.rs:680-690` — the `next()` helper
   wraps a 10s timeout per record. Workers 2 and 3 dequeue E_2, E_3 and
   are similarly captured by P2, P3.
7. **[math]** Sustained pace:
   - 9s per record × 10⁵ records per active epoch ≈ 10⁶ seconds ≈ 11.5
     days. The attacker can pace indefinitely; even a typical node
     restarts within 11.5 days, but **the attacker's slot is renewed
     after each restart** because gossip will re-enqueue.
8. **[F-11 — denial of denial]** Honest peer H attempts to sync from V
   by sending V a `StreamEpoch { epoch }` request. V's
   `process_epoch_stream` —
   `crates/consensus/primary/src/network/mod.rs:666-749` — runs
   `try_acquire_owned()`. The 5 permits are NOT actually held by the 3
   attacker streams (those are the REQUESTER side; the
   `epoch_stream_semaphore` governs the RESPONDER side — sub-finding
   F-11). So V's responder side is fine — but V is so far behind on
   sync that V cannot serve any historical epochs, returning
   `StreamUnavailable(epoch)` → `None` penalty (F-4). H gets nothing
   from V; H may evict V from its mesh (no penalty applied → just
   timeout-driven score decay).
9. **[steady-state]** All 3 fetcher workers are pinned to attacker
   streams. The `epoch_request_queue` accumulates undeliverable
   `EpochRecord(...)` entries up to the 10_000 mpsc cap
   (`crates/state-sync/src/consensus.rs:93`). New gossip events that
   call `request_epoch_pack_file` (consensus_bus.rs:551) silently drop
   when the channel is full (it's a fire-and-forget `let _ =`).
10. **[recovery attempt]** V can only escape by:
    - Restarting (which re-fetches state from gossip; the same
      attackers pin again).
    - The libp2p layer evicting one of P1/P2/P3 due to score decay —
      but `Penalty::Mild` is applied ONLY on Err return; in this
      journey there is no Err (peer is making progress at 9s/record,
      no error). **No penalty fires at all.**
    - An honest peer with the epoch entering V's selection set in
      `send_request_any`. With only 2-3 connected peers, this is
      unlikely.
11. **[validator state]** With sync stalled, V cannot apply incentives or
    track committee membership for new epochs. V's CVV (committee voting
    validator) status decays. After a few epoch boundaries, V is no
    longer in the active committee. To rejoin, V must catch up — which
    requires sync — which the attacker prevents.

### Final state

- 3 fetcher worker slots permanently occupied by 3 attacker streams.
- Each stream is making technically-valid progress (records validate,
  indexes accept) — the import will complete at glacial pace.
- Other epochs in the queue starve.
- Victim drops out of CVV; cannot rejoin without operator intervention.

### Detection signals

- `info!` `"epoch consensus fetcher {i} retrieving epoch {E}"` —
  `state-sync/consensus.rs:182` — fires once but the corresponding
  `info!` `"streamed epoch pack file"` —
  `crates/consensus/primary/src/network/mod.rs:387` — never fires.
- `pending_epoch_requests` is empty (this is the requester side, not
  responder side).
- Epoch height of V (`consensus_chain.latest_consensus_epoch()`) stalls
  while gossip-published epoch advances.
- libp2p incoming bandwidth from P1/P2/P3 trickles at ~50-100 bytes per
  9 seconds — distinctive low-rate signature.

### Mitigation breakage

- **F-7 (no whole-stream cap):** there is NO `tokio::time::timeout` wrapping
  `consensus_chain.stream_import(...)` at
  `crates/consensus/primary/src/network/mod.rs:373-378`.
- **No record-count cap:** `Inner::stream_import` while-loop runs until
  the stream ends. No `MAX_RECORDS_PER_PACK`.
- **F-4 (no penalty on success-trickle):** `Penalty::Mild` fires on
  `res.is_err()`; this attack avoids ever returning Err.
- **F-3 (mis-attribution + multi-BLS):** the attacker's 3 BLS positions
  are independently valid; per-peer cap doesn't help (cap is per-BLS).
- **No escape hatch:** there is no exponential give-up after N seconds
  in `request_epoch_pack` — only the per-record timeout drives the
  worker out, and that timeout is satisfied at 9s/record.

---

## J5.7 — Transient `current_pack=None` reader race

**Goal:** Within the visible window between `*current_pack = None`
(consensus.rs:410) and `fs::rename(...)` (consensus.rs:422), a concurrent
gossip / RPC / state-sync reader gets `None` from
`consensus_header_by_*` and propagates the inconsistency.

**Findings composed:** J-1 (the new joint finding), F-10 (watch can lead
the pack file).

**Severity (journey-level):** **MEDIUM** — flapping availability; no
permanent corruption but observable inconsistency to RPC clients and
potential mis-decisions in `behind_consensus`.

### Steps

1. **[setup]** Victim V is in the middle of a same-epoch
   `stream_import(E)` because gossip indicates E's final-consensus
   advanced. The current `current_pack` epoch is also E (an in-place
   replacement, not a new epoch).
2. **[victim]** `ConsensusChain::stream_import` reaches line 403:
   `let mut current_pack = self.current_pack.lock();`
   (`crates/storage/src/consensus.rs:403`).
3. **[victim]** Line 404-410 evaluates
   `current_pack.epoch() == epoch` → true → `*current_pack = None`.
4. **[victim]** Lines 412-413 drop the old `pack` and the
   `current_pack` mutex guard. Now any reader can take the mutex and see
   `None`.
5. **[victim]** Line 415: `if std::fs::exists(&base_dir)
   .unwrap_or_default()` — true; line 420:
   `let _ = std::fs::remove_dir_all(&base_dir)`. **The on-disk
   `epoch-{E}/` directory is now gone.** This is the explicit "tiny
   window" comment at lines 416-419.
6. **[concurrent reader R]** R calls
   `consensus_chain.consensus_header_by_digest(E, some_digest)` —
   `crates/storage/src/consensus.rs:506` (or
   `consensus_header_by_number`). The function reaches `current_pack()`
   clone at line 678-680 — gets `None`. Falls through to `get_static(E)`
   (line 683). `get_static` checks `recent_packs` (line 690-697) — may
   contain a stale `ConsensusPack` handle whose underlying FDs point to
   the now-unlinked inodes. On Linux: reads via the FDs still succeed,
   returning OLD-pack data. On Windows: `ERROR_SHARING_VIOLATION` from
   `remove_dir_all` (which the unwrap-default at line 415 silently
   suppresses).
7. **[reader R, branch A — Linux + cache hit]** R gets the OLD pack's
   header via the orphan FD. R returns this header to its caller (RPC,
   state-sync, gossip handler). **R is serving content from a
   directory that no longer exists.**
8. **[reader R, branch B — cache miss]** `recent_packs` doesn't have E.
   `get_static` falls to `ConsensusPack::open_static(&self.base_path,
   epoch)` at line 700 — opens files at the path that was just
   `remove_dir_all`'d. Returns an `OpenError::IoError(ENOENT)` →
   propagates as `PackError` →
   `consensus_chain.consensus_header_by_digest` returns
   `Err(...)` → caller maps to `None` or surfaces error.
9. **[victim]** Meanwhile, line 422:
   `std::fs::rename(&path_base_dir, &base_dir)?` — succeeds. Line 428:
   `recent_packs.lock().retain(|p| p.epoch() != epoch)` — purges any
   stale entry of E (so future reads after this point see the new
   on-disk pack via `open_static`).
10. **[reader R, post-rename]** R retries (RPC client retry cycle).
    `current_pack()` is still `None` — it gets RESET at the NEXT
    `new_epoch` call (`consensus.rs:325-348`), not by `stream_import`.
    `get_static(E)` → `recent_packs` empty for E → `open_static(E)` →
    SUCCESS, returns a fresh `ConsensusPack` for the new on-disk pack.
    **R now sees the NEW pack, but `current_pack` is still `None` until
    the next epoch transition.**
11. **[F-10 cross-effect]** While `current_pack = None`,
    `consensus_chain.consensus_header_latest()` at
    `crates/storage/src/consensus.rs:544-548` — uses
    `latest_consensus.epoch()` which is in-memory, NOT
    `current_pack`. So `latest_consensus_header` watch
    (`tx_last_consensus_header`) is unaffected. **But:**
    `consensus_header_by_digest(E, d)` returning `None` for a `d` whose
    header IS in the just-renamed pack triggers the F-10 asymmetry —
    watch claims advance, lookup fails — for the duration of the race
    window.
12. **[downstream propagation]** If R is part of `process_request_consensus_header`
    (responding to another peer Q's `RequestConsensusHeader(d)` request),
    Q sees `None` → Q logs an error → Q applies no penalty
    (`UnknownConsensusHeaderDigest` → `Penalty::Mild` per
    `crates/consensus/primary/src/error/network.rs:117`). **Honest V is
    penalised by Q for transient unavailability that V created via its
    own correct same-epoch import.**

### Final state

- A short observable window (typically a few ms but can stretch to
  seconds under fs slowness) where `consensus_header_by_*(E, ...)`
  returns `None` or stale data.
- After the rename, eventually consistent again.
- `current_pack` is `None` until the next epoch transition; this leans
  more heavily on `recent_packs` cache behaviour than steady-state.
- If the rename fails (ENOSPC, IO error), the error path
  (lines 432-435) does NOT restore `current_pack` from the prior
  snapshot — V's live pack handle is lost permanently until next
  process restart.

### Detection signals

- RPC client retry storms — JSON-RPC `eth_getBlockByHash` analog returns
  errors during the window.
- `error!` from `get_static` if `open_static` fires during the
  unlink-rename gap.
- Peer-score deltas: V receives `Penalty::Mild` from peers requesting
  consensus headers during the window —
  `crates/consensus/primary/src/network/handler.rs:821-834` returns
  errors which propagate to `Penalty::Mild` for the requester (Q) →
  Q applies `Penalty::Mild` to V.
- `current_pack` `is_none()` log line `"no current pack"` if any caller
  unwraps it (audit code search confirms most callers handle `None`
  gracefully).

### Mitigation breakage

- **J-1 (transient `current_pack=None`):** the unlink-then-rename is
  intentional but the in-memory handle is nulled BEFORE the rename
  succeeds.
- **No restore on Err:** the error path at line 432-435 doesn't
  re-instate the prior `current_pack`.
- **`recent_packs` not purged before unlink:** the cache invalidation at
  line 428 happens AFTER the rename, so within the window the cache may
  serve old data.

---

## J5.8 — Restart-after-OOM compounded with idempotent replacement

**Goal:** Sustain F-1 OOM crashes that interleave with the responder-side
idempotent replacement (cb0e147c). Show that across restarts, the residual
disk hazard plus the requester-side dedup absence (F-8) creates a
self-reinforcing wedge.

**Findings composed:** F-1, F-14, F-13 (panic-guard / supervisor reset),
F-8 (no requester-side dedup; per-task semaphore).

**Severity (journey-level):** **HIGH** — process kill compounded with
disk leakage and immediate re-entry into the attacker's frame.

### Steps

1. **[t=0, victim]** Victim is mid-`Inner::stream_import` for epoch E
   from peer P. P has streamed `DataHeader`, `EpochMeta`, and is on
   record 50 of 100. `import-{E}/data` has 50 records' worth of bytes
   plus index entries.
2. **[t=t_oom, attacker]** P sends record 51 with
   `val_size = 0xFFFF_FFFF`.
3. **[victim]** `AsyncPackIter::read_record_file` —
   `crates/storage/src/archive/pack_iter.rs:147-150` — calls
   `buffer.resize(4 GiB, 0)`. Linux overcommit + `read_exact` on the
   peer-fed payload triggers a page fault. OOM-killer fires.
4. **[victim]** Process-wide SIGKILL. **Crucially:** SIGKILL bypasses
   Drop. `PackInner::Drop` (which would have called `commit()` —
   `crates/storage/src/archive/pack.rs:163-164`) does NOT run.
   Whatever bytes were in the kernel page cache for `import-{E}/data`
   are flushed by the kernel asynchronously; whatever wasn't yet written
   is lost. **`import-{E}/` survives partially populated.**
5. **[restart, t=t_oom + restart_delay]** Process restarts.
   `epoch_task_manager` (rebuilt every epoch — Q7) creates a new
   `PrimaryNetwork` with fresh `pending_epoch_requests` (in-memory) and
   fresh `epoch_stream_semaphore` (cap 5).
6. **[victim]** `ConsensusChain::new` at
   `crates/storage/src/consensus.rs:294-303` opens
   `current_pack` via `open_append_exists` — targets the LIVE pack
   directory `epoch-{latest}/`, not `import-*/`. The orphan
   `import-{E}/` is invisible to startup (F-14).
7. **[victim, post-restart gossip drives re-fetch]** Gossip immediately
   replays the `last_published_consensus_num_hash` watch, triggering
   `spawn_track_recent_consensus` (consensus.rs:102) which re-enqueues
   `EpochRecord(E)` (because `consensus_header_by_number(E.final.number)`
   returns `None` — line 114).
8. **[victim]** All 3 fetcher workers wake; one or more dequeue E
   (F-8: no enqueue dedup; per-task semaphore).
9. **[victim]** Worker calls `request_epoch_pack(E)`. `send_request_any`
   selects a peer; with high probability P is selected again (P is in
   the mesh).
10. **[victim]** `ConsensusChain::stream_import` at
    `crates/storage/src/consensus.rs:357`. Line 365's `get_static(E)`
    looks for the LIVE epoch dir — `epoch-{E}/` doesn't exist → returns
    `Err(...)`. Line 379 does
    `let _ = std::fs::remove_dir_all(&path)` where
    `path = base_path/import-{E}` → **wipes the orphan from step 4**.
    Good: orphan cleaned.
11. **[attacker P]** P receives V's stream-open request and writes
    `DataHeader`, `EpochMeta`, then the SAME OOM record at position 51
    (or earlier). **OOM cycle restarts.**
12. **[steady-state]** Crash → restart → re-enqueue → re-stream → crash.
    Each iteration:
    - Wipes the prior orphan via line 379.
    - Creates a new orphan via the next OOM.
    - Net disk usage per cycle ≈ size_of_partial_pack; bounded.
    - Net CPU per cycle ≈ libp2p reconnect + index file open + 50 record
      writes ≈ negligible.
13. **[F-13 / J-2 reassessment]** The original Phase 3 concern was that
    `cleanup_stale_pending_requests` panic could leak permits forever.
    In this journey, the panic happens in a DIFFERENT critical task
    (`spawn_fetch_consensus`), not the prune task. Both are
    `spawn_critical_task`; per the supervisor model, ANY critical-task
    panic propagates a `CriticalExitError` and triggers full process
    shutdown (`task_manager.rs:400/411`). So the prune task's permits
    don't leak — they're erased by restart. **F-13 is not weaponised
    here, but the restart cycle itself IS the attack.**
14. **[idempotent replacement irrelevance]** Commit cb0e147c's
    responder-side idempotency (mod.rs:693-711) protects the responder
    from a peer holding a slot indefinitely. **It does nothing for the
    requester-side wedge.** The attacker is not abusing the responder
    slot; they're feeding malformed data to the requester.

### Final state

- Victim is in a crash-loop driven by P's single `val_size = u32::MAX`
  record.
- Disk: `import-{E}/` cycles between empty / partial; no permanent
  growth (each cycle's cleanup is one `remove_dir_all` later).
- In-memory state: nothing leaks; everything is rebuilt on restart.
- Liveness: V cannot complete sync as long as P is selected by
  `send_request_any`. V cannot pre-blacklist P because P's
  `Penalty::Mild` only fires when `res.is_err()` AFTER the import
  returns — but the OOM kills the process BEFORE the
  `report_penalty(peer, Penalty::Mild)` call at
  `crates/consensus/primary/src/network/mod.rs:381` runs. **No penalty is
  ever applied across the crash.**

### Detection signals

- Same as J5.1 plus:
  - `info!` `"streamed epoch pack file"` never fires for E.
  - On every restart, the same `info!` `"epoch consensus fetcher {i}
    retrieving epoch {E}"` for the same E.
  - Process supervisor restart count climbs in lockstep with libp2p
    connection count to P.
  - `report_penalty` for P never fires (P is unpunished by V because V
    crashes before the penalty point).

### Mitigation breakage

- **F-1 (uncapped resize):** no length cap before
  `buffer.resize(val_size as usize, 0)`.
- **F-14 (no boot-time reaper):** acknowledged but bounded by line 379's
  unconditional clean.
- **F-8 (no enqueue dedup):** the same epoch is re-enqueued immediately
  on restart by gossip-driven `request_epoch_pack_file`.
- **`Penalty::Mild` placement (F-3, F-4):** the penalty point is AFTER
  the import returns Err, but the crash short-circuits the call.
- **No peer-blacklist persistence:** even if a penalty did fire, the
  in-memory peer-score state resets across crash. (libp2p typically
  persists scores via gossipsub state; out of scope but flag-worthy.)

---

## J5.9 — Probe-driven peer-score asymmetric drain (extra)

**Goal:** Bleed the responder's compute budget without triggering eviction.
A standalone variant of J5.3 that doesn't aim to evict — just drain.

**Findings composed:** F-6 (predictable digest), F-4 (no escalation),
F-11 (rejection has no peer cost on the responder side).

**Severity (journey-level):** **MEDIUM** — sustained low-grade
griefing.

### Steps

1. **[attacker A]** A computes
   `request_digest = digest(E.to_le_bytes())` for any known epoch E.
2. **[attacker A]** A opens an inbound libp2p stream to V (no
   `StreamEpoch` request preceded). Writes the 32-byte digest.
3. **[victim V]** `process_inbound_stream` —
   `crates/consensus/primary/src/network/mod.rs:772-813` — reads the
   digest, removes from `pending_epoch_requests` (no entry → returns
   `None`), calls `process_request_epoch_stream(peer=A_libp2p,
   opt_pending_req=None, ...)` —
   `crates/consensus/primary/src/network/handler.rs:941-990` — returns
   `PrimaryNetworkError::UnknownStreamRequest(request_digest)`.
4. **[victim V]** `(&err).into()` → `Penalty::Mild` (F-4). V applies
   `Penalty::Mild` to A (the libp2p PeerId, which IS the actual sender
   here).
5. **[attacker A]** A's score drops by `mild_weight ≈ 0.1`.
6. **[scaling]** A iterates: for every epoch number A knows V is at, A
   probes once. With ~20 epochs known and ~5s between probes, A's
   score-spend rate is ~0.1 per 5s ≈ 1.2 per minute. Default ban
   threshold ~100 → ~83 minutes from A starting probes to A being
   banned.
7. **[F-11 contribution]** During those 83 minutes, V's `process_inbound_stream`
   spawns one task per probe (line 778) — each task allocates a `digest_buf`,
   takes the parking_lot mutex on `pending_epoch_requests`, runs the
   error-handler branch, and reports the penalty via libp2p. Net cost
   to V: ~1 task per probe. With many attackers (Sybil), the responder
   CPU + lock contention become non-trivial.
8. **[steady-state]** Each attacker is banned after ~83 minutes — but
   A can rotate libp2p PeerIds (cheap on libp2p; the BLS key is the
   trusted binding, not the PeerId). With a rotating identity, the
   ban resets.

### Final state

- V's CPU is mildly elevated; lock contention measurable.
- libp2p score state has constant churn.
- No persistent corruption.

### Detection signals

- Spike in `process_inbound_stream` frequency vs `process_epoch_stream`
  ack count — inbound streams without preceding `StreamEpoch`.
- `warn!` log frequency at
  `crates/consensus/primary/src/network/mod.rs:807`:
  `"error processing request batches stream"` with
  `UnknownStreamRequest`.
- libp2p score ban events for attackers cycling.

### Mitigation breakage

- **F-6 (predictable digest):** A doesn't need to coordinate; epoch
  numbers are public.
- **F-11 (no responder-side cost on rejection):** even if A's request
  is denied, A pays nothing on the responder's bandwidth budget.
- **F-4 (no escalation):** `Penalty::Mild` is the only ladder.

---

## J5.10 — Worker batch-stream mis-attribution (extra)

**Goal:** F-3 and J5.3 mirror, but on the worker network. F-16 is the
unique vector here: the `request_digest` is computed over a TRUNCATED
digest set on the responder side, causing a mismatch with the requester
that the requester is then penalised for.

**Findings composed:** F-16 (silent truncation of oversized requests),
F-3 (BLS-claim binding), F-4 (no escalation).

**Severity (journey-level):** **MEDIUM** — worker analog griefing.

### Steps

1. **[network]** Worker requester R issues a batch-stream request for
   501 batch digests (just over `MAX_BATCH_DIGESTS_PER_REQUEST = 500` —
   inferred from `crates/consensus/worker/src/network/mod.rs:289-300`).
2. **[worker responder Q]** Q receives the request,
   `process_request_batches_stream` —
   `crates/consensus/worker/src/network/mod.rs:280-387` — runs the
   `take(500)` truncation at line 289-300.
3. **[worker responder Q]** Q computes
   `request_digest = generate_batch_request_id(&batch_digests_truncated)`
   at line ~325. **Note:** content-addressed over the truncated set.
4. **[worker responder Q]** Q inserts under
   `(R_bls, request_digest_truncated)` in `pending_batch_requests`.
5. **[worker requester R]** R computes its own `request_digest` over
   the FULL 501-entry set →
   `request_digest_full ≠ request_digest_truncated`.
6. **[worker requester R]** R opens a stream to Q, writes
   `request_digest_full`.
7. **[worker responder Q]** Q's `process_inbound_stream` —
   `crates/consensus/worker/src/network/mod.rs:392-433` — looks up
   `(Q_bls, request_digest_full)` → not found (Q only has the
   truncated digest indexed) → `UnknownStreamRequest`.
8. **[worker responder Q]** Q applies `Penalty::Mild` to R (the
   stream-opening libp2p peer). **R is penalised for asking for too
   many digests, even though Q silently agreed to the request and
   truncated it.**
9. **[scaling]** R re-tries; each retry burns R's score on Q's side.
   With Q broadcasting `Penalty::Mild` reports, R's mesh-wide score
   degrades.
10. **[asymmetric attack]** A malicious requester can DELIBERATELY send
    501-digest requests against an honest Q, framing R if R was the
    victim of a `peer` field forgery (F-3 on the worker side
    if the worker has equivalent semantics; verify in Phase 6).

### Final state

- R's libp2p score is degraded by Q for making oversized requests.
- Q is unaware of R's confusion (Q logged truncation silently).

### Detection signals

- `warn!` from worker mod.rs (truncation log line, if present).
- `Penalty::Mild` applied with `UnknownStreamRequest` against requesters
  who are NOT actually misbehaving from their own perspective.

### Mitigation breakage

- **F-16 (silent truncation):** the responder doesn't reject the
  oversized request; it silently truncates and proceeds.
- **F-4 (no escalation):** the penalty is `Penalty::Mild` regardless.

---

## Cross-Journey Patterns

### Findings reused across multiple journeys (highest-leverage to fix)

| Finding | Used in journeys | Type |
|---------|------------------|------|
| **F-1** (uncapped `val_size`) | J5.1, J5.8 | Memory/process kill |
| **F-3** (penalty mis-attribution) | J5.2, J5.3, J5.5, J5.6, J5.10 | Reputation/identity |
| **F-4** (no penalty escalation) | J5.3, J5.6, J5.9, J5.10 | Reputation arithmetic |
| **F-6** (predictable digest) | J5.1, J5.2, J5.3, J5.6, J5.9 | Identity/correlation |
| **F-7** (no whole-stream cap) | J5.5, J5.6 | Time/resource bound |
| **F-8** (no enqueue dedup; per-task semaphore) | J5.1, J5.2, J5.8 | Concurrency coordination |
| **F-2** (write-before-validate / final-only check) | J5.4 (primary), J5.2 (assist) | Validation ordering |
| **F-14** (no boot reaper) | J5.1, J5.8 | Recovery |
| **F-11** (rejection costs nothing) | J5.6, J5.9 | Asymmetric griefing |

### Reuse insight

- **F-3 + F-4 + F-6 form a "griefing triangle":** identity confusion
  (F-3) + no escalation (F-4) + predictable correlator (F-6) appears
  in J5.3, J5.6, J5.9, J5.10 and partially in J5.1/J5.2/J5.5.
- **F-1 + F-8 + F-14 form a "crash-loop triangle":** OOM
  vector (F-1) + immediate re-fetch (F-8) + no startup cleanup (F-14)
  appears in J5.1 and J5.8.
- **F-2 + final-header-only check** is the unique gold-finding for
  J5.4; absent the other two triangles, this is the silent-data-poison
  vector.

---

## Phase 5 Recommendation Hierarchy

Ranked by **journey-coverage** (number of journeys closed), not by
individual finding severity.

### Top 3 fixes (by journey-coverage)

1. **Cap `val_size` against a constant `MAX_RECORD_SIZE` in
   `pack_iter::read_record_file` (and the 3 sibling sites)** — closes
   **J5.1, J5.8** entirely. Suggested: 16 MiB (largest reasonable
   batch + header) or 64 MiB defensive ceiling. Compare against
   `val_size` as a `u64` BEFORE the `as usize` cast and `resize`.
   File:line: `crates/storage/src/archive/pack_iter.rs:147-150`,
   `pack_iter.rs:74-77`, `crates/storage/src/archive/pack.rs:319-321`,
   `pack.rs:343-345`.

2. **Bind `report_penalty` to the actual on-wire libp2p PeerId, NOT
   the `peer: BlsPublicKey` claim in the response payload** — closes
   the heart of **J5.3, J5.10**, and removes the penalty-spoofing
   amplifier from **J5.2, J5.5, J5.6, J5.9**. The `network_handle`
   already knows the on-wire PeerId at the point of receiving
   `RequestEpochStream` (libp2p surfaces it via the response
   metadata); thread it through to `request_epoch_pack`. File:line:
   `crates/consensus/primary/src/network/mod.rs:342, 358, 381`.
   Implementation hint: have `send_request_any` return a
   `(BlsPublicKey_actual, PrimaryResponse)` tuple; cross-check
   `peer_actual == peer_claimed` and either reject or rebind the
   penalty target to `peer_actual`.

3. **Single shared `Arc<Semaphore::new(1)>` (or a per-epoch dedup map)
   keyed by epoch across all `spawn_fetch_consensus` workers, AND
   a small dedup window in `request_epoch_pack_file`** — closes
   **J5.2** entirely and removes a key amplifier from **J5.1, J5.6,
   J5.8**. Move the `next_sem` ownership out of the function body
   into the spawn-three loop in
   `crates/node/src/manager/node.rs:286-299` so all three workers share
   it, OR replace it with a `HashMap<Epoch, OneShotInFlight>` guard.
   File:line: `crates/state-sync/src/consensus.rs:174`;
   `crates/consensus/primary/src/consensus_bus.rs:551-553`.

### Honourable mentions (closes 1-2 journeys but high-impact)

- **Add a whole-stream cap** (e.g.
  `tokio::time::timeout(WHOLE_STREAM_CAP, consensus_chain.stream_import(...))`
  at `crates/consensus/primary/src/network/mod.rs:373-378`) — closes
  **J5.6** definitively.
- **Validate `epoch_meta.start_consensus_number` and
  `epoch_meta.committee` against `previous_epoch.next_committee` and
  `previous_epoch.final_consensus.number + 1` at
  `crates/storage/src/consensus_pack.rs:702-704`** — closes
  **J5.5** and the F-17 escalation path inside **J5.4**.
- **Re-validate intermediate consensus headers' BLS signatures (or
  verify via a light per-header quorum hint) at
  `crates/storage/src/consensus_pack.rs:750-781`** — closes the
  silent-data-poison core of **J5.4**. This is the highest-impact
  but also highest-cost fix; it requires the responder to send
  per-header attestations with the pack stream.
- **Add domain separation to `request_digest` (include
  `committee_id || requester_bls || epoch || protocol_version`)** —
  reduces the predictability axis of **J5.3, J5.6, J5.9** and aligns
  with the worker's content-addressed `generate_batch_request_id`
  symmetry (Phase 2 F-6 note). File:line:
  `crates/consensus/primary/src/network/mod.rs:333-336, 690-692`.
- **Add a boot-time reaper of `import-*/`** at
  `crates/storage/src/consensus.rs:294-303` — closes **F-14** and the
  residual concern in **J5.1, J5.8**, even though those journeys are
  already bounded by line 379's per-attempt cleanup.

### Skip-this-cycle (low ROI relative to other fixes)

- F-13 (prune-loop panic guard): supervisor restart already bounds the
  leak; closing this fix doesn't close any journey above LOW.
- F-15 (`reset_for_epoch` not clearing handler maps): handler is
  rebuilt per epoch (Q7); no reachable journey.
- F-9/F-10 (vote replay across epochs): dismissed by Q1 + Q7.

### Top-3 fix journey-coverage table

| Fix                                      | Journeys closed       | Total |
|------------------------------------------|-----------------------|-------|
| 1. Cap `val_size`                        | J5.1, J5.8            | 2     |
| 2. PeerId-bound penalty                  | J5.3, J5.10 (+ amp in J5.2, J5.5, J5.6, J5.9) | 2 direct + 4 amplifier |
| 3. Shared `next_sem` + enqueue dedup     | J5.2 (+ amp J5.1, J5.6, J5.8)              | 1 direct + 3 amplifier |

Together these three fixes close 5 of 10 journeys completely (J5.1,
J5.2, J5.3, J5.8, J5.10) and de-fang the remaining 5 by removing
amplifiers. The remaining hardening (whole-stream cap, EpochMeta
validation, intermediate-header verification) closes the rest.
