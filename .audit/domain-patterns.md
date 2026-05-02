_Git hash: 9b398938_
_Target scope: crates/consensus/primary/src/network/{mod,handler,message}.rs, crates/consensus/primary/src/error/network.rs, crates/consensus/primary/src/consensus_bus.rs, crates/state-sync/src/{consensus,epoch}.rs, crates/storage/src/archive/{pack,pack_iter}.rs, crates/storage/src/{consensus,consensus_pack}.rs, crates/consensus/worker/src/network/mod.rs, crates/e2e-tests/tests/it/{epochs,restarts,common}.rs_
_User hints: BFT consensus historical state sync — peer requests streamed epoch pack files containing consensus headers + worker batches; structural validation (parent-hash chain, batch-ref reconciliation, CRC32) is performed inline as bytes are written to the consensus DB. Trust model: epoch certificate verified before fetch; pack contents trusted to belong to that epoch if structure matches. Threat actor: malicious peer in the mesh._
_Generated: 2026-04-27_

# Domain Patterns — Streaming Epoch Pack-File Sync (issue-629)

## Domain summary

Branch `issue-629` introduces a new pack-file streaming protocol for historical state sync in a DAG-BFT chain (Bullshark-style consensus, libp2p networking, REDB storage):

1. A node fetches an epoch certificate from gossip, verifies it (out of scope for this audit), then issues a `StreamEpoch` request to a peer over a libp2p stream protocol.
2. The peer replies by streaming a sequence of `(record_kind, val_size, crc32, bytes)` records: alternating worker batches and consensus headers, framed and CRC-checked.
3. The receiver feeds bytes into `consensus_pack::stream_import`, which **writes batches to the data file and updates indices BEFORE the consensus header is parsed and its parent-hash chain validated**.
4. The receiver enforces concurrency caps: `MAX_CONCURRENT_EPOCH_STREAMS = 5`, `MAX_PENDING_REQUESTS_PER_PEER = 2`, per-record `PACK_RECORD_TIMEOUT_SECS = 10`. There is no whole-stream cap.
5. Pending request entries are reclaimed by a 15-second background prune loop.

This is a brand-new untrusted-input deserialization path with consensus-database-write authority. The threat is: a single malicious connected peer producing pack-file bytes that pass structural checks but corrupt local consensus DB state, exhaust memory, or stall the import pipeline.

## Trust model

| Trusted | Untrusted |
|---------|-----------|
| The epoch certificate (verified via committee BLS quorum before fetch) | The pack-file byte stream from the responding peer |
| Local-only state (`pending_requests`, `epoch_request_queue`) | Any field in the streamed records (`val_size`, `crc32`, batch contents, header parent hash) |
| libp2p PeerId after handshake | Application-layer claim of which `BlsPublicKey` is responding |
| The 32-byte `request_digest` correlator | Replay across reconnects, cross-epoch reuse of the digest |

Any unsoundness in the pack-file path can poison the consensus DB or break peer scoring. Structural validation is **the only** check; semantic validation (e.g., do these batches actually belong to this epoch's headers?) is implicit from the parent-hash chain.

## Worked examples

### Example 1 — orphan-batch scar
```text
1. Receiver opens stream, allocates a fresh stream_import context.
2. Peer streams 1 batch record. consensus_pack writes batch bytes to data file
   AND updates batch_digests index (offset, len) BEFORE the header arrives.
3. Peer sends RST or feeds an invalid CRC on the next record.
4. stream_import returns Err(...). Caller drops the import context.
5. The batch_digests update is NOT rolled back. The data file still has bytes.
   Result: the local DB has a batch entry that no consensus header references —
   silent index pollution. On next epoch fetch from another peer, the entry is
   either duplicated or contradicts the new well-formed chain.
```

### Example 2 — cross-epoch digest collision
```text
1. Node A fetches epoch E1 from peer P. request_digest = digest(committee_id || E1 || ...).
2. Node A reconnects (libp2p session lost, new PeerId session).
3. P (or any peer reusing the same correlator namespace) sends a stream
   labeled with a digest that collides with a stale pending_requests entry
   for an unrelated epoch. The receiver attaches the stream to the wrong
   pending request, importing E1-format bytes into the E2 import context.
```

### Example 3 — slot-leak DoS via stalled prune
```text
1. Attacker peer P sends 2 valid StreamEpoch requests (the per-peer cap).
   Server side spawns handlers, allocates pending_requests slots.
2. P stalls during the slow-read window — handler eventually times out, but
   the prune loop is what reclaims the slot 15s later.
3. P's request volume keeps the prune task queue busy or its panic is silenced.
4. Slots leak. Within seconds the global MAX_CONCURRENT_EPOCH_STREAMS=5 is full.
5. Honest peers cannot sync. The node falls further behind, eventually exits
   CVV, then can't rejoin because it can't catch up.
```

### Example 4 — unbounded allocation OOM
```text
1. Peer announces a record with val_size = u64::MAX or any value larger than
   physical RAM. pack_iter::read_record does buffer.resize(val_size as usize, 0).
2. Allocator returns ENOMEM or kills the process. No length cap is enforced
   before the resize.
3. Repeated by a single peer within rate-limit windows = repeatable OOM.
```

### Example 5 — transient header visibility
```text
1. Peer P streams 50 (batch, header) record pairs.
2. After header #25 is appended to consensus storage, the watch channel
   ConsensusBus.last_consensus_header advances to header #25.
3. Concurrent JSON-RPC subscriber reads the watch and sees header #25
   as authoritative.
4. Stream fails at record #30. The import is rolled back? Or is it?
   If watch never retracts, downstream consumers have already acted on
   non-final state.
```

## Adversarial sequences (focus list for Phase 5)

1. **Many small records vs whole-stream cap absence** — Sustain a 10-second-per-record pace forever to occupy 1 of 5 importer slots; coordinate 5 peers to lock all slots.
2. **Race on `MAX_PENDING_REQUESTS_PER_PEER`** — Two RPC handlers from same peer concurrently pass the count<2 check before either inserts.
3. **Forged `BlsPublicKey` claim → stream misdirection** — Peer P opens stream claiming to be peer Q; receiver indexes the response under Q's identity for scoring/penalty. Mild penalty hits Q instead of P.
4. **`request_digest` replay** — After the receiver disconnects mid-stream, attacker reconnects and sends a fresh stream with the recycled digest. Does the receiver accept the late "successful" response into a new pending request slot, or treat it as orphan?
5. **Idempotent replacement weaponized** — An in-flight successful import gets cancelled when the same epoch's request is replaced by gossip-triggered re-issue (commit `cb0e147c`). Attacker triggers re-issue at the moment of near-completion to discard the import.
6. **Malicious-peer write-before-validate** — Stream `(batch1, batch2, ..., batchN, valid_header_for_batch1_only)` so all N batches are written; only batch1 is validated by the header parent-hash check; batches 2..N orphan.
7. **Mild penalty arbitrage** — Repeat a misbehavior with cost-per-mistake <  cost-per-honest-fetch. Penalty scoring never escalates so peer remains in mesh indefinitely.
8. **Header watch advance during import** — Concurrent gossip `respond_to_request` reads `latest_consensus_header()` and broadcasts mid-import state to other peers, propagating poisoned local view.
9. **Three-index torn write** — Crash at the precise moment between `consensus_idx` update and `consensus_digests` update; on restart, indices disagree on what the latest header is.
10. **Pending-request slot leak via panic** — A panic inside the streaming handler that is caught by tokio task panic isolation but leaves the pending_requests entry in place. 15s prune may or may not match the entry's stored shape.

## Coupled state pairs (focus list for Phase 3)

| State A | State B (coupled) | Coupling Invariant | Breaking operation |
|---|---|---|---|
| `consensus_idx` table (round → header digest) | `consensus_digests` table (digest → round) | Bidirectional injective; A.get(r)=d ⟺ B.get(d)=r | Update one without the other (consensus_pack.rs:840-957) |
| `consensus_idx`/`consensus_digests` | `batch_digests` table | Every batch_digest entry has its referencing header committed in idx | Batch written and indexed before header validates (consensus_pack.rs:737-760) |
| Consensus DB data file content | Indices above | Offsets in indices point inside data file | Data file appended; index update fails or vice versa |
| `pending_requests: HashMap<digest, ...>` | `peer_in_flight_count: HashMap<peer, u8>` | Sum of in-flight per peer ≤ MAX_PENDING_PER_PEER, AND ⩾ live entries in pending_requests | Race on count check, or panic between insertion of one but not the other |
| `pending_requests` entry lifetime | Importer task running | Either a live importer or no entry | Importer panics; entry not removed until 15s prune |
| `stream_concurrency_semaphore` permits | Active import tasks | permits + active = MAX_CONCURRENT_EPOCH_STREAMS | Permit acquired, task spawn fails or panics before drop |
| `ConsensusBus.last_consensus_header` watch | Fully validated headers in DB | Watch points only at validated, durable headers | Watch advanced per-record before header parent-chain check |
| `epoch_request_queue` channel | Idempotency dedup state from #633 | Channel never has two live entries for the same epoch | Replacement cancels in-flight successful import |
| Worker `network/mod.rs` peer-score state | Primary network's penalty calls | Penalty applied to PeerId of actual responder | Penalty applied to claimed BLS, not actual libp2p PeerId |
| `auth_last_vote` style maps in scope | Current epoch | Map cleared on epoch transition | Stale entries influence cross-epoch routing decisions |

## Domain-specific red flags (Phase 2 targets)

- [ ] Any `as usize` cast on a peer-supplied length — should be bounds-checked first
- [ ] Any DB write inside a streaming loop with no rollback path on later failure
- [ ] Any non-atomic increment of a counter with concurrent inbound RPC handlers
- [ ] Any `Penalty::Mild` or `Penalty::Light` applied without escalation tracking
- [ ] Any single-shot prune loop with no panic guard / heartbeat / restart
- [ ] Any "trust the response identity" pattern (BLS in payload vs PeerId on transport)
- [ ] Any 32-byte digest used for correlation that lacks (epoch || version || nonce) framing
- [ ] Any per-record timeout with no whole-stream timeout
- [ ] Any watch channel `send` inside a stream-import loop (should be only after final commit)
- [ ] Any cross-table update with multiple `insert` calls outside a single REDB transaction
- [ ] Any `unwrap()`, `expect()`, or panic inside async stream handler — slot leak vector
- [ ] Any drop-on-drop reliance for slot reclamation with no Drop impl that catches panic

## Determinism note (out of scope but worth flagging)

Stream import is local-only — non-determinism in the import path doesn't directly diverge consensus, but it can diverge **what nodes have access to** post-sync. If two honest nodes import the same pack file and one applies write-before-validate orphaning while the other doesn't, their state-sync responses to a third node will differ.

## Test surface gaps to interrogate

The `crates/e2e-tests/tests/it/` files (epochs, restarts, common) define the only end-to-end tests for this path. The Phase 2 agent should read them and ask:

- Do any tests inject a malicious peer with a malformed pack file?
- Do any tests crash a node mid-import and verify consistency on recovery?
- Do any tests verify a slow-stream peer cannot starve the importer pool?
- Do any tests verify the watch channel does not advance during a failing import?
- Do any tests verify the per-peer rate limit holds under concurrent inbound requests?

If a test does not exist for a behavior, the implementation almost certainly has not been exercised against the adversary that behavior was meant to defend against.
