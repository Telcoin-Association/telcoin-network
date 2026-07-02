# Code Review: simplified node startup stack (main..startup-5-e2e-startup-flows)
Date: 2026-07-01
Scope: 5-commit stack — bootstrap decoupling, drop --observer, record-sync gate + app-level
bring-up, gossip-after-gate + startup role seed, e2e startup tests. Files:
crates/config/src/{network,node}.rs, crates/types/src/committee.rs,
crates/node/src/manager/node.rs, crates/node/src/manager/node/{run_epoch,start_epoch}.rs,
crates/state-sync/src/{epoch,lib,consensus}.rs, crates/telcoin-network-cli/src/{node,cli}.rs,
crates/telcoin-network-cli/README.md,
crates/e2e-tests/tests/it/{startup,epochs,main,common,restarts}.rs, etc/local-testnet.sh.

## Summary
Consolidated tn-review pass plus a 10-agent tn-security-eval over the full stack. Two
per-PR domain reviews (epoch/networking/consensus) had already approved PRs 3 and 4. The
final gate surfaced and fixed a High gate-stall on simultaneous cohort boot (via
RECORD_SYNC_PASS_CAP) and fixed the double-log nit. All remaining findings were verified
benign, refuted, acknowledged as bounded/operator-facing, or determined
pre-existing/out-of-scope. Note on #5: an attempted fix (binding listeners after the gate)
was implemented, found to break full-cohort-restart liveness in e2e, and reverted — the
finding is documented as a bounded known trade-off of app-scope bring-up.

| # | Title | Severity | Category | Status |
|---|-------|----------|----------|--------|
| 0 | Gate burns its full 20s deadline on every simultaneous cohort boot | High | Bugs | CONFIRMED-FIXED (RECORD_SYNC_PASS_CAP) |
| 4 | Listener address double-logged in bring-up info line | Informational | Bugs | FIXED |
| 5 | App-scope listener opens before its per-epoch drainer, buffering inbound requests | Medium | DoS | ACKNOWLEDGED-BOUNDED (fix reverted: broke cohort restart) |
| 1 | Gate cancellation makes the epoch-DB `final_numbers` drift window newly reachable | Low | Concurrency | REFUTED (unreachable + benign + self-heals) |
| 2 | Gate can conclude quiescent on repeated invalid records from a byzantine peer | Low | Consensus Safety | REFUTED (stale seed superseded before any reader) |
| 10 | Gate can silently no-op in a dial race (peer count includes dialing; requests need established) | Medium | Bugs | ACKNOWLEDGED-BOUNDED (never incorrect; follow-up) |
| 6 | `--observer` removal removes the same-key warm-standby non-signing brake | Medium | Elevation of Privilege | ACKNOWLEDGED — operator migration guidance (fail-closed CLI) |
| 7 | `NetworkConfig.bootstrap_peers` non-empty fully overrides genesis set | Medium | Tampering | ACKNOWLEDGED — requires local config write; eclipse only, records self-authenticating |
| 8 | `epoch_committee_valid` 2/3 tolerance uses `(len/3)*2` not `(len*2)/3` | Medium(crypto)/INFO(consensus) | Crypto | OUT OF SCOPE — pre-existing, unchanged by stack; safe under f<n/3 |
| 9 | `collect_epoch_records` runs BLS verify_with_cert before cheap checks | Low | DoS | ACKNOWLEDGED — pre-existing; cheap future hardening (short-circuit) |
| 11 | `request_missing_packs` fills bounded 1024 queue before workers spawn | Low | Bugs | ACKNOWLEDGED-BOUNDED (needs 1024+ epochs behind; follow-up) |
| 3 | Gossip on app topics dropped during the gate window | Informational | Architecture | disclosed by design |

## Findings

### 0. Gate burns its full 20s deadline on every simultaneous cohort boot
- **Severity**: High — **CONFIRMED-FIXED**
- **Location**: `crates/state-sync/src/epoch.rs` (`sync_epoch_records_to_tip`)
- **Claim**: When a whole cohort boots/restarts at once, each node's gate pass issues
  `request_epoch_cert` to peers that are connected but not yet serving; the request neither
  succeeds nor fails fast, so the pass hangs to the 20s `RECORD_SYNC_DEADLINE`, delaying
  epoch start and RPC availability ~20s on every node. Confirmed empirically:
  `test_epoch_boundary`/`test_epoch_sync` failed their 20s RPC-availability deadline.
- **Fix**: `RECORD_SYNC_PASS_CAP` (5s) bounds a single pass; on cut, the DB is consulted for
  real progress (records save as they verify) — progress ⇒ continue from the DB's latest
  epoch, none ⇒ exit as starved and let the maintenance collector take over. 20s deadline
  retained as backstop.
- **Verification (findings-verifier, anti-confirmation-bias)**: CONFIRMED-FIXED (HIGH
  confidence). Multi-pass catch-up reaches the tip against responsive peers (equality clause
  fires as soon as tip is reached, not at the deadline); the loop provably terminates on all
  four exits; premature quiescent-below-tip with responsive peers is impossible (a
  timeout-progress pass sets `pass = db_latest > previous_pass`, so equality can only fire in
  the `Ok` branch when the next fetch fails, which honest peers don't). The one reachable
  early exit (false starvation when an honest fetch would finish just after 5s) is benign:
  verify-then-save is idempotent, the collector re-fetches, `open_epoch_pack` waits 30s, and
  it only affects the advisory startup seed.
- **Relevant Files**: crates/state-sync/src/epoch.rs, crates/node/src/manager/node.rs

### 4. Listener address double-logged in bring-up info line — FIXED
- **Severity**: Informational — **FIXED**
- **Location**: `crates/node/src/manager/node.rs` (bring-up listener log)
- **Fix**: the primary listener log dropped the redundant message interpolation, keeping the
  structured `?primary_address` field: `"primary network listening"`.

### 5. App-scope listener opens before its per-epoch drainer, buffering inbound requests
- **Severity**: Medium — **ACKNOWLEDGED-BOUNDED** (attempted fix reverted)
- **Location**: `crates/node/src/manager/node.rs` (app-scope bring-up)
- **Claim**: `start_listening` moved to app scope (the point of PR3) binds the listener before
  the per-epoch `PrimaryNetwork` drains `primary_network_events`, a
  `QueChannel::new_always_subscribed()` mpsc (capacity 10,000) that `try_send`-buffers inbound
  request-response events with no receiver taken (overflow is dropped with a log, so it is
  *bounded* at 10k messages, not unbounded). During the pre-consensus startup window, inbound
  requests from connected peers accumulate up to that bound before the epoch loop drains them.
- **Attempted fix and why it was reverted**: binding both listeners *after* the gate (so the
  gate uses only outbound connectivity) shrank the window — but the e2e suite showed it
  **breaks full-cohort restart liveness** (`test_startup_restart_mid_epoch_and_full_restart`
  failed "Network not responding within 45s"): when every node defers listening until after
  its own gate, no node is dialable by its restarting peers during startup, so the cohort
  cannot re-form connections promptly. Listening before the gate is load-bearing for
  simultaneous restart. The move was reverted; the code comment now records why the listener
  binds before the gate.
- **Residual assessment**: bounded (10k-message cap, drop-on-full), requires an attacker that
  is a connected peer flooding max-size requests during a restart window, and the memory is
  released seconds later when the epoch loop's drainer starts. Never affects correctness. The
  window is inherent to hoisting bring-up to app scope; a proper fix (bound the pre-consensus
  event buffer, or stand up an early minimal drainer that the per-epoch handler hands off
  from) is a scoped follow-up, not a startup-reorder.
- **Relevant Files**: crates/node/src/manager/node.rs, crates/consensus/primary/src/consensus_bus.rs, crates/types/src/sync.rs

### 10. Gate can silently no-op in a dial race (peer count includes dialing; requests need established connections)
- **Severity**: Medium — **ACKNOWLEDGED-BOUNDED** (never incorrect; follow-up)
- **Location**: `crates/state-sync/src/epoch.rs` (`sync_epoch_records_to_tip`), `crates/node/src/manager/node.rs` (`wait_for_network_peers_soft`)
- **Claim**: `connected_peer_count()` resolves to the peer manager's
  `connected_or_dialing_peers()` (includes peers still in `Dialing`), but the gate's
  `request_epoch_cert` → `send_request_any` selects from the swarm's `connected_peers` deque
  (populated only on `ConnectionEstablished`) and returns `NoPeers` instantly otherwise. In a
  dial race (dials issued, QUIC handshake not yet complete), the soft wait returns early and
  the gate's zero-peer short-circuit is skipped, yet two fast `NoPeers` passes satisfy the
  equality-quiescence rule — so the gate can exit having synced nothing. Verified against the
  code (count path and established-only request path both confirmed).
- **Assessment**: never causes incorrect behavior. The stale role seed is superseded by
  `identify_node_mode` (execution tip) before any consensus reader; missing records are
  backfilled by the maintenance collector and `open_epoch_pack`'s 30s wait. Worst case the
  gate degrades to the pre-existing collector-driven catch-up (the status quo before this
  feature). A proper fix is an established-only peer-readiness check before the gate — a
  network-libp2p API addition (new established-connection count) outside this stack's scope,
  filed as a follow-up. Not fixed here to avoid widening the change surface into the network
  layer late in the stack.
- **Relevant Files**: crates/state-sync/src/epoch.rs, crates/node/src/manager/node.rs, crates/network-libp2p/src/peers/all_peers.rs, crates/network-libp2p/src/consensus.rs

### 11. `request_missing_packs` fills a bounded 1024 queue before its workers spawn
- **Severity**: Low — **ACKNOWLEDGED-BOUNDED** (follow-up)
- **Location**: `crates/node/src/manager/node.rs` (`request_missing_packs` before the fetch-worker spawn loop), `crates/state-sync/src/consensus.rs` (`request_epochs`)
- **Claim**: `request_missing_packs` → `request_epochs` does one `send().await` per missing
  pack into a bounded 1024-capacity channel whose three consumer workers spawn only after the
  call returns; a node more than 1024 epochs behind its record tip would block forever inside
  `request_missing_packs` and never reach the epoch loop. The record-sync gate makes large
  fills more reachable by populating the full certified record chain synchronously before this
  call (on `main` the collector populated it asynchronously, so the queue was typically small
  here).
- **Assessment**: requires 1024+ epochs behind — unreachable at mainnet epoch durations for
  years, reachable only on short-epoch testnets after a very long outage. Pre-existing
  ordering (`main` also spawned the workers after this call). Cheap follow-up: spawn the fetch
  workers before `request_missing_packs`, or use a non-blocking enqueue. Not fixed here as it
  is pre-existing and non-blocking for realistic deployments.
- **Relevant Files**: crates/node/src/manager/node.rs, crates/state-sync/src/consensus.rs

### 1. Gate cancellation makes the epoch-DB `final_numbers` drift window newly reachable
- **Severity**: Low — **REFUTED** (HIGH confidence)
- **Location**: `crates/storage/src/epoch_records.rs:216-239`
- **Claim**: `save`/`save_record` mutate in-memory `final_numbers` before the async actor
  `send(...).await`; the gate is the first caller to cancel `collect_epoch_records` mid-flight,
  so a cancellation on that send could leave `final_numbers` one entry ahead of the store.
- **Verification**: the mutate-before-send ordering is real, but the send only yields
  `Pending` (the sole cancellation point) when the bounded 1000-slot DB channel is full,
  which cannot happen during the gate (single strictly-sequential producer, dedicated drain
  thread, at most a handful in flight — the future is essentially always suspended at
  `request_epoch_cert().await`, not inside `save`). Even if it occurred, `final_numbers`
  would hold the correct value, all consumers degrade gracefully on a not-yet-stored record,
  and it is in-memory-only (rebuilt on restart) and overwritten idempotently by the collector.
  Defense-in-depth option only: move `update_finals` onto the DB thread.

### 2. Gate can conclude quiescent on repeated invalid records from a byzantine peer
- **Severity**: Low — **REFUTED** (HIGH confidence)
- **Location**: `crates/state-sync/src/epoch.rs` (`sync_epoch_records_to_tip` + `record_sync_quiescent`)
- **Claim**: a peer serving an invalid record for epoch N+1 on two consecutive passes makes
  the gate exit quiescent at N below the true tip; the startup role seed then reads a stale
  committee.
- **Verification**: mechanism confirmed, but the stale seed is provably harmless. The seed
  writes only `CvvActive`/`Observer` (never `CvvInactive`); `identify_node_mode` — the first
  line of `create_consensus` — re-derives the mode from per-epoch execution/on-chain state
  before any consensus-driving `node_mode` reader is spawned (the only tasks spawned in the
  seed→override window, the record and vote collectors, read neither). The collector retries
  from epoch 0 every 5s and `request_epoch_cert` tries up to 3 peers, so a single byzantine
  peer cannot reliably poison two consecutive passes if any honest peer is reachable.

### 6. `--observer` removal removes the same-key warm-standby non-signing brake
- **Severity**: Medium — **ACKNOWLEDGED** (operator migration guidance; no code change)
- **Location**: `crates/node/src/manager/node/start_epoch.rs` (`identify_node_mode`); `crates/config/src/node.rs`; `crates/telcoin-network-cli/src/node.rs`
- **Claim**: role now derives purely from committee membership; there is no longer a way to
  run a node holding a committee BLS key as a forced non-signing Observer. An HA operator
  running a warm standby with the *same* committee key (previously kept safe by `--observer`)
  who mis-migrates could turn the standby into an active co-signer → equivocation/slashing.
- **Assessment**: not remotely exploitable (requires already holding a committee key, which
  enters the committee only via on-chain staking+activation); fail-closed at the CLI (a stale
  `--observer` is now a clap parse error, forcing the operator to notice); `Config.observer`
  was never persisted, so no silent on-disk flip; a single equivocator stays within f<n/3.
  Remediation is operator migration documentation, not a code brake. This is the one
  user-visible semantic change of the stack — the PR body's BREAKING CHANGE note covers it.

### 7. `NetworkConfig.bootstrap_peers` non-empty fully overrides the genesis set
- **Severity**: Medium — **ACKNOWLEDGED** (bounded; requires local config write)
- **Location**: `crates/node/src/manager/node.rs` (run(): `if !bootstrap_peers().is_empty() { override }`)
- **Claim**: a non-empty `bootstrap_peers` fully replaces (not merges) the genesis bootstrap
  set; an attacker with write access to the network-config file can insert one entry and
  eclipse the node onto an attacker-chosen peer set.
- **Assessment**: blast radius is eclipse/liveness only — epoch records are self-authenticating
  (2f+1 BLS cert + parent-hash chain + committee-validity), so no forged state can be injected
  even to a fully eclipsed node; committee dials derive from on-chain state via signed kad
  discovery, independent of the bootstrap seed, so the node self-corrects. An attacker with
  config-file write already subsumes this (can stop the node / corrupt keys). Full-override
  (vs merge) is the intended semantics for operator-run private bootstrap fleets. No change.

### 8. `epoch_committee_valid` 2/3 tolerance uses `(len/3)*2` not a true 2/3 floor
- **Severity**: Medium (crypto agent) / INFO (consensus agent) — **OUT OF SCOPE (pre-existing)**
- **Location**: `crates/state-sync/src/epoch.rs` `epoch_committee_valid` Greater arm; `crates/types/src/primary/epoch.rs` `verify_with_cert`
- **Claim**: the Greater-arm tolerance `(committee_len / 3) * 2` under-approximates a true 2/3
  for N≡2(mod3) (N=8 accepts a 4-member subset whose `super_quorum` is only 3 sigs), eroding
  the certification margin on the trustless-sync path.
- **Assessment**: verified UNCHANGED by this stack (`git diff` on both functions is empty
  aside from new tests) — pre-existing logic the stack only *reaches* from the new gate. Not a
  break under the protocol's `<N/3` byzantine bound: honest committee members never sign a
  reduced-committee variant, so a sub-N/3 coalition cannot reach even the reduced quorum (the
  consensus-safety agent downgraded it to INFO on exactly this basis). Out of scope for this
  stack; worth a separate pre-existing-hardening ticket.

### 9. `collect_epoch_records` runs the BLS `verify_with_cert` before cheaper checks
- **Severity**: Low — **ACKNOWLEDGED** (pre-existing; cheap future hardening)
- **Location**: `crates/state-sync/src/epoch.rs` (the three predicates are `let`-bound before the `if`)
- **Claim**: `epoch_valid = verify_with_cert(...)` is always evaluated regardless of
  `parents_match`/`epoch_committee_valid`, so a peer can force an O(committee_size) BLS
  aggregate verify per bogus record; unbounded via the background collector's 5s cadence.
- **Assessment**: pre-existing evaluation order the stack newly reaches from the gate. It
  cannot be driven past the real tip (a valid record needs a real chained committee + quorum
  the attacker can't forge). Cheap future hardening: reorder to `parents_match &&
  epoch_committee_valid && verify_with_cert(...)` to short-circuit the BLS work. Left as a
  follow-up since it is pre-existing and non-blocking.

### 3. Gossip on app topics dropped during the gate window
- **Severity**: Informational — disclosed by design. Missed gossip is recoverable via record
  sync / `request_epoch_cert` / fetch-by-number.

### 4. Listener address double-logged in bring-up info line
- **Severity**: Informational — style nit carried verbatim from the old per-epoch code.

## Verification
findings-verifier (anti-confirmation-bias: verifiers received only claim + key question +
file pointers, instructed to refute). #0 CONFIRMED-FIXED (HIGH). #1, #2 REFUTED (HIGH each).
#5 fix validated by the full e2e suite. #3/#4 Informational (skipped). Security-eval agents
(10) produced no CRITICAL/HIGH beyond #0; findings #6-#9 are operator-facing, bounded, or
pre-existing as noted. Determinism, contract-safety, dependency-audit, and DREAD returned no
findings above threshold.
