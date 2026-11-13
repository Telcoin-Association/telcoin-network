# Verification Report: storage-v2 review, round 3 (T8: node, consensus-side guards, state sync, e2e)
Date: 2026-11-13
Scope: branch sls_storage_v2_3 at HEAD 4acc752fb vs origin/main (merge base a3df64830)
Source: tasks/review-storage-v2/T8.md (raw findings T8-1..T8-3)

## Summary
| # | Title | Severity (initial) | Verdict | Severity (verified) |
|---|-------|-------------------|---------|---------------------|
| T8-2 | Replay/drain closes clear a later epoch's `Votes`/`LastProposed` (equivocation guards) | Medium | PARTIALLY_VALID (mechanism confirmed, reach narrower than claimed) | Medium |
| T8-1 | `db reset-execution` on a `db load-state` datadir: the next start fails at the epoch-0 replay, on every restart | Medium | CONFIRMED | Medium (High arguable) |
| T8-3 | No lane runs the v0.16.0-adiri upgrade path; the README presents it as the release path | Low | CONFIRMED | Low |

Verification: 3 submitted -> 3 valid (2 CONFIRMED, 1 PARTIALLY_VALID), 0 false positives (0%).
T8-1 and T8-2 were each verified alone (Tier 1). T8-2 got a second, independent verifier
(the re-verification rule: MEDIUM confidence on reachability). T8-3 was verified alone. All
verifiers were read-only. The coordinator spot-checked the load-bearing lines of each verdict at HEAD.

Tests run by verifiers: `cargo nextest run -p tn-storage -E
'test(=consensus::test::test_is_epoch_complete_fresh_epoch0_pack)'` -> 1 passed. It pins the T8-1
step "a by-number read on a freshly opened empty epoch-0 pack returns
`Err(PackError(ConsensusNumberTooHigh))`, not `Ok(None)`". No other tests were run. The
existing `current_epoch_guard_row_*` / `stale_epoch_table_row_*` tests (close_epoch.rs:1771-1888)
cover only the entry-time helper, not the close-time clear.

## Findings

### T8-2. Replay/drain closes clear a later epoch's equivocation guards (`Votes`, `LastProposed`)
- **Verdict**: PARTIALLY_VALID. Verifier 1: CONFIRMED, HIGH confidence on the mechanism, MEDIUM
  on reachability. Verifier 2: PARTIALLY_VALID, HIGH on mechanism and gates.
- **Severity**: initial Medium, verified Medium. Both verifiers chose Medium and so does the
  coordinator.
- **Location**: `crates/node/src/manager/node/close_epoch.rs:1089-1102` (the clear),
  call sites `run_epoch.rs:254` (replay-and-close), `run_epoch.rs:654` (`finish_epoch`: the live
  boundary arm and the leftover drain), `close_epoch.rs:692` and `:780` (entry-time recovery);
  `crates/consensus/primary/src/network/handler.rs:1365-1461` (vote handler);
  `crates/consensus/primary/src/proposer.rs:254-273`; `crates/tn-reth/README.md:903-925`.

**Mechanism (confirmed by both verifiers and the coordinator):**
- `db reset-execution` moves only `db`, `static_files` and `restored-state-floor`
  (`crates/telcoin-network-cli/src/db/reset_execution.rs:7-8, 34-40`). `consensus-db` stays, so
  epoch K's `Votes`/`LastProposed`/`ProposedCertificates` rows survive the reset.
  `refuse_interrupted_reset` (:72-83) checks only for a `.partial` directory.
- After the reset the tip is genesis. The node enters epoch 0 (`start_epoch.rs:230-271`,
  `tn-reth/src/env/epoch.rs:343-355`), and `recover_previous_epoch_record(0)` returns at once
  (`close_epoch.rs:565-567`). The local epoch-0 pack replays to the boundary, and
  `run_epoch.rs:246-255` runs `close_epoch` -> `write_epoch_record` ->
  `clear_consensus_db_for_next_epoch()`.
- That clear takes no epoch and clears `LastProposed` (:1096) and `Votes` (:1097) whole. Epoch K's
  rows are gone after the first replay-close.
- The epoch-aware helpers run only on entry. `stale_first_row` (:1307-1315) flags `epoch <
  current`, and `current_epoch_guard_row` (:1324-1334) protects only `epoch == current`. Neither
  sees rows of a *later* epoch, and no close path compares row epochs against the epoch it closes.
- The `Votes` row is the only per-(author, round) vote guard. With no row, `read_vote_info`
  returns `None` and the vote-once block (handler.rs:1365-1444) is skipped. The handler then checks
  `is_active_cvv()` (:1448) and signs (:1452-1461). The certificate tables and digest indexes are
  cleared too, and the in-memory `auth_last_vote` cache starts empty after a restart.
- `LastProposed` is cleared as well, so `Proposer::new` (proposer.rs:257-273) starts at
  `primary_round` with no memory of the round it last proposed.
- The README "In place" section (`tn-reth/README.md:903-925`) offers this flow to validators with
  no stay-stopped rule. The fresh-datadir case (:944-946) and Rollback (:978-980) both state one.
- No test covers later-epoch guard rows surviving any close.

**What narrows the claim (why PARTIALLY_VALID, and why Medium rather than High):**
- **Mode gate.** The node starts `CvvInactive` (`node_mode_from_epoch_record`, node.rs:1991-2021),
  and the mode stays inactive (`identify_node_mode`, start_epoch.rs:1339-1346). Before promotion
  the vote path refuses with `NotActiveCvv` (handler.rs:860-863, 1448-1451). Promotion needs one
  of two rules:
  - `rejoin_ready` (subscriber.rs:136-149): a fresh head, and handled output within 20 rounds of it.
  - `stalled_head_ready` (subscriber.rs:173-192): the head unchanged for 20 s or more.
- **Round gate.** `prime_consensus` (state-sync lib.rs:79-126) sets `committed_round` to the
  last executed leader round. The vote path's header check
  (`state_sync/header_validator.rs:94-100`) refuses `TooOld` for `round < committed_round`.
  Coordinator-checked at HEAD.
- **The window this leaves.** A re-vote can only hit rounds from the committed round at rejoin
  up to the node's highest pre-reset vote round. On a progressing network the rebuild from
  genesis takes hours across K epochs, so the window is empty. It is non-empty only if the
  network stalled for the whole rebuild (the node rejoins through the stalled-head rule).
- **The conflicting header.** Honest authors re-send their byte-identical stored header, so a
  conflict needs either a Byzantine author or another validator that also lost its guards (e.g.
  several validators rebuilding in place at once).
- **Own proposals.** In the stalled case the node can also build a second header for a round it
  already proposed in. A lone reset node costs liveness only: peers with intact `Votes` answer
  `AlreadyVoted`. Several validators rebuilt in place together can lose safety: two
  certificates for one (author, round).
- **Severity call.** The flow loses a durable consensus guard through a documented operator path,
  with no refusal and no doc warning: a fail-closed rule with a gap under a specific condition
  (Medium). The guard loss can turn into an equivocation only on a stalled network plus a second
  conflicting header. It becomes High if the round gate is relaxed, or if validators are told to
  rebuild in place together.

**Proposed fix (smallest; no hot path, epoch close only):**
Make the clear epoch-aware for the two guard tables, keeping rows of epochs after the one being
closed. `Votes` holds at most one row per authority and `LastProposed` one row, so a scan is
cheap. Pass the closed epoch at the four call sites: `committee.epoch()` at run_epoch.rs:254,
`epoch` at :654, `previous_epoch` at close_epoch.rs:692, `current_epoch - 1` at :780.

```rust
// close_epoch.rs
pub(super) fn clear_consensus_db_for_next_epoch(&self, closed_epoch: Epoch) -> eyre::Result<()> {
    self.consensus_db.clear_table::<Payload>()?;
    self.consensus_db.clear_table::<NodeBatchesCache>()?;
    self.consensus_db.clear_table::<CertificateDigestByRound>()?;
    self.consensus_db.clear_table::<CertificateDigestByOrigin>()?;
    self.consensus_db.clear_table::<ProposedCertificates>()?;
    // a `db reset-execution` rebuild replays and closes epochs older than one this node voted or
    // proposed in: never take that later epoch's equivocation guards
    clear_guard_rows_through::<LastProposed, _>(&self.consensus_db, closed_epoch, Header::epoch)?;
    clear_guard_rows_through::<Votes, _>(&self.consensus_db, closed_epoch, VoteInfo::epoch)?;
    self.consensus_db.clear_table::<Certificates>()?;
    Ok(())
}

/// Remove `T`'s rows of `closed_epoch` or earlier; keep rows of later epochs (logged).
fn clear_guard_rows_through<T: Table, DB: TNDatabase>(
    db: &DB,
    closed_epoch: Epoch,
    epoch_of: fn(&T::Value) -> Epoch,
) -> eyre::Result<()> {
    if db.iter::<T>().all(|(_, v)| epoch_of(&v) <= closed_epoch) {
        return db.clear_table::<T>(); // the common case, unchanged
    }
    warn!(target: "epoch-manager", table = T::NAME, closed_epoch,
          "keeping equivocation-guard rows of a later epoch across a replayed epoch close");
    let stale: Vec<T::Key> =
        db.iter::<T>().filter(|(_, v)| epoch_of(v) <= closed_epoch).map(|(k, _)| k).collect();
    for key in stale {
        db.remove::<T>(&key)?;
    }
    Ok(())
}
```

**Why the kept rows are safe:**
- No primary runs during replay-close epochs.
- On entry to K, `current_epoch_guard_row` sees rows `== K` and keeps them, so the vote check at
  handler.rs:1370-1416 refuses again.
- `Proposer::new` filters `LastProposed` to the current epoch (proposer.rs:265-269).
- At K+1 the rows are older than the current epoch and are cleared as usual.

The same filter can cover `ProposedCertificates` if the certifier guard (certifier.rs:586-625)
should also survive.

**Regression test:** seed a `Votes` row and a `LastProposed` header of epoch 3, call the clear with
`closed_epoch = 0`, and assert both rows survive and the epoch-0 rows are gone. A fuller version
drives the `replay_anchored_on_block1_stops_at_the_epoch_close` fixture through `run_epoch(Initial)`.

**README** (`tn-reth/README.md`, "In place"): add "a validator stays stopped after the reset until
the network has closed the epoch it was in" until a release carries the fix. This is the rule the
fresh-datadir and Rollback sections already state.

### T8-1. `db reset-execution` on a snapshot-restored datadir: the next start fails at the epoch-0 replay, on every restart
- **Verdict**: CONFIRMED (HIGH confidence).
- **Severity**: initial Medium, verified Medium. The verifier proposed High; the coordinator
  calibrated it down (see below).
- **Location**: `crates/telcoin-network-cli/src/db/reset_execution.rs:115-135` (no check on
  consensus packs); `crates/telcoin-network-cli/src/db.rs:1296-1300, 1316-1340, 1367-1379`
  (load-state saves records 0..=N, imports only `epoch-{n}`, hint `(n, final)`);
  `crates/storage/src/consensus.rs:516-541, 678-718, 1575-1594`;
  `crates/storage/src/consensus_pack.rs:5134-5142`; `crates/state-sync/src/lib.rs:283-318`;
  `crates/tn-reth/README.md:912, 920-923`; `crates/telcoin-network-cli/README.md:472-477`.

**Evidence (traced at HEAD):**
- **What load-state writes.** It writes the consensus pack for the restored epoch N only (db.rs
  1316-1340, coordinator-checked) plus records 0..=N and the slot hint `(N, final)`. No
  `epoch-0..epoch-{N-1}` directories exist. Nothing in production prunes epoch packs, so a node
  that synced from genesis always has them; the case is specific to restored datadirs.
- **What the reset leaves.** The reset touches no consensus data. The next start opens hint-epoch
  pack N (consensus.rs:516-541) and passes the startup checks: no finalized header, tip-0
  exemption at node.rs:1966, the record-0 guard at node.rs:1293.
- **No pack fetch below N.** `request_missing_packs` starts at `latest_consensus_epoch()` = N
  (state-sync consensus.rs:974-981, 302-338). `run_epochs` does not wait for fetches, and the
  only other pack request in `run_epoch` is after the replay (run_epoch.rs:365-373).
- **The failing read.** The node enters epoch 0. `new_epoch` opens a new, empty `epoch-0` pack via
  `open_append` and seals N (consensus.rs:678-718). `get_missing_consensus` reads
  `consensus_header_latest()` (epoch N's final header F > 0), then `consensus_header_by_number(1)`.
  `number_to_epoch(1)` gives 0, the empty pack returns `ConsensusNumberTooHigh`
  (consensus_pack.rs:5134-5142), and with no staging pack (staging is removed at `new`) the error
  propagates. The path is lib.rs:309 -> start_epoch.rs:148 -> run_epoch.rs:245 -> `run_epochs`
  (node.rs:1779-1781) -> node exit. There is no fallback to a peer sync.
- **Every restart fails the same way.** The hint still names N, and the empty `epoch-0` pack
  stays on disk. The error names only `ConsensusNumberTooHigh`, with no remedy.
- **The docs promise otherwise.** `tn-reth/README.md:920-923` says that on a datadir restored from
  a state pack the rebuild "executes and compares every epoch, the restored one included".
- **No test** combines `load-state` with `reset-execution`: `resync.rs` resets a full-history
  validator, and `state_export_import.rs` never resets.

**Severity calibration:** the failure is closed. Nothing is lost: the old `db/`,
`static_files/` and floor are kept under `execution-reset-*`. The README's Rollback ("move the
entries of `execution-reset-<UTC time>/` back") restores the node. `new_epoch` does not rewrite the
slot hint (only `write_latest_consensus_hint`, consensus.rs:1860, does), so after a rollback
`ConsensusChain::new` reopens epoch N as before. The stray empty `epoch-0` pack is left behind;
the coordinator read this rather than ran it. That fits Medium (a documented flow fails with a
bad diagnostic and a manual recovery). It would be High ("a step that leaves a datadir no binary
opens") only if the rollback did not work, which a PoC test should pin.

**Proposed fix (smallest; CLI + docs, no hot path):** refuse in `reset_execution` before planning
the moves.

```rust
// crates/telcoin-network-cli/src/db/reset_execution.rs, in reset_execution after the PID lock
// A `db load-state` datadir holds consensus only from the restored epoch; a rebuild from genesis
// replays epoch 0 from the local packs first and has none to replay.
let floor = tn_reth::RethEnv::restored_state_floor_marker(datadir);
let epoch0 = datadir.epochs_db_path().join("epoch-0");
if exists(&floor)? || !exists(&epoch0)? {
    bail!(
        "{} was restored from a state snapshot (`db load-state`) and has no consensus output \
         before the restored epoch, so it cannot rebuild execution from genesis. Start a fresh \
         datadir that keeps node-keys and config and let it sync from genesis",
        datadir.display()
    );
}
```

- The `epoch-0` check is needed because a second reset has already moved the floor aside.
- Add a unit test: a datadir with `restored-state-floor` and no `epoch-0` is refused and nothing
  moves.

**Docs:**
- Replace `tn-reth/README.md:920-923` with: "`db reset-execution` refuses a datadir bootstrapped
  with `db load-state` (it holds consensus only from the restored epoch); rebuild such a node in a
  fresh datadir."
- Qualify :912 ("and from peers"): the local replay comes first.
- Add the same caveat at `telcoin-network-cli/README.md:472-477`.

**Longer term (not required):** `request_missing_packs` could start at the executed epoch rather
than the hint epoch. `run_epoch` could then wait for missing packs before the replay instead of
failing.

### T8-3. No lane runs the v0.16.0-adiri upgrade path
- **Verdict**: CONFIRMED (HIGH confidence).
- **Severity**: initial Low, verified Low.
- **Location**: `Makefile:257-263, 273, 421-424, 490-491, 501-511`;
  `etc/test-and-attest.sh:159-176`; `.github/workflows/durable-e2e.yaml:80-81, 99-103`;
  `crates/e2e-tests/tests/it/resync.rs:413-460`; `crates/tn-reth/README.md:869-887`.

**Evidence:**
- **The lanes' previous binary.** Both lanes use `TN_RESYNC_BIN_PREV ?= $(E2E_BIN_MDBX)` (Makefile:506), which is this tree
  built with `telcoin-network/mdbx`. That feature switches only the reth backend: the consensus DB
  is tndb in both builds (`crates/storage/Cargo.toml:64-65`), and `layout.rs:285-294` writes the
  marker ungated.
- **The two branches never run.** `consensus_on_mdbx` (resync.rs:415) is always false in a lane,
  and the marker-recording branch (:457-460) never runs.
- **The manual target.** `make test-upgrade` is manual and pinned to `TN_PREV_REF ?= ba7654d8a`
  (Makefile:273), not a release.
- **Mitigations:**
  - The gap is written down in the attest script (:170-174) and the Makefile (:501-505). The
    reason is that v0.16.0-adiri and this tree do not peer since #1788, so the mixed-committee
    resync design cannot cover the whole-network upgrade even by hand.
  - Both code paths have unit tests: `tn_storage` `test_migrate_from_mdbx_*` (storage/src/lib.rs
    481-713) and `tn_reth` `env::layout::a_datadir_without_the_marker_is_v1_and_gets_it`
    (layout.rs:580).
  - What is missing is an end-to-end run on real v0.16.0-adiri on-disk state, and a README line
    saying so.

**Proposed fix:**
- Add after `tn-reth/README.md:887`: "No automated lane runs this upgrade from v0.16.0-adiri: the
  e2e lanes start from this tree's `mdbx` build, which already keeps the consensus DB in tndb and
  writes the layout marker. The consensus-DB migration and the marker recording on a pre-marker
  datadir are covered by unit tests (`test_migrate_from_mdbx_*`,
  `a_datadir_without_the_marker_is_v1_and_gets_it`); the whole-network upgrade was verified by
  hand on <date>."
- For real coverage: a manual target (e.g. `make test-upgrade-all-together
  TN_PREV_REF=v0.16.0-adiri`) that stops all PREV validators and restarts them together on this
  build. That avoids the peering break and exercises both resync.rs branches.

## False positives eliminated
None.

## Action items (by verified severity)
1. T8-2 (Medium): make `clear_consensus_db_for_next_epoch` keep `Votes`/`LastProposed` rows of
   epochs after the closed one (four call sites). Add the regression test and the README "stay
   stopped" line.
2. T8-1 (Medium): `db reset-execution` refuses a datadir with `restored-state-floor` or without
   `consensus-db/epochs/epoch-0`. Fix README :912/:920-923 and the CLI README. Add a PoC/IT
   asserting the refusal, and that Rollback restores a datadir after the failed start.
3. T8-3 (Low): README note on the upgrade path's coverage; optionally an all-together upgrade target.
