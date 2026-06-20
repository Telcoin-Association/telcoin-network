# ExEx Code-Review Remediation — 2026-06-19 (branch `exex2`)

Source: fresh code review of `git diff main...exex2`. Confirmed findings 2–8
(finding 1 refuted → no action). One commit per finding; **no Claude co-author**.

## Plan (one commit per finding) — DONE
- [x] **#2** canon-stream gap detection → `Lagged` (`21a80367`)
- [x] **#3** graceful reorg handling: `unreachable!` → `handle_canon_commit` from both arms (`c41a5fc3`)
- [x] **#4** bounded `events` channel (latest-wins) (`6339f3f6`)
- [x] **#5** configurable per-ExEx capacity, default 64→256 (`c64a5db4`)
- [x] **#6** stateful indexer example crate (`76f81e55`)
- [x] incidental: fix pre-existing broken `[Future]` intra-doc link (`5f760062`)
- [x] **#7** replay/live `BundleState` asymmetry → crate-level docs (`ac2ea141`)
- [x] **#8** dapp-dev guide + `RethEnv` read surface (`41b3fe81`)
- [x] trailing `cargo +nightly fmt` reflow (`762ead58`)

## Verification (Wave 3) — DONE
- [x] `cargo +nightly fmt` (clean after the reflow commit)
- [x] `cargo clippy --all-targets`: tn-exex / exex-indexer / exex-lifecycle exit 0; tn-node `--lib` exit 0
- [x] `cargo nextest run`: tn-exex 5/5, exex-indexer 1/1, tn-node exex isolation 2/2
- [x] `cargo build -p exex-lifecycle -p exex-indexer` exit 0
- [x] `RUSTDOCFLAGS=-D warnings cargo doc -p tn-exex --no-deps` clean
- [x] final diff review (498 insertions across 11 files; no Claude co-author trailers)

## Known pre-existing (NOT introduced here — do not block on)
- `cargo clippy -p tn-primary --all-targets` fails on `clippy::never_loop` (+ `useless_vec`,
  `field_reassign_with_default`, `needless_borrow`) in unmodified test files
  (`certificate_fetcher_tests.rs`, `certifier_tests.rs`, `recent_blocks.rs`) — fails identically on base.
- `tn-node` integration ("it") test has pre-existing `needless mut` warnings.

## Review

All confirmed findings (2–8) implemented, each as its own commit; no Claude co-author.

**Scope note:** this remediation touches only `crates/exex`, `crates/node` (engine/mod.rs,
manager/node.rs), `examples/*`, and `Cargo.{toml,lock}`. tn-primary / tn-reth were *not*
modified here, so the Wave-3 clippy/test gate covers the crates actually changed
(tn-exex, tn-node lib, the two examples). The pre-existing tn-primary `--all-targets`
test lints and the tn-node `"it"` integration-test lints are unrelated and untouched.

**Notable implementation choices**
- #2/#3: the gap-check lives in one shared `handle_canon_commit`; the `Reorg` arm degrades to
  it (log + treat-as-commit) instead of `unreachable!`. Reorg log uses `Chain::len()` so the
  graceful path can't itself panic.
- #3 broadcast arms: matched `BroadcastStreamRecvError::Lagged(n)` directly in the arm
  (exhaustive, single-variant) rather than an irrefutable `if let`.
- #6: used the `replay_from` + live-loop pattern (not `replay_and_subscribe`). The convenience
  method consumes the context and drops `events`, so it can't both report `FinishedHeight` and
  re-`replay_from` on `Lagged` — the example needs both. Documented this tradeoff in the example.
- fmt: rustfmt reflows doc comments to the workspace width; since findings share files
  (manager.rs spans #2–#5) they can't be split back per-commit without interactive rebase, so
  formatting landed as one trailing `style` commit. The branch tip is fmt-clean.
