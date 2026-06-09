# Pin closing-epoch execution block as source of truth for epoch state

## Risk validation (DONE)
- [x] Confirm epoch-N `EpochVote` signers are a subset (in order) of pinned `committee(N)`.
  VERDICT: **SAFE**. `manage_epoch_votes` (epoch_votes.rs:51-53) builds both the membership
  gate (`committee_keys`) and the bitmap indices (`committee_index`) from the SAME
  `epoch_rec.committee` Vec that is persisted and later read by `verify_with_cert`. The bitmap
  is constructed against the exact Vec it is verified against — indices align by construction.
  Persist the pinned `committee(N)` (plan's primary path). No fallback to `prev.next_committee`.

## Part A — RethEnv: block-pinned BLS-pubkey read (crates/tn-reth/src/lib.rs)
- [x] Extract `bls_pubkeys_for_epoch_inner(&self, header: &SealedHeader, epoch) -> Result<Vec<Bytes>>`
- [x] Keep `bls_pubkeys_for_epoch` as thin canonical-tip wrapper over inner
- [x] Add `bls_pubkeys_for_epoch_at_block(&self, block_hash, epoch)` (sealed_header_by_hash → inner)

## Part B — Unify BLS decode to hard-fail (crates/node/src/engine/inner.rs, mod.rs)
- [x] Add private `decode_bls_keys(raw: Vec<Bytes>) -> Result<Vec<BlsPublicKey>>` (collect Result)
- [x] Change `validators_for_epoch` (inner.rs:347) to use it (drop silent filter_map)
- [x] Add `validators_for_epoch_at_block(block_hash, epoch)` in inner.rs
- [x] Expose async wrappers on ExecutionNode (mod.rs): `validators_for_epoch_at_block`,
      `sealed_header_by_hash`

## Part C — Verified closing-block capture
- [x] `RecentBlocks::execution_block_for_consensus(hash) -> Option<SealedHeader>` (recent_blocks.rs)
      + 2 unit tests (closing matches; empty-extra_data sibling rejected) — PASS
- [x] `ConsensusBusApp::wait_for_consensus_execution_block(hash) -> Result<SealedHeader, _>`
      (consensus_bus.rs) + unit test — PASS
      NOTE: method lives on ConsensusBusApp (same impl as wait_for_consensus_execution, which is
      what close_epoch calls on self.consensus_bus: ConsensusBusApp).

## Part D — EpochManager field + write_epoch_record (node.rs, epoch.rs)
- [x] New field `closing_epoch_state_blockhash: Option<BlockHash>` + doc, init None
- [x] `close_epoch`: &self → &mut self; use wait_for_consensus_execution_block; set field
      (verified all 3 callers pass epoch-close hashes ⇒ closing block always produced ⇒ no hang)
- [x] `write_epoch_record`: pin committee/next_committee/final_state to close_hash;
      downgrade cross-check error → warn!

## Verification (ALL GREEN)
- [x] Production lib clippy clean across tn-reth, tn-primary, tn-node (no warnings)
- [x] cargo nextest --lib (3 crates): 144 passed, 0 failed (incl. epoch_votes, test_close_epochs,
      test_get_worker_fee_configs)
- [x] 3 new unit tests pass
- [x] e2e `test_epoch_boundary` PASS (120.7s) — built with my changes; 6 nodes; asserts
      verify_with_cert + final_state across epochs/validators with committee shuffle
- [x] e2e `test_cli_keygen_to_stake` PASS (regression)
- [x] intra-doc links resolve (only pre-existing broken links remain, none mine)
- [x] static-verified: closing block (tip of epoch-close output) is always sent via
      finish_executing_output → recent_blocks ⇒ wait resolves deterministically

## Review notes
- Risk validated SAFE: bitmap (`committee_index`) + membership gate (`committee_keys`) in
  `manage_epoch_votes` are both built from the SAME persisted `epoch_rec.committee` later read by
  `verify_with_cert` — indices align by construction. Persisted pinned `committee(N)` (primary
  path); no fallback to `prev.next_committee` needed.
- Hard-fail decode now also applies to the canonical-tip `validators_for_epoch` (prefetch /
  configure_consensus paths) — intentional per plan; e2e confirms no regression.
- Pre-existing (NOT mine, left untouched): clippy `never_loop` error in
  certificate_fetcher_tests.rs; `field_reassign_with_default` in recent_blocks.rs:172;
  legacy-numeric warning in tn-node "it"; assorted broken intra-doc links.
- Follow-up PR (out of scope): migrate get_committee_with_epoch_start_info / configure_consensus /
  create_consensus to pin reads at closing_epoch_state_blockhash; populate the field on
  startup/sync from prev EpochRecord.final_state.
