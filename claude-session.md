Claude Output

What was built

Fork machinery — new crates/types/src/fork.rs with the ForkId::DedupBatchDigests variant, a pure fork_active_at comparison, and is_active(epoch) that is always true in
Fork machinery — new crates/types/src/fork.rs with the ForkId::DedupBatchDigests variant, a pure fork_active_at comparison, and is_active(epoch) that is always true in
mainnet builds and epoch-gated in faucet builds (placeholder activation Epoch::MAX, guarded by a sentinel unit test so the eventual ops change is a conspicuous diff). A
per-fork atomic test override exists only under faucet+test-utils. The faucet Cargo feature now chains tn-types → executor/engine/storage → node → the binary, and the
broken Makefile test-faucet target was repointed at e2e-tests where the faucet IT actually lives.

Correctness fix (fork-gated on sub_dag.leader_epoch()) — the subscriber keeps only the first occurrence of a duplicate digest so batch_digests stays 1:1 with the deduped
batches (fixing both the silently-skipped epoch-closing call and the wrong ommers/mix hashes); the engine's debug_assert_eq! is now a hard
TnEngineError::BatchDigestMismatch; and pack replay was brought under the same gate — it had been loading duplicate batches twice, which already diverged from live
execution, so fresh-sync nodes would have forked at the historical duplicate regardless. Legacy branches compile only under #[cfg(feature = "faucet")]. The finding-26 log
fix (dropping the batch_digests dump) went in alongside.

Root-cause prevention (ungated) — the batch builder records digests on quorum ack and, when the pool re-accepts resubmitted transactions and rebuilds a byte-identical
batch, skips the worker send and acks locally so the pool re-cleans; quorum failures are never recorded, so legitimate retries keep their digest. The proposer dedups
digest ingestion in a new handle_our_digest while leaving the retransmit path untouched.

Verification highlights

- The batch-builder test reproduced the incident exactly: the reth pool did re-accept the resubmitted mined transactions, the identical rebuild was suppressed, and the
  pool still drained to zero — no fallback needed.
- The executor IT now forces a deterministic intra-output duplicate (shared batch across three same-round authorities) and asserts full 1:1 digest/batch alignment under
  default features and the legacy +dup_count shape under faucet.
- New engine ITs prove the epoch-closing system call fires on the last block of a deduped output, misalignment hard-errors on mainnet builds, and pre-activation replay
  reproduces the historical wrong-digest blocks; the existing duplicate-transactions test passes unchanged.

Flags for ops (unchanged from plan)

1. Set the real adiri activation epoch in ForkId::activation_epoch before release — it's Epoch::MAX until then, meaning faucet builds stay on legacy behavior everywhere.
2. etc/Dockerfile:30 builds without --features faucet; confirm the testnet image build command now that the binary exposes the feature.
3. Two pre-existing issues left alone: the tn-primary clippy never_loop error in certificate_fetcher_tests.rs and the tn-types lib-only test-utils check failure (both
   reproduce without these changes).
