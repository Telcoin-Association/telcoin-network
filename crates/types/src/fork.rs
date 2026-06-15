//! Epoch-gated protocol forks.
//!
//! A fork changes execution behavior at a known epoch boundary so that all nodes switch
//! together. Forks only matter for chains with pre-fork history (testnet): release builds
//! without the `faucet` feature treat every fork as always active and carry no legacy code —
//! legacy branches at call sites are compiled out with `#[cfg(feature = "faucet")]`.
//!
//! # Adding a new fork
//!
//! 1. Add a variant to [`ForkId`] documenting the behavior change.
//! 2. Add its testnet activation epoch to [`ForkId::activation_epoch`].
//! 3. At each call site, implement the new behavior unconditionally and guard the legacy
//!    branch with `#[cfg(feature = "faucet")]` + `!ForkId::YourFork.is_active(epoch)`.
//! 4. Gate on the epoch of the data being processed (e.g. `sub_dag.leader_epoch()`), not the
//!    node's current epoch — observers and fresh-sync nodes replay historical epochs.

use crate::Epoch;

/// Identifier for an epoch-gated protocol fork.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ForkId {
    /// Deduplicate batch digests in `ConsensusOutput` so `batch_digests` stays 1:1 with the
    /// deduped `batches` (first occurrence wins), and the engine hard-errors on any
    /// misalignment instead of executing blocks with the wrong digest.
    DedupBatchDigests,
}

/// Pure activation comparison, unit-testable on both sides of the `faucet` cfg.
pub const fn fork_active_at(epoch: Epoch, activation: Epoch) -> bool {
    epoch >= activation
}

impl ForkId {
    /// Activation epoch for testnet (`faucet`) builds.
    ///
    /// TODO(ops): set the real adiri upgrade epoch for `DedupBatchDigests` before release.
    /// `Epoch::MAX` keeps the legacy behavior everywhere, matching the deployed testnet.
    #[cfg(feature = "faucet")]
    pub const fn activation_epoch(&self) -> Epoch {
        match self {
            ForkId::DedupBatchDigests => Epoch::MAX,
        }
    }

    /// True if this fork's behavior applies at `epoch`.
    ///
    /// Non-faucet builds have no pre-fork history, so every fork is always active.
    #[cfg(not(feature = "faucet"))]
    pub fn is_active(&self, _epoch: Epoch) -> bool {
        true
    }

    /// True if this fork's behavior applies at `epoch`.
    ///
    /// Faucet (testnet) builds activate at [`Self::activation_epoch`], or at the per-fork
    /// test override when set via [`Self::set_activation_for_test`].
    #[cfg(feature = "faucet")]
    pub fn is_active(&self, epoch: Epoch) -> bool {
        #[cfg(feature = "test-utils")]
        if let Some(activation) = self.test_activation_override() {
            return fork_active_at(epoch, activation);
        }
        fork_active_at(epoch, self.activation_epoch())
    }
}

#[cfg(all(feature = "faucet", feature = "test-utils"))]
mod test_override {
    use super::ForkId;
    use crate::Epoch;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Sentinel meaning "no override set". `Epoch` is `u32`, so every real epoch fits below.
    const UNSET: u64 = u64::MAX;

    static DEDUP_BATCH_DIGESTS_OVERRIDE: AtomicU64 = AtomicU64::new(UNSET);

    impl ForkId {
        fn override_cell(&self) -> &'static AtomicU64 {
            match self {
                ForkId::DedupBatchDigests => &DEDUP_BATCH_DIGESTS_OVERRIDE,
            }
        }

        /// Override this fork's activation epoch for the current process.
        ///
        /// HAZARD: process-global state. Safe under `cargo nextest` (one process per test)
        /// but races between threads under plain `cargo test`.
        pub fn set_activation_for_test(&self, epoch: Epoch) {
            self.override_cell().store(epoch as u64, Ordering::SeqCst);
        }

        /// Clear the test override, restoring [`ForkId::activation_epoch`].
        pub fn clear_activation_for_test(&self) {
            self.override_cell().store(UNSET, Ordering::SeqCst);
        }

        /// The current test override, if one is set.
        pub(super) fn test_activation_override(&self) -> Option<Epoch> {
            let value = self.override_cell().load(Ordering::SeqCst);
            (value != UNSET).then_some(value as Epoch)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fork_active_at_boundaries() {
        assert!(!fork_active_at(4, 5));
        assert!(fork_active_at(5, 5));
        assert!(fork_active_at(6, 5));
        // epoch 0 activation means active from genesis
        assert!(fork_active_at(0, 0));
    }

    #[cfg(not(feature = "faucet"))]
    #[test]
    fn test_always_active_without_faucet() {
        assert!(ForkId::DedupBatchDigests.is_active(0));
        assert!(ForkId::DedupBatchDigests.is_active(Epoch::MAX));
    }

    /// The placeholder keeps faucet builds on legacy behavior everywhere. Setting the real
    /// adiri activation epoch must change this test — that diff is intentional and conspicuous.
    #[cfg(feature = "faucet")]
    #[test]
    fn test_activation_epoch_is_placeholder() {
        assert_eq!(ForkId::DedupBatchDigests.activation_epoch(), Epoch::MAX);
    }

    #[cfg(all(feature = "faucet", feature = "test-utils"))]
    #[test]
    fn test_faucet_inactive_until_override() {
        ForkId::DedupBatchDigests.clear_activation_for_test();
        // placeholder activation (Epoch::MAX) => inactive at any real epoch
        assert!(!ForkId::DedupBatchDigests.is_active(0));
        assert!(!ForkId::DedupBatchDigests.is_active(100));

        ForkId::DedupBatchDigests.set_activation_for_test(0);
        assert!(ForkId::DedupBatchDigests.is_active(0));

        ForkId::DedupBatchDigests.set_activation_for_test(5);
        assert!(!ForkId::DedupBatchDigests.is_active(4));
        assert!(ForkId::DedupBatchDigests.is_active(5));

        ForkId::DedupBatchDigests.clear_activation_for_test();
        assert!(!ForkId::DedupBatchDigests.is_active(100));
    }
}
