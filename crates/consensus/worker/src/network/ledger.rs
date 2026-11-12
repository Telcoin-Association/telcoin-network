//! Node-wide ledger of the reported batches that are stored and not yet committed (issue #1794).

use super::handler::MAX_UNCOMMITTED_REPORTED_BATCHES_PER_REPORTER;
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tn_types::{BlsPublicKey, Epoch, Round, B256, MAX_GC_DEPTH};

/// The verdict of [`ReportedBatchLedger::check`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum QuotaCheck {
    /// The batch may be stored. `aged` holds the digests that aged out of the set of the
    /// committee key to make room. The caller deletes their rows from `NodeBatchesCache`.
    Admit { aged: Vec<B256> },
    /// The committee key is still at the bound after the age-out.
    OverQuota,
}

/// The rows of a [`ReportedBatchLedger`].
#[derive(Debug, Default)]
struct LedgerRows {
    /// The counted digests of each committee key.
    by_reporter: HashMap<BlsPublicKey, HashSet<B256>>,
    /// The committee key that a digest counts against, and the last committed round that the
    /// ledger had seen when it counted the digest.
    by_digest: HashMap<B256, (BlsPublicKey, Round)>,
    /// The last committed round that the ledger has seen.
    committed_round: Round,
    /// The epoch of the rows. A new epoch starts with an empty ledger.
    epoch: Epoch,
}

impl LedgerRows {
    /// The number of digests counted against `reporter`.
    fn counted(&self, reporter: &BlsPublicKey) -> usize {
        self.by_reporter.get(reporter).map_or(0, HashSet::len)
    }

    /// See [`ReportedBatchLedger::check`].
    fn check(&mut self, reporter: &BlsPublicKey, digest: &B256) -> QuotaCheck {
        let has_room = self.by_digest.contains_key(digest)
            || self.counted(reporter) < MAX_UNCOMMITTED_REPORTED_BATCHES_PER_REPORTER;
        let aged = if has_room { Vec::new() } else { self.age_out(reporter) };
        if has_room || self.counted(reporter) < MAX_UNCOMMITTED_REPORTED_BATCHES_PER_REPORTER {
            QuotaCheck::Admit { aged }
        } else {
            QuotaCheck::OverQuota
        }
    }

    /// Remove and return the digests of `reporter` that were counted more than
    /// [`MAX_GC_DEPTH`] committed rounds ago. Such a batch can no longer be committed: its
    /// certificate left the DAG.
    fn age_out(&mut self, reporter: &BlsPublicKey) -> Vec<B256> {
        let horizon = self.committed_round.saturating_sub(MAX_GC_DEPTH);
        let aged: Vec<B256> = self
            .by_reporter
            .get(reporter)
            .map(|counted| {
                counted
                    .iter()
                    .filter(|d| self.by_digest.get(*d).is_some_and(|(_, round)| *round < horizon))
                    .copied()
                    .collect()
            })
            .unwrap_or_default();
        self.remove(&aged);
        aged
    }

    /// See [`ReportedBatchLedger::record`].
    fn record(&mut self, reporter: &BlsPublicKey, digest: B256) {
        if !self.by_digest.contains_key(&digest) {
            self.by_digest.insert(digest, (*reporter, self.committed_round));
            self.by_reporter.entry(*reporter).or_default().insert(digest);
        }
    }

    /// Remove `digests` from the ledger. A set that becomes empty is removed, so the map keeps
    /// no entry for a committee key with nothing counted.
    fn remove<'a>(&mut self, digests: impl IntoIterator<Item = &'a B256>) {
        digests.into_iter().for_each(|digest| {
            if let Some((owner, _)) = self.by_digest.remove(digest) {
                let now_empty = self.by_reporter.get_mut(&owner).is_some_and(|counted| {
                    counted.remove(digest);
                    counted.is_empty()
                });
                if now_empty {
                    self.by_reporter.remove(&owner);
                }
            }
        });
    }
}

/// The batches that each committee key stored on this node through a batch report or a gossip
/// prefetch and that are not yet committed (issue #1794).
///
/// An entry means "stored": the handler checks the bound before validation without counting,
/// and counts a digest only after the store write succeeds. A digest that is already counted
/// is not counted again, so the first committee key that stored it keeps it. The checks of two
/// tasks of one key can pass before either stores, so the set of a key can pass
/// [`MAX_UNCOMMITTED_REPORTED_BATCHES_PER_REPORTER`] by at most the number of reports and
/// prefetches of that key in flight at once.
///
/// A commit removes the committed digests through [`Self::forget`], which also records the
/// committed round. When a key is at the bound, the digests counted more than
/// [`MAX_GC_DEPTH`] committed rounds earlier age out, and the caller deletes their rows from
/// `NodeBatchesCache`. Such a batch can no longer be committed, so the age-out covers failed
/// seals, reporter restarts, failed evictions and late re-stores of committed batches. The vote
/// path fetches a deleted body again if a header needs it.
///
/// The node builds one ledger per epoch and shares it with every worker, so the bound is one
/// per committee key for the whole node, like [`tn_types::MAX_HEADER_NUM_OF_BATCHES`]. The
/// ledger is kept in memory only: a process restart starts it empty while the
/// `NodeBatchesCache` rows survive, so after a restart a key can store up to the bound again.
#[derive(Debug, Clone, Default)]
pub struct ReportedBatchLedger {
    /// The rows, shared by every clone.
    rows: Arc<Mutex<LedgerRows>>,
}

impl ReportedBatchLedger {
    /// Create an empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Start `epoch`. A new epoch empties the ledger, because the epoch close clears
    /// `NodeBatchesCache`. A re-entry into the current epoch (a mode change) keeps the rows.
    pub fn enter_epoch(&self, epoch: Epoch) {
        let mut rows = self.rows.lock();
        if rows.epoch != epoch {
            *rows = LedgerRows { epoch, ..LedgerRows::default() };
        }
    }

    /// Check the bound of `reporter` for `digest` without counting it.
    ///
    /// A digest that is already counted, or a key under the bound, is admitted. A key at the
    /// bound first loses its aged digests (see the type doc). The caller deletes the rows of
    /// the aged digests after this call, outside the lock.
    pub(crate) fn check(&self, reporter: &BlsPublicKey, digest: &B256) -> QuotaCheck {
        self.rows.lock().check(reporter, digest)
    }

    /// Count `digest` against `reporter`. Call it only after the store write succeeded.
    pub(crate) fn record(&self, reporter: &BlsPublicKey, digest: B256) {
        self.rows.lock().record(reporter, digest);
    }

    /// Remove the committed `digests` and record `round` as the last committed round.
    ///
    /// Called for each consensus output, next to the eviction of its batches from
    /// `NodeBatchesCache`.
    pub fn forget<'a>(&self, digests: impl IntoIterator<Item = &'a B256>, round: Round) {
        let mut rows = self.rows.lock();
        rows.remove(digests);
        rows.committed_round = rows.committed_round.max(round);
    }

    /// The number of digests counted against `reporter`.
    pub fn counted(&self, reporter: &BlsPublicKey) -> usize {
        self.rows.lock().counted(reporter)
    }
}
