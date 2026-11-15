//! The startup check of a datadir that stores transactions by position: the consensus pack of
//! every epoch its rows can point into is on disk.
//!
//! The rows of a pointer datadir name batches of the packs of the epochs from the one of the block
//! holding its lowest transaction to the one of the block holding its highest. Every executed
//! output was saved to its leader epoch's pack before its blocks were stored, and execution skips
//! no epoch, so an epoch in that range without a pack lost it: the node refuses to start rather
//! than answer reads of those transactions with errors. The check reads the two blocks from the
//! `TransactionBlocks` index and their headers, and looks for each epoch's data log
//! (`epoch-{N}/data`) without opening it: an open reads a few MiB, and a damaged or legacy pack
//! surfaces at its first read as a provider error. The live epoch passes too: the chain opened its
//! pack, data log included, before the check.

use std::{
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use reth_db::{cursor::DbCursorRO as _, tables, transaction::DbTx as _};
use reth_provider::{DatabaseProviderFactory as _, HeaderProvider as _};
use tn_storage::consensus_pack::DATA_NAME;
use tn_types::{BlockNumber, Epoch};

use crate::RethEnv;

/// A datadir that stores transactions by position lacks the consensus packs of some epochs its
/// rows can point into (see the [module docs](self)): the node refuses to start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "the execution datadir stores transactions of blocks {}..={} by their position in consensus \
     packs of epochs {}..={}, and {} lacks epoch(s) {}. This node does not fetch past epochs from \
     peers, and a rebuild of the execution database re-executes from its own packs, so it stops at \
     the same gap. Restore `consensus-db` from a backup; without one, start over as a new node, \
     moving both the execution database (`telcoin-network db reset-execution`) and `consensus-db` \
     aside, or swap in the execution database of an observer bootstrapped with \
     `telcoin-network db load-state` from epoch {} or a later one (`db reset-execution --for-swap`)",
    blocks.start(),
    blocks.end(),
    epochs.start(),
    epochs.end(),
    epochs_dir.display(),
    ranges(missing),
    missing.last().copied().unwrap_or_default()
)]
pub struct TxPackCoverageError {
    /// The blocks holding the datadir's lowest and highest transactions.
    pub blocks: RangeInclusive<BlockNumber>,
    /// Their epochs: the packs the rows can point into.
    pub epochs: RangeInclusive<Epoch>,
    /// The directory holding the `epoch-{N}` directories.
    pub epochs_dir: PathBuf,
    /// The epochs of `epochs` without a pack, lowest first.
    pub missing: Vec<Epoch>,
}

impl RethEnv {
    /// Refuse a datadir storing transactions by position whose rows can point into a pack that is
    /// not under `epochs_dir` ([module docs](self)). A datadir with no transaction passes.
    pub(crate) fn check_tx_pack_coverage(&self, epochs_dir: &Path) -> eyre::Result<()> {
        let Some((blocks, epochs)) = self.stored_tx_epochs()? else {
            return Ok(());
        };
        let missing = missing_packs(epochs_dir, epochs.clone())?;
        if missing.is_empty() {
            return Ok(());
        }
        Err(TxPackCoverageError { blocks, epochs, epochs_dir: epochs_dir.to_owned(), missing }
            .into())
    }

    /// The blocks holding the lowest and the highest stored transaction, and their epochs (the
    /// nonce's high 32 bits); `None` without a transaction.
    fn stored_tx_epochs(
        &self,
    ) -> eyre::Result<Option<(RangeInclusive<BlockNumber>, RangeInclusive<Epoch>)>> {
        let provider = self.inner.blockchain_provider.database_provider_ro()?;
        let (first, last) = {
            // keyed by each block's last transaction number: the first entry is the block holding
            // the lowest transaction, the last the block holding the highest
            let mut cursor = provider.tx_ref().cursor_read::<tables::TransactionBlocks>()?;
            match (cursor.first()?, cursor.last()?) {
                (Some((_, first)), Some((_, last))) => (first, last),
                _ => return Ok(None),
            }
        };
        let epoch = |block: BlockNumber| -> eyre::Result<Epoch> {
            let header = provider.header_by_number(block)?.ok_or_else(|| {
                eyre::eyre!("block {block} holds stored transactions, but its header is missing")
            })?;
            Ok(Self::extract_epoch_from_header(&header))
        };
        Ok(Some((first..=last, epoch(first)?..=epoch(last)?)))
    }
}

/// The epochs of `epochs` with no data log under `epochs_dir`. A failed look is an error, never
/// a missing pack.
fn missing_packs(epochs_dir: &Path, epochs: RangeInclusive<Epoch>) -> eyre::Result<Vec<Epoch>> {
    let mut missing = Vec::new();
    for epoch in epochs {
        let data = epochs_dir.join(format!("epoch-{epoch}")).join(DATA_NAME);
        let found = data.try_exists().map_err(|e| {
            eyre::eyre!("failed to look for the consensus pack {}: {e}", data.display())
        })?;
        if !found {
            missing.push(epoch);
        }
    }
    Ok(missing)
}

/// `epochs` (ascending) as a list of runs: `1, 3..=5`.
fn ranges(epochs: &[Epoch]) -> String {
    let mut runs: Vec<RangeInclusive<Epoch>> = Vec::new();
    for &epoch in epochs {
        match runs.last_mut() {
            Some(run) if run.end().checked_add(1) == Some(epoch) => {
                *run = *run.start()..=epoch;
            }
            _ => runs.push(epoch..=epoch),
        }
    }
    runs.iter()
        .map(|run| {
            if run.start() == run.end() {
                run.start().to_string()
            } else {
                format!("{}..={}", run.start(), run.end())
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_epochs_print_as_runs() {
        assert_eq!(ranges(&[]), "");
        assert_eq!(ranges(&[1]), "1");
        assert_eq!(ranges(&[1, 3, 4, 5, 7, 8]), "1, 3..=5, 7..=8");
        assert_eq!(
            ranges(&[Epoch::MAX - 1, Epoch::MAX]),
            format!("{}..={}", Epoch::MAX - 1, Epoch::MAX)
        );
    }

    #[test]
    fn only_epochs_without_a_data_log_are_missing() -> eyre::Result<()> {
        let dir = tempfile::TempDir::new()?;
        for epoch in [0, 2, 3] {
            std::fs::create_dir_all(dir.path().join(format!("epoch-{epoch}")))?;
        }
        for epoch in [0, 3] {
            std::fs::write(dir.path().join(format!("epoch-{epoch}")).join(DATA_NAME), b"")?;
        }
        // epoch 1 has no directory, epoch 2 a directory without its data log
        assert_eq!(missing_packs(dir.path(), 0..=3)?, [1, 2]);
        assert_eq!(missing_packs(dir.path(), 3..=3)?, Vec::<Epoch>::new());
        Ok(())
    }
}
