//! Example ExEx: a minimal stateful block indexer.
//!
//! Where `exex-lifecycle` is the "hello world" that logs every notification,
//! this example is the skeleton for a *stateful* consumer — a block explorer,
//! analytics pipeline, or bridge monitor — that must not lose or double-count
//! data across restarts and lag. It models the four things such an ExEx must do:
//!
//! 1. **Replay on startup.** Load the last durably-indexed height and replay from there *before*
//!    processing live notifications, so a restart resumes exactly where it left off instead of
//!    silently skipping history.
//! 2. **Process replay and live uniformly.** The same `index_chain` handles a [`ChainExecuted`]
//!    whether it arrived via replay or the live channel.
//! 3. **Reconcile on [`Lagged`].** When the node signals dropped notifications, replay from the
//!    last indexed height to fill the gap rather than carry a silent hole.
//! 4. **Report progress.** Send [`FinishedHeight`] (non-blocking) so the node knows how far this
//!    ExEx has durably processed.
//!
//! # State-diff fidelity (replay vs. live)
//!
//! Replayed `ChainExecuted` notifications carry block data and **receipts** but
//! an **empty `BundleState`** (account/storage diffs) — see
//! [`TnExExContext::replay_from`]. This indexer only needs block-level data, so
//! it processes both paths identically. An indexer that needs account/storage
//! diffs must read them from [`TnExExContext::reth_env`] on the replay path
//! (live notifications do carry the full `BundleState`).
//!
//! # Why not `replay_and_subscribe`?
//!
//! [`TnExExContext::replay_and_subscribe`] is the simplest catch-up path. It still
//! lets you report [`FinishedHeight`] — it hands back a
//! [`FinishedHeightReporter`] alongside the combined stream — but it returns *one*
//! combined stream, so it can't itself re-replay to reconcile a `Lagged` gap. This
//! example drives [`replay_from`] plus the live channel directly to keep that
//! reconciliation control. Reach for `replay_and_subscribe` when your ExEx does not
//! need to re-replay after lag.
//!
//! [`FinishedHeightReporter`]: tn_exex::FinishedHeightReporter
//!
//! [`ChainExecuted`]: tn_exex::TnExExNotification::ChainExecuted
//! [`Lagged`]: tn_exex::TnExExNotification::Lagged
//! [`FinishedHeight`]: tn_exex::TnExExEvent::FinishedHeight
//! [`report_finished_height`]: tn_exex::TnExExContext::report_finished_height
//! [`replay_from`]: tn_exex::TnExExContext::replay_from
//! [`TnExExContext::replay_from`]: tn_exex::TnExExContext::replay_from
//! [`TnExExContext::replay_and_subscribe`]: tn_exex::TnExExContext::replay_and_subscribe
//! [`TnExExContext::reth_env`]: tn_exex::TnExExContext::reth_env

use futures::StreamExt as _;
use std::collections::BTreeMap;
use tn_exex::{Chain, TnExExContext, TnExExNotification};
use tn_types::{BlockHeader as _, BlockNumber};
use tracing::{info, warn};

/// A single indexed block record.
///
/// A real indexer persists records like this to durable storage (Postgres,
/// RocksDB, ...); this example keeps them in memory for illustration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRecord {
    /// Canonical block number.
    pub number: BlockNumber,
    /// Number of transactions in the block.
    pub tx_count: u64,
    /// Total gas used by the block.
    pub gas_used: u64,
}

/// Minimal in-memory index.
///
/// The `REAL DB SEAM` comments mark every point where a production indexer would
/// touch durable storage instead of this in-memory state.
#[derive(Debug, Default)]
pub struct Indexer {
    /// Indexed blocks, keyed by number.
    blocks: BTreeMap<BlockNumber, BlockRecord>,
    /// Highest block durably indexed so far, if any.
    last_indexed: Option<BlockNumber>,
}

impl Indexer {
    /// The next block this indexer needs: one past the last it durably indexed
    /// (`0` on a fresh index).
    pub fn next_block(&self) -> BlockNumber {
        // REAL DB SEAM: on startup, load the persisted cursor here.
        self.last_indexed.map_or(0, |n| n + 1)
    }

    /// Returns the indexed record for `number`, if any.
    pub fn get(&self, number: BlockNumber) -> Option<&BlockRecord> {
        self.blocks.get(&number)
    }

    /// Record one block, advancing the cursor monotonically.
    fn record_block(&mut self, record: BlockRecord) {
        // REAL DB SEAM: write `record` and advance the persisted cursor in the
        // SAME transaction, so a crash between the two can't lose or skip a block.
        let number = record.number;
        self.blocks.insert(number, record);
        self.last_indexed = Some(self.last_indexed.map_or(number, |last| last.max(number)));
    }

    /// Index every not-yet-seen block in `chain`.
    ///
    /// Idempotent and monotonic: blocks at or below the cursor are skipped, so
    /// re-delivery of the replay/live overlap (or a `Lagged` re-replay) can't
    /// double-count. Processes replayed and live chains identically.
    fn index_chain(&mut self, chain: &Chain) {
        for block in chain.blocks().values() {
            let number = block.number();
            if self.last_indexed.is_some_and(|last| number <= last) {
                continue;
            }
            self.record_block(BlockRecord {
                number,
                tx_count: block.body().transactions().count() as u64,
                gas_used: block.header().gas_used(),
            });
        }
    }
}

/// The indexer ExEx.
///
/// Replays history to catch up, then follows live notifications, reconciling any
/// `Lagged` gap by replaying again. See the module docs for the full contract.
///
/// # Registering the ExEx
///
/// `indexer_exex` matches the signature `TnBuilder::install_exex` expects:
///
/// ```
/// use exex_indexer::indexer_exex;
/// use tn_exex::TnExExContext;
///
/// // In node setup this is simply:
/// //     builder.install_exex("indexer", indexer_exex);
/// //
/// // which requires the function to satisfy this bound:
/// fn assert_installable<F, Fut>(_f: F)
/// where
///     F: FnOnce(TnExExContext) -> Fut,
///     Fut: std::future::Future<Output = eyre::Result<()>>,
/// {
/// }
/// assert_installable(indexer_exex);
/// ```
pub async fn indexer_exex(mut ctx: TnExExContext) -> eyre::Result<()> {
    // REAL DB SEAM: a restart would load `indexer` (and its cursor) from storage.
    let mut indexer = Indexer::default();
    info!(target: "exex::indexer", "Indexer ExEx started");

    // 1. Catch up on history before processing live data.
    catch_up(&mut indexer, &ctx).await?;

    // 2. Follow live notifications.
    while let Some(notification) = ctx.next_notification().await {
        match notification {
            TnExExNotification::ChainExecuted { new } => {
                indexer.index_chain(&new);
                report_progress(&ctx, &indexer);
            }
            TnExExNotification::Lagged { missed } => {
                // The node dropped notifications (upstream or downstream). Re-replay
                // from our cursor to fill the gap rather than carry a silent hole.
                warn!(target: "exex::indexer", missed, "lagged; reconciling via replay");
                catch_up(&mut indexer, &ctx).await?;
            }
            // This indexer only cares about executed blocks; the earlier lifecycle
            // signals are ignored (handled explicitly so a new variant won't be
            // silently dropped).
            TnExExNotification::CertificateAccepted { .. }
            | TnExExNotification::ConsensusOutput { .. } => {}
        }
    }

    info!(target: "exex::indexer", last_indexed = ?indexer.last_indexed, "Indexer ExEx shutting down");
    Ok(())
}

/// Replay from the indexer's cursor to the current chain tip.
async fn catch_up(indexer: &mut Indexer, ctx: &TnExExContext) -> eyre::Result<()> {
    let start = indexer.next_block();
    // `reth_env` is the ExEx's read handle to the chain; here we use it to log the
    // catch-up target. It also exposes block/receipt/state reads for indexers that
    // need more than the notification payload (e.g. account state on the replay
    // path, where the `BundleState` is empty).
    let tip = ctx.reth_env().last_block_number()?;
    info!(target: "exex::indexer", start, tip, "catching up via replay");

    let mut replay = ctx.replay_from(start)?;
    while let Some(result) = replay.next().await {
        // Surface replay/tip errors instead of silently stalling.
        if let TnExExNotification::ChainExecuted { new } = result? {
            indexer.index_chain(&new);
        }
    }
    report_progress(ctx, indexer);
    Ok(())
}

/// Report durable progress to the node (non-blocking; latest-wins).
fn report_progress(ctx: &TnExExContext, indexer: &Indexer) {
    if let Some(height) = indexer.last_indexed {
        ctx.report_finished_height(height);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_advances_monotonically_and_records() {
        let mut idx = Indexer::default();
        assert_eq!(idx.next_block(), 0); // fresh index starts at genesis

        idx.record_block(BlockRecord { number: 5, tx_count: 1, gas_used: 100 });
        assert_eq!(idx.next_block(), 6);
        assert_eq!(idx.get(5).map(|r| r.tx_count), Some(1));

        // Re-recording an older block (e.g. a replay/live overlap) doesn't rewind
        // the cursor.
        idx.record_block(BlockRecord { number: 3, tx_count: 2, gas_used: 200 });
        assert_eq!(idx.next_block(), 6);
    }
}
