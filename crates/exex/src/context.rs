//! The context provided to each ExEx at launch.

use crate::{replay::ReplayStream, TnExExEvent, TnExExNotification};
use futures::{stream, StreamExt};
use std::pin::Pin;
use tn_reth::RethEnv;
use tn_storage::consensus::ConsensusChain;
use tn_types::{BlockHeader as _, BlockNumber};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Everything an ExEx needs to operate, provided at launch.
///
/// Each ExEx receives its own context, which gives it:
/// - live lifecycle notifications via [`next_notification`](Self::next_notification) (or
///   [`replay_and_subscribe`](Self::replay_and_subscribe) for replay-then-live),
/// - progress reporting back to the node via
///   [`report_finished_height`](Self::report_finished_height),
/// - read-only chain access via [`reth_env`](Self::reth_env) (EVM state) and
///   [`consensus_chain`](Self::consensus_chain) (consensus DB).
///
/// Fields are private on purpose: an ExEx interacts with the context only
/// through these methods, so third-party ExEx code cannot, for example, `await`
/// a send that would back-pressure the manager.
#[derive(Debug)]
pub struct TnExExContext {
    /// Async stream of lifecycle notifications. Live delivery is best-effort
    /// (see [`TnExExNotification`]); replay is the authoritative catch-up path.
    notifications: mpsc::Receiver<TnExExNotification>,
    /// Bounded channel to report events (e.g. `FinishedHeight`) back to the
    /// manager. Only ever written via a non-blocking `try_send`, in
    /// [`report_finished_height`](Self::report_finished_height).
    events: mpsc::Sender<TnExExEvent>,
    /// Read-only handle to reth for EVM chain state and history.
    reth_env: RethEnv,
    /// Read-only handle to the consensus chain (consensus DB).
    consensus_chain: ConsensusChain,
}

impl TnExExContext {
    /// Create a new ExEx context.
    ///
    /// Called by the node when launching an ExEx; third-party ExEx code never
    /// constructs this directly.
    pub fn new(
        notifications: mpsc::Receiver<TnExExNotification>,
        events: mpsc::Sender<TnExExEvent>,
        reth_env: RethEnv,
        consensus_chain: ConsensusChain,
    ) -> Self {
        Self { notifications, events, reth_env, consensus_chain }
    }

    /// Read-only handle to reth for querying EVM chain state and history.
    ///
    /// `RethEnv`'s public surface exposes no DB-mutating methods, so ExExes use it
    /// for reads only. It is *not* a write capability; ExEx code must treat it as
    /// read-only.
    ///
    /// Reads commonly useful to an ExEx (see [`RethEnv`] for the full surface):
    ///
    /// - `last_block_number()` / `canonical_tip()` / `finalized_header()` — current chain
    ///   position.
    /// - `sealed_block_by_number(n)` / `sealed_block_with_senders(..)` — a full block, optionally
    ///   with recovered senders.
    /// - `header_by_number(n)` / `sealed_header_by_number(n)` — a single header.
    /// - `blocks_for_range(start..=end)` — sealed headers across a block range.
    /// - `replay_block_as_chain(n)` — a historical block rebuilt as a `Chain` (receipts included,
    ///   empty `BundleState`); this is what [`replay_from`](Self::replay_from) uses.
    /// - `latest()` — a state provider for account/storage queries against the latest committed
    ///   state.
    pub fn reth_env(&self) -> &RethEnv {
        &self.reth_env
    }

    /// Read-only handle to the consensus chain (the consensus DB).
    ///
    /// Lets an ExEx read consensus-level data directly — consensus headers, epoch
    /// records, and committed sub-DAGs by number or digest — alongside the
    /// EVM-level reads available through [`reth_env`](Self::reth_env). Like
    /// `reth_env`, treat it as read-only.
    pub fn consensus_chain(&self) -> &ConsensusChain {
        &self.consensus_chain
    }

    /// Report durable progress to the node (non-blocking; latest-wins).
    ///
    /// Sends [`TnExExEvent::FinishedHeight`] with a non-blocking `try_send`, so an
    /// ExEx can never back-pressure the manager — "never await a send" is enforced
    /// here by construction rather than by convention. `FinishedHeight` is
    /// latest-wins, so dropping an intermediate report when the channel is
    /// momentarily full is harmless: the next report carries a higher height.
    pub fn report_finished_height(&self, height: BlockNumber) {
        let _ = self.events.try_send(TnExExEvent::FinishedHeight(height));
    }

    /// Await the next live notification, or `None` once the channel closes.
    ///
    /// This is the live-consumption path. For startup catch-up, replay first via
    /// [`replay_from`](Self::replay_from) or
    /// [`replay_and_subscribe`](Self::replay_and_subscribe).
    pub async fn next_notification(&mut self) -> Option<TnExExNotification> {
        self.notifications.recv().await
    }

    /// Replay historical blocks from `start_block` to the current chain tip.
    ///
    /// Returns a stream of [`TnExExNotification::ChainExecuted`] for each
    /// historical block. After the stream is exhausted, the ExEx should switch
    /// to `self.notifications` for live data.
    ///
    /// # Replay fidelity
    ///
    /// Replayed `ChainExecuted` notifications carry an **empty `BundleState`**
    /// (account/storage diffs) — that state is already committed to the DB and
    /// is not re-derived here. ExExes that need historical state diffs must read
    /// them from [`reth_env`](Self::reth_env) by block number. Live
    /// `ChainExecuted` notifications *do* carry the full `BundleState`.
    ///
    /// # Errors
    ///
    /// Returns an error if the current chain tip cannot be read from the DB.
    pub fn replay_from(&self, start_block: BlockNumber) -> eyre::Result<ReplayStream> {
        let tip = self.reth_env.last_block_number()?;
        Ok(ReplayStream::new(self.reth_env.clone(), start_block, tip))
    }

    /// Convenience: replay from `start_block` then seamlessly chain with live notifications.
    ///
    /// This is the recommended way for stateful ExExes to initialize:
    /// ```rust,ignore
    /// let mut stream = ctx.replay_and_subscribe(last_indexed_block + 1);
    /// while let Some(result) = stream.next().await {
    ///     let notification = result?; // surface replay/tip errors
    ///     // Process identically — no distinction between replay and live
    /// }
    /// ```
    ///
    /// # Delivery contract
    ///
    /// `ChainExecuted` notifications are delivered **at-least-once and in
    /// monotonic height order**: the overlap window (blocks buffered on the live
    /// channel while replay runs) is de-duplicated by height so a block is never
    /// processed twice. If the live channel overflowed during a slow replay, the
    /// resulting gap surfaces as a [`TnExExNotification::Lagged`] marker rather
    /// than silently — re-`replay_from` the gap to reconcile. See
    /// [`replay_from`](Self::replay_from) for replay state-diff fidelity.
    ///
    /// Note: this consumes `self` because the live notification stream is moved
    /// into the returned combined stream. The event sender is dropped, so
    /// [`report_finished_height`](Self::report_finished_height) can no longer be
    /// called — use [`replay_from`](Self::replay_from) directly if you need to
    /// keep reporting `FinishedHeight` while catching up.
    pub fn replay_and_subscribe(
        self,
        start_block: BlockNumber,
    ) -> impl futures::Stream<Item = eyre::Result<TnExExNotification>> {
        let TnExExContext { notifications, reth_env, events: _, consensus_chain: _ } = self;

        // Live notifications buffered since context creation (wrapped as `Ok`).
        let live = ReceiverStream::new(notifications).map(Ok);

        // Replay the historical range first. If the tip can't be read we can't
        // bound the range: surface the error but still deliver live
        // notifications (best-effort) rather than going dark.
        let replay: Pin<Box<dyn futures::Stream<Item = eyre::Result<TnExExNotification>> + Send>> =
            match reth_env.last_block_number() {
                Ok(tip) => Box::pin(ReplayStream::new(reth_env, start_block, tip)),
                Err(e) => Box::pin(stream::once(async move {
                    Err(eyre::eyre!("ExEx replay skipped: failed to read chain tip: {e}"))
                })),
            };

        // Guard the replay/live overlap window: while replay runs, live blocks
        // are buffered on `notifications`, so a block in `[start_block, tip]` can
        // arrive via BOTH the replay range and the buffered live channel.
        // De-duplicating by monotonic execution height drops the second copy.
        // This is unrelated to BFT/reorgs (TN has none) — it is purely the
        // replay/live seam.
        dedup_chain_executed(replay.chain(live))
    }
}

/// Returns whether a `ChainExecuted` notification at `height` should be kept,
/// updating `last_height` when it is.
///
/// Drops anything not strictly newer than the last delivered height (the
/// replay/live overlap), keeping the stream monotonic.
fn keep_height(last_height: &mut Option<BlockNumber>, height: BlockNumber) -> bool {
    if matches!(*last_height, Some(prev) if height <= prev) {
        false
    } else {
        *last_height = Some(height);
        true
    }
}

/// Drop any `ChainExecuted` notification whose height was already delivered,
/// keeping the stream monotonic. Non-execution items (errors, certificate/output
/// signals, `Lagged` markers) have no height and always pass through.
fn dedup_chain_executed(
    stream: impl futures::Stream<Item = eyre::Result<TnExExNotification>>,
) -> impl futures::Stream<Item = eyre::Result<TnExExNotification>> {
    use futures::StreamExt;

    stream
        .scan(None::<BlockNumber>, |last_height, item| {
            // Borrow `item` only long enough to read the execution height.
            let height = match &item {
                Ok(TnExExNotification::ChainExecuted { new }) => Some(new.tip().number()),
                _ => None,
            };
            let keep = match height {
                Some(h) => keep_height(last_height, h).then_some(item),
                None => Some(item),
            };
            futures::future::ready(Some(keep))
        })
        .filter_map(futures::future::ready)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    #[test]
    fn keep_height_is_monotonic_and_dedups() {
        let mut last = None;
        assert!(keep_height(&mut last, 5)); // first delivery → keep
        assert_eq!(last, Some(5));
        assert!(!keep_height(&mut last, 5)); // duplicate height → drop
        assert!(!keep_height(&mut last, 3)); // older height → drop
        assert_eq!(last, Some(5)); // unchanged by drops
        assert!(keep_height(&mut last, 6)); // newer → keep
        assert!(keep_height(&mut last, 7)); // newer → keep
        assert_eq!(last, Some(7));
    }

    #[tokio::test]
    async fn dedup_passes_through_non_execution_items() {
        // Non-`ChainExecuted` items (Lagged markers, errors) carry no height and
        // must never be dropped by the de-duplication pass.
        let items: Vec<eyre::Result<TnExExNotification>> = vec![
            Ok(TnExExNotification::Lagged { missed: 1 }),
            Err(eyre::eyre!("replay error")),
            Ok(TnExExNotification::Lagged { missed: 2 }),
        ];
        let out: Vec<_> = dedup_chain_executed(stream::iter(items)).collect().await;
        assert_eq!(out.len(), 3);
        assert!(matches!(out[0], Ok(TnExExNotification::Lagged { missed: 1 })));
        assert!(out[1].is_err());
        assert!(matches!(out[2], Ok(TnExExNotification::Lagged { missed: 2 })));
    }
}
