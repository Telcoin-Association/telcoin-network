//! The context provided to each ExEx at launch.

use crate::{replay::ReplayStream, TnExExEvent, TnExExNotification};
use futures::{stream, StreamExt};
use std::pin::Pin;
use tn_reth::RethEnv;
use tn_types::{BlockHeader as _, BlockNumber};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Everything an ExEx needs to operate, provided at launch.
///
/// Each ExEx receives its own context with:
/// - A notification stream covering the full transaction lifecycle
/// - An event channel to report processing progress
/// - Access to reth's blockchain provider for querying chain state
#[derive(Debug)]
pub struct TnExExContext {
    /// Async stream of lifecycle notifications (certificates, commits, executions).
    ///
    /// Live delivery is best-effort (see [`TnExExNotification`]); replay is the
    /// authoritative catch-up path.
    pub notifications: mpsc::Receiver<TnExExNotification>,
    /// Channel to send events (e.g., FinishedHeight) back to the manager.
    pub events: mpsc::UnboundedSender<TnExExEvent>,
    /// Read-only handle to reth's blockchain provider for querying chain
    /// state/history.
    ///
    /// `RethEnv`'s public surface exposes no DB-mutating methods, so ExExes use
    /// it for reads only (block/receipt/state lookups, replay). It is *not*
    /// intended as a write capability; ExEx code must treat it as read-only.
    pub reth_env: RethEnv,
}

impl TnExExContext {
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
    /// into the returned combined stream. The [`events`](Self::events) sender is
    /// dropped, so use [`replay_from`](Self::replay_from) directly if you need to
    /// keep reporting `FinishedHeight` while catching up.
    pub fn replay_and_subscribe(
        self,
        start_block: BlockNumber,
    ) -> impl futures::Stream<Item = eyre::Result<TnExExNotification>> {
        let TnExExContext { notifications, reth_env, events: _ } = self;

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

        // De-duplicate by monotonic execution height so the overlap window is
        // not delivered twice.
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
/// keeping the stream monotonic. Non-execution items (errors, certificate/commit
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
