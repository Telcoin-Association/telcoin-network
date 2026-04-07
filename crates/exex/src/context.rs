//! The context provided to each ExEx at launch.

use crate::{replay::ReplayStream, TnExExEvent, TnExExNotification};
use tn_reth::RethEnv;
use tn_types::BlockNumber;
use tokio::sync::mpsc;

/// Everything an ExEx needs to operate, provided at launch.
///
/// Each ExEx receives its own context with:
/// - A notification stream covering the full transaction lifecycle
/// - An event channel to report processing progress
/// - Access to reth's blockchain provider for querying chain state
#[derive(Debug)]
pub struct TnExExContext {
    /// Async stream of lifecycle notifications (certificates, commits, executions).
    pub notifications: mpsc::Receiver<TnExExNotification>,
    /// Channel to send events (e.g., FinishedHeight) back to the manager.
    pub events: mpsc::UnboundedSender<TnExExEvent>,
    /// Access to reth's blockchain provider for querying chain state/history.
    pub reth_env: RethEnv,
}

impl TnExExContext {
    /// Replay historical blocks from `start_block` to the current chain tip.
    ///
    /// Returns a stream of `TnExExNotification::ChainExecuted` for each historical block.
    /// After the stream is exhausted, the ExEx should switch to `self.notifications` for
    /// live data.
    pub fn replay_from(&self, start_block: BlockNumber) -> ReplayStream {
        let tip = self.reth_env.last_block_number().unwrap_or(0);
        ReplayStream::new(self.reth_env.clone(), start_block, tip)
    }

    /// Convenience: replay from `start_block` then seamlessly chain with live notifications.
    ///
    /// This is the recommended way for stateful ExExes to initialize:
    /// ```rust,ignore
    /// let mut stream = ctx.replay_and_subscribe(last_indexed_block + 1);
    /// while let Some(notification) = stream.next().await {
    ///     // Process identically — no distinction between replay and live
    /// }
    /// ```
    ///
    /// Note: this consumes `self` because the live notification stream is moved
    /// into the returned combined stream.
    pub fn replay_and_subscribe(
        self,
        start_block: BlockNumber,
    ) -> impl futures::Stream<Item = eyre::Result<TnExExNotification>> {
        use futures::StreamExt;
        use tokio_stream::wrappers::ReceiverStream;

        let tip = self.reth_env.last_block_number().unwrap_or(0);
        let replay = ReplayStream::new(self.reth_env, start_block, tip);

        // Chain replay stream with live notifications (wrapped as Ok)
        let live = ReceiverStream::new(self.notifications).map(Ok);
        replay.chain(live)
    }
}
