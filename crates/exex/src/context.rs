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
    /// manager. Written only via a non-blocking `try_send` — directly in
    /// [`report_finished_height`](Self::report_finished_height), or through the
    /// [`FinishedHeightReporter`] that
    /// [`replay_and_subscribe`](Self::replay_and_subscribe) carries out when it
    /// consumes the context.
    events: mpsc::Sender<TnExExEvent>,
    /// Read-only handle to reth for EVM chain state and history.
    reth_env: RethEnv,
    /// Handle to the consensus chain (consensus DB); exposed for reads via
    /// [`consensus_chain`](Self::consensus_chain).
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

    /// Handle to reth for querying EVM chain state and history — for reads only.
    ///
    /// Use this **only** for reads. `RethEnv`'s public surface is *not*
    /// mutation-free: it also exposes DB-writing methods — notably
    /// `finish_executing_output` (commits blocks and broadcasts a canonical-state
    /// notification) and `finalize_block` (persists the finalized/safe block
    /// numbers) — and this handle shares state with the node's live execution
    /// writer. Calling a writing method from an ExEx would corrupt the follower's
    /// chain state. The read-only contract here is by convention, not enforced by
    /// the type.
    ///
    /// Reads commonly useful to an ExEx (see [`RethEnv`] for the full surface):
    ///
    /// - `last_block_number()` / `canonical_tip()` / `finalized_header()` — current chain position.
    /// - `sealed_block_by_number(n)` / `sealed_block_with_senders(..)` — a full block, optionally
    ///   with recovered senders.
    /// - `header_by_number(n)` / `sealed_header_by_number(n)` — a single header.
    /// - `blocks_for_range(start..=end)` — sealed headers across a block range.
    /// - `replay_block_as_chain(n)` — a historical block rebuilt as a `Chain` (receipts included,
    ///   empty `BundleState`); this is what [`replay_from`](Self::replay_from) uses.
    /// - `latest()` — a state provider for account/storage queries against the latest committed
    ///   state.
    /// - `transaction_by_hash_with_meta(hash)` — a transaction with its recovered sender and block
    ///   metadata (block number/hash, index, timestamp).
    /// - `receipt_by_hash(hash)` / `receipts_by_block(..)` — transaction receipts.
    /// - `total_transactions()` + `transactions_by_tx_range_with_meta(range)` — the chain-wide
    ///   sequential transaction feed (serve "latest N transactions" pages without visiting empty
    ///   blocks).
    /// - `retrieve_account(&addr)` / `account_code(&addr)` — account nonce/balance/code-hash and
    ///   deployed bytecode at the latest state.
    /// - `read_contract(contract, calldata)` — an `eth_call`-style read-only contract call at the
    ///   canonical tip (`read_contract_at_block` pins it to a block hash).
    pub fn reth_env(&self) -> &RethEnv {
        &self.reth_env
    }

    /// Handle to the consensus chain (the consensus DB) — for reads only.
    ///
    /// Lets an ExEx read consensus-level data directly — consensus headers, epoch
    /// records, and committed sub-DAGs by number or digest — alongside the
    /// EVM-level reads available through [`reth_env`](Self::reth_env).
    ///
    /// Unlike `reth_env`, whose public surface is mutation-free, `ConsensusChain`
    /// also exposes DB-mutating methods, and this handle shares state with the
    /// live consensus writer (it is `Clone` over `Arc`). Use it for **reads
    /// only**: calling a mutating method would corrupt the follower's consensus
    /// DB. The read-only contract here is by convention, not enforced by the type.
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
    /// let (reporter, mut stream) = ctx.replay_and_subscribe(last_indexed_block + 1);
    /// while let Some(result) = stream.next().await {
    ///     let notification = result?; // surface replay/tip errors
    ///     // Process identically — no distinction between replay and live
    ///     // ...then, once the work is durably persisted:
    ///     reporter.report_finished_height(durable_height); // keep the pruning floor advancing
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
    /// # Progress reporting
    ///
    /// This consumes `self` because the live notification stream is moved into the
    /// returned combined stream, so the context's own
    /// [`report_finished_height`](Self::report_finished_height) goes with it. To
    /// keep progress reporting on this path, a [`FinishedHeightReporter`] is
    /// returned **alongside** the stream. A stateful ExEx that catches up here MUST
    /// still call
    /// [`report_finished_height`](FinishedHeightReporter::report_finished_height)
    /// after it durably persists each height: an ExEx that never reports leaves its
    /// finished height at `None`, and a single such ExEx pins the node-wide
    /// [`min_finished_height`](crate::TnExExManagerHandle::min_finished_height) to
    /// `None` (pruning disabled) for **every** ExEx, permanently. See
    /// [`TnExExEvent::FinishedHeight`].
    ///
    /// This path returns one combined stream, so it cannot itself re-`replay_from`
    /// to reconcile a [`Lagged`](TnExExNotification::Lagged) gap; an ExEx that must
    /// reconcile lag should drive [`replay_from`](Self::replay_from) and the live
    /// channel directly instead.
    pub fn replay_and_subscribe(
        self,
        start_block: BlockNumber,
    ) -> (FinishedHeightReporter, ReplaySubscribeStream) {
        let TnExExContext { notifications, reth_env, events, consensus_chain: _ } = self;

        // Carry the event sender out with the stream so an ExEx on this path can
        // still report `FinishedHeight` after it durably persists. Dropping it here
        // (the previous behavior) left the ExEx structurally unable to report,
        // which pins the node-wide `min_finished_height` to `None` for every ExEx.
        let reporter = FinishedHeightReporter { events };

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
        //
        // Boxed because `impl Trait` cannot be nested inside the tuple return
        // (E0562); the one allocation happens once, at ExEx startup.
        (reporter, Box::pin(dedup_chain_executed(replay.chain(live))))
    }
}

/// The combined replay-then-live notification stream returned by
/// [`TnExExContext::replay_and_subscribe`], boxed so it can be returned alongside
/// a [`FinishedHeightReporter`] (a bare `impl Stream` cannot be nested in a tuple
/// return). Poll it with [`futures::StreamExt::next`].
pub type ReplaySubscribeStream =
    Pin<Box<dyn futures::Stream<Item = eyre::Result<TnExExNotification>> + Send>>;

/// Reports durable ExEx progress back to the node after
/// [`replay_and_subscribe`](TnExExContext::replay_and_subscribe) has consumed the
/// context.
///
/// [`replay_and_subscribe`](TnExExContext::replay_and_subscribe) moves the live
/// notification stream out of the [`TnExExContext`], consuming it — so the
/// context's own [`report_finished_height`](TnExExContext::report_finished_height)
/// is no longer reachable on that path. This handle carries the event sender out
/// alongside the combined stream so a stateful ExEx can keep reporting how far it
/// has durably processed.
///
/// Reporting matters even though nothing prunes yet: an ExEx whose finished height
/// stays `None` pins the node-wide
/// [`min_finished_height`](crate::TnExExManagerHandle::min_finished_height) to
/// `None` for every ExEx (see [`TnExExEvent::FinishedHeight`]). The handle is cheap
/// to clone (an [`mpsc::Sender`] over an `Arc`), so it can be moved into whatever
/// sub-task owns durable persistence.
#[derive(Clone, Debug)]
pub struct FinishedHeightReporter {
    /// Bounded event channel back to the manager: the originating
    /// [`TnExExContext`]'s `events` sender, moved out when
    /// [`replay_and_subscribe`](TnExExContext::replay_and_subscribe) consumed the
    /// context.
    events: mpsc::Sender<TnExExEvent>,
}

impl FinishedHeightReporter {
    /// Report durable progress to the node (non-blocking; latest-wins).
    ///
    /// Identical semantics to
    /// [`TnExExContext::report_finished_height`](TnExExContext::report_finished_height):
    /// sends [`TnExExEvent::FinishedHeight`] with a non-blocking `try_send`, so an
    /// ExEx can never back-pressure the manager. `FinishedHeight` is latest-wins,
    /// so a report dropped on a momentarily-full channel is harmless — the next
    /// report carries a higher height.
    pub fn report_finished_height(&self, height: BlockNumber) {
        let _ = self.events.try_send(TnExExEvent::FinishedHeight(height));
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

    #[tokio::test]
    async fn finished_height_reporter_delivers_on_the_event_channel() {
        // The reporter that `replay_and_subscribe` hands back must put a
        // `FinishedHeight` event on the same channel the manager drains. This is
        // exactly the reporting the convenience path used to drop: the event sender
        // was bound to `_` and discarded, leaving the ExEx unable to report.
        let (events_tx, mut events_rx) = mpsc::channel(4);
        let reporter = FinishedHeightReporter { events: events_tx };

        reporter.report_finished_height(42);

        assert!(matches!(events_rx.try_recv(), Ok(TnExExEvent::FinishedHeight(42))));
        assert!(events_rx.try_recv().is_err());
    }
}
