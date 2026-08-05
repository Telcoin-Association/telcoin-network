//! Historical block replay for ExEx catch-up.
//!
//! When an ExEx starts (or restarts after a crash), it may need to catch up
//! to the current chain tip. Since TN has no reorgs, this is simply reading
//! finalized blocks from the database in ascending order.
//!
//! Replay errors are terminal: the stream fuses on the first error, so an
//! `Err` item is always the last item; a consumer that keeps polling sees
//! end-of-stream, never a silently-skipped block.

use crate::{Chain, TnExExNotification};
use futures::Stream;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tn_reth::RethEnv;
use tn_types::BlockNumber;

/// Source a replay reads from: maps a block number to the chain segment
/// stored for it.
///
/// The one production implementation is [`RethEnv`]; tests substitute a
/// source that fails on demand to pin the stream's fused error semantics.
trait ReplaySource {
    /// Read block `block_number` back as a single-block [`Chain`].
    ///
    /// Returns `Ok(None)` if the block is not in the database.
    fn replay(&self, block_number: BlockNumber) -> eyre::Result<Option<Arc<Chain>>>;
}

impl ReplaySource for RethEnv {
    fn replay(&self, block_number: BlockNumber) -> eyre::Result<Option<Arc<Chain>>> {
        self.replay_block_as_chain(block_number).map_err(Into::into)
    }
}

/// Cursor for a replay range: the next block to replay, or the terminal state.
///
/// `Done` is entered when the range is exhausted **or on the first error**, so
/// an error is a stream terminator, not a per-item event. Once an `Err` has
/// been yielded every later poll returns `None`, making "abort on error" a
/// property of the stream itself rather than of consumer discipline.
#[derive(Clone, Copy, Debug)]
enum ReplayCursor {
    /// The next block number to replay.
    Active(BlockNumber),
    /// Terminal: the range is exhausted or an error was yielded; every
    /// subsequent poll returns `None`.
    Done,
}

/// The replay state machine behind [`ReplayStream`], generic over its block
/// source so the fused error semantics can be driven in unit tests without a
/// database.
#[derive(Debug)]
struct ReplayCore<S> {
    /// Where replayed blocks are read from.
    source: S,
    /// Next block to replay, or the terminal state.
    cursor: ReplayCursor,
    /// Last block of the range (inclusive).
    end_block: BlockNumber,
}

impl<S> ReplayCore<S> {
    /// Create a core replaying `start_block..=end_block` from `source`.
    fn new(source: S, start_block: BlockNumber, end_block: BlockNumber) -> Self {
        Self { source, cursor: ReplayCursor::Active(start_block), end_block }
    }
}

impl<S: ReplaySource + Unpin> Stream for ReplayCore<S> {
    type Item = eyre::Result<TnExExNotification>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match this.cursor {
            ReplayCursor::Done => Poll::Ready(None),
            ReplayCursor::Active(current) if current > this.end_block => {
                this.cursor = ReplayCursor::Done;
                Poll::Ready(None)
            }
            ReplayCursor::Active(current) => match this.source.replay(current) {
                Ok(Some(chain)) => {
                    // Close the range exactly at `end_block` so the cursor
                    // arithmetic is total even at `BlockNumber::MAX`.
                    this.cursor = if current == this.end_block {
                        ReplayCursor::Done
                    } else {
                        ReplayCursor::Active(current.saturating_add(1))
                    };
                    Poll::Ready(Some(Ok(TnExExNotification::ChainExecuted { new: chain })))
                }
                Ok(None) => {
                    // Fuse: an absent block is a hole in the finalized range.
                    // Yield the error once and terminate, so a consumer that
                    // keeps polling observes end-of-stream rather than a
                    // silently-skipped block.
                    this.cursor = ReplayCursor::Done;
                    Poll::Ready(Some(Err(eyre::eyre!(
                        "block {current} not found in database during replay"
                    ))))
                }
                Err(e) => {
                    // Fuse on a failed read for the same reason. The height is
                    // attached because the error is now the stream's final
                    // item: it doubles as the resume point for a consumer that
                    // re-replays the fused gap.
                    this.cursor = ReplayCursor::Done;
                    Poll::Ready(Some(Err(e.wrap_err(format!("replay failed at block {current}")))))
                }
            },
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self.cursor {
            ReplayCursor::Done => (0, Some(0)),
            ReplayCursor::Active(current) => {
                if current > self.end_block {
                    (0, Some(0))
                } else {
                    // The next poll always yields an item, but an error fuses
                    // the stream, so only 1 more item is guaranteed; the exact
                    // count is only an upper bound.
                    let remaining = usize::try_from((self.end_block - current).saturating_add(1))
                        .unwrap_or(usize::MAX);
                    (1, Some(remaining))
                }
            }
        }
    }
}

/// Stream that replays historical blocks from the database as ExEx notifications.
///
/// Reads finalized blocks from `start_block` to `end_block` (inclusive) and yields
/// `TnExExNotification::ChainExecuted` for each block.
///
/// If `start_block > end_block`, the stream is immediately empty.
///
/// # Error semantics (fused)
///
/// The first non-successful outcome (a block missing from the database or a
/// failed read) is **terminal**: the stream yields it as its final item and
/// every later poll returns `None`. An error is a stream terminator, not a
/// per-item event, so a consumer that logs-and-continues past an `Err`
/// observes end-of-stream rather than a silently-skipped block, and the
/// delivered `ChainExecuted` sequence can never contain a silent hole.
///
/// # Replay fidelity
///
/// Replayed `ChainExecuted` notifications carry an **empty `BundleState`** (state
/// diffs are read from the DB by block number, not re-derived here); see
/// [`RethEnv::replay_block_as_chain`](tn_reth::RethEnv::replay_block_as_chain).
#[derive(Debug)]
pub struct ReplayStream {
    /// The replay state machine bound to the production block source.
    core: ReplayCore<RethEnv>,
}

impl ReplayStream {
    /// Create a new replay stream from `start_block` to `end_block` (inclusive).
    pub fn new(reth_env: RethEnv, start_block: BlockNumber, end_block: BlockNumber) -> Self {
        Self { core: ReplayCore::new(reth_env, start_block, end_block) }
    }
}

impl Stream for ReplayStream {
    type Item = eyre::Result<TnExExNotification>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().core).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.core.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    /// How [`FailingSource`] fails at its designated block.
    #[derive(Clone, Copy, Debug)]
    enum FailureMode {
        /// `Ok(None)`: the block is absent from the database.
        Missing,
        /// `Err(..)`: the read itself fails.
        ReadError,
    }

    /// Source that succeeds with an empty chain everywhere except `fail_at`,
    /// where it fails per `mode`. Blocks after `fail_at` succeed again, so an
    /// unfused stream would keep yielding `Ok` items past the error; exactly
    /// the silent hole the fuse must prevent.
    #[derive(Debug)]
    struct FailingSource {
        /// The block number that fails.
        fail_at: BlockNumber,
        /// How that block fails.
        mode: FailureMode,
    }

    impl ReplaySource for FailingSource {
        fn replay(&self, block_number: BlockNumber) -> eyre::Result<Option<Arc<Chain>>> {
            if block_number == self.fail_at {
                match self.mode {
                    FailureMode::Missing => Ok(None),
                    FailureMode::ReadError => Err(eyre::eyre!("replay read failed")),
                }
            } else {
                Ok(Some(Arc::new(Chain::default())))
            }
        }
    }

    /// Source that always succeeds with an empty chain.
    #[derive(Debug)]
    struct HappySource;

    impl ReplaySource for HappySource {
        fn replay(&self, _block_number: BlockNumber) -> eyre::Result<Option<Arc<Chain>>> {
            Ok(Some(Arc::new(Chain::default())))
        }
    }

    #[tokio::test]
    async fn missing_block_fuses_the_stream() {
        let mut core =
            ReplayCore::new(FailingSource { fail_at: 3, mode: FailureMode::Missing }, 1, 5);

        assert!(matches!(core.next().await, Some(Ok(TnExExNotification::ChainExecuted { .. }))));
        assert!(matches!(core.next().await, Some(Ok(TnExExNotification::ChainExecuted { .. }))));
        let message =
            core.next().await.and_then(Result::err).map(|e| e.to_string()).unwrap_or_default();
        assert!(message.contains("block 3 not found in database during replay"), "{message}");

        // Blocks 4 and 5 would succeed, but the error is terminal: the very
        // next poll (and every one after) is end-of-stream.
        assert!(core.next().await.is_none());
        assert!(core.next().await.is_none());
        assert_eq!(core.size_hint(), (0, Some(0)));
    }

    #[tokio::test]
    async fn read_error_fuses_the_stream() {
        let mut core =
            ReplayCore::new(FailingSource { fail_at: 2, mode: FailureMode::ReadError }, 1, 5);

        assert!(matches!(core.next().await, Some(Ok(TnExExNotification::ChainExecuted { .. }))));
        let message =
            core.next().await.and_then(Result::err).map(|e| e.to_string()).unwrap_or_default();
        assert!(message.contains("replay failed at block 2"), "{message}");

        // Fused: no item is ever yielded after the error.
        assert!(core.next().await.is_none());
        assert!(core.next().await.is_none());
        assert_eq!(core.size_hint(), (0, Some(0)));
    }

    #[tokio::test]
    async fn clean_range_yields_every_block_then_none() {
        let mut core = ReplayCore::new(HappySource, 1, 4);
        // Only 1 item is guaranteed (an error would fuse the stream); the
        // exact remaining count is the upper bound.
        assert_eq!(core.size_hint(), (1, Some(4)));

        let items: Vec<_> = core.by_ref().collect().await;
        assert_eq!(items.len(), 4);
        assert!(items
            .iter()
            .all(|item| matches!(item, Ok(TnExExNotification::ChainExecuted { .. }))));
        assert!(core.next().await.is_none());
    }

    #[tokio::test]
    async fn empty_range_is_immediately_terminated() {
        let mut core = ReplayCore::new(HappySource, 5, 4);
        assert_eq!(core.size_hint(), (0, Some(0)));
        assert!(core.next().await.is_none());
    }

    #[tokio::test]
    async fn range_ending_at_max_terminates_cleanly() {
        let mut core =
            ReplayCore::new(HappySource, BlockNumber::MAX.saturating_sub(1), BlockNumber::MAX);

        assert!(matches!(core.next().await, Some(Ok(_))));
        assert!(matches!(core.next().await, Some(Ok(_))));
        assert!(core.next().await.is_none());
        assert_eq!(core.size_hint(), (0, Some(0)));
    }
}
