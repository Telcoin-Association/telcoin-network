// SPDX-License-Identifier: MIT OR Apache-2.0
//! Historical block replay for ExEx tasks.
//!
//! Provides [`ReplayStream`] which yields [`TnExExNotification::ChainCommitted`] for
//! a range of historical blocks, allowing ExEx tasks to replay and index blocks
//! they may have missed (e.g. when starting from genesis or after downtime).

use crate::TnExExNotification;
use reth_provider::Chain;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tn_reth::RethEnv;

/// Result of a single block replay on the blocking thread.
type ReplayResult = tn_reth::error::TnRethResult<Option<Arc<Chain>>>;

/// A stream that yields [`TnExExNotification::ChainCommitted`] for historical blocks.
///
/// `ReplayStream` iterates over a range of block numbers, reconstructing each block
/// as a [`Chain`](reth_provider::Chain) from the database and wrapping it in a notification.
/// This allows ExEx tasks to process historical blocks identically to live ones.
///
/// Database reads are offloaded to [`tokio::task::spawn_blocking`] so the async
/// runtime is never blocked by disk I/O.
///
/// # Usage
///
/// ```ignore
/// use tn_exex::ReplayStream;
/// use tokio_stream::StreamExt;
///
/// let replay = ReplayStream::new(reth_env, 0, 100);
/// while let Some(notification) = replay.next().await {
///     // Process historical block notification
/// }
/// // Then switch to live notifications from the ExEx context
/// ```
///
/// # Notes
///
/// - Blocks that do not exist in the database are silently skipped.
/// - The reconstructed chains contain receipts but no state diffs (bundle state)
///   or trie data, since those are not needed for indexing/analytics replay.
/// - Errors during block retrieval terminate the stream.
#[derive(Debug)]
pub struct ReplayStream {
    /// The Reth environment used to query historical blocks.
    reth_env: RethEnv,
    /// The next block number to replay.
    current_block: u64,
    /// The last block number to replay (inclusive).
    target_block: u64,
    /// Set to `true` when an error occurs or replay is complete.
    finished: bool,
    /// In-flight blocking task for the current block read.
    pending: Option<JoinHandle<ReplayResult>>,
}

impl ReplayStream {
    /// Create a new replay stream for the given block range (inclusive).
    ///
    /// # Arguments
    ///
    /// * `reth_env` - The Reth environment for database access
    /// * `from_block` - First block number to replay
    /// * `to_block` - Last block number to replay (inclusive)
    pub fn new(reth_env: RethEnv, from_block: u64, to_block: u64) -> Self {
        Self {
            reth_env,
            current_block: from_block,
            target_block: to_block,
            finished: from_block > to_block,
            pending: None,
        }
    }

    /// Returns the next block number that will be replayed.
    pub fn current_block(&self) -> u64 {
        self.current_block
    }

    /// Returns the target (last) block number.
    pub fn target_block(&self) -> u64 {
        self.target_block
    }

    /// Returns `true` if the replay stream has finished.
    pub fn is_finished(&self) -> bool {
        self.finished
    }
}

impl Stream for ReplayStream {
    type Item = eyre::Result<TnExExNotification>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.finished {
            return Poll::Ready(None);
        }

        loop {
            // If there's an in-flight blocking task, poll it.
            if let Some(handle) = &mut this.pending {
                match Pin::new(handle).poll(cx) {
                    Poll::Ready(join_result) => {
                        this.pending = None;
                        let result = join_result
                            .map_err(|e| eyre::eyre!("spawn_blocking panicked: {e}"))
                            .and_then(|inner| inner.map_err(Into::into));

                        match result {
                            Ok(Some(chain)) => {
                                let notification =
                                    TnExExNotification::ChainCommitted { new: chain };
                                this.current_block += 1;
                                if this.current_block > this.target_block {
                                    this.finished = true;
                                }
                                return Poll::Ready(Some(Ok(notification)));
                            }
                            Ok(None) => {
                                // Block doesn't exist — skip it and try the next one.
                                this.current_block += 1;
                                if this.current_block > this.target_block {
                                    this.finished = true;
                                    return Poll::Ready(None);
                                }
                                // Fall through to spawn the next block read.
                            }
                            Err(e) => {
                                this.finished = true;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // Spawn a blocking task for the next block.
            let env = this.reth_env.clone();
            let block_num = this.current_block;
            this.pending =
                Some(tokio::task::spawn_blocking(move || env.replay_block_as_chain(block_num)));
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.finished {
            (0, Some(0))
        } else {
            let remaining = (self.target_block - self.current_block + 1) as usize;
            (0, Some(remaining))
        }
    }
}

#[cfg(test)]
mod tests {
    // Note: ReplayStream requires a real RethEnv backed by a database,
    // so meaningful tests live in crates/tn-exex/tests/it/ as integration tests.
    // The stream's correctness is exercised there.
}
